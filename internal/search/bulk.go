package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

const bulkWriteBytes = 4 << 20
const bulkWriteRows = 500

// PutBatch preserves strict external versions for each item, including two
// versions of the same ticket in one request. A partially applied request is
// retried as a whole by the caller; accepted/older versions remain safe no-ops.
func (es *Elasticsearch) PutBatch(ctx context.Context, documents []Document) error {
	if len(documents) == 0 {
		return nil
	}
	if len(documents) > bulkWriteRows {
		return fmt.Errorf("ES batch exceeds 500 document budget")
	}
	var payload bytes.Buffer
	start := 0
	flush := func(end int) error {
		if payload.Len() == 0 {
			return nil
		}
		err := es.putBulk(ctx, documents[start:end], payload.Bytes())
		if err != nil {
			return err
		}
		payload.Reset()
		start = end
		return nil
	}
	for i, d := range documents {
		if err := d.Validate(); err != nil {
			return err
		}
		meta, err := json.Marshal(map[string]any{"index": map[string]any{"_id": strconv.FormatInt(d.ID, 10), "version": d.Version, "version_type": "external"}})
		if err != nil {
			return err
		}
		source, err := json.Marshal(d)
		if err != nil {
			return err
		}
		size := len(meta) + len(source) + 2
		if size > bulkWriteBytes {
			return fmt.Errorf("ES document exceeds 4 MiB batch request budget")
		}
		if payload.Len()+size > bulkWriteBytes {
			if err := flush(i); err != nil {
				return err
			}
		}
		payload.Write(meta)
		payload.WriteByte(10)
		payload.Write(source)
		payload.WriteByte(10)
	}
	return flush(len(documents))
}
func (es *Elasticsearch) putBulk(ctx context.Context, documents []Document, payload []byte) error {
	status, data, err := es.request(ctx, http.MethodPost, "/"+es.index+"/_bulk", payload)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return writeError(status, data)
	}
	if len(data) >= 8<<20 {
		return fmt.Errorf("ES bulk response exceeds complete-result budget")
	}
	var response struct {
		Items []map[string]json.RawMessage `json:"items"`
	}
	if err = json.Unmarshal(data, &response); err != nil {
		return err
	}
	if len(response.Items) != len(documents) {
		return fmt.Errorf("ES bulk response item count differs: received %d expected %d", len(response.Items), len(documents))
	}
	var firstError error
	for i, item := range response.Items {
		var result struct {
			ID      string `json:"_id"`
			Version *int64 `json:"_version"`
			Status  int    `json:"status"`
			Error   *struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		raw, exists := item["index"]
		if len(item) != 1 || !exists || json.Unmarshal(raw, &result) != nil || result.ID != strconv.FormatInt(documents[i].ID, 10) {
			return fmt.Errorf("ES bulk item %d has incomplete or mismatched identity", i)
		}
		var problem error
		switch {
		case result.Status == 200 || result.Status == 201:
			if result.Error != nil || result.Version == nil || *result.Version != documents[i].Version {
				problem = fmt.Errorf("ES bulk item %d success lacks authoritative version", i)
			}
		case result.Status == 409 && result.Error != nil && result.Error.Type == "version_conflict_engine_exception":
			// Strict external-version retry/old arrival is the sole safe conflict.
		default:
			code := "bulk_item_failed"
			if result.Error != nil && result.Error.Type != "" {
				code = result.Error.Type
			}
			problem = &WriteError{Status: result.Status, Code: code}
		}
		if problem != nil {
			if firstError == nil {
				firstError = problem
			} else if write, ok := problem.(*WriteError); ok && write.Permanent() {
				firstError = problem
			}
		}
	}
	return firstError
}
