package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// ReconcileHit retains both source and external version; neither can stand in
// for the other when a projection was manually edited.
type ReconcileHit struct {
	ID              string            `json:"_id"`
	ExternalVersion int64             `json:"_version"`
	Document        Document          `json:"_source"`
	Sort            []json.RawMessage `json:"sort"`
}

func (es *Elasticsearch) OpenReconcileView(ctx context.Context) (string, error) {
	status, data, err := es.request(ctx, http.MethodPost, "/"+es.index+"/_pit?keep_alive=2m&allow_partial_search_results=false", nil)
	if err != nil {
		return "", err
	}
	if status != 200 {
		return "", writeError(status, data)
	}
	var result struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(data, &result); err != nil {
		return "", err
	}
	if result.ID == "" {
		return "", fmt.Errorf("ES missing point-in-time identity")
	}
	return result.ID, nil
}
func (es *Elasticsearch) CloseReconcileView(ctx context.Context, id string) error {
	status, data, err := es.request(ctx, http.MethodDelete, "/_pit", map[string]any{"id": id})
	if err != nil {
		return err
	}
	if status != 200 {
		return writeError(status, data)
	}
	return nil
}

// A PIT and search_after provide complete, bounded pagination, including
// extra documents and repeated source IDs under different ES document IDs.
func (es *Elasticsearch) ReconcilePage(ctx context.Context, pit string, org int64, after []json.RawMessage, size int) ([]ReconcileHit, string, error) {
	if size < 1 || size > 1000 {
		return nil, pit, fmt.Errorf("invalid ES reconciliation batch")
	}
	query := map[string]any{"match_all": map[string]any{}}
	if org > 0 {
		query = map[string]any{"term": map[string]any{"organization_id": org}}
	}
	body := map[string]any{"size": size, "version": true, "track_total_hits": false, "timeout": "4s", "pit": map[string]any{"id": pit, "keep_alive": "2m"}, "query": query, "sort": []any{map[string]any{"organization_id": "asc"}, map[string]any{"id": "asc"}, map[string]any{"_shard_doc": "asc"}}}
	if len(after) > 0 {
		body["search_after"] = after
	}
	status, data, err := es.request(ctx, http.MethodPost, "/_search?allow_partial_search_results=false", body)
	if err != nil {
		return nil, pit, err
	}
	if status != 200 {
		return nil, pit, writeError(status, data)
	}
	if len(data) >= 8<<20 {
		return nil, pit, fmt.Errorf("ES response exceeds complete-result budget")
	}
	var result struct {
		PIT      string `json:"pit_id"`
		TimedOut *bool  `json:"timed_out"`
		Shards   *struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
		Hits *struct {
			Hits []ReconcileHit `json:"hits"`
		} `json:"hits"`
	}
	if err = json.Unmarshal(data, &result); err != nil {
		return nil, pit, err
	}
	if result.TimedOut == nil || *result.TimedOut || result.Shards == nil || result.Shards.Failed != 0 || result.Hits == nil || result.Hits.Hits == nil {
		return nil, pit, fmt.Errorf("ES reconciliation response is incomplete")
	}
	if len(result.Hits.Hits) > size {
		return nil, pit, fmt.Errorf("ES reconciliation response exceeds batch")
	}
	for _, hit := range result.Hits.Hits {
		if len(hit.Sort) != 3 || hit.ID == "" {
			return nil, pit, fmt.Errorf("ES reconciliation hit lacks identity/cursor")
		}
	}
	if result.PIT != "" {
		pit = result.PIT
	}
	return result.Hits.Hits, pit, nil
}

// Useful for recording the actual isolated target in the audit report.
func (es *Elasticsearch) ReconcileTarget() string {
	return es.endpoint + "/" + url.PathEscape(es.index)
}
