package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

func (es *Elasticsearch) Endpoint() string { return es.endpoint }
func (es *Elasticsearch) Index() string    { return es.index }

// Identity reads a concrete index's UUID. Aliases, missing indices and partial
// metadata cannot be mistaken for a verified projection. It never creates one.
func (es *Elasticsearch) Identity(ctx context.Context) (string, bool, error) {
	status, data, err := es.request(ctx, http.MethodGet, "/"+es.index+"/_settings", nil)
	if err != nil {
		return "", false, err
	}
	if status == http.StatusNotFound {
		return "", false, nil
	}
	if status != http.StatusOK {
		return "", false, fmt.Errorf("ES identity HTTP %d", status)
	}
	var response map[string]struct {
		Settings struct {
			Index struct {
				UUID string `json:"uuid"`
			} `json:"index"`
		} `json:"settings"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return "", false, err
	}
	value, ok := response[es.index]
	if !ok || len(response) != 1 || value.Settings.Index.UUID == "" {
		return "", false, fmt.Errorf("concrete ES index identity unavailable")
	}
	return value.Settings.Index.UUID, true, nil
}
