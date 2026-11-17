package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const Mapping = `{
  "settings":{"number_of_shards":1,"number_of_replicas":0,"refresh_interval":"1s"},
  "mappings":{"dynamic":"strict","properties":{
    "id":{"type":"long"},"organization_id":{"type":"long"},
    "ticket_no":{"type":"keyword"},
    "title":{"type":"text","analyzer":"standard","fields":{"exact":{"type":"keyword","ignore_above":512}}},
    "description":{"type":"text","analyzer":"standard"},
    "status":{"type":"keyword"},"priority":{"type":"keyword"},"priority_rank":{"type":"byte"},"category":{"type":"keyword"},
    "creator_id":{"type":"long"},"assignee_id":{"type":"long"},"initial_team_id":{"type":"long"},"current_team_id":{"type":"long"},
    "created_at":{"type":"date_nanos"},"updated_at":{"type":"date_nanos"},"version":{"type":"long"}
  }}
}`

type Elasticsearch struct {
	endpoint, index string
	client          *http.Client
}

func NewElasticsearch(endpoint, index string) (*Elasticsearch, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" || !regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,200}$`).MatchString(index) {
		return nil, fmt.Errorf("invalid Elasticsearch endpoint or index")
	}
	// The default transport retains only two idle connections per host. Search
	// keeps up to twenty authorization snapshots active, so rapid close/redial
	// cycles can exhaust local ephemeral ports even when ES is healthy. Keep a
	// bounded idle pool while preserving native dial/TLS/timeouts and concurrency.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 32
	return &Elasticsearch{strings.TrimRight(endpoint, "/"), index, &http.Client{Timeout: 5 * time.Second, Transport: transport}}, nil
}

type WriteError struct {
	Status int
	Code   string
}

func (e *WriteError) Error() string {
	return fmt.Sprintf("Elasticsearch write: status=%d code=%s", e.Status, e.Code)
}
func (e *WriteError) Permanent() bool {
	return e.Status >= 400 && e.Status < 500 && e.Status != 408 && e.Status != 429
}

func (es *Elasticsearch) request(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var payload []byte
	var err error
	switch value := body.(type) {
	case []byte:
		payload = value
	case nil:
	case string:
		payload = []byte(value)
	default:
		payload, err = json.Marshal(value)
	}
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, es.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if d := Diagnostic(ctx); d != nil {
		d.ESRequestBytes += len(payload)
		if d.RequestID != "" {
			req.Header.Set("X-Opaque-ID", "ticketpulse-"+d.RequestID)
		}
	}
	res, err := es.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	return res.StatusCode, data, err
}

func (es *Elasticsearch) EnsureIndex(ctx context.Context) error {
	status, _, err := es.request(ctx, http.MethodHead, "/"+es.index, nil)
	if err != nil {
		return err
	}
	if status == 200 {
		return nil
	}
	if status != 404 {
		return &WriteError{status, "index_check_failed"}
	}
	status, data, err := es.request(ctx, http.MethodPut, "/"+es.index, Mapping)
	if err != nil {
		return err
	}
	if status == 200 {
		return nil
	}
	if status == 400 && strings.Contains(string(data), "resource_already_exists_exception") {
		return nil
	}
	return writeError(status, data)
}

func writeError(status int, data []byte) error {
	var body struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &body)
	return &WriteError{status, body.Error.Type}
}

// Strict external version: an identical retry or older arrival is a no-op, never an overwrite.
func (es *Elasticsearch) Put(ctx context.Context, d Document) (bool, error) {
	if err := d.Validate(); err != nil {
		return false, err
	}
	path := "/" + es.index + "/_doc/" + strconv.FormatInt(d.ID, 10) + "?version_type=external&version=" + strconv.FormatInt(d.Version, 10)
	status, data, err := es.request(ctx, http.MethodPut, path, d)
	if err != nil {
		return false, err
	}
	if status == 200 || status == 201 {
		return false, nil
	}
	if status == 409 {
		// Only an external-version conflict is safe to treat as an already applied/old event.
		var result struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		_ = json.Unmarshal(data, &result)
		if result.Error.Type == "version_conflict_engine_exception" {
			return true, nil
		}
	}
	return false, writeError(status, data)
}

func (es *Elasticsearch) Refresh(ctx context.Context) error {
	status, data, err := es.request(ctx, http.MethodPost, "/"+es.index+"/_refresh", nil)
	if err != nil {
		return err
	}
	if status != 200 {
		return writeError(status, data)
	}
	return nil
}

func BuildQuery(r Request, uOrg, uID int64, manager bool, teams []int64, after []json.RawMessage, size int) map[string]any {
	filters := []any{map[string]any{"term": map[string]any{"organization_id": uOrg}}}
	if !manager {
		scope := []any{map[string]any{"term": map[string]any{"creator_id": uID}}}
		if len(teams) > 0 {
			scope = append(scope, map[string]any{"terms": map[string]any{"current_team_id": teams}})
		}
		filters = append(filters, map[string]any{"bool": map[string]any{"should": scope, "minimum_should_match": 1}})
	}
	add := func(field string, values any) {
		filters = append(filters, map[string]any{"terms": map[string]any{field: values}})
	}
	if r.TicketNo != "" {
		filters = append(filters, map[string]any{"term": map[string]any{"ticket_no": r.TicketNo}})
	}
	if len(r.Statuses) > 0 {
		add("status", r.Statuses)
	}
	if len(r.Priorities) > 0 {
		add("priority", r.Priorities)
	}
	if len(r.Categories) > 0 {
		add("category", r.Categories)
	}
	if len(r.CreatorIDs) > 0 {
		add("creator_id", r.CreatorIDs)
	}
	if len(r.TeamIDs) > 0 {
		add("current_team_id", r.TeamIDs)
	}
	if len(r.AssigneeIDs) > 0 {
		var assigned []int64
		unassigned := false
		for _, id := range r.AssigneeIDs {
			if id == 0 {
				unassigned = true
			} else {
				assigned = append(assigned, id)
			}
		}
		parts := []any{}
		if len(assigned) > 0 {
			parts = append(parts, map[string]any{"terms": map[string]any{"assignee_id": assigned}})
		}
		if unassigned {
			parts = append(parts, map[string]any{"bool": map[string]any{"must_not": map[string]any{"exists": map[string]any{"field": "assignee_id"}}}})
		}
		filters = append(filters, map[string]any{"bool": map[string]any{"should": parts, "minimum_should_match": 1}})
	}
	for _, window := range []struct {
		field    string
		from, to *time.Time
	}{{"created_at", r.CreatedFrom, r.CreatedTo}, {"updated_at", r.UpdatedFrom, r.UpdatedTo}} {
		terms := map[string]any{}
		if window.from != nil {
			terms["gte"] = window.from.UTC().Format(time.RFC3339Nano)
		}
		if window.to != nil {
			terms["lt"] = window.to.UTC().Format(time.RFC3339Nano)
		}
		if len(terms) > 0 {
			filters = append(filters, map[string]any{"range": map[string]any{window.field: terms}})
		}
	}
	condition := map[string]any{"filter": filters}
	if strings.TrimSpace(r.Query) != "" {
		condition["must"] = map[string]any{"multi_match": map[string]any{"query": r.Query, "fields": []string{"title^2", "description"}, "operator": "and"}}
	}
	field := r.Sort
	if field == "priority" {
		field = "priority_rank"
	}
	sorting := []any{map[string]any{field: map[string]any{"order": r.Order}}}
	if field != "id" {
		sorting = append(sorting, map[string]any{"id": map[string]any{"order": r.Order}})
	}
	body := map[string]any{"query": map[string]any{"bool": condition}, "sort": sorting, "size": size, "track_total_hits": false, "_source": []string{"id", "version"}, "timeout": "3s"}
	if len(after) > 0 {
		body["search_after"] = after
	}
	return body
}

func (es *Elasticsearch) Candidates(ctx context.Context, body map[string]any) ([]Candidate, error) {
	start := time.Now()
	d := Diagnostic(ctx)
	if d != nil {
		defer func() { d.ESMS += Milliseconds(start) }()
	}
	status, data, err := es.request(ctx, http.MethodPost, "/"+es.index+"/_search?allow_partial_search_results=false", body)
	if d != nil {
		d.ESStatus = status
		d.ESResponseBytes += len(data)
	}
	if err != nil {
		if d != nil {
			d.ESErrorCategory = ErrorCategory(err)
			if d.ESErrorCategory == "other" {
				if status == 0 {
					d.ESErrorCategory = "transport_request"
				} else {
					d.ESErrorCategory = "transport_body"
				}
			}
		}
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil, ErrTimeout
		}
		return nil, ErrUnavailable
	}
	if status != 200 {
		if d != nil {
			d.ESErrorCategory = safeESError(data)
		}
		return nil, ErrUnavailable
	}
	var result struct {
		Took     int64 `json:"took"`
		TimedOut *bool `json:"timed_out"`
		Shards   *struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
		Hits *struct {
			Hits []struct {
				Source Candidate         `json:"_source"`
				Sort   []json.RawMessage `json:"sort"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if json.Unmarshal(data, &result) != nil {
		if d != nil {
			d.ESErrorCategory = "response_json"
		}
		return nil, ErrUnavailable
	}
	if result.TimedOut == nil || result.Shards == nil || result.Hits == nil || result.Hits.Hits == nil {
		if d != nil {
			d.ESErrorCategory = "response_contract"
		}
		return nil, ErrUnavailable
	}
	if *result.TimedOut {
		if d != nil {
			d.ESErrorCategory = "query_timeout"
			d.ESTookMS += result.Took
		}
		return nil, ErrTimeout
	}
	if result.Shards.Failed > 0 {
		if d != nil {
			d.ESErrorCategory = "shard_failure"
		}
		return nil, ErrUnavailable
	}
	items := make([]Candidate, 0, len(result.Hits.Hits))
	if d != nil {
		d.ESTookMS += result.Took
	}
	for _, hit := range result.Hits.Hits {
		if hit.Source.ID <= 0 || hit.Source.Version <= 0 || len(hit.Sort) == 0 {
			if d != nil {
				d.ESErrorCategory = "candidate_contract"
			}
			return nil, ErrUnavailable
		}
		hit.Source.Sort = hit.Sort
		items = append(items, hit.Source)
	}
	return items, nil
}
