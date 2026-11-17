package search

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"ticketpulse/internal/ticket"
)

type fakeAccess struct {
	u     ticket.User
	items map[int64]ticket.TicketSummary
}

func (a *fakeAccess) Identity() ticket.User { return a.u }
func (a *fakeAccess) TeamIDs() []int64      { return []int64{10} }
func (a *fakeAccess) Close() error          { return nil }
func (a *fakeAccess) Authorized(_ context.Context, ids []int64) (map[int64]ticket.TicketSummary, error) {
	out := map[int64]ticket.TicketSummary{}
	for _, id := range ids {
		if item, ok := a.items[id]; ok {
			out[id] = item
		}
	}
	return out, nil
}
func (a *fakeAccess) BeginSearch(context.Context, int64) (Access, error) { return a, nil }

func TestAuthorizedPaginationSkipsStaleAndUnauthorized(t *testing.T) {
	access := &fakeAccess{u: ticket.User{ID: 3, OrganizationID: 1, Role: ticket.RoleAgent, Active: true}, items: map[int64]ticket.TicketSummary{2: {ID: 2, Version: 2, Title: "current"}, 3: {ID: 3, Version: 2}, 4: {ID: 4, Version: 1}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["track_total_hits"] != false {
			t.Error("raw total requested")
		}
		if source := body["_source"].([]any); len(source) != 2 {
			t.Error("sensitive ES fields requested")
		}
		after := float64(0)
		if values, ok := body["search_after"].([]any); ok {
			after = values[0].(float64)
		}
		hits := []any{}
		for _, id := range []int64{1, 2, 3, 4} {
			if float64(id) > after {
				hits = append(hits, map[string]any{"_source": map[string]any{"id": id, "version": 1}, "sort": []int64{id}})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"timed_out": false, "_shards": map[string]any{"failed": 0}, "hits": map[string]any{"total": 999, "hits": hits}})
	}))
	defer server.Close()
	es, _ := NewElasticsearch(server.URL, "test_tickets")
	service, _ := NewService(access, es, nil, make([]byte, 32))
	request := Request{Sort: "id", Order: "asc", PageSize: 1}
	result, err := service.Query(context.Background(), 3, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].ID != 4 || result.HasMore || result.Total != nil {
		t.Fatalf("unexpected result: %+v", result)
	}
	access.items[2] = ticket.TicketSummary{ID: 2, Version: 1}
	result, err = service.Query(context.Background(), 3, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Items[0].ID != 2 || !result.HasMore || result.NextCursor == "" {
		t.Fatalf("missing authorized lookahead: %+v", result)
	}
	request.Cursor = result.NextCursor
	result, err = service.Query(context.Background(), 3, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].ID != 4 || result.HasMore {
		t.Fatalf("bad second page: %+v", result)
	}
	request.Query = "changed"
	if _, err := service.Query(context.Background(), 3, request); err == nil {
		t.Fatal("cursor allowed another query")
	}
	request.Query = ""
	access.u.ID = 4
	if _, err := service.Query(context.Background(), 4, request); err == nil {
		t.Fatal("cursor allowed another user")
	}
}

func TestSearchTimeoutAndShardFailureNeverSuccess(t *testing.T) {
	for _, body := range []string{`{}`, `{"timed_out":false}`, `{"timed_out":false,"_shards":{"failed":0},"hits":{"hits":null}}`, `{"timed_out":true,"_shards":{"failed":0},"hits":{"hits":[]}}`, `{"timed_out":false,"_shards":{"failed":1},"hits":{"hits":[]}}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
		es, _ := NewElasticsearch(server.URL, "test_tickets")
		if _, err := es.Candidates(context.Background(), map[string]any{}); err == nil {
			t.Error("partial failure returned success")
		}
		server.Close()
	}
}

func TestQueryContract(t *testing.T) {
	r := Request{Query: "network outage", Statuses: []string{"pending", "in_progress"}, AssigneeIDs: []int64{0, 3}, Sort: "priority", Order: "desc", PageSize: 20}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	body := BuildQuery(r, 1, 3, false, []int64{10}, nil, 20)
	data, _ := json.Marshal(body)
	for _, fragment := range []string{`"priority_rank"`, `"minimum_should_match":1`, `"operator":"and"`, `"track_total_hits":false`, `"_source":["id","version"]`} {
		if !strings.Contains(string(data), fragment) {
			t.Errorf("missing %s", fragment)
		}
	}
	invalid := []Request{{Sort: "_score", Order: "desc", PageSize: 20}, {Sort: "id", Order: "desc", PageSize: 0}, {Sort: "id", Order: "desc", PageSize: 20, TeamIDs: []int64{0}}, {Sort: "id", Order: "desc", PageSize: 20, Statuses: []string{"secret"}}}
	for _, request := range invalid {
		if request.Validate() == nil {
			t.Errorf("accepted %+v", request)
		}
	}
}

// A one-connection pool models the production bounded pool. All searches must
// finish without waiting for a second connection while their snapshot is open.
type singleConnectionAccess struct {
	*fakeAccess
	pool chan struct{}
}

func (a *singleConnectionAccess) Close() error { <-a.pool; return nil }

type singleConnectionStore struct{ pool chan struct{} }

func (s *singleConnectionStore) BeginSearch(ctx context.Context, id int64) (Access, error) {
	select {
	case s.pool <- struct{}{}:
		return &singleConnectionAccess{&fakeAccess{u: ticket.User{ID: id, OrganizationID: 1, Role: ticket.RoleAgent}, items: map[int64]ticket.TicketSummary{}}, s.pool}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *singleConnectionStore) SearchFreshness(ctx context.Context) (Freshness, error) {
	select {
	case s.pool <- struct{}{}:
		defer func() { <-s.pool }()
		return Freshness{State: "running"}, nil
	case <-ctx.Done():
		return Freshness{}, ctx.Err()
	}
}
func TestConcurrentSearchWithOnePoolConnection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"timed_out":false,"_shards":{"failed":0},"hits":{"hits":[]}}`))
	}))
	defer server.Close()
	es, _ := NewElasticsearch(server.URL, "test_tickets")
	store := &singleConnectionStore{make(chan struct{}, 1)}
	service, _ := NewService(store, es, store, make([]byte, 32))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := service.Query(ctx, 3, Request{Sort: "id", Order: "asc", PageSize: 20})
			if err != nil {
				t.Error(err)
			} else if result.Freshness.State != "running" {
				t.Error("freshness lost")
			}
		}()
	}
	wg.Wait()
}
