package search

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ticketpulse/internal/ticket"
)

func TestCursorVersionUsesSameKeyAndRejectsOtherTarget(t *testing.T) {
	access := &fakeAccess{u: ticket.User{ID: 3, OrganizationID: 1, Role: ticket.RoleAgent}, items: map[int64]ticket.TicketSummary{1: {ID: 1, Version: 1}, 2: {ID: 2, Version: 1}}}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var query struct {
			After []int64 `json:"search_after"`
		}
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			t.Error(err)
		}
		hits := []any{}
		for _, id := range []int64{1, 2} {
			if len(query.After) == 0 || id > query.After[0] {
				hits = append(hits, map[string]any{"_source": map[string]any{"id": id, "version": 1}, "sort": []int64{id}})
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"timed_out": false, "_shards": map[string]int{"failed": 0}, "hits": map[string]any{"hits": hits}})
	}))
	defer server.Close()
	es, _ := NewElasticsearch(server.URL, "cursor_tickets")
	key := make([]byte, 32)
	v1, _ := NewService(access, es, nil, key, WithTargetVersion("v1:uuid_a", nil))
	request := Request{Sort: "id", Order: "asc", PageSize: 1}
	first, err := v1.Query(context.Background(), 3, request)
	if err != nil || !first.HasMore || first.Items[0].ID != 1 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	request.Cursor = first.NextCursor
	// Reconstruct the service with the identical encryption key and version.
	restarted, _ := NewService(access, es, nil, key, WithTargetVersion("v1:uuid_a", nil))
	second, err := restarted.Query(context.Background(), 3, request)
	if err != nil || len(second.Items) != 1 || second.Items[0].ID != 2 || second.HasMore {
		t.Fatalf("same version=%+v err=%v", second, err)
	}
	before := calls
	v2, _ := NewService(access, es, nil, key, WithTargetVersion("v2:uuid_b", nil))
	if _, err := v2.Query(context.Background(), 3, request); !errors.Is(err, ErrCursorTargetChanged) {
		t.Fatalf("old cursor error=%v", err)
	}
	if calls != before {
		t.Fatal("old cursor queried a different target")
	}
	// Cursors issued before the new contract have no target and must restart.
	old, err := v1.encodeCursor(cursor{UserID: 3, OrgID: 1, Query: request.Fingerprint(), Expires: time.Now().Add(time.Hour).Unix(), After: []json.RawMessage{json.RawMessage("1")}})
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = old
	if _, err := v1.Query(context.Background(), 3, request); !errors.Is(err, ErrCursorTargetChanged) {
		t.Fatalf("legacy cursor=%v", err)
	}
}

func TestESIdentityRequiresConcreteTargetAndNeverCreates(t *testing.T) {
	for _, response := range []struct {
		status int
		body   string
		exists bool
		failed bool
	}{
		{200, `{"target":{"settings":{"index":{"uuid":"abc"}}}}`, true, false},
		{404, `{}`, false, false},
		{503, `{}`, false, true},
		{200, `{"other":{"settings":{"index":{"uuid":"abc"}}}}`, false, true},
		{200, `{"target":{"settings":{"index":{}}}}`, false, true},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" || r.URL.Path != "/target/_settings" {
				t.Error("identity performed a mutation or chose another index")
			}
			w.WriteHeader(response.status)
			w.Write([]byte(response.body))
		}))
		es, _ := NewElasticsearch(server.URL, "target")
		_, exists, err := es.Identity(context.Background())
		if exists != response.exists || (err != nil) != response.failed {
			t.Fatalf("exists=%v err=%v case=%+v", exists, err, response)
		}
		server.Close()
	}
}
