package search

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"ticketpulse/internal/ticket"
)

func TestAdaptiveBatchPreservesSparsePagesAndBudget(t *testing.T) {
	for _, spacing := range []int64{1, 2, 10, 100, 3000} {
		access := &fakeAccess{u: ticket.User{ID: 3, OrganizationID: 1, Role: ticket.RoleAgent}, items: map[int64]ticket.TicketSummary{}}
		for id := spacing; id <= 3000; id += spacing {
			access.items[id] = ticket.TicketSummary{ID: id, Version: 1}
		}
		scanned := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Size  int
				After []int64 `json:"search_after"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.Size < 1 || body.Size > 400 {
				t.Error("unbounded batch")
			}
			after := int64(0)
			if len(body.After) > 0 {
				after = body.After[0]
			}
			hits := []any{}
			for id := after + 1; id <= 3000 && len(hits) < body.Size; id++ {
				hits = append(hits, map[string]any{"_source": map[string]int64{"id": id, "version": 1}, "sort": []int64{id}})
			}
			scanned += len(hits)
			json.NewEncoder(w).Encode(map[string]any{"timed_out": false, "_shards": map[string]int{"failed": 0}, "hits": map[string]any{"hits": hits}})
		}))
		es, _ := NewElasticsearch(srv.URL, "test_tickets")
		service, _ := NewService(access, es, nil, make([]byte, 32))
		req := Request{Sort: "id", Order: "asc", PageSize: 20}
		first, err := service.Query(context.Background(), 3, req)
		if scanned > 2000 {
			t.Fatalf("scanned=%d breached budget", scanned)
		}
		if spacing >= 100 {
			if err != ErrBudget || len(first.Items) != 0 {
				t.Fatalf("spacing %d must fail without partial page: %+v %v", spacing, first, err)
			}
		} else {
			if err != nil || len(first.Items) != 20 || !first.HasMore {
				t.Fatalf("spacing %d: %+v %v", spacing, first, err)
			}
			for i, item := range first.Items {
				if item.ID != int64(i+1)*spacing {
					t.Fatalf("missing or wrong first item %d", item.ID)
				}
			}
			req.Cursor = first.NextCursor
			scanned = 0
			next, err := service.Query(context.Background(), 3, req)
			if err != nil || len(next.Items) != 20 {
				t.Fatalf("second page: %+v %v", next, err)
			}
			for i, item := range next.Items {
				if item.ID != int64(i+21)*spacing {
					t.Fatalf("cursor skipped or repeated %d", item.ID)
				}
			}
		}
		srv.Close()
	}
}
