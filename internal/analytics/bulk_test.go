package analytics

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClickHouseBatchSynchronousExplicitColumnsNullTime(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if q.Get("async_insert") != "0" || q.Get("wait_end_of_query") != "1" || q.Get("input_format_skip_unknown_fields") != "0" {
			t.Fatal("synchronous/strict insert options missing")
		}
		if !strings.Contains(q.Get("query"), "first_resolved_at") || !strings.Contains(q.Get("query"), "FORMAT JSONEachRow") {
			t.Fatal("explicit columns missing")
		}
		body, _ := io.ReadAll(r.Body)
		lines := strings.Split(strings.TrimSpace(string(body)), string([]byte{10}))
		if len(lines) != 2 {
			t.Fatal("wrong row count")
		}
		for _, line := range lines {
			var row map[string]any
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				t.Fatal(err)
			}
			if row["first_resolved_at"] != nil || row["assignee_id"] != nil {
				t.Fatal("null overwritten")
			}
			if strings.Contains(q.Get("query"), "ticket_states") && row["updated_at"] != "2026-11-01 00:00:00.123456" {
				t.Fatal("time precision changed")
			}
		}
	}))
	defer server.Close()
	ck, _ := NewClickHouse(server.URL, "test")
	f := Fact{OrganizationID: 1, TicketID: 1, Version: 1, OccurredAt: "2026-11-01 00:00:00.123456", CreatedAt: "2026-11-01 00:00:00.123456"}
	if err := ck.InsertFacts(context.Background(), []Fact{f, f}); err != nil {
		t.Fatal(err)
	}
	c := Current{OrganizationID: 1, TicketID: 1, Version: 1, UpdatedAt: "2026-11-01 00:00:00.123456"}
	if err := ck.InsertCurrents(context.Background(), []Current{c, c}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}
func TestClickHouseBatchRejectsHTTPAndUnexpectedBody(t *testing.T) {
	for _, status := range []int{500, 200} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			io.WriteString(w, "incomplete insert error")
		}))
		ck, _ := NewClickHouse(server.URL, "test")
		if err := ck.InsertFacts(context.Background(), []Fact{{TicketID: 1}}); err == nil {
			t.Fatal("failure accepted", status)
		}
		server.Close()
	}
}
func TestClickHouseBatchRequestAndCountBudget(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		data, _ := io.ReadAll(r.Body)
		if len(data) > 4<<20 {
			t.Fatal("oversized request")
		}
	}))
	defer server.Close()
	ck, _ := NewClickHouse(server.URL, "test")
	rows := make([]Fact, 50)
	for i := range rows {
		rows[i] = Fact{TransportEventID: strings.Repeat("x", 100000)}
	}
	if err := ck.InsertFacts(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("request budget not split", calls)
	}
	if err := ck.InsertCurrents(context.Background(), make([]Current, 501)); err == nil {
		t.Fatal("oversized count accepted")
	}
}
