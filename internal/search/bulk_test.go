package search

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func bulkDocument() Document {
	return Document{ID: 1, OrganizationID: 1, TicketNo: "TP-1", Title: "bulk source title", Description: "recover bulk description", Status: "pending", Priority: "medium", PriorityRank: 2, Category: "network", CreatorID: 1, InitialTeamID: 10, CurrentTeamID: 10, Version: 1, CreatedAt: time.Date(2026, 11, 1, 1, 2, 3, 123456000, time.UTC), UpdatedAt: time.Date(2026, 11, 1, 1, 2, 3, 123456000, time.UTC)}
}
func TestPutBatchStrictVersionsAndDuplicateNullTime(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/test/_bulk" {
			t.Error(r.URL.Path)
		}
		scanner := bufio.NewScanner(r.Body)
		lines := []string{}
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		if len(lines) != 4 {
			t.Fatalf("line count=%d", len(lines))
		}
		for i := 0; i < 4; i += 2 {
			var meta map[string]map[string]any
			json.Unmarshal([]byte(lines[i]), &meta)
			if meta["index"]["version_type"] != "external" || meta["index"]["_id"] != "1" {
				t.Fatal("external version missing")
			}
			var d Document
			if err := json.Unmarshal([]byte(lines[i+1]), &d); err != nil {
				t.Fatal(err)
			}
			if d.AssigneeID != nil || d.UpdatedAt.Nanosecond() != 123456000 {
				t.Fatal("null/time changed")
			}
		}
		io.WriteString(w, `{"errors":true,"items":[{"index":{"_id":"1","_version":1,"status":201}},{"index":{"_id":"1","status":409,"error":{"type":"version_conflict_engine_exception"}}}]}`)
	}))
	defer server.Close()
	es, _ := NewElasticsearch(server.URL, "test")
	d := bulkDocument()
	if err := es.PutBatch(context.Background(), []Document{d, d}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal(calls)
	}
}
func TestPutBatchRejectsPartialOrFailedItemsDespiteTopFlag(t *testing.T) {
	for _, response := range []string{
		`{"errors":false,"items":[{"index":{"_id":"1","_version":1,"status":201}}]}`,
		`{"errors":false,"items":[{"index":{"_id":"1","_version":1,"status":201}},{"index":{"_id":"1","status":400,"error":{"type":"mapper_parsing_exception"}}}]}`,
		`{"errors":false,"items":[{"index":{"_id":"1","_version":1,"status":201}},{"index":{"_id":"1","status":409,"error":{"type":"another_conflict"}}}]}`,
		`{"errors":false,"items":[{"index":{"_id":"1","_version":1,"status":201}},{"index":{"_id":"1","status":201}}]}`,
		`{"errors":false,"items":[{"index":{"_id":"1","_version":1,"status":201}},{"index":{"_id":"wrong","_version":1,"status":201}}]}`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, response) }))
		es, _ := NewElasticsearch(server.URL, "test")
		d := bulkDocument()
		if err := es.PutBatch(context.Background(), []Document{d, d}); err == nil {
			t.Fatal("incomplete/failed response accepted", response)
		}
		server.Close()
	}
}
func TestPutBatchSplitsRequestsAtBudgetAndRejectsCount(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		data, _ := io.ReadAll(r.Body)
		if len(data) > 4<<20 {
			t.Fatal("oversized request")
		}
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		scanner.Buffer(make([]byte, 4096), 1<<20)
		rows := 0
		for scanner.Scan() {
			rows++
		}
		items := make([]string, rows/2)
		for i := range items {
			items[i] = `{"index":{"_id":"1","_version":1,"status":201}}`
		}
		fmt.Fprintf(w, `{"items":[%s]}`, strings.Join(items, ","))
	}))
	defer server.Close()
	es, _ := NewElasticsearch(server.URL, "test")
	d := bulkDocument()
	d.Description = strings.Repeat("x", 100000)
	if err := es.PutBatch(context.Background(), []Document{d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d, d}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("request budget not split", calls)
	}
	if err := es.PutBatch(context.Background(), make([]Document, 501)); err == nil {
		t.Fatal("oversized count accepted")
	}
}
