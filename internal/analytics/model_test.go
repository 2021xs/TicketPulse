package analytics

import (
	"net/url"
	"testing"
	"time"
)

func TestBeijingWindowAndBacklogDrilldown(t *testing.T) {
	start, end, err := (Request{Start: "2026-11-03", End: "2026-11-04", TeamIDs: []int64{10, 20}}).Window()
	if err != nil || !start.Equal(time.Date(2026, 11, 2, 16, 0, 0, 0, time.UTC)) || !end.Equal(time.Date(2026, 11, 3, 16, 0, 0, 0, time.UTC)) {
		t.Fatal(start, end, err)
	}
	for _, r := range []Request{{Start: "2026-11-03", End: "2026-11-03"}, {Start: "2026-11-03", End: "2026-11-02"}, {Start: "2026-02-30", End: "2026-11-03"}, {Start: "2025-01-01", End: "2026-11-03"}, {Start: "2026-11-03", End: "2026-11-04", TeamIDs: []int64{0}}} {
		if _, _, err := r.Window(); err == nil {
			t.Error("accepted invalid window", r)
		}
	}
	link, err := url.Parse(Drilldown(20))
	if err != nil {
		t.Fatal(err)
	}
	q := link.Query()
	if link.Path != "/search" || q.Get("current_team_id") != "20" || len(q["status"]) != 2 || q.Has("created_from") || q.Has("updated_from") {
		t.Fatal(link)
	}
}

func TestClickHouseConfigAndRetryErrorBoundary(t *testing.T) {
	for _, endpoint := range []string{"file:///tmp/data", "http://user:password@localhost:8123", "http://localhost:8123/?query=bad", "http://localhost:8123/path"} {
		if _, err := NewClickHouse(endpoint, "owned_test"); err == nil {
			t.Error("accepted", endpoint)
		}
	}
	if _, err := NewClickHouse("http://127.0.0.1:18123", "bad;DROP"); err == nil {
		t.Fatal("accepted SQL identifier")
	}
	if (&WriteError{HTTPStatus: 404, Code: 60}).Permanent() {
		t.Fatal("temporarily missing table must retry")
	}
	if !(&WriteError{HTTPStatus: 500, Code: 47}).Permanent() {
		t.Fatal("schema mismatch must persist isolation")
	}
}
