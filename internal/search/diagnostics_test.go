package search

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDiagnosticErrorCategoriesAndPrivacy(t *testing.T) {
	const secret = "secret-query-cookie-bearer-ticket-text"
	for _, tc := range []struct {
		name           string
		status         int
		body, category string
		err            error
	}{
		{"rejected", 429, "{\"error\":{\"type\":\"es_rejected_execution_exception\",\"reason\":\"" + secret + "\"}}", "es_rejected_execution_exception", ErrUnavailable},
		{"bad_json", 200, secret, "response_json", ErrUnavailable},
		{"missing_fields", 200, "{}", "response_contract", ErrUnavailable},
		{"timeout", 200, "{\"timed_out\":true,\"took\":3000,\"_shards\":{\"failed\":0},\"hits\":{\"hits\":[]}}", "query_timeout", ErrTimeout},
		{"shards", 200, "{\"timed_out\":false,\"_shards\":{\"failed\":1},\"hits\":{\"hits\":[]}}", "shard_failure", ErrUnavailable},
		{"bad_candidate", 200, "{\"timed_out\":false,\"_shards\":{\"failed\":0},\"hits\":{\"hits\":[{\"_source\":{\"id\":1,\"version\":0},\"sort\":[1]}]}}", "candidate_contract", ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Opaque-ID") != "ticketpulse-test-correlation" {
					t.Error("correlation ID absent")
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			es, _ := NewElasticsearch(srv.URL, "test_tickets")
			d := &Diagnostics{RequestID: "test-correlation"}
			_, err := es.Candidates(WithDiagnostics(context.Background(), d), map[string]any{"query": secret})
			if err != tc.err || d.ESErrorCategory != tc.category || d.ESStatus != tc.status {
				t.Fatalf("err=%v diagnostic=%+v", err, d)
			}
			raw, _ := json.Marshal(d)
			if strings.Contains(string(raw), secret) {
				t.Fatal("sensitive input recorded")
			}
		})
	}
}
