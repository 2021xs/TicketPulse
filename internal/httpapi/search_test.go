package httpapi

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"ticketpulse/internal/search"
)

func TestSearchParameters(t *testing.T) {
	for _, query := range []string{"role=admin", "status=invalid", "q=a&q=b", "created_from=2026-11-01", "created_from=1000-01-01T00:00:00Z", "created_from=2026-11-03T00:00:00Z&created_to=2026-11-02T00:00:00Z", "page_size=101", "assignee_id=-1", "current_team_id=0", "sort=title"} {
		values, _ := url.ParseQuery(query)
		if _, err := searchRequest(values); err == nil {
			t.Error("accepted " + query)
		}
	}
	values, _ := url.ParseQuery("status=pending&status=in_progress&assignee_id=0&assignee_id=3&created_from=2026-11-01T00:00:00%2B08:00&created_to=2026-11-02T00:00:00%2B08:00")
	result, err := searchRequest(values)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Statuses) != 2 || len(result.AssigneeIDs) != 2 || result.CreatedFrom.Hour() != 16 {
		t.Fatal(result)
	}
}

type diagnosticAuth struct{}

func (diagnosticAuth) Authenticate(context.Context, string) (int64, error) { return 3, nil }

type diagnosticSearch struct{ err error }

func (s diagnosticSearch) Query(ctx context.Context, _ int64, _ search.Request) (search.Result, error) {
	search.Diagnostic(ctx).Stage = "mysql_authorization"
	return search.Result{}, s.err
}

func TestSearchCorrelationAndSafeFailureLogs(t *testing.T) {
	const secret = "credential-cookie-query-ticket-secret-marker"
	for _, err := range []error{search.ErrUnavailable, context.DeadlineExceeded, errors.New(secret)} {
		var log bytes.Buffer
		var observed search.Diagnostics
		api := NewWithOptions(nil, diagnosticAuth{}, slog.New(slog.NewJSONHandler(&log, nil)), Options{Search: diagnosticSearch{err}, SearchObserver: func(d search.Diagnostics) { observed = d }})
		request := httptest.NewRequest(http.MethodGet, "/api/v1/search/tickets?q="+secret, nil)
		request.Header.Set("Authorization", "Bearer "+secret)
		request.Header.Set("Cookie", "ticketpulse_session="+secret)
		request.Header.Set("X-Request-ID", secret)
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		id := response.Header().Get("X-Request-ID")
		if len(id) != 32 || id != observed.RequestID || id == secret {
			t.Fatal("server correlation ID invalid")
		}
		if observed.Stage != "mysql_authorization" {
			t.Fatal(observed)
		}
		if strings.Contains(log.String(), secret) || strings.Contains(response.Body.String(), secret) {
			t.Fatal("raw credentials/input/error logged or exposed")
		}
		if response.Code != http.StatusServiceUnavailable && response.Code != http.StatusInternalServerError {
			t.Fatal(response.Code)
		}
		if err == context.DeadlineExceeded && observed.TimeoutAt != "mysql_authorization" {
			t.Fatal("timeout stage lost")
		}
	}
}
