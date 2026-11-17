package search

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"syscall"
	"time"
)

// Diagnostics contains only timings, counters and a closed vocabulary of error
// categories. It deliberately has no query, cursor, credential, ticket or raw
// error fields. Each instance belongs to one HTTP request.
type Diagnostics struct {
	RequestID            string  `json:"request_id"`
	Stage                string  `json:"stage"`
	Outcome              string  `json:"outcome"`
	ErrorCategory        string  `json:"error_category,omitempty"`
	TimeoutAt            string  `json:"timeout_at,omitempty"`
	TotalMS              float64 `json:"total_ms"`
	IdentityMS           float64 `json:"identity_ms"`
	FreshnessMS          float64 `json:"freshness_ms"`
	BeginMS              float64 `json:"begin_ms"`
	AcquireAndBeginMS    float64 `json:"connection_acquire_and_begin_ms"`
	ConnectionHeldMS     float64 `json:"connection_held_ms"`
	AuthorizationMS      float64 `json:"authorization_ms"`
	MySQLQueries         int     `json:"mysql_queries"`
	AuthorizationQueries int     `json:"authorization_queries"`
	Batches              int     `json:"candidate_batches"`
	Candidates           int     `json:"candidates"`
	Allowed              int     `json:"mysql_allowed"`
	VersionRejected      int     `json:"version_rejected"`
	Matched              int     `json:"matched"`
	Returned             int     `json:"returned"`
	ESMS                 float64 `json:"es_ms"`
	ESTookMS             int64   `json:"es_took_ms"`
	ESStatus             int     `json:"es_http_status"`
	ESErrorCategory      string  `json:"es_error_category,omitempty"`
	ESRequestBytes       int     `json:"es_request_bytes"`
	ESResponseBytes      int     `json:"es_response_bytes"`
}

type diagnosticsKey struct{}

func WithDiagnostics(ctx context.Context, d *Diagnostics) context.Context {
	return context.WithValue(ctx, diagnosticsKey{}, d)
}
func Diagnostic(ctx context.Context) *Diagnostics {
	d, _ := ctx.Value(diagnosticsKey{}).(*Diagnostics)
	return d
}
func Milliseconds(start time.Time) float64 { return float64(time.Since(start).Nanoseconds()) / 1e6 }

func ErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, ErrTimeout) {
		return "search_timeout"
	}
	if errors.Is(err, ErrBudget) {
		return "candidate_budget"
	}
	if errors.Is(err, ErrUnavailable) {
		return "search_unavailable"
	}
	if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) {
		return "file_descriptors"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection_refused"
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return "connection_reset"
	}
	if errors.Is(err, syscall.EADDRNOTAVAIL) {
		return "address_not_available"
	}
	if errors.Is(err, syscall.ENOBUFS) {
		return "socket_buffers"
	}
	if errors.Is(err, syscall.EADDRINUSE) {
		return "address_in_use"
	}
	if errors.Is(err, syscall.EPIPE) {
		return "broken_pipe"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "unexpected_eof"
	}
	var netError net.Error
	if errors.As(err, &netError) && netError.Timeout() {
		return "transport_timeout"
	}
	return "other"
}

func safeESError(data []byte) string {
	// Error types only; ES reason/root_cause can contain sensitive input.
	var body struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &body) != nil {
		return "http_error"
	}
	switch body.Error.Type {
	case "es_rejected_execution_exception", "circuit_breaking_exception",
		"index_not_found_exception", "search_phase_execution_exception",
		"timeout_exception", "security_exception", "illegal_argument_exception":
		return body.Error.Type
	default:
		return "http_error"
	}
}
