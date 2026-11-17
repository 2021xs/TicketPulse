package syncpipe

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash"
	"reflect"
	"sort"
	"strconv"
	"time"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

const (
	ReconcileConsistent       = "consistent"
	ReconcileInconsistent     = "inconsistent"
	ReconcileCatchingUp       = "catching_up"
	ReconcileUncheckable      = "uncheckable"
	reconcileDifferenceBudget = 1000
)

type ReconcileOptions struct {
	BatchSize             int
	LagTimeout            time.Duration
	OrganizationID        int64
	MetricsOrganizationID int64
	MetricsRequest        analytics.Request
	// Called after establishing the source RR snapshot, before observing targets.
	// A live rebuild passes its own bounded marker-and-tail confirmation here.
	// nil with q=nil is reserved for callers that have confirmed static input.
	ConfirmSourceBoundary func(context.Context) error
	// Optional read-only diagnostics. Never changes source/target selection.
	ObservePage func(ReconcilePageObservation)
}

type ReconcilePageObservation struct {
	Phase      string                 `json:"phase"`
	Source     string                 `json:"source"`
	Query      string                 `json:"query"`
	Parameters []any                  `json:"parameters"`
	Cursor     analytics.ReconcileKey `json:"cursor"`
	Rows       int                    `json:"rows"`
	DurationMS float64                `json:"duration_ms"`
	Error      string                 `json:"error,omitempty"`
}
type reconcileObservationContext struct {
	phase   string
	observe func(ReconcilePageObservation)
}
type reconcileObservationKey struct{}

func reconcileObservedContext(ctx context.Context, phase string, o ReconcileOptions) context.Context {
	return context.WithValue(ctx, reconcileObservationKey{}, reconcileObservationContext{phase, o.ObservePage})
}
func reconcileObservePage(ctx context.Context, kind, query string, params []any, key analytics.ReconcileKey, rows int, started time.Time, err error) {
	o, _ := ctx.Value(reconcileObservationKey{}).(reconcileObservationContext)
	if o.observe == nil {
		return
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	o.observe(ReconcilePageObservation{o.phase, kind, query, params, key, rows, float64(time.Since(started).Microseconds()) / 1000, message})
}

type ReconcileCounts struct {
	Tickets       int64 `json:"tickets"`
	Histories     int64 `json:"histories"`
	FirstResolved int64 `json:"first_resolved"`
	Reopened      int64 `json:"reopened"`
}
type ReconcileSource struct {
	ReconcileCounts
	Fingerprint   string `json:"fingerprint"`
	CurrentSHA256 string `json:"current_sha256"`
	HistorySHA256 string `json:"history_sha256"`
}
type ReconcileDifference struct {
	Component      string `json:"component"`
	OrganizationID int64  `json:"organization_id,omitempty"`
	TicketID       int64  `json:"ticket_id,omitempty"`
	Version        int64  `json:"version,omitempty"`
	Field          string `json:"field"`
	Expected       any    `json:"expected"`
	Actual         any    `json:"actual"`
}
type ReconcileProjection struct {
	Target              string `json:"target"`
	CurrentCount        int64  `json:"current_count"`
	HistoryCount        int64  `json:"history_count"`
	PhysicalCurrentRows int64  `json:"physical_current_rows,omitempty"`
	PhysicalHistoryRows int64  `json:"physical_history_rows,omitempty"`
	ConflictingKeys     int64  `json:"conflicting_keys"`
	FirstResolved       int64  `json:"first_resolved"`
	Reopened            int64  `json:"reopened"`
	CurrentSHA256       string `json:"current_sha256"`
	HistorySHA256       string `json:"history_sha256"`
}
type ReconcileMetrics struct {
	OrganizationID int64                 `json:"organization_id"`
	Request        analytics.Request     `json:"request"`
	MySQL          []analytics.MetricRow `json:"mysql"`
	ClickHouse     []analytics.MetricRow `json:"clickhouse"`
}
type ReconcileBoundary struct {
	CallerConfirmed bool                           `json:"caller_confirmed"`
	SourcePosition  Position                       `json:"source_position"`
	Checkpoint      *Checkpoint                    `json:"checkpoint,omitempty"`
	EndOffsets      map[int32]int64                `json:"end_offsets"`
	Progress        map[string][]PartitionProgress `json:"consumer_progress"`
	Confirmed       bool                           `json:"confirmed"`
	DeadlineExpired bool                           `json:"deadline_expired"`
}
type ReconcileReport struct {
	State                string                `json:"state"`
	StartedAt            time.Time             `json:"started_at"`
	CompletedAt          time.Time             `json:"completed_at"`
	OrganizationID       int64                 `json:"organization_id"`
	Source               ReconcileSource       `json:"source"`
	SourceAfter          *ReconcileSource      `json:"source_after,omitempty"`
	SourceChanged        bool                  `json:"source_changed"`
	Boundary             ReconcileBoundary     `json:"boundary"`
	ES                   *ReconcileProjection  `json:"es,omitempty"`
	CK                   *ReconcileProjection  `json:"ck,omitempty"`
	Metrics              *ReconcileMetrics     `json:"metrics,omitempty"`
	Differences          []ReconcileDifference `json:"differences"`
	DifferenceCount      int64                 `json:"difference_count"`
	DifferencesTruncated bool                  `json:"differences_truncated"`
	Problems             []string              `json:"problems"`
	differenceBytes      int
}

func (r *ReconcileReport) difference(component string, key analytics.ReconcileKey, field string, expected, actual any) {
	r.DifferenceCount++
	difference := ReconcileDifference{component, key.OrganizationID, key.TicketID, key.Version, field, expected, actual}
	payload, _ := json.Marshal(difference)
	if len(r.Differences) < reconcileDifferenceBudget && r.differenceBytes+len(payload) <= 8<<20 {
		r.Differences = append(r.Differences, difference)
		r.differenceBytes += len(payload)
	} else {
		r.DifferencesTruncated = true
	}
}
func (r *ReconcileReport) compare(component string, key analytics.ReconcileKey, expected, actual any) {
	e, _ := json.Marshal(expected)
	a, _ := json.Marshal(actual)
	var em, am map[string]json.RawMessage
	if json.Unmarshal(e, &em) != nil || json.Unmarshal(a, &am) != nil {
		r.difference(component, key, "value", expected, actual)
		return
	}
	fields := make([]string, 0, len(em))
	for field := range em {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		if !jsonEqual(em[field], am[field]) {
			r.difference(component, key, field, em[field], am[field])
		}
	}
}
func jsonEqual(a, b json.RawMessage) bool {
	var av, bv any
	ad, bd := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	ad.UseNumber()
	bd.UseNumber()
	if ad.Decode(&av) != nil || bd.Decode(&bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

// Reconcile freezes one repeatable-read source view. Streaming merges cover both
// missing and extra keys, and a second independent source digest detects writes
// during the observation. A frozen synchronization boundary has a finite deadline.
func Reconcile(ctx context.Context, store *StateStore, q *Queue, es *search.Elasticsearch, ck *analytics.ClickHouse, options ReconcileOptions) (report ReconcileReport, err error) {
	report = ReconcileReport{State: ReconcileUncheckable, StartedAt: time.Now().UTC(), OrganizationID: options.OrganizationID, Differences: []ReconcileDifference{}, Problems: []string{}}
	defer func() {
		report.CompletedAt = time.Now().UTC()
		if err != nil {
			report.State = ReconcileUncheckable
			report.Problems = append(report.Problems, err.Error())
		}
	}()
	if store == nil || store.DB == nil || es == nil && ck == nil || options.OrganizationID < 0 || options.MetricsOrganizationID < 0 {
		return report, fmt.Errorf("reconciliation requires MySQL and at least one projection")
	}
	if options.BatchSize == 0 {
		options.BatchSize = 250
	}
	if options.BatchSize < 1 || options.BatchSize > 1000 || options.LagTimeout < 0 || options.LagTimeout > 5*time.Minute {
		return report, fmt.Errorf("invalid reconciliation batch or lag timeout")
	}
	metrics := options.MetricsRequest.Start != "" || options.MetricsRequest.End != "" || len(options.MetricsRequest.TeamIDs) > 0
	if options.MetricsOrganizationID == 0 {
		options.MetricsOrganizationID = options.OrganizationID
	}
	if metrics {
		if ck == nil || options.MetricsOrganizationID <= 0 {
			return report, fmt.Errorf("metrics reconciliation requires CK and one organization")
		}
		if _, _, err = options.MetricsRequest.Window(); err != nil {
			return report, err
		}
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	var mysqlMetrics []analytics.MetricRow
	report.Source, mysqlMetrics, err = reconcileSource(reconcileObservedContext(ctx, "source_before", options), tx, options, metrics)
	if err != nil {
		return report, err
	}
	report.Boundary = ReconcileBoundary{CallerConfirmed: q == nil, Confirmed: q == nil, EndOffsets: map[int32]int64{}, Progress: map[string][]PartitionProgress{}}
	if q == nil && options.ConfirmSourceBoundary != nil {
		report.Boundary.Confirmed = false
		if err = options.ConfirmSourceBoundary(ctx); err != nil {
			return report, err
		}
		report.Boundary.Confirmed = true
	}
	if q != nil {
		report.Boundary.SourcePosition, err = CurrentPosition(ctx, store.DB)
		if err != nil {
			return report, err
		}
		report.Boundary, err = reconcileWaitBoundary(ctx, store, q, report.Boundary, es != nil, ck != nil, options.LagTimeout)
		if err != nil {
			return report, err
		}
	}
	if es != nil {
		report.ES = &ReconcileProjection{Target: es.ReconcileTarget()}
		if err = reconcileES(reconcileObservedContext(ctx, "es", options), tx, es, options, &report); err != nil {
			return report, err
		}
	}
	if ck != nil {
		report.CK = &ReconcileProjection{Target: ck.ReconcileTarget()}
		if err = reconcileCK(reconcileObservedContext(ctx, "ck", options), tx, ck, options, &report); err != nil {
			return report, err
		}
	}
	if metrics && report.CK.ConflictingKeys > 0 {
		// Full content comparison already proved an equal-key conflict. The
		// dashboard rejects those facts; do not replace that confirmed difference
		// with an unavailable-query diagnosis or compute arbitrary metrics. Keep
		// the source-after and boundary checks below, including catching-up priority.
		report.Problems = append(report.Problems, "metrics omitted: confirmed conflicting ClickHouse business versions")
	} else if metrics {
		actual, problem := ck.Metrics(ctx, options.MetricsOrganizationID, options.MetricsRequest)
		if problem != nil {
			return report, problem
		}
		report.Metrics = &ReconcileMetrics{options.MetricsOrganizationID, options.MetricsRequest, mysqlMetrics, actual}
		reconcileCompareMetrics(&report, mysqlMetrics, actual)
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	afterTx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return report, err
	}
	defer afterTx.Rollback()
	after, _, err := reconcileSource(reconcileObservedContext(ctx, "source_after", options), afterTx, options, false)
	if err != nil {
		return report, err
	}
	if err = afterTx.Commit(); err != nil {
		return report, err
	}
	report.SourceAfter = &after
	report.SourceChanged = report.Source.Fingerprint != after.Fingerprint
	report.State = ReconcileConsistent
	if report.SourceChanged {
		report.State = ReconcileCatchingUp
		report.Problems = append(report.Problems, "source changed during reconciliation; a new confirmed boundary is required")
	} else if !report.Boundary.Confirmed {
		if options.LagTimeout == 0 {
			report.State = ReconcileCatchingUp
		} else {
			report.State = ReconcileUncheckable
			report.Problems = append(report.Problems, "frozen synchronization boundary was not confirmed before the finite deadline")
		}
	} else if report.DifferenceCount > 0 {
		report.State = ReconcileInconsistent
	}
	return report, nil
}

type reconcileQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}
type reconcileCurrentRow struct {
	Document    search.Document
	First       *time.Time
	ReopenCount int64
}

func reconcileCurrentPage(ctx context.Context, db reconcileQuery, org int64, after analytics.ReconcileKey, size int) (result []reconcileCurrentRow, err error) {
	started := time.Now()
	query := "SELECT " + projectionColumns + ",first_resolved_at,reopen_count FROM tickets FORCE INDEX (uk_tickets_org_id) WHERE (?=0 OR organization_id=?) AND (organization_id>? OR (organization_id=? AND id>?)) ORDER BY organization_id,id LIMIT ?"
	params := []any{org, org, after.OrganizationID, after.OrganizationID, after.TicketID, size}
	defer func() { reconcileObservePage(ctx, "mysql_current", query, params, after, len(result), started, err) }()
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result = make([]reconcileCurrentRow, 0)
	responseBytes := 0
	for rows.Next() {
		var row reconcileCurrentRow
		d := &row.Document
		if err = rows.Scan(&d.ID, &d.OrganizationID, &d.TicketNo, &d.Title, &d.Description, &d.Status, &d.Priority, &d.Category, &d.CreatorID, &d.AssigneeID, &d.InitialTeamID, &d.CurrentTeamID, &d.CreatedAt, &d.UpdatedAt, &d.Version, &row.First, &row.ReopenCount); err != nil {
			return nil, err
		}
		d.PriorityRank = map[string]int{"low": 1, "medium": 2, "high": 3, "urgent": 4}[d.Priority]
		d.CreatedAt = d.CreatedAt.UTC()
		d.UpdatedAt = d.UpdatedAt.UTC()
		if row.First != nil {
			at := row.First.UTC()
			row.First = &at
		}
		result = append(result, row)
		payload, _ := json.Marshal(row)
		responseBytes += len(payload)
		if responseBytes > 8<<20 {
			return nil, fmt.Errorf("MySQL current batch exceeds reconciliation response budget; use a smaller batch")
		}
	}
	return result, rows.Err()
}

type reconcileHistoryRow struct {
	History        ticket.History
	OrganizationID int64
	TicketNo       string
	CreatedAt      time.Time
}

func reconcileHistoryPage(ctx context.Context, db reconcileQuery, org int64, after analytics.ReconcileKey, size int) (result []reconcileHistoryRow, err error) {
	started := time.Now()
	query := `SELECT h.id,h.organization_id,h.ticket_id,h.action,h.operator_id,h.version,h.before_state,h.after_state,h.responsible_team_id,h.occurred_at,h.reason,h.comment_id,t.ticket_no,t.created_at
 FROM ticket_histories h FORCE INDEX (idx_history_reconcile) JOIN tickets t ON t.id=h.ticket_id AND t.organization_id=h.organization_id
 WHERE (?=0 OR h.organization_id=?) AND (h.organization_id>? OR (h.organization_id=? AND (h.ticket_id>? OR (h.ticket_id=? AND h.version>?)))) ORDER BY h.organization_id,h.ticket_id,h.version LIMIT ?`
	params := []any{org, org, after.OrganizationID, after.OrganizationID, after.TicketID, after.TicketID, after.Version, size}
	defer func() { reconcileObservePage(ctx, "mysql_history", query, params, after, len(result), started, err) }()
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result = make([]reconcileHistoryRow, 0)
	responseBytes := 0
	for rows.Next() {
		var row reconcileHistoryRow
		h := &row.History
		var before, afterState []byte
		if err = rows.Scan(&h.ID, &row.OrganizationID, &h.TicketID, &h.Action, &h.OperatorID, &h.Version, &before, &afterState, &h.ResponsibleTeamID, &h.OccurredAt, &h.Reason, &h.CommentID, &row.TicketNo, &row.CreatedAt); err != nil {
			return nil, err
		}
		if len(before) > 0 && string(before) != "null" {
			h.Before = &ticket.State{}
			if err = json.Unmarshal(before, h.Before); err != nil {
				return nil, err
			}
		}
		if err = json.Unmarshal(afterState, &h.After); err != nil {
			return nil, err
		}
		h.OccurredAt = h.OccurredAt.UTC()
		row.CreatedAt = row.CreatedAt.UTC()
		result = append(result, row)
		payload, _ := json.Marshal(row)
		responseBytes += len(payload)
		if responseBytes > 8<<20 {
			return nil, fmt.Errorf("MySQL history batch exceeds reconciliation response budget; use a smaller batch")
		}
	}
	return result, rows.Err()
}
func reconcileHistoryFact(row reconcileHistoryRow) (analytics.Fact, error) {
	h := row.History
	s := h.After
	d := search.FromTicket(ticket.Ticket{ID: h.TicketID, OrganizationID: row.OrganizationID, TicketNo: row.TicketNo, Title: s.Title, Description: s.Description, Status: s.Status, Priority: s.Priority, Category: s.Category, CreatorID: s.CreatorID, AssigneeID: s.AssigneeID, InitialTeamID: s.InitialTeamID, CurrentTeamID: s.CurrentTeamID, CreatedAt: row.CreatedAt, UpdatedAt: h.OccurredAt, Version: h.Version})
	fact, _, err := AnalysisProjection(Event{Schema: 2, Kind: "ticket", ID: fmt.Sprintf("reconcile:%d:%d:%d", row.OrganizationID, h.TicketID, h.Version), Source: "snapshot", TicketID: h.TicketID, Version: h.Version, EventAt: h.OccurredAt, Document: d, History: &h, FirstResolvedAt: s.FirstResolvedAt})
	if err != nil {
		return analytics.Fact{}, err
	}
	fact.TransportEventID = ""
	return *fact, nil
}
func reconcileCurrent(row reconcileCurrentRow) analytics.Current {
	d := row.Document
	return analytics.Current{OrganizationID: d.OrganizationID, TicketID: d.ID, Version: d.Version, Complete: 1, CurrentTeamID: d.CurrentTeamID, AssigneeID: d.AssigneeID, Status: d.Status, InitialTeamID: d.InitialTeamID, CreatedAt: analytics.Timestamp(d.CreatedAt), UpdatedAt: analytics.Timestamp(d.UpdatedAt), FirstResolvedAt: analytics.OptionalTimestamp(row.First)}
}
func reconcileKey(org, id, version int64) analytics.ReconcileKey {
	return analytics.ReconcileKey{OrganizationID: org, TicketID: id, Version: version}
}
func reconcileKeyCompare(a, b analytics.ReconcileKey) int {
	for _, p := range [][2]int64{{a.OrganizationID, b.OrganizationID}, {a.TicketID, b.TicketID}, {a.Version, b.Version}} {
		if p[0] < p[1] {
			return -1
		}
		if p[0] > p[1] {
			return 1
		}
	}
	return 0
}
func reconcileHash(h hash.Hash, value any) {
	data, _ := json.Marshal(value)
	h.Write(data)
	h.Write([]byte{'\n'})
}
func reconcileSHA(h hash.Hash) string { return fmt.Sprintf("%x", h.Sum(nil)) }

func reconcileWaitBoundary(ctx context.Context, store *StateStore, q *Queue, b ReconcileBoundary, es, ck bool, timeout time.Duration) (ReconcileBoundary, error) {
	deadline := time.Now().Add(timeout)
	components := []string{}
	if es {
		components = append(components, "es")
	}
	if ck {
		components = append(components, "ck")
	}
	for {
		cp, err := store.Checkpoint(ctx)
		if err != nil && err != sql.ErrNoRows {
			return b, err
		}
		if err == nil {
			copy := cp
			b.Checkpoint = &copy
		}
		captured := err == nil && cp.BootstrapComplete && reconcilePositionAtLeast(cp.Position, b.SourcePosition)
		if captured && len(b.EndOffsets) == 0 {
			p, err := q.Progress(ctx, components[0])
			if err != nil {
				return b, err
			}
			for _, row := range p {
				b.EndOffsets[row.Partition] = row.End
			}
		}
		confirmed := captured && len(b.EndOffsets) == int(q.Partitions)
		for _, component := range components {
			p, err := q.Progress(ctx, component)
			if err != nil {
				return b, err
			}
			b.Progress[component] = p
			for _, row := range p {
				if row.Committed < b.EndOffsets[row.Partition] {
					confirmed = false
				}
			}
		}
		if confirmed {
			b.Confirmed = true
			return b, nil
		}
		if timeout == 0 || !time.Now().Before(deadline) {
			b.DeadlineExpired = timeout > 0
			return b, nil
		}
		wait := 100 * time.Millisecond
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		select {
		case <-ctx.Done():
			return b, ctx.Err()
		case <-time.After(wait):
		}
	}
}
func reconcilePositionAtLeast(a, b Position) bool {
	if a.ServerUUID != b.ServerUUID || a.File == "" || b.File == "" {
		return false
	}
	if a.File == b.File {
		return a.Pos >= b.Pos
	}
	parse := func(file string) (string, int64, error) {
		for i := len(file) - 1; i >= 0; i-- {
			if file[i] == '.' {
				n, err := strconv.ParseInt(file[i+1:], 10, 64)
				return file[:i], n, err
			}
		}
		return "", 0, fmt.Errorf("invalid binlog name")
	}
	ap, an, ae := parse(a.File)
	bp, bn, be := parse(b.File)
	return ae == nil && be == nil && ap == bp && an > bn
}
