//go:build integration && m2 && m3

package mysqlstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	assets "ticketpulse/db"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

type m3fixture struct {
	*m2fixture
	ck       *analytics.ClickHouse
	analysis *analytics.Service
	ckCancel context.CancelFunc
	ckDone   chan error
}

func newM3(t *testing.T) *m3fixture {
	t.Helper()
	m := newM2(t)
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = m.name
	cfg.MultiStatements = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(assets.AnalyticsSyncMigration); err != nil {
		t.Fatal(err)
	}
	db.Close()
	ck, err := analytics.NewClickHouse(os.Getenv("TICKETPULSE_CK_URL"), m.name+"_analytics_v1")
	if err != nil {
		t.Fatal(err)
	}
	if err = ck.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	f := &m3fixture{m2fixture: m, ck: ck}
	f.analysis = &analytics.Service{Auth: m.store, CK: ck, Status: &syncpipe.AnalysisStatus{Store: m.state, Queue: m.queue}}
	m.http.Close()
	m.http = httptest.NewServer(httpapi.NewWithOptions(m.service, m.store, nil, httpapi.Options{SessionTTL: time.Hour, Search: m.search, SearchOptions: m.store, Analytics: f.analysis}))
	t.Cleanup(func() {
		f.stopCK(t)
		if os.Getenv("TICKETPULSE_RETAIN_C3_RESOURCES") == "true" {
			return
		}
		_, err := ck.Query(context.Background(), "DROP DATABASE "+m.name+"_analytics_v1")
		if err != nil {
			t.Errorf("owned CK cleanup: %v", err)
		}
	})
	return f
}
func (f *m3fixture) startCK(t *testing.T) {
	t.Helper()
	if f.ckCancel != nil {
		t.Fatal("CK consumer already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.ckCancel = cancel
	f.ckDone = make(chan error, 1)
	c := &syncpipe.CKConsumer{Store: f.state, Queue: f.queue, CK: f.ck, RedeliveryDelay: 100 * time.Millisecond}
	go func() { f.ckDone <- c.Run(ctx) }()
	f.await(t, "CK consumer running", func() bool {
		var state string
		_ = f.db.QueryRow("SELECT state FROM sync_worker_state WHERE component='ck'").Scan(&state)
		return state == "running"
	})
}
func (f *m3fixture) stopCK(t *testing.T) {
	t.Helper()
	if f.ckCancel == nil {
		return
	}
	f.ckCancel()
	select {
	case err := <-f.ckDone:
		if err != nil && !strings.Contains(err.Error(), "canceled") {
			t.Logf("CK stop: %v", err)
		}
	case <-time.After(12 * time.Second):
		t.Error("CK stop timeout")
	}
	f.ckCancel = nil
}
func (f *m3fixture) await(t *testing.T, label string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		select {
		case err := <-f.ckDone:
			t.Fatalf("%s CK exited: %v", label, err)
		default:
		}
		select {
		case err := <-f.captureDone:
			t.Fatalf("%s CDC exited: %v", label, err)
		default:
		}
		select {
		case err := <-f.consumerDone:
			t.Fatalf("%s ES exited: %v", label, err)
		default:
		}
		time.Sleep(40 * time.Millisecond)
	}
	status, _ := (&syncpipe.AnalysisStatus{Store: f.state, Queue: f.queue}).AnalyticsStatus(context.Background())
	t.Fatalf("%s timeout: %+v", label, status)
}
func (f *m3fixture) ckSQL(t *testing.T, query string) string {
	t.Helper()
	data, err := f.ck.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("CK query: %s: %v", query, err)
	}
	return strings.TrimSpace(string(data))
}
func (f *m3fixture) applied(t *testing.T, sequence syncpipe.RecordRef) {
	t.Helper()
	f.await(t, "CK durable disposition", func() bool {
		var state string
		_ = f.db.QueryRow("SELECT status FROM sync_kafka_ck_deliveries WHERE partition_id=? AND record_offset=?", sequence.Partition, sequence.Offset).Scan(&state)
		return state == "applied"
	})
}
func (f *m3fixture) latest(t *testing.T, id, version int64) {
	t.Helper()
	f.await(t, "CK latest version", func() bool {
		data, err := f.ck.Query(context.Background(), fmt.Sprintf("SELECT max(version) FROM ticket_states WHERE ticket_id=%d FORMAT TabSeparated", id))
		return err == nil && strings.TrimSpace(string(data)) == fmt.Sprint(version)
	})
}
func (f *m3fixture) stats(t *testing.T, query string) analytics.Result {
	t.Helper()
	var value analytics.Result
	if err := json.Unmarshal(f.call(t, "local-supervisor", "GET", "/analytics?"+query, nil, 200), &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func (f *m3fixture) event(t *testing.T, id, version int64, label string) syncpipe.Event {
	t.Helper()
	d, err := f.service.Detail(context.Background(), 5, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range d.History {
		if h.Version == version {
			s := h.After
			tkt := ticket.Ticket{ID: id, OrganizationID: d.Ticket.OrganizationID, TicketNo: d.Ticket.TicketNo, Title: s.Title, Description: s.Description, Category: s.Category, Priority: s.Priority, CreatorID: s.CreatorID, Status: s.Status, AssigneeID: s.AssigneeID, InitialTeamID: s.InitialTeamID, CurrentTeamID: s.CurrentTeamID, Version: version, CreatedAt: d.Ticket.CreatedAt, UpdatedAt: h.OccurredAt}
			return syncpipe.Event{Schema: 2, Kind: "ticket", ID: label, Source: "snapshot", TicketID: id, Version: version, EventAt: h.OccurredAt, Document: search.FromTicket(tkt), History: &h, FirstResolvedAt: s.FirstResolvedAt}
		}
	}
	t.Fatalf("missing immutable version %d", version)
	return syncpipe.Event{}
}
func (f *m3fixture) publish(t *testing.T, e syncpipe.Event) syncpipe.RecordRef {
	t.Helper()
	sequence, err := f.queue.Publish(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	return sequence
}
func (f *m3fixture) gold(t *testing.T) analytics.Result {
	t.Helper()
	result := f.stats(t, "start=2026-11-01&end=2026-11-04")
	data, err := os.ReadFile("../../tests/analytics/prd_gold.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected struct {
		Daily    []analytics.Day    `json:"daily"`
		Overview analytics.Overview `json:"overview"`
	}
	if err := json.Unmarshal(data, &expected); err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(result.Daily)
	b, _ := json.Marshal(expected.Daily)
	if string(a) != string(b) {
		t.Fatalf("PRD daily actual=%s independent Gold=%s", a, b)
	}
	a, _ = json.Marshal(result.Overview)
	b, _ = json.Marshal(expected.Overview)
	if string(a) != string(b) {
		t.Fatalf("PRD overview actual=%s Gold=%s", a, b)
	}
	return result
}

func TestM3PRDTenTicketsRealCDCAndHistoricalInitialization(t *testing.T) {
	f := newM3(t)
	input, err := os.ReadFile("../../tests/analytics/prd_ten_tickets.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Six pre-existing tickets test historical import; four load while real CDC is active.
	blocks := strings.Split(string(input), "-- FIXTURE T")
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName = f.name
	cfg.MultiStatements = true
	loader, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer loader.Close()
	for i := 1; i <= 6; i++ {
		if _, err := loader.Exec("-- FIXTURE T" + blocks[i]); err != nil {
			t.Fatal(err)
		}
	}
	f.startCapture(t)
	f.startConsumer(t)
	f.startCK(t)
	for i := 7; i <= 10; i++ {
		if _, err := loader.Exec("-- FIXTURE T" + blocks[i]); err != nil {
			t.Fatal(err)
		}
	}
	f.await(t, "all historical facts", func() bool {
		data, err := f.ck.Query(context.Background(), "SELECT uniqExact(tuple(ticket_id,version)) FROM ticket_events FORMAT TabSeparated")
		return err == nil && strings.TrimSpace(string(data)) == "46"
	})
	result := f.gold(t)
	// Strict independent per-team and left-closed right-open expectations.
	for _, tc := range []struct {
		query                    string
		created, solved, backlog int64
		average                  float64
	}{
		{"start=2026-11-01&end=2026-11-02&team_id=10", 3, 1, 1, 7200},
		{"start=2026-11-02&end=2026-11-03&team_id=10", 2, 1, 1, 86400},
		{"start=2026-11-02&end=2026-11-03&team_id=20", 1, 1, 4, 3600},
		{"start=2026-11-03&end=2026-11-04&team_id=10", 2, 2, 1, 45000},
		{"start=2026-11-03&end=2026-11-04&team_id=20", 1, 1, 4, 176400},
		{"start=2026-11-04&end=2026-11-05", 0, 0, 5, 0},
	} {
		got := f.stats(t, tc.query)
		if got.Overview.Created != tc.created || got.Overview.FirstResolved != tc.solved || got.Overview.Backlog != tc.backlog {
			t.Fatalf("team/date: %s %+v", tc.query, got.Overview)
		}
		if tc.solved > 0 && (got.Overview.AverageFirstResolutionSeconds == nil || *got.Overview.AverageFirstResolutionSeconds != tc.average) {
			t.Fatal(got.Overview)
		}
	}
	for _, id := range []int64{1, 4, 10} {
		var version int64
		f.db.QueryRow("SELECT version FROM tickets WHERE id=?", id).Scan(&version)
		f.projected(t, id, version)
	}
	backlog := f.query(t, "local-supervisor", "current_team_id=20&status=pending&status=in_progress&sort=id&order=asc")
	ids := []int64{}
	for _, item := range backlog.Items {
		ids = append(ids, item.ID)
	}
	if !slices.Equal(ids, []int64{3, 4, 6, 8}) {
		t.Fatalf("current B backlog drilldown=%v", ids)
	}
	// Repeated history/current import overlaps real live messages. No query assumes merges.
	for _, table := range []string{"ticket_events", "ticket_states"} {
		f.ckSQL(t, "SYSTEM STOP MERGES "+f.name+"_analytics_v1."+table)
		defer f.ckSQL(t, "SYSTEM START MERGES "+f.name+"_analytics_v1."+table)
	}
	_, end, err := syncpipe.AnalyticsSnapshot(context.Background(), f.state, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	f.await(t, "snapshot offsets committed", func() bool {
		progress, err := f.queue.Progress(context.Background(), "ck")
		if err != nil {
			return false
		}
		for _, p := range progress {
			if p.Committed < end[p.Partition] {
				return false
			}
		}
		return true
	})
	f.applied(t, f.publish(t, f.event(t, 1, 1, "repeated-create")))
	f.applied(t, f.publish(t, f.event(t, 4, 4, "repeated-first-resolve")))
	f.applied(t, f.publish(t, f.event(t, 4, 1, "late-old-version")))
	f.gold(t)
	if got := f.ckSQL(t, "SELECT tupleElement(argMax(tuple(version,current_team_id,status,first_resolved_at),tuple(version,complete)),2) FROM ticket_states WHERE ticket_id=4 FORMAT TabSeparated"); got != "20" {
		t.Fatal(got)
	}
	if got := f.ckSQL(t, "SELECT count() FROM ticket_events WHERE ticket_id=4 AND action='resolved' FORMAT TabSeparated"); got == "2" {
		t.Fatal("test did not produce actual duplicate physical resolution rows")
	}
	for _, token := range []string{"local-alice", "local-it-agent"} {
		f.call(t, token, "GET", "/analytics?start=2026-11-01&end=2026-11-04", nil, 403)
	}
	f.call(t, "", "GET", "/analytics", nil, 401)
	f.call(t, "local-supervisor", "GET", "/analytics?role=admin", nil, 400)
	f.call(t, "local-supervisor", "GET", "/analytics?start=2026-11-04&end=2026-11-01", nil, 400)
	f.call(t, "local-supervisor", "GET", "/analytics?team_id=99999", nil, 400)
	f.exec(t, "UPDATE users SET active=FALSE WHERE id=5")
	f.call(t, "local-supervisor", "GET", "/analytics", nil, 401)
	f.exec(t, "UPDATE users SET active=TRUE,role='agent' WHERE id=5")
	f.call(t, "local-supervisor", "GET", "/analytics", nil, 403)
	f.exec(t, "UPDATE users SET role='supervisor' WHERE id=5")
	f.await(t, "real source barrier caught up", func() bool {
		state, _ := (&syncpipe.AnalysisStatus{Store: f.state, Queue: f.queue}).AnalyticsStatus(context.Background())
		return state.CaughtUp && state.BootstrapComplete
	})
	// Migration repeated with existing data: no business/history rows rewritten.
	if _, err := loader.Exec(assets.AnalyticsSyncMigration); err != nil {
		t.Fatal(err)
	}
	var tickets, histories int
	f.db.QueryRow("SELECT COUNT(*) FROM tickets").Scan(&tickets)
	f.db.QueryRow("SELECT COUNT(*) FROM ticket_histories").Scan(&histories)
	if tickets != 10 || histories != 46 {
		t.Fatal(tickets, histories)
	}
	t.Logf("RUNTIME PRD daily=%+v overview=%+v B-backlog=%v; physical events=%s canonical histories=46 merges-paused=true", result.Daily, result.Overview, ids, f.ckSQL(t, "SELECT count() FROM ticket_events FORMAT TabSeparated"))
}

func TestM3PartialWritesPreACKInterruptionAndIndependentConsumers(t *testing.T) {
	f := newM3(t)
	f.startCapture(t)
	f.startConsumer(t)
	f.startCK(t)
	baseline := f.create(t, 1, request("m3-before-failure"))
	f.latest(t, baseline.Ticket.ID, 1)
	namespace := f.name + "_analytics_v1"
	// Real ClickHouse table unavailability after a successful fact insert.
	f.ckSQL(t, "RENAME TABLE ticket_states TO ticket_states_unavailable")
	restored := false
	defer func() {
		if !restored {
			_, _ = f.ck.Query(context.Background(), "RENAME TABLE ticket_states_unavailable TO ticket_states")
		}
	}()
	var created ticket.CreateResult
	json.Unmarshal(f.call(t, "local-alice", "POST", "/tickets", request("m3-partial-table-failure"), 201), &created)
	id := created.Ticket.ID
	failedSequence := syncpipe.RecordRef{Topic: f.queue.Topic}
	f.await(t, "real CK partial write/retry", func() bool {
		return f.db.QueryRow("SELECT partition_id,record_offset FROM sync_kafka_ck_deliveries WHERE ticket_id=? AND status='retrying'", id).Scan(&failedSequence.Partition, &failedSequence.Offset) == nil
	})
	f.projected(t, id, 1)
	progress, err := f.queue.Progress(context.Background(), "ck")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range progress {
		if p.Partition == failedSequence.Partition && p.Committed > failedSequence.Offset {
			t.Fatal("Offset crossed failed state write")
		}
	}
	if f.ckSQL(t, fmt.Sprintf("SELECT count() FROM ticket_events WHERE ticket_id=%d FORMAT TabSeparated", id)) == "0" {
		t.Fatal("fact insert did not actually commit")
	}
	state, err := (&syncpipe.AnalysisStatus{Store: f.state, Queue: f.queue}).AnalyticsStatus(context.Background())
	if err != nil || state.CaughtUp || state.Retrying == 0 {
		t.Fatal(state, err)
	}
	f.call(t, "local-supervisor", "GET", "/analytics", nil, 503)
	f.ckSQL(t, "RENAME TABLE ticket_states_unavailable TO ticket_states")
	restored = true
	f.applied(t, failedSequence)
	f.latest(t, id, 1)
	if got := f.stats(t, "start=2026-11-01&end=2026-12-01"); got.Overview.Created != 2 || got.Overview.Backlog != 2 {
		t.Fatal(got.Overview)
	}
	// Fail durable disposition after both CK tables wrote: Run exits without ACK.
	f.exec(t, "CREATE TRIGGER ck_ack_fault BEFORE UPDATE ON sync_kafka_ck_deliveries FOR EACH ROW BEGIN IF NEW.status='applied' AND NEW.ticket_id IS NOT NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='M3 injected pre-ACK interruption'; END IF; END")
	current := f.actionHTTP(t, "local-it-agent", "claim", id, 1, nil)
	select {
	case err := <-f.ckDone:
		if err == nil || !strings.Contains(err.Error(), "M3 injected") {
			t.Fatalf("expected real MySQL pre-ACK failure: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("pre-ACK consumer did not interrupt")
	}
	f.ckCancel()
	f.ckCancel = nil
	f.exec(t, "DROP TRIGGER ck_ack_fault")
	if got := f.ckSQL(t, fmt.Sprintf("SELECT max(version) FROM ticket_states WHERE ticket_id=%d FORMAT TabSeparated", id)); got != "2" {
		t.Fatal("both CK writes must precede interrupted ACK", got)
	}
	interrupted := syncpipe.RecordRef{Topic: f.queue.Topic}
	f.db.QueryRow("SELECT partition_id,record_offset FROM sync_kafka_ck_deliveries WHERE ticket_id=? AND ticket_version=2", id).Scan(&interrupted.Partition, &interrupted.Offset)
	progress, err = f.queue.Progress(context.Background(), "ck")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range progress {
		if p.Partition == interrupted.Partition && p.Committed > interrupted.Offset {
			t.Fatal("incorrect pre-offset confirmation")
		}
	}
	f.startCK(t)
	f.applied(t, interrupted)
	if got := f.ckSQL(t, fmt.Sprintf("SELECT count() FROM ticket_events WHERE ticket_id=%d AND version=2 FORMAT TabSeparated", id)); got == "1" {
		t.Fatal("ACK redelivery was not physically replayed")
	}
	// ES independently continues while CK is stopped, then CK resumes from its own ACK.
	f.stopCK(t)
	current = f.actionHTTP(t, "local-it-agent", "start", id, current.Version, nil)
	f.projected(t, id, current.Version)
	esInfo, _ := f.queue.Progress(context.Background(), "es")
	ckInfo, _ := f.queue.Progress(context.Background(), "ck")
	var esOffset, ckOffset int64
	for _, p := range esInfo {
		if p.Partition == interrupted.Partition {
			esOffset = p.Committed
		}
	}
	for _, p := range ckInfo {
		if p.Partition == interrupted.Partition {
			ckOffset = p.Committed
		}
	}
	if esOffset <= ckOffset {
		t.Fatal("independent groups did not advance separately", esInfo, ckInfo)
	}
	f.startCK(t)
	f.latest(t, id, 3)
	// Actual permanent CK schema failure is persistently isolated, then repaired/replayed.
	f.ckSQL(t, "ALTER TABLE ticket_states RENAME COLUMN current_team_id TO unavailable_team_id")
	repaired := false
	defer func() {
		if !repaired {
			_, _ = f.ck.Query(context.Background(), "ALTER TABLE ticket_states RENAME COLUMN unavailable_team_id TO current_team_id")
		}
	}()
	current = f.actionHTTP(t, "local-it-agent", "comments", id, current.Version, map[string]any{"content": "M3 CK isolation recovery comment"})
	isolated := syncpipe.RecordRef{Topic: f.queue.Topic}
	f.await(t, "CK permanent write isolated", func() bool {
		return f.db.QueryRow("SELECT partition_id,record_offset FROM sync_kafka_ck_deliveries WHERE ticket_id=? AND ticket_version=4 AND status='isolated' AND isolated_payload IS NOT NULL", id).Scan(&isolated.Partition, &isolated.Offset) == nil
	})
	f.ckSQL(t, "ALTER TABLE ticket_states RENAME COLUMN unavailable_team_id TO current_team_id")
	repaired = true
	replay, err := syncpipe.Replay(context.Background(), f.state.ForConsumer("ck"), f.queue, isolated)
	if err != nil {
		t.Fatal(err)
	}
	f.applied(t, replay)
	f.await(t, "CK original recovery persisted", func() bool {
		var status string
		f.db.QueryRow("SELECT status FROM sync_kafka_ck_deliveries WHERE partition_id=? AND record_offset=?", isolated.Partition, isolated.Offset).Scan(&status)
		return status == "recovered"
	})
	f.latest(t, id, 4)
	f.stopCapture(t)
	current = f.actionHTTP(t, "local-it-agent", "resolve", id, current.Version, map[string]any{"resolution_note": "CDC checkpoint restart / CK recovery"})
	f.startCapture(t)
	f.latest(t, id, 5)
	f.projected(t, id, 5)
	f.await(t, "final independent progress caught up", func() bool {
		s, _ := (&syncpipe.AnalysisStatus{Store: f.state, Queue: f.queue}).AnalyticsStatus(context.Background())
		return s.CaughtUp && s.Recovered == 1
	})
	status, _ := (&syncpipe.AnalysisStatus{Store: f.state, Queue: f.queue}).AnalyticsStatus(context.Background())
	t.Logf("RUNTIME CK partial-write record=%+v pre-offset record=%+v recovered=%+v→%+v ES-before=%v CK-before=%v current=v%d status=%+v namespace=%s", failedSequence, interrupted, isolated, replay, esInfo, ckInfo, current.Version, status, namespace)
}
