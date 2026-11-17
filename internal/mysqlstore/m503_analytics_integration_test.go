//go:build integration && m2 && m3

package mysqlstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/analytics"
)

// Equal-key content conflicts must fail the whole dashboard, even when the
// conflicting version is old, unrelated to a metric, or outside date/team scope.
// The frozen PRD oracle continues to be hand calculated, never production SQL.
func TestM503AnalyticsDuplicateLateVersionAndConflicts(t *testing.T) {
	f := newM3(t)
	empty, err := f.ck.Metrics(context.Background(), 1, analytics.Request{Start: "2026-11-01", End: "2026-11-04"})
	if err != nil || len(empty) != 1 || empty[0].Metric != "observed" || empty[0].Amount != 0 {
		t.Fatal("empty namespace lost the observed-zero reconciliation row", empty, err)
	}
	m4LoadGoldForM503(t, f)
	f.startCapture(t)
	f.startConsumer(t)
	f.startCK(t)
	f.await(t, "M503 all PRD business facts", func() bool {
		data, err := f.ck.Query(context.Background(), "SELECT uniqExact(tuple(ticket_id,version)) FROM ticket_events FORMAT TabSeparated")
		return err == nil && string(data) == "46\n"
	})
	before := f.gold(t)
	for _, table := range []string{"ticket_events", "ticket_states"} {
		f.ckSQL(t, "SYSTEM STOP MERGES "+f.name+"_analytics_v1."+table)
		defer f.ckSQL(t, "SYSTEM START MERGES "+f.name+"_analytics_v1."+table)
	}
	for _, item := range []struct {
		id, version int64
		label       string
	}{{4, 4, "m503-duplicate-first-resolution"}, {4, 6, "m503-duplicate-again-resolution"}, {4, 1, "m503-late-creation"}, {1, 4, "m503-late-transfer"}, {4, 5, "m503-late-reopen"}} {
		f.applied(t, f.publish(t, f.event(t, item.id, item.version, item.label)))
	}
	after := f.gold(t)
	if after.Overview.Created != before.Overview.Created || after.Overview.FirstResolved != before.Overview.FirstResolved || after.Overview.Backlog != before.Overview.Backlog {
		t.Fatal("replayed facts changed overview", before.Overview, after.Overview)
	}
	window := analytics.Request{Start: "2026-11-02", End: "2026-11-03", TeamIDs: []int64{20}}
	rows, err := f.ck.Metrics(context.Background(), 1, window)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Metric == "resolved" && (row.TeamID != 20 || row.Amount != 1 || row.Duration != 3600) {
			t.Fatal("again-resolution replaced historical first", rows)
		}
	}
	evidence := map[string]any{"gold_before": before, "gold_after_replay": after, "filtered_rows": rows,
		"replays": "actual Kafka messages with merges paused: first/again resolve, late create/transfer/reopen"}
	// Reconcile consumes old versions too. Inject conflicts into otherwise valid
	// persisted copies, one field at a time; table/schema/protocol remain intact.
	for _, tc := range []struct {
		name, table, changed, where string
	}{
		{"event-team", "ticket_events", "event_team_id+1 AS event_team_id", "ticket_id=1 AND version=1"},
		{"event-action", "ticket_events", "'commented' AS action", "ticket_id=1 AND version=1"},
		{"event-null-assignee", "ticket_events", "toNullable(toUInt64(0)) AS assignee_id", "ticket_id=1 AND version=1"},
		{"event-first-time", "ticket_events", "toNullable(created_at) AS first_resolved_at", "ticket_id=1 AND version=1"},
		{"old-state-team", "ticket_states", "current_team_id+1 AS current_team_id", "ticket_id=1 AND version=1 AND complete=1"},
		{"old-state-null-assignee", "ticket_states", "toNullable(toUInt64(0)) AS assignee_id", "ticket_id=1 AND version=1 AND complete=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saved := "m503_conflict_source"
			f.ckSQL(t, "CREATE TABLE "+saved+" AS "+tc.table)
			f.ckSQL(t, "INSERT INTO "+saved+" SELECT * FROM "+tc.table)
			defer f.ckSQL(t, "DROP TABLE "+saved)
			f.ckSQL(t, "INSERT INTO "+tc.table+" SELECT * REPLACE("+tc.changed+") FROM "+tc.table+" WHERE "+tc.where+" LIMIT 1")
			_, err := f.ck.Metrics(context.Background(), 1, analytics.Request{Start: "2027-01-01", End: "2027-01-02", TeamIDs: []int64{20}})
			if err != analytics.ErrUnavailable {
				t.Fatal("equal business key conflict was hidden by date/team/latest filter", err)
			}
			f.call(t, "local-supervisor", "GET", "/analytics?start=2027-01-01&end=2027-01-02&team_id=20", nil, 503)
			evidence[tc.name] = map[string]any{"injected_sql": tc.changed, "http_status": 503, "conflict_detected": true}
			f.ckSQL(t, "TRUNCATE TABLE "+tc.table)
			f.ckSQL(t, "INSERT INTO "+tc.table+" SELECT * FROM "+saved)
			f.gold(t)
		})
	}
	m503SaveEvidence(t, "gold-duplicates-conflicts.json", evidence)
	f.gold(t)
}

func m4LoadGoldForM503(t *testing.T, f *m3fixture) {
	t.Helper()
	input, err := os.ReadFile("../../tests/analytics/prd_ten_tickets.sql")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName, cfg.MultiStatements = f.name, true
	loader, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer loader.Close()
	if _, err := loader.Exec(string(input)); err != nil {
		t.Fatal(fmt.Errorf("M503 Gold input: %w", err))
	}
}

func TestM503LiveAgainResolutionReopenAndTeamTransfer(t *testing.T) {
	f := newM3(t)
	m4LoadGoldForM503(t, f)
	f.startCapture(t)
	f.startConsumer(t)
	f.startCK(t)
	f.await(t, "all historical facts before live lifecycle", func() bool {
		data, err := f.ck.Query(context.Background(), "SELECT uniqExact(tuple(ticket_id,version)) FROM ticket_events FORMAT TabSeparated")
		return err == nil && string(data) == "46\n"
	})
	baseline := f.gold(t)
	// T04 already resolved twice and was reopened/transferred to team20.
	// All new resolves occur outside the PRD interval and cannot become first.
	current := f.actionHTTP(t, "local-device-agent", "resolve", 4, 10, map[string]any{"resolution_note": "M503 actual third resolution"})
	f.latest(t, 4, current.Version)
	if got := f.stats(t, "start=2026-11-01&end=2026-11-04"); got.Overview.Backlog != 4 || got.Overview.FirstResolved != 6 {
		t.Fatal("again resolve changed first or failed to remove current backlog", got.Overview)
	}
	current = f.actionHTTP(t, "local-alice", "reopen", 4, current.Version, nil)
	f.latest(t, 4, current.Version)
	if got := f.stats(t, "start=2026-11-01&end=2026-11-04"); got.Overview.Backlog != 5 {
		t.Fatal("live reopen did not restore current backlog", got.Overview)
	}
	current = f.actionHTTP(t, "local-supervisor", "transfer-team", 4, current.Version, map[string]any{"team_id": 10, "reason": "M503 cross-team transfer after first"})
	current = f.actionHTTP(t, "local-it-agent", "claim", 4, current.Version, nil)
	current = f.actionHTTP(t, "local-it-agent", "start", 4, current.Version, nil)
	current = f.actionHTTP(t, "local-it-agent", "resolve", 4, current.Version, map[string]any{"resolution_note": "M503 again resolve after team change"})
	current = f.actionHTTP(t, "local-alice", "reopen", 4, current.Version, nil)
	f.latest(t, 4, current.Version)
	stable := f.stats(t, "start=2026-11-01&end=2026-11-04")
	if !reflect.DeepEqual(stable.Daily, baseline.Daily) || !reflect.DeepEqual(stable.Overview, baseline.Overview) {
		t.Fatal("historical first changed during new lifecycle", stable.Daily, stable.Overview)
	}
	for _, team := range stable.Teams {
		if team.ID == 10 && (team.FirstResolved != 4 || team.Backlog != 2) {
			t.Fatal("team10 historical/current scope", team)
		}
		if team.ID == 20 && (team.FirstResolved != 2 || team.Backlog != 3) {
			t.Fatal("team20 historical/current scope", team)
		}
	}
	today := time.Now().In(analytics.Shanghai)
	newWindow := "start=" + today.Format("2006-01-02") + "&end=" + today.AddDate(0, 0, 1).Format("2006-01-02")
	if got := f.stats(t, newWindow); got.Overview.FirstResolved != 0 || got.Overview.Backlog != 5 || got.Overview.AverageFirstResolutionSeconds != nil {
		t.Fatal("later interval treated again resolve as first", got.Overview)
	}
	for _, version := range []int64{1, 4, 6, 8, 11, 16, 17} {
		f.applied(t, f.publish(t, f.event(t, 4, version, fmt.Sprintf("m503-live-replay-%d", version))))
	}
	after := f.stats(t, "start=2026-11-01&end=2026-11-04")
	if !reflect.DeepEqual(stable.Daily, after.Daily) || !reflect.DeepEqual(stable.Overview, after.Overview) || !reflect.DeepEqual(stable.Teams, after.Teams) {
		t.Fatal("live replay changed formal result", after)
	}
	m503SaveEvidence(t, "live-lifecycle-replay.json", map[string]any{"baseline": baseline, "after_http_lifecycle": stable,
		"after_kafka_replay": after, "latest_ticket": current, "new_window": newWindow})
}

func m503SaveEvidence(t *testing.T, name string, value any) {
	t.Helper()
	out := os.Getenv("TICKETPULSE_M503_EVIDENCE_DIR")
	if out == "" {
		return
	}
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, name), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
