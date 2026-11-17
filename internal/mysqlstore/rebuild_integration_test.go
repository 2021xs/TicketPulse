//go:build integration && m2 && m3 && m4

package mysqlstore_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kadm"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

var m4Window = analytics.Request{Start: "2026-11-01", End: "2026-11-04"}

func newM4(t *testing.T) (*m3fixture, *syncpipe.Rebuilder) {
	t.Helper()
	f := newM3(t)
	if err := syncpipe.EnsureRebuildSchema(context.Background(), f.db); err != nil {
		t.Fatal(err)
	}
	r := &syncpipe.Rebuilder{Store: f.state, Queue: f.queue, ESURL: os.Getenv("TICKETPULSE_ES_URL"), CKURL: os.Getenv("TICKETPULSE_CK_URL"), BatchSize: 2, CatchupTimeout: 30 * time.Second}
	return f, r
}

func m4LoadGold(t *testing.T, f *m3fixture) {
	t.Helper()
	input, err := os.ReadFile("../../tests/analytics/prd_ten_tickets.sql")
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName, cfg.MultiStatements = f.name, true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(string(input)); err != nil {
		t.Fatal(err)
	}
	// The fixture has IDs 1–10 without create reservations. New HTTP creates
	// must use fresh IDs, keeping the independently frozen PRD inputs intact.
	if _, err := db.Exec("ALTER TABLE ticket_create_requests AUTO_INCREMENT=100"); err != nil {
		t.Fatal(err)
	}
}

func m4Job(t *testing.T, f *m3fixture, r *syncpipe.Rebuilder, id, target string) syncpipe.RebuildJob {
	t.Helper()
	options := syncpipe.RebuildOptions{ID: id, Target: target, BatchSize: 2, OrganizationID: 1}
	if target != "es" {
		options.MetricsRequest = m4Window
	}
	j, err := r.Create(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if os.Getenv("TICKETPULSE_RETAIN_C3_RESOURCES") == "true" {
			return
		}
		if !strings.HasPrefix(j.Index, f.name+"_rebuild_") || !strings.HasPrefix(j.Database, f.name+"_rebuild_") {
			t.Error("refusing non-fixture cleanup")
			return
		}
		m4HTTP(t, http.MethodDelete, os.Getenv("TICKETPULSE_ES_URL")+"/"+j.Index, nil, 200, 404)
		if _, err := f.ck.Query(context.Background(), "DROP DATABASE IF EXISTS "+j.Database); err != nil {
			t.Errorf("owned rebuild CK cleanup: %v", err)
		}
	})
	return j
}

func m4Run(t *testing.T, r *syncpipe.Rebuilder, id string) syncpipe.RebuildJob {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	j, err := r.Run(ctx, id)
	if err != nil {
		t.Fatalf("rebuild %s job=%+v error=%v", id, j, err)
	}
	if j.Status != "completed" || j.Applied != j.Total || j.Report == nil || j.Report.State != "consistent" {
		t.Fatalf("incomplete rebuild %+v", j)
	}
	return j
}

func m4HTTP(t *testing.T, method, endpoint string, body []byte, statuses ...int) []byte {
	t.Helper()
	req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range statuses {
		if res.StatusCode == status {
			return data
		}
	}
	t.Fatalf("%s %s HTTP=%d body=%s", method, endpoint, res.StatusCode, data)
	return nil
}

func m4CK(t *testing.T, j syncpipe.RebuildJob) *analytics.ClickHouse {
	t.Helper()
	ck, err := analytics.NewClickHouse(os.Getenv("TICKETPULSE_CK_URL"), j.Database)
	if err != nil {
		t.Fatal(err)
	}
	return ck
}
func m4CKSQL(t *testing.T, ck *analytics.ClickHouse, query string) string {
	t.Helper()
	data, err := ck.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("independent CK SQL %s: %v", query, err)
	}
	return strings.TrimSpace(string(data))
}

// Raw ES HTTP and CK SQL compare all key fields with current MySQL rows and
// immutable histories. The production report is never the only oracle.
func m4AssertProjection(t *testing.T, f *m3fixture, j syncpipe.RebuildJob) {
	t.Helper()
	rows, err := f.db.Query("SELECT id FROM tickets ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if j.Target != "ck" {
		m4HTTP(t, http.MethodPost, os.Getenv("TICKETPULSE_ES_URL")+"/"+j.Index+"/_refresh", nil, 200)
		var count struct{ Count int }
		json.Unmarshal(m4HTTP(t, http.MethodGet, os.Getenv("TICKETPULSE_ES_URL")+"/"+j.Index+"/_count", nil, 200), &count)
		if count.Count != len(ids) {
			t.Fatalf("ES count=%d SQL count=%d", count.Count, len(ids))
		}
	}
	var ck *analytics.ClickHouse
	if j.Target != "es" {
		ck = m4CK(t, j)
		if got := m4CKSQL(t, ck, "SELECT uniqExact(ticket_id) FROM ticket_states FORMAT TabSeparated"); got != fmt.Sprint(len(ids)) {
			t.Fatalf("CK current count=%s SQL=%v", got, ids)
		}
	}
	for _, id := range ids {
		detail, err := f.service.Detail(context.Background(), 5, id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Target != "ck" {
			data := m4HTTP(t, http.MethodGet, fmt.Sprintf("%s/%s/_doc/%d", os.Getenv("TICKETPULSE_ES_URL"), j.Index, id), nil, 200)
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(data, &raw); err != nil {
				t.Fatal(err)
			}
			var version int64
			var doc search.Document
			json.Unmarshal(raw["_version"], &version)
			json.Unmarshal(raw["_source"], &doc)
			if version != detail.Ticket.Version || !reflect.DeepEqual(doc, search.FromTicket(detail.Ticket)) {
				t.Fatalf("ES full source mismatch id=%d SQL=%+v ES=%+v", id, search.FromTicket(detail.Ticket), doc)
			}
		}
		if ck != nil {
			v := detail.Ticket
			// Build one argMax tuple so nullable assignee/first-resolution values
			// belong to the chosen version rather than being skipped separately.
			query := fmt.Sprintf("SELECT organization_id,ticket_id,argMax(tuple(version,complete,current_team_id,assignee_id,status,initial_team_id,toString(created_at),toString(updated_at),toString(first_resolved_at)),tuple(version,complete)) AS current FROM ticket_states WHERE ticket_id=%d GROUP BY organization_id,ticket_id FORMAT JSONEachRow", id)
			var current map[string]json.RawMessage
			if err := json.Unmarshal([]byte(m4CKSQL(t, ck, query)), &current); err != nil {
				t.Fatal(err)
			}
			var actual []any
			if err := json.Unmarshal(current["current"], &actual); err != nil {
				t.Fatal(err)
			}
			expected := []any{v.Version, 1, v.CurrentTeamID, v.AssigneeID, v.Status, v.InitialTeamID, analytics.Timestamp(v.CreatedAt), analytics.Timestamp(v.UpdatedAt), analytics.OptionalTimestamp(v.FirstResolvedAt)}
			a, _ := json.Marshal(actual)
			b, _ := json.Marshal(expected)
			if string(a) != string(b) {
				t.Fatalf("CK latest mismatch id=%d SQL=%s CK=%s", id, b, a)
			}
			for _, h := range detail.History {
				query := fmt.Sprintf("SELECT count() AS physical,uniqExact(tuple(action,event_team_id,toString(occurred_at),after_status)) AS variants,any(tuple(action,event_team_id,toString(occurred_at),after_status)) AS fact FROM ticket_events WHERE ticket_id=%d AND version=%d FORMAT JSONEachRow", id, h.Version)
				var fact struct {
					Physical, Variants int64
					Fact               []any
				}
				if err := json.Unmarshal([]byte(m4CKSQL(t, ck, query)), &fact); err != nil {
					t.Fatal(err)
				}
				a, _ := json.Marshal(fact.Fact)
				b, _ := json.Marshal([]any{h.Action, h.ResponsibleTeamID, analytics.Timestamp(h.OccurredAt), h.After.Status})
				if fact.Physical < 1 || fact.Variants != 1 || string(a) != string(b) {
					t.Fatalf("CK immutable fact mismatch id=%d v=%d SQL=%s CK=%+v", id, h.Version, b, fact)
				}
			}
		}
	}
	if ck != nil {
		var count int64
		if err := f.db.QueryRow("SELECT count(*) FROM ticket_histories").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if got := m4CKSQL(t, ck, "SELECT uniqExact(tuple(organization_id,ticket_id,version)) FROM ticket_events FORMAT TabSeparated"); got != fmt.Sprint(count) {
			t.Fatalf("CK canonical=%s SQL=%d", got, count)
		}
	}
}

func m4AssertGold(t *testing.T, f *m3fixture, j syncpipe.RebuildJob) analytics.Result {
	t.Helper()
	svc := analytics.Service{Auth: f.store, CK: m4CK(t, j)}
	got, err := svc.Query(context.Background(), 5, m4Window)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../../tests/analytics/prd_gold.json")
	if err != nil {
		t.Fatal(err)
	}
	var gold struct {
		Daily    []analytics.Day
		Overview analytics.Overview
	}
	if err := json.Unmarshal(data, &gold); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Daily, gold.Daily) || !reflect.DeepEqual(got.Overview, gold.Overview) {
		t.Fatalf("independent Gold actual=%+v expected=%+v", got, gold)
	}
	var raw map[string]json.RawMessage
	json.Unmarshal(data, &raw)
	var teams map[string]map[string]int64
	json.Unmarshal(raw["teams"], &teams)
	for _, team := range got.Teams {
		if want, ok := teams[fmt.Sprint(team.ID)]; ok && (team.Created != want["created"] || team.FirstResolved != want["first_resolved"] || team.Backlog != want["current_backlog"]) {
			t.Fatalf("Gold team actual=%+v expected=%v", team, want)
		}
	}
	return got
}

func m4Rows(t *testing.T, f *m3fixture, query string, args ...any) []map[string]any {
	t.Helper()
	rows, err := f.db.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	result := []map[string]any{}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		row := map[string]any{}
		for i, column := range columns {
			v := values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			row[column] = v
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func m4Evidence(t *testing.T, f *m3fixture, j syncpipe.RebuildJob, label string, extra any) {
	t.Helper()
	dir := os.Getenv("TICKETPULSE_M4_EVIDENCE_DIR")
	if dir == "" {
		dir = "../../artifacts/m4-01/fault-tests"
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	evidence := map[string]any{"test": t.Name(), "label": label, "at": time.Now().UTC(), "mysql_schema": f.name, "kafka_topic": f.queue.Topic, "job": j, "extra": extra,
		"mysql_tickets": m4Rows(t, f, "SELECT * FROM tickets ORDER BY id"), "mysql_histories": m4Rows(t, f, "SELECT * FROM ticket_histories ORDER BY ticket_id,version"),
		"job_timeline":   m4Rows(t, f, "SELECT * FROM sync_rebuild_log WHERE job_id=? ORDER BY id", j.ID),
		"query_commands": []string{"SELECT * FROM tickets ORDER BY id", "SELECT * FROM ticket_histories ORDER BY ticket_id,version", "GET /" + j.Index + "/_search?size=1000", "SELECT * FROM ticket_states ORDER BY ticket_id,version FORMAT JSONEachRow", "SELECT * FROM ticket_events ORDER BY ticket_id,version FORMAT JSONEachRow"}}
	if j.Target != "ck" {
		m4HTTP(t, http.MethodPost, os.Getenv("TICKETPULSE_ES_URL")+"/"+j.Index+"/_refresh", nil, 200)
		evidence["es_response"] = json.RawMessage(m4HTTP(t, http.MethodGet, os.Getenv("TICKETPULSE_ES_URL")+"/"+j.Index+"/_search?size=1000&sort=id:asc", nil, 200))
	}
	if j.Target != "es" {
		ck := m4CK(t, j)
		evidence["ck_states_json_each_row"] = m4CKSQL(t, ck, "SELECT * FROM ticket_states ORDER BY ticket_id,version FORMAT JSONEachRow")
		evidence["ck_events_json_each_row"] = m4CKSQL(t, ck, "SELECT * FROM ticket_events ORDER BY ticket_id,version FORMAT JSONEachRow")
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, t.Name()+"-"+label+".json")
	if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("RUNTIME M4 evidence=%s isolated=%s job=%s status=%s applied=%d/%d", path, f.name, j.ID, j.Status, j.Applied, j.Total)
}

func TestM4PRDRebuildWithoutLiveConsumersAndTargetRecovery(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	f.startCapture(t)
	// No demo ES/CK consumer starts; the rebuild owns its Kafka tail.
	j := m4Job(t, f, r, "gold", "both")
	j = m4Run(t, r, j.ID)
	m4AssertProjection(t, f, j)
	m4Evidence(t, f, j, "before-destruction", m4AssertGold(t, f, j))
	before := m4Rows(t, f, "SELECT id,version,status,current_team_id,assignee_id FROM tickets ORDER BY id")
	m4HTTP(t, http.MethodDelete, os.Getenv("TICKETPULSE_ES_URL")+"/"+j.Index, nil, 200)
	if _, err := r.Run(context.Background(), j.ID); err == nil || !strings.Contains(err.Error(), "REBUILD_TARGET_CHANGED") {
		t.Fatalf("removed claimed target must fail visibly: %v", err)
	}
	esJob := m4Job(t, f, r, "es_recovered", "es")
	esJob = m4Run(t, r, esJob.ID)
	m4AssertProjection(t, f, esJob)
	m4Evidence(t, f, esJob, "deleted-es-recovered", nil)
	ck := m4CK(t, j)
	for _, table := range []string{"ticket_states", "ticket_events"} {
		m4CKSQL(t, ck, "TRUNCATE TABLE "+table)
		if got := m4CKSQL(t, ck, "SELECT count() FROM "+table+" FORMAT TabSeparated"); got != "0" {
			t.Fatal(got)
		}
	}
	ckJob := m4Job(t, f, r, "ck_recovered", "ck")
	ckJob = m4Run(t, r, ckJob.ID)
	m4AssertProjection(t, f, ckJob)
	m4Evidence(t, f, ckJob, "truncated-ck-recovered", m4AssertGold(t, f, ckJob))
	if after := m4Rows(t, f, "SELECT id,version,status,current_team_id,assignee_id FROM tickets ORDER BY id"); !reflect.DeepEqual(before, after) {
		t.Fatal("rebuild changed authoritative MySQL")
	}
}

func TestM4ConcurrentHTTPVersionsAndDurableBatchResume(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	f.startCapture(t)
	f.startConsumer(t)
	f.startCK(t)
	j := m4Job(t, f, r, "overlap", "both")
	injected := errors.New("M4 real durable batch interruption")
	wrote := false
	r.AfterBatch = func(progress syncpipe.RebuildJob) error {
		if wrote {
			return nil
		}
		wrote = true
		// Newest direct target writes race the remaining old frozen versions,
		// while actual HTTP, CDC and live consumers continue.
		current := f.actionHTTP(t, "local-device-agent", "comments", 4, 10, map[string]any{"content": "M4 during frozen snapshot"})
		current = f.actionHTTP(t, "local-device-agent", "resolve", 4, current.Version, map[string]any{"resolution_note": "M4 overlap new resolution"})
		current = f.actionHTTP(t, "local-alice", "reopen", 4, current.Version, nil)
		var created ticket.CreateResult
		json.Unmarshal(f.call(t, "local-alice", "POST", "/tickets", request("m4-during-rebuild"), 201), &created)
		es, err := search.NewElasticsearch(r.ESURL, j.Index)
		if err != nil {
			return err
		}
		if _, err := es.Put(context.Background(), search.FromTicket(current)); err != nil {
			return err
		}
		fact, state, err := syncpipe.AnalysisProjection(f.event(t, 4, current.Version, "overlap-direct-new"))
		if err != nil {
			return err
		}
		ck := m4CK(t, j)
		if fact != nil {
			if err := ck.InsertFact(context.Background(), *fact); err != nil {
				return err
			}
		}
		if err := ck.InsertCurrent(context.Background(), state); err != nil {
			return err
		}
		f.publish(t, f.event(t, 4, 1, "m4-late-old"))
		f.publish(t, f.event(t, 4, current.Version, "m4-new-duplicate"))
		return injected
	}
	failed, err := r.Run(context.Background(), j.ID)
	if !errors.Is(err, injected) || failed.Applied <= 0 || failed.Applied >= failed.Total || failed.Status != "failed" {
		t.Fatalf("expected persisted batch failure: job=%+v err=%v", failed, err)
	}
	m4Evidence(t, f, failed, "durable-batch-failed", nil)
	latestVersion := int64(13)
	continuedWrites := 0
	r.AfterBatch = func(progress syncpipe.RebuildJob) error {
		if progress.Applied%8 == 0 {
			current := f.actionHTTP(t, "local-device-agent", "comments", 4, latestVersion, map[string]any{"content": fmt.Sprintf("M4 continuing business change at snapshot cursor %d", progress.Applied)})
			latestVersion = current.Version
			continuedWrites++
		}
		return nil
	}
	j = m4Run(t, r, j.ID)
	r.AfterBatch = nil
	if continuedWrites < 3 {
		t.Fatal("continuous writes did not overlap several rebuild batches")
	}
	m4AssertProjection(t, f, j)
	m4Evidence(t, f, j, "resumed-overlap", nil)
	ck := m4CK(t, j)
	before := m4CKSQL(t, ck, "SELECT uniqExact(tuple(ticket_id,version)) FROM ticket_events FORMAT TabSeparated")
	repeated := m4Run(t, r, j.ID)
	m4AssertProjection(t, f, repeated)
	if after := m4CKSQL(t, ck, "SELECT uniqExact(tuple(ticket_id,version)) FROM ticket_events FORMAT TabSeparated"); after != before {
		t.Fatalf("same-job count %s -> %s", before, after)
	}
	if got := m4CKSQL(t, ck, "SELECT argMin(tuple(version,event_team_id),version) FROM ticket_events WHERE ticket_id=4 AND action='resolved' FORMAT TabSeparated"); got != "(4,10)" {
		t.Fatal(got)
	}
	f.projected(t, 4, latestVersion)
	f.latest(t, 4, latestVersion)
	m4Evidence(t, f, repeated, "same-job-repeated", map[string]any{"canonical_events_before": before, "business_writes_across_rebuild_batches": continuedWrites})
}

func TestM4KafkaRetentionLossRebuildFromMySQL(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	f.startCapture(t)
	_, _, err := syncpipe.AnalyticsSnapshot(context.Background(), f.state, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	ends, err := f.queue.Admin.ListEndOffsets(context.Background(), f.queue.Topic)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(f.queue.Topic, "ticketpulse."+f.name+".") {
		t.Fatal("refusing retention fault outside test topic")
	}
	positions := kadm.Offsets{f.queue.Topic: {}}
	for p := int32(0); p < f.queue.Partitions; p++ {
		end, ok := ends.Lookup(f.queue.Topic, p)
		if !ok || end.Err != nil || end.Offset <= 0 {
			t.Fatal("no historical records", p, end)
		}
		positions[f.queue.Topic][p] = kadm.Offset{Topic: f.queue.Topic, Partition: p, At: end.Offset}
	}
	response, err := f.queue.Admin.DeleteRecords(context.Background(), positions)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Error(); err != nil {
		t.Fatal(err)
	}
	starts, err := f.queue.Admin.ListStartOffsets(context.Background(), f.queue.Topic)
	if err != nil {
		t.Fatal(err)
	}
	for p, requested := range positions[f.queue.Topic] {
		start, ok := starts.Lookup(f.queue.Topic, p)
		if !ok || start.Err != nil || start.Offset < requested.At {
			t.Fatalf("retention p%d did not advance: %+v", p, start)
		}
	}
	j := m4Job(t, f, r, "after_retention", "both")
	j = m4Run(t, r, j.ID)
	m4AssertProjection(t, f, j)
	m4Evidence(t, f, j, "expired-kafka-history-restored", map[string]any{"delete_records_requested": positions, "delete_records_response": response, "log_start_after": starts, "gold": m4AssertGold(t, f, j)})
}

func TestM4CLIHardKillAndResume(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	f.startCapture(t)
	j := m4Job(t, f, r, "hard_kill", "both")
	binary := os.Getenv("TICKETPULSE_REBUILD_BINARY")
	if binary == "" {
		binary = filepath.Join(t.TempDir(), "ticketpulse-rebuild")
		build := exec.Command("go", "build", "-o", binary, "./cmd/ticketpulse-rebuild")
		build.Dir = "../.."
		if data, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build CLI: %v %s", err, data)
		}
	}
	// Delay actual CK HTTP to guarantee a hard-kill window after durable
	// progress, without inventing a test-only CLI success path.
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		time.Sleep(35 * time.Millisecond)
		upstream, err := http.NewRequestWithContext(req.Context(), req.Method, r.CKURL+req.URL.RequestURI(), req.Body)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		res, err := http.DefaultClient.Do(upstream)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer res.Body.Close()
		for key, values := range res.Header {
			for _, v := range values {
				w.Header().Add(key, v)
			}
		}
		w.WriteHeader(res.StatusCode)
		io.Copy(w, res.Body)
	}))
	defer proxy.Close()
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName = f.name
	cmd := exec.Command(binary, "-mode=run", "-job="+j.ID)
	cmd.Env = append(os.Environ(), "TICKETPULSE_SYNC_DSN="+cfg.FormatDSN(), "TICKETPULSE_REBUILD_DSN="+cfg.FormatDSN(), "TICKETPULSE_CK_URL="+proxy.URL)
	var cli bytes.Buffer
	cmd.Stdout, cmd.Stderr = &cli, &cli
	started := time.Now().UTC()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	reaped := false
	defer func() {
		if !reaped {
			cmd.Process.Kill()
			<-wait
		}
	}()
	deadline := time.Now().Add(30 * time.Second)
	var interrupted syncpipe.RebuildJob
	for time.Now().Before(deadline) {
		select {
		case err := <-wait:
			reaped = true
			t.Fatalf("CLI exited before kill: %v %s", err, cli.String())
		default:
		}
		status, err := r.Status(context.Background(), j.ID)
		if err == nil && status.Applied > 0 && status.Applied < status.Total {
			interrupted = status
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if interrupted.Applied == 0 {
		t.Fatal("CLI durable progress not observed")
	}
	killed := time.Now().UTC()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err := <-wait
	reaped = true
	if err == nil {
		t.Fatal("hard kill exited successfully")
	}
	j = m4Run(t, r, j.ID)
	m4AssertProjection(t, f, j)
	m4Evidence(t, f, j, "sigkill-resumed", map[string]any{"started_at": started, "sigkill_at": killed, "recovered_at": time.Now().UTC(), "durable_progress_before_sigkill": interrupted, "cli_command": []string{filepath.Base(binary), "-mode=run", "-job=" + j.ID}, "cli_output": cli.String(), "gold": m4AssertGold(t, f, j)})
}

func TestM4ProjectionCommittedBeforeCursorFailure(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	f.startCapture(t)
	j := m4Job(t, f, r, "before_cursor", "both")
	f.exec(t, "CREATE TRIGGER m4_cursor_failure BEFORE UPDATE ON sync_rebuild_jobs FOR EACH ROW BEGIN IF NEW.applied>OLD.applied THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='M4 projection committed before cursor'; END IF; END")
	defer func() { _, _ = f.db.Exec("DROP TRIGGER IF EXISTS m4_cursor_failure") }()
	failed, err := r.Run(context.Background(), j.ID)
	if err == nil || !strings.Contains(err.Error(), "M4 projection committed before cursor") || failed.Applied != 0 || failed.Status != "failed" {
		t.Fatalf("real cursor write fault job=%+v err=%v", failed, err)
	}
	ck := m4CK(t, j)
	if rows := m4CKSQL(t, ck, "SELECT count() FROM ticket_events FORMAT TabSeparated"); rows != "2" {
		t.Fatalf("downstream writes must commit before cursor fault: %s", rows)
	}
	m4Evidence(t, f, failed, "projection-written-cursor-failed", nil)
	f.exec(t, "DROP TRIGGER m4_cursor_failure")
	j = m4Run(t, r, j.ID)
	m4AssertProjection(t, f, j)
	gold := m4AssertGold(t, f, j)
	if physical := m4CKSQL(t, ck, "SELECT count() FROM ticket_events WHERE ticket_id=1 AND version IN (1,2) FORMAT TabSeparated"); physical != "4" {
		t.Fatalf("actual replay physical duplicates missing: %s", physical)
	}
	m4Evidence(t, f, j, "cursor-failure-resumed", gold)
}

func TestM4ReconciliationLagTimeoutAndCorruptProjection(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	f.startCapture(t)
	f.startConsumer(t)
	f.startCK(t)
	for id := int64(1); id <= 10; id++ {
		var version int64
		f.db.QueryRow("SELECT version FROM tickets WHERE id=?", id).Scan(&version)
		f.projected(t, id, version)
		f.latest(t, id, version)
	}
	opts := syncpipe.ReconcileOptions{BatchSize: 2, OrganizationID: 1, MetricsRequest: m4Window, LagTimeout: 10 * time.Second}
	ready, err := syncpipe.Reconcile(context.Background(), f.state, f.queue, f.es, f.ck, opts)
	if err != nil || ready.State != "consistent" {
		t.Fatalf("live ready report=%+v err=%v", ready, err)
	}
	f.stopCapture(t)
	f.actionHTTP(t, "local-device-agent", "comments", 4, 10, map[string]any{"content": "M4 paused CDC reconciliation"})
	opts.LagTimeout = 0
	lag, err := syncpipe.Reconcile(context.Background(), f.state, f.queue, f.es, f.ck, opts)
	if err != nil || lag.State != "catching_up" || lag.DifferenceCount == 0 {
		t.Fatalf("sync lag misclassified report=%+v err=%v", lag, err)
	}
	opts.LagTimeout = 300 * time.Millisecond
	timed, err := syncpipe.Reconcile(context.Background(), f.state, f.queue, f.es, f.ck, opts)
	if err != nil || timed.State != "uncheckable" || !timed.Boundary.DeadlineExpired {
		t.Fatalf("finite delay report=%+v err=%v", timed, err)
	}
	f.startCapture(t)
	f.projected(t, 4, 11)
	f.latest(t, 4, 11)
	j := m4Job(t, f, r, "corruption", "both")
	j = m4Run(t, r, j.ID)
	m4AssertProjection(t, f, j)
	// Corrupt only this job's owned targets. external_gte is deliberately used
	// by the test injector to retain the authoritative version while changing
	// a searchable field, proving version equality alone cannot pass audit.
	detail, err := f.service.Detail(context.Background(), 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := search.FromTicket(detail.Ticket)
	corrupt.Title = "M4 corrupted isolated searchable title"
	payload, _ := json.Marshal(corrupt)
	m4HTTP(t, http.MethodPut, fmt.Sprintf("%s/%s/_doc/1?version=%d&version_type=external_gte", r.ESURL, j.Index, corrupt.Version), payload, 200)
	fact, _, err := syncpipe.AnalysisProjection(f.event(t, 1, 1, "m4-conflicting-fact"))
	if err != nil {
		t.Fatal(err)
	}
	fact.EventTeamID = 20
	ck := m4CK(t, j)
	if err := ck.InsertFact(context.Background(), *fact); err != nil {
		t.Fatal(err)
	}
	opts.LagTimeout = 0
	es, _ := search.NewElasticsearch(r.ESURL, j.Index)
	bad, err := syncpipe.Reconcile(context.Background(), f.state, nil, es, ck, opts)
	if err != nil || bad.State != "inconsistent" || bad.DifferenceCount == 0 || bad.CK.ConflictingKeys == 0 {
		t.Fatalf("confirmed corrupt projection must be inconsistent report=%+v err=%v", bad, err)
	}
	m4Evidence(t, f, j, "four-reconcile-states", map[string]any{"consistent": ready, "catching_up": lag, "uncheckable": timed, "inconsistent": bad})
	repair := m4Job(t, f, r, "corrupt_recovered", "both")
	repair = m4Run(t, r, repair.ID)
	m4AssertProjection(t, f, repair)
	m4Evidence(t, f, repair, "corrupt-targets-new-job-restored", nil)
}

func m4Wait(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("M4 %s not observed within finite deadline", label)
}

func m4SaveStatus(t *testing.T, f *m3fixture, r *syncpipe.Rebuilder, id, label string, extra any) syncpipe.RebuildJob {
	t.Helper()
	j, err := r.Status(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	dir := os.Getenv("TICKETPULSE_M4_EVIDENCE_DIR")
	if dir == "" {
		dir = "../../artifacts/m4-01/fault-tests"
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(map[string]any{"test": t.Name(), "label": label, "at": time.Now().UTC(), "job": j, "extra": extra, "mysql_tickets": m4Rows(t, f, "SELECT * FROM tickets ORDER BY id"), "job_timeline": m4Rows(t, f, "SELECT * FROM sync_rebuild_log WHERE job_id=? ORDER BY id", id)}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, t.Name()+"-"+label+".json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	return j
}

func TestM4FollowLiveWritesCancelResumeAndTopicIdentityChange(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	f.startCapture(t)
	j := m4Job(t, f, r, "follow_live", "both")
	j = m4Run(t, r, j.ID)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Follow(ctx, j.ID) }()
	defer cancel()
	m4Wait(t, "persistent following status", func() bool {
		status, err := r.Status(context.Background(), j.ID)
		return err == nil && status.Status == "following"
	})
	for v := int64(10); v < 13; v++ {
		f.actionHTTP(t, "local-device-agent", "comments", 4, v, map[string]any{"content": fmt.Sprintf("M4 follow real HTTP v%d", v+1)})
	}
	m4Wait(t, "follow caught latest HTTP version", func() bool {
		data := m4HTTP(t, http.MethodGet, fmt.Sprintf("%s/%s/_doc/4", r.ESURL, j.Index), nil, 200)
		var raw map[string]json.RawMessage
		var version int64
		return json.Unmarshal(data, &raw) == nil && json.Unmarshal(raw["_version"], &version) == nil && version == 13
	})
	current, err := r.Status(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	m4AssertProjection(t, f, current)
	cancel()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("follow cancel error=%v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("follow cancellation did not release job")
	}
	interrupted := m4SaveStatus(t, f, r, j.ID, "following-cancelled", nil)
	if interrupted.Status != "interrupted" || interrupted.CompletedAt != nil {
		t.Fatalf("follow cancellation state=%+v", interrupted)
	}
	f.actionHTTP(t, "local-device-agent", "comments", 4, 13, map[string]any{"content": "M4 after stopped follow"})
	j = m4Run(t, r, j.ID)
	m4AssertProjection(t, f, j)
	m4Evidence(t, f, j, "follow-resumed", nil)
	activeCtx, activeCancel := context.WithCancel(context.Background())
	defer activeCancel()
	activeDone := make(chan error, 1)
	go func() { activeDone <- r.Follow(activeCtx, j.ID) }()
	m4Wait(t, "active follow holds lock", func() bool {
		status, err := r.Status(context.Background(), j.ID)
		return err == nil && status.Status == "following" && status.WorkerActive
	})
	// Stop capture before changing only this fixture's topic, preserving the
	// original source checkpoint and all live demonstration resources.
	f.stopCapture(t)
	if !strings.HasPrefix(f.queue.Topic, "ticketpulse."+f.name+".") {
		t.Fatal("unowned Kafka topic")
	}
	if err := f.queue.DeleteTestTopic(context.Background()); err != nil {
		t.Fatal(err)
	}
	m4Wait(t, "old topic absent", func() bool {
		topics, err := f.queue.Admin.ListTopics(context.Background(), f.queue.Topic)
		d, ok := topics[f.queue.Topic]
		return err == nil && (!ok || d.Err != nil)
	})
	// Broker topic creation may precede leader metadata readiness. Bounded
	// retries make the fault injector wait for its actual replacement topic.
	m4Wait(t, "replacement topic leader ready", func() bool { return f.queue.Ensure(context.Background()) == nil })
	changed, err := f.queue.Admin.ListTopics(context.Background(), f.queue.Topic)
	if err != nil {
		t.Fatal(err)
	}
	if changed[f.queue.Topic].ID.String() == j.TopicID {
		t.Fatal("topic identity was not actually changed")
	}
	select {
	case err = <-activeDone:
		if err == nil || !strings.Contains(err.Error(), "REBUILD_KAFKA_CHANGED") {
			t.Fatalf("active Follow missed Topic identity change: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("active Follow failed to detect replaced topic")
	}
	activeFailed := m4SaveStatus(t, f, r, j.ID, "active-follow-topic-replaced", map[string]any{"new_topic_id": changed[f.queue.Topic].ID.String(), "error": err.Error()})
	if activeFailed.Status == "completed" || activeFailed.CompletedAt != nil || activeFailed.WorkerActive {
		t.Fatalf("active Follow invalid final state: %+v", activeFailed)
	}
	followCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	err = r.Follow(followCtx, j.ID)
	if err == nil || !strings.Contains(err.Error(), "REBUILD_KAFKA_CHANGED") {
		t.Fatalf("recreated same-name topic must fail: %v", err)
	}
	failed := m4SaveStatus(t, f, r, j.ID, "recreated-topic-rejected", map[string]any{"new_topic_id": changed[f.queue.Topic].ID.String(), "error": err.Error()})
	if failed.Status == "completed" || failed.CompletedAt != nil {
		t.Fatalf("invalid topic left successful status: %+v", failed)
	}
}

func TestM4CheckTimeoutAndRemovedTargetInvalidateCompletion(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	f.startCapture(t)
	j := m4Job(t, f, r, "check_state", "both")
	j = m4Run(t, r, j.ID)
	f.stopCapture(t)
	f.actionHTTP(t, "local-device-agent", "comments", 4, 10, map[string]any{"content": "M4 Check must invalidate stale complete status"})
	r.CatchupTimeout = 300 * time.Millisecond
	report, err := r.Check(context.Background(), j.ID)
	if err == nil || report.State != "uncheckable" {
		t.Fatalf("stopped CDC must make finite Check fail report=%+v err=%v", report, err)
	}
	failed := m4SaveStatus(t, f, r, j.ID, "check-source-timeout", map[string]any{"report": report, "error": err.Error()})
	if failed.Status == "completed" || failed.CompletedAt != nil {
		t.Fatalf("timeout still reports successful completion %+v", failed)
	}
	r.CatchupTimeout = 30 * time.Second
	f.startCapture(t)
	j = m4Run(t, r, j.ID)
	m4AssertProjection(t, f, j)
	m4HTTP(t, http.MethodDelete, r.ESURL+"/"+j.Index, nil, 200)
	_, err = r.Check(context.Background(), j.ID)
	if err == nil || !strings.Contains(err.Error(), "REBUILD_TARGET_CHANGED") {
		t.Fatalf("deleted target Check must fail %v", err)
	}
	failed = m4SaveStatus(t, f, r, j.ID, "check-missing-target", map[string]any{"error": err.Error()})
	if failed.Status == "completed" || failed.CompletedAt != nil {
		t.Fatalf("missing target retained successful completion %+v", failed)
	}
}

func TestM4FrozenKafkaOverlapExpiryFailsThenNewJobRecovers(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	f.startCapture(t)
	j := m4Job(t, f, r, "expired_overlap", "both")
	interrupt := errors.New("M4 frozen overlap pause")
	r.AfterBatch = func(progress syncpipe.RebuildJob) error { return interrupt }
	frozen, err := r.Run(context.Background(), j.ID)
	if !errors.Is(err, interrupt) || frozen.Applied == 0 || frozen.Applied >= frozen.Total {
		t.Fatalf("could not freeze partial task %+v %v", frozen, err)
	}
	r.AfterBatch = nil
	f.actionHTTP(t, "local-device-agent", "comments", 4, 10, map[string]any{"content": "M4 overlap to be expired"})
	f.publish(t, f.event(t, 4, 11, "M4-expired-overlap-real"))
	ends, err := f.queue.Admin.ListEndOffsets(context.Background(), f.queue.Topic)
	if err != nil {
		t.Fatal(err)
	}
	requested := kadm.Offsets{f.queue.Topic: {}}
	actuallyExpires := false
	for p := int32(0); p < f.queue.Partitions; p++ {
		end, ok := ends.Lookup(f.queue.Topic, p)
		if !ok || end.Err != nil {
			t.Fatal(end)
		}
		requested[f.queue.Topic][p] = kadm.Offset{Topic: f.queue.Topic, Partition: p, At: end.Offset}
		if end.Offset > frozen.NextOffsets[p] {
			actuallyExpires = true
		}
	}
	if !actuallyExpires {
		t.Fatal("no frozen overlap actually available to expire")
	}
	response, err := f.queue.Admin.DeleteRecords(context.Background(), requested)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Error(); err != nil {
		t.Fatal(err)
	}
	failed, err := r.Run(context.Background(), j.ID)
	if err == nil || !strings.Contains(err.Error(), "REBUILD_KAFKA_OFFSET_EXPIRED") {
		t.Fatalf("expired frozen offsets silently resumed %+v %v", failed, err)
	}
	m4Evidence(t, f, failed, "frozen-overlap-expired", map[string]any{"deleted_offsets": requested, "error": err.Error()})
	repair := m4Job(t, f, r, "expired_recovered", "both")
	repair = m4Run(t, r, repair.ID)
	m4AssertProjection(t, f, repair)
	m4Evidence(t, f, repair, "new-freeze-after-expiry", m4AssertGold(t, f, repair))
}

func TestM4MetricsOrganizationDoesNotHideOtherOrganizations(t *testing.T) {
	f, r := newM4(t)
	m4LoadGold(t, f)
	request := request("M4-other-organization")
	other, err := f.store.Create(context.Background(), 9, request, [32]byte{4, 2}, []int64{200})
	if err != nil {
		t.Fatal(err)
	}
	f.startCapture(t)
	j := m4Job(t, f, r, "all_organizations", "both")
	j = m4Run(t, r, j.ID)
	if j.Report.Source.Tickets != 11 || j.Report.ES.CurrentCount != 11 || j.Report.CK.CurrentCount != 11 || j.Report.Source.Histories != 47 {
		t.Fatalf("metrics org1 hid org2 source rows %+v", j.Report)
	}
	// Deliberately erase only org2 from the owned candidate. Organization 1
	// still matches the PRD statistics, but the full-source audit must fail.
	m4HTTP(t, http.MethodDelete, fmt.Sprintf("%s/%s/_doc/%d", r.ESURL, j.Index, other.Ticket.ID), nil, 200)
	ck := m4CK(t, j)
	m4CKSQL(t, ck, "ALTER TABLE ticket_states DELETE WHERE organization_id=2 SETTINGS mutations_sync=2")
	m4CKSQL(t, ck, "ALTER TABLE ticket_events DELETE WHERE organization_id=2 SETTINGS mutations_sync=2")
	es, _ := search.NewElasticsearch(r.ESURL, j.Index)
	bad, err := syncpipe.Reconcile(context.Background(), f.state, nil, es, ck, syncpipe.ReconcileOptions{BatchSize: 2, MetricsOrganizationID: 1, MetricsRequest: m4Window})
	if err != nil || bad.State != "inconsistent" || bad.Source.Tickets != 11 || bad.Source.Histories != 47 {
		t.Fatalf("org1 metrics bypassed whole source audit %+v %v", bad, err)
	}
	detected := false
	for _, difference := range bad.Differences {
		if difference.OrganizationID == 2 && difference.TicketID == other.Ticket.ID {
			detected = true
		}
	}
	if !detected {
		t.Fatalf("missing org2 records omitted from differences %+v", bad)
	}
	m4Evidence(t, f, j, "other-organization-loss-detected", bad)
	repair := m4Job(t, f, r, "other_org_recovered", "both")
	repair = m4Run(t, r, repair.ID)
	m4HTTP(t, http.MethodGet, fmt.Sprintf("%s/%s/_doc/%d", r.ESURL, repair.Index, other.Ticket.ID), nil, 200)
	if count := m4CKSQL(t, m4CK(t, repair), "SELECT uniqExact(ticket_id) FROM ticket_states WHERE organization_id=2 FORMAT TabSeparated"); count != "1" {
		t.Fatal(count)
	}
	m4Evidence(t, f, repair, "both-organizations-recovered", nil)
}
