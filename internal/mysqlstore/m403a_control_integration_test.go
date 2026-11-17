//go:build integration && m2 && m3 && m4 && m403a

package mysqlstore_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/benchseed"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

func m403aOwned(t *testing.T) {
	t.Helper()
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c3" {
		for key, want := range map[string]string{"TICKETPULSE_ES_URL": "http://127.0.0.1:19209", "TICKETPULSE_CK_URL": "http://127.0.0.1:18132", "TICKETPULSE_KAFKA_BROKERS": "127.0.0.1:19108", "TICKETPULSE_CDC_ADDR": "127.0.0.1:33315"} {
			if os.Getenv(key) != want {
				t.Fatal("C3 foundation endpoint mismatch")
			}
		}
		return
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c2" {
		for key, want := range map[string]string{"TICKETPULSE_ES_URL": "http://127.0.0.1:19208", "TICKETPULSE_CK_URL": "http://127.0.0.1:18131", "TICKETPULSE_KAFKA_BROKERS": "127.0.0.1:19106", "TICKETPULSE_CDC_ADDR": "127.0.0.1:33314"} {
			if os.Getenv(key) != want {
				t.Fatal("C2 foundation endpoint mismatch")
			}
		}
		return
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c1" {
		for key, want := range map[string]string{"TICKETPULSE_ES_URL": "http://127.0.0.1:19207", "TICKETPULSE_CK_URL": "http://127.0.0.1:18130", "TICKETPULSE_KAFKA_BROKERS": "127.0.0.1:19104", "TICKETPULSE_CDC_ADDR": "127.0.0.1:33313"} {
			if os.Getenv(key) != want {
				t.Fatal("C1 foundation endpoint mismatch")
			}
		}
		return
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03b" {
		for key, want := range map[string]string{"TICKETPULSE_ES_URL": "http://127.0.0.1:19206", "TICKETPULSE_CK_URL": "http://127.0.0.1:18129", "TICKETPULSE_KAFKA_BROKERS": "127.0.0.1:19102", "TICKETPULSE_CDC_ADDR": "127.0.0.1:33312"} {
			if os.Getenv(key) != want {
				t.Fatal("M4-03B foundation regression endpoint mismatch")
			}
		}
		return
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03a" {
		t.Fatal("M4-03A requires the explicitly owned suite environment")
	}
	for key, want := range map[string]string{"TICKETPULSE_ES_URL": "http://127.0.0.1:19205", "TICKETPULSE_CK_URL": "http://127.0.0.1:18128", "TICKETPULSE_KAFKA_BROKERS": "127.0.0.1:19100", "TICKETPULSE_CDC_ADDR": "127.0.0.1:33311"} {
		if os.Getenv(key) != want {
			t.Fatalf("%s is not M4-03A's independent endpoint", key)
		}
	}
}
func m403aSave(t *testing.T, name string, value any) {
	t.Helper()
	dir := os.Getenv("TICKETPULSE_M403A_EVIDENCE_DIR")
	if dir == "" {
		t.Fatal("M4-03A evidence directory is required")
	}
	if err := os.MkdirAll(filepath.Join(dir, t.Name()), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, t.Name(), name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	out := json.NewEncoder(file)
	out.SetIndent("", "  ")
	if err = out.Encode(value); err != nil {
		t.Fatal(err)
	}
}
func m403aInstall(t *testing.T, f *fixture) {
	t.Helper()
	if err := projection.EnsureSchema(context.Background(), f.db); err != nil {
		t.Fatal(err)
	}
	f.store.RequireBusinessGate()
}
func m403aGate(t *testing.T, f *fixture) projection.Gate {
	t.Helper()
	g, err := projection.ReadGate(context.Background(), f.db)
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func m403aFreeze(t *testing.T, f *fixture) projection.Gate {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g, err := projection.Freeze(ctx, f.db, "M4-03A independent test")
	if err != nil || g.State != "FROZEN" || g.FreezeID == nil {
		t.Fatalf("freeze=%+v err=%v", g, err)
	}
	return g
}
func m403aWait(t *testing.T, label string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(label + " deadline")
}
func m403aDSN(t *testing.T, name string) string {
	t.Helper()
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = name
	return cfg.FormatDSN()
}

func TestM403ALegacyGateAndFrozenBusinessCoverage(t *testing.T) {
	m403aOwned(t)
	f := newFixture(t)
	first := f.create(t, 1, request("m403a-legacy"))
	if _, err := projection.ReadGate(context.Background(), f.db); !projection.MissingTable(err) {
		t.Fatalf("legacy schema unexpectedly migrated: %v", err)
	}
	required := mysqlstore.New(f.db)
	required.RequireBusinessGate()
	_, err := required.Comment(context.Background(), 1, ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: first.Ticket.ID, ExpectedVersion: 1}, Content: "required gate"})
	if err == nil {
		t.Fatal("required gate silently fell back")
	}
	m403aInstall(t, f)
	gate := m403aFreeze(t, f)
	// The bulk fixture writer intentionally refuses a managed source, even an
	// empty one, instead of attempting to fence its allocator DDL as business work.
	bulkName := "ticketpulse_test_m5_" + strings.TrimPrefix(f.name, "ticketpulse_test_")
	f.exec(t, "CREATE DATABASE "+bulkName)
	t.Cleanup(func() { f.exec(t, "DROP DATABASE "+bulkName) })
	bulkDB, err := mysqlstore.Open(context.Background(), m403aDSN(t, bulkName))
	if err != nil {
		t.Fatal(err)
	}
	if err = projection.EnsureSchema(context.Background(), bulkDB); err != nil {
		t.Fatal(err)
	}
	_, err = benchseed.Generate(context.Background(), bulkDB, benchseed.Options{Tickets: 1, Seed: 20261109, BatchSize: 1, Schema: bulkName, ExpectedUUID: os.Getenv("TICKETPULSE_TEST_SERVER_UUID")})
	bulkDB.Close()
	if err == nil || !strings.Contains(err.Error(), "bulk fixture import requires an unmanaged") {
		t.Fatalf("bulk managed source was not refused: %v", err)
	}
	before := map[string]int64{}
	for _, table := range []string{"tickets", "ticket_histories", "ticket_comments", "ticket_create_requests"} {
		var n int64
		if err := f.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		before[table] = n
	}
	action := ticket.ActionRequest{TicketID: first.Ticket.ID, ExpectedVersion: 1}
	operations := map[string]func() error{
		"create":        func() error { _, e := f.service.Create(context.Background(), 1, request("m403a-frozen")); return e },
		"create_replay": func() error { _, e := f.service.Create(context.Background(), 1, request("m403a-legacy")); return e },
		"claim":         func() error { _, e := f.store.Claim(context.Background(), 3, action); return e },
		"start":         func() error { _, e := f.store.Start(context.Background(), 3, action); return e },
		"resolve": func() error {
			_, e := f.store.Resolve(context.Background(), 3, ticket.ResolveRequest{ActionRequest: action, ResolutionNote: "synthetic"})
			return e
		},
		"close":  func() error { _, e := f.store.Close(context.Background(), 1, action); return e },
		"reopen": func() error { _, e := f.store.Reopen(context.Background(), 1, action); return e },
		"transfer": func() error {
			_, e := f.store.Transfer(context.Background(), 5, ticket.TransferRequest{ActionRequest: action, AssigneeID: 3})
			return e
		},
		"transfer_team": func() error {
			_, e := f.store.TransferTeam(context.Background(), 5, ticket.TeamTransferRequest{ActionRequest: action, TeamID: 20, Reason: "synthetic"})
			return e
		},
		"management_reopen": func() error {
			_, e := f.store.ManagementReopen(context.Background(), 5, ticket.ManagementReopenRequest{ActionRequest: action, TeamID: 20, Reason: "synthetic"})
			return e
		},
		"manual_close": func() error {
			_, e := f.store.ManualClose(context.Background(), 5, ticket.ReasonRequest{ActionRequest: action, Reason: "synthetic"})
			return e
		},
		"comment": func() error {
			_, e := f.store.Comment(context.Background(), 1, ticket.CommentRequest{ActionRequest: action, Content: "synthetic"})
			return e
		},
	}
	refused := []string{}
	for name, operation := range operations {
		if err := operation(); !errors.Is(err, ticket.ErrWritesFrozen) {
			t.Fatalf("%s=%v", name, err)
		}
		refused = append(refused, name)
	}
	after := map[string]int64{}
	for table := range before {
		var n int64
		if err := f.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		after[table] = n
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("frozen writes changed business/history/comment rows")
	}
	server := httptest.NewServer(httpapi.New(f.service, f.store, nil))
	defer server.Close()
	httpCall := func(method, path string, body any, want int) []byte {
		data, _ := json.Marshal(body)
		req, _ := http.NewRequest(method, server.URL+path, bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer local-alice")
		req.Header.Set("Content-Type", "application/json")
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		out, _ := io.ReadAll(res.Body)
		if res.StatusCode != want {
			t.Fatalf("%s=%d body=%s", path, res.StatusCode, out)
		}
		return out
	}
	frozenBody := httpCall("POST", "/api/v1/tickets", request("m403a-http-frozen"), 503)
	if !bytes.Contains(frozenBody, []byte("BUSINESS_WRITES_FROZEN")) {
		t.Fatal("frozen HTTP error not recognizable")
	}
	httpCall("GET", fmt.Sprintf("/api/v1/tickets/%d", first.Ticket.ID), nil, 200)
	httpCall("GET", "/api/v1/tickets?view=created_by_me", nil, 200)
	httpCall("GET", "/healthz", nil, 200)
	httpCall("GET", "/readyz", nil, 200)
	// Authentication/session rows do not change projection business data.
	browser := newBrowserClient(server)
	browser.login(t, "alice")
	logout, logoutBody := browser.send(t, "POST", "/api/v1/auth/logout", nil, server.URL, browser.csrf)
	if logout.StatusCode != 200 {
		t.Fatalf("maintenance logout=%d %s", logout.StatusCode, logoutBody)
	}
	if _, err := projection.Resume(context.Background(), f.db, "wrong-id"); err == nil {
		t.Fatal("stale resume ID accepted")
	}
	if m403aGate(t, f).State != "FROZEN" {
		t.Fatal("invalid resume opened gate")
	}
	if err := projection.EnsureSchema(context.Background(), f.db); err != nil {
		t.Fatal(err)
	}
	if g := m403aGate(t, f); g.FreezeID == nil || *g.FreezeID != *gate.FreezeID {
		t.Fatal("rerunnable migration cleared freeze")
	}
	if _, err := projection.Resume(context.Background(), f.db, *gate.FreezeID); err != nil {
		t.Fatal(err)
	}
	updated, err := f.service.Comment(context.Background(), 1, ticket.CommentRequest{ActionRequest: action, Content: "after resume"})
	if err != nil || updated.Ticket.Version != 2 {
		t.Fatalf("resumed comment=%+v err=%v", updated, err)
	}
	detail, err := f.service.Detail(context.Background(), 1, first.Ticket.ID)
	if err != nil || len(detail.History) != 2 || len(detail.Comments) != 1 {
		t.Fatalf("restored data=%+v err=%v", detail, err)
	}
	// Once this Store has seen a gate, metadata loss is conservatively rejected.
	f.exec(t, "DROP TABLE sync_business_write_gate")
	if _, err = f.service.Create(context.Background(), 1, request("m403a-lost-gate")); err == nil {
		t.Fatal("metadata loss downgraded into legacy mode")
	}
	m403aSave(t, "coverage.json", map[string]any{"gate": gate, "refused": refused, "before": before, "after": after, "resumed_version": detail.Ticket.Version, "histories": len(detail.History), "comments": len(detail.Comments), "http_frozen": json.RawMessage(frozenBody), "metadata_loss_rejected": true})
}

func TestM403AFreezeDrainsActualTransactionAndConcurrentCreates(t *testing.T) {
	m403aOwned(t)
	f := newFixture(t)
	m403aInstall(t, f)
	first := f.create(t, 1, request("m403a-drain"))
	blocker, err := f.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err = blocker.Exec("UPDATE tickets SET title=title WHERE id=?", first.Ticket.ID); err != nil {
		t.Fatal(err)
	}
	writer := make(chan error, 1)
	go func() {
		_, err := f.service.Comment(context.Background(), 1, ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: first.Ticket.ID, ExpectedVersion: 1}, Content: "in-flight actual comment"})
		writer <- err
	}()
	granted := func() bool {
		var n int
		_ = f.db.QueryRow("SELECT count(*) FROM performance_schema.data_locks WHERE OBJECT_SCHEMA=? AND OBJECT_NAME='sync_business_write_gate' AND LOCK_TYPE='RECORD' AND LOCK_MODE LIKE 'S%' AND LOCK_STATUS='GRANTED'", f.name).Scan(&n)
		return n > 0
	}
	m403aWait(t, "writer holding shared gate", granted)
	start := time.Now().UTC()
	type freezeResult struct {
		gate projection.Gate
		err  error
		at   time.Time
	}
	frozen := make(chan freezeResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		g, e := projection.Freeze(ctx, f.db, "drain race")
		frozen <- freezeResult{g, e, time.Now().UTC()}
	}()
	m403aWait(t, "freeze exclusive lock waiting", func() bool {
		var n int
		_ = f.db.QueryRow("SELECT count(*) FROM performance_schema.data_locks WHERE OBJECT_SCHEMA=? AND OBJECT_NAME='sync_business_write_gate' AND LOCK_STATUS='WAITING'", f.name).Scan(&n)
		return n > 0
	})
	select {
	case r := <-frozen:
		t.Fatalf("freeze confirmed before actual business transaction drained: %+v", r)
	default:
	}
	lockRows, err := f.db.Query("SELECT OBJECT_NAME,LOCK_MODE,LOCK_STATUS FROM performance_schema.data_locks WHERE OBJECT_SCHEMA=? AND OBJECT_NAME IN ('tickets','sync_business_write_gate')", f.name)
	if err != nil {
		t.Fatal(err)
	}
	locks := []map[string]string{}
	for lockRows.Next() {
		var name, mode, status string
		if err = lockRows.Scan(&name, &mode, &status); err != nil {
			t.Fatal(err)
		}
		locks = append(locks, map[string]string{"table": name, "mode": mode, "status": status})
	}
	lockRows.Close()
	var wg sync.WaitGroup
	results := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := f.service.Create(context.Background(), 1, request(fmt.Sprintf("m403a-race-%d", i)))
			results <- e
		}(i)
	}
	releaseAt := time.Now().UTC()
	if err = blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-writer; err != nil {
		t.Fatal(err)
	}
	r := <-frozen
	if r.err != nil || r.gate.State != "FROZEN" {
		t.Fatalf("freeze result=%+v", r)
	}
	wg.Wait()
	close(results)
	created, rejected := 0, 0
	for err := range results {
		if err == nil {
			created++
		} else if errors.Is(err, ticket.ErrWritesFrozen) {
			rejected++
		} else {
			t.Fatal(err)
		}
	}
	var late int
	if err = f.db.QueryRow("SELECT count(*) FROM ticket_histories WHERE occurred_at>?", *r.gate.FrozenAt).Scan(&late); err != nil {
		t.Fatal(err)
	}
	if late != 0 {
		t.Fatal("business history committed beyond freeze boundary")
	}
	counts := map[string]int64{}
	for _, table := range []string{"tickets", "ticket_histories", "ticket_comments"} {
		var n int64
		if err = f.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		counts[table] = n
	}
	for i := 0; i < 20; i++ {
		if _, err = f.service.Create(context.Background(), 1, request(fmt.Sprintf("m403a-after-ack-%d", i))); !errors.Is(err, ticket.ErrWritesFrozen) {
			t.Fatalf("write after freeze confirmation=%v", err)
		}
	}
	if _, err = projection.Resume(context.Background(), f.db, *r.gate.FreezeID); err != nil {
		t.Fatal(err)
	}
	last := f.create(t, 1, request("m403a-after-resume"))
	if last.Ticket.ID <= first.Ticket.ID {
		t.Fatal("resume lost allocation/history")
	}
	m403aSave(t, "drain-race.json", map[string]any{"schema": f.name, "lock_observation": locks, "freeze_started_at": start, "blocker_released_at": releaseAt, "freeze_confirmed_at": r.at, "gate": r.gate, "inflight_comment_committed": true, "concurrent_creates": created, "concurrent_rejections": rejected, "counts_at_freeze": counts, "post_confirmation_rejections": 20, "late_histories": late, "resumed_ticket_id": last.Ticket.ID})
}

func m403aControl(t *testing.T, f *fixture, args ...string) []byte {
	t.Helper()
	cmd := exec.Command(os.Getenv("TICKETPULSE_CONTROL_BINARY"), args...)
	cmd.Env = append(os.Environ(), "TICKETPULSE_CONTROL_DSN="+m403aDSN(t, f.name))
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("operator command %v err=%v output=%s", args, err, data)
	}
	return data
}

func TestM403AFreezeSurvivesCLIKillApplicationKillAndResume(t *testing.T) {
	m403aOwned(t)
	f := newFixture(t)
	m403aInstall(t, f)
	first := f.create(t, 1, request("m403a-crash"))
	// Kill an operator that has confirmed a real freeze but cannot finish
	// encoding it: a full stdout pipe forces it to stay alive after commit.
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	fillDone := make(chan struct{})
	go func() { writeEnd.Write(bytes.Repeat([]byte("x"), 2<<20)); close(fillDone) }()
	defer readEnd.Close()
	defer writeEnd.Close()
	// The filler occupies pipe capacity before the operator writes its result.
	time.Sleep(30 * time.Millisecond)
	operator := exec.Command(os.Getenv("TICKETPULSE_CONTROL_BINARY"), "-mode=freeze", "-reason=CLI crash test")
	operator.Env = append(os.Environ(), "TICKETPULSE_CONTROL_DSN="+m403aDSN(t, f.name))
	operator.Stdout = writeEnd
	operator.Stderr = io.Discard
	if err = operator.Start(); err != nil {
		t.Fatal(err)
	}
	defer operator.Process.Kill()
	m403aWait(t, "durable freeze before CLI SIGKILL", func() bool { return m403aGate(t, f).State == "FROZEN" })
	before := m403aGate(t, f)
	if err = operator.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = operator.Wait(); err == nil {
		t.Fatal("operator was not killed")
	}
	readEnd.Close()
	writeEnd.Close()
	<-fillDone
	afterCLI := m403aGate(t, f)
	if afterCLI.FreezeID == nil || *afterCLI.FreezeID != *before.FreezeID {
		t.Fatal("CLI crash cleared freeze")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	logFile, err := os.CreateTemp("", "ticketpulse-m403a-app-log-*")
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	launch := func() *exec.Cmd {
		cmd := exec.Command(os.Getenv("TICKETPULSE_API_BINARY"), "-listen="+address, "-routing=../../configs/local.json")
		env := []string{}
		for _, item := range os.Environ() {
			if !strings.HasPrefix(item, "TICKETPULSE_") {
				env = append(env, item)
			}
		}
		cmd.Env = append(env, "TICKETPULSE_DSN="+m403aDSN(t, f.name), "TICKETPULSE_REQUIRE_WRITE_GATE=true")
		cmd.Stdout, cmd.Stderr = logFile, logFile
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		m403aWait(t, "owned application ready", func() bool {
			res, e := http.Get("http://" + address + "/readyz")
			if e != nil {
				return false
			}
			res.Body.Close()
			return res.StatusCode == 200
		})
		return cmd
	}
	app := launch()
	defer app.Process.Kill()
	detailRequest, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/api/v1/tickets/%d", address, first.Ticket.ID), nil)
	detailRequest.Header.Set("Authorization", "Bearer local-alice")
	res, err := http.DefaultClient.Do(detailRequest)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("frozen real process detail=%d", res.StatusCode)
	}
	createData, _ := json.Marshal(request("m403a-child-frozen"))
	createRequest, _ := http.NewRequest("POST", "http://"+address+"/api/v1/tickets", bytes.NewReader(createData))
	createRequest.Header.Set("Authorization", "Bearer local-alice")
	createRequest.Header.Set("Content-Type", "application/json")
	res, err = http.DefaultClient.Do(createRequest)
	if err != nil {
		t.Fatal(err)
	}
	createResponse, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 503 || !bytes.Contains(createResponse, []byte("BUSINESS_WRITES_FROZEN")) {
		t.Fatalf("frozen real process write=%d %s", res.StatusCode, createResponse)
	}
	if err = app.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	app.Wait()
	afterApp := m403aGate(t, f)
	if afterApp.FreezeID == nil || *afterApp.FreezeID != *before.FreezeID {
		t.Fatal("application SIGKILL cleared freeze")
	}
	app = launch()
	defer app.Process.Kill()
	data := m403aControl(t, f, "-mode=resume", "-freeze-id="+*before.FreezeID)
	var resumed projection.Gate
	if err = json.Unmarshal(data, &resumed); err != nil {
		t.Fatal(err)
	}
	if resumed.State != "OPEN" {
		t.Fatal("explicit resume did not confirm OPEN")
	}
	last := f.create(t, 1, request("m403a-crash-resume"))
	if last.Ticket.Version != 1 {
		t.Fatal("resumed data invalid")
	}
	app.Process.Signal(os.Interrupt)
	app.Wait()
	if err = logFile.Sync(); err != nil {
		t.Fatal(err)
	}
	appLog, err := os.ReadFile(logFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	m403aSave(t, "process-crash.json", map[string]any{"operator_pid": operator.Process.Pid, "signal": "SIGKILL", "before": before, "after_cli": afterCLI, "after_app": afterApp, "resumed": resumed, "tickets_after_resume": 2, "original_ticket_id": first.Ticket.ID, "resumed_ticket_id": last.Ticket.ID, "application_log": string(appLog)})
}

func m403aControlFor(f *m3fixture) *syncpipe.ProjectionControl {
	return &syncpipe.ProjectionControl{Store: f.state, Queue: f.queue, ES: f.es, CK: f.ck}
}

func TestM403AManagedExistingWorkersResumeAndLateIdentityLoss(t *testing.T) {
	m403aOwned(t)
	f, _ := newM4(t)
	m403aInstall(t, f.fixture)
	first := f.create(t, 1, request("m403a-managed-workers"))
	f.startCapture(t)
	f.startConsumer(t)
	f.startCK(t)
	f.projected(t, first.Ticket.ID, 1)
	f.latest(t, first.Ticket.ID, 1)
	gate := m403aFreeze(t, f.fixture)
	control := m403aControlFor(f)
	var version projection.Version
	f.await(t, "existing managed enrollment", func() bool {
		var err error
		version, err = control.EnrollExisting(context.Background(), "managed_workers")
		return err == nil
	})
	f.stopCapture(t)
	f.stopConsumer(t)
	f.stopCK(t)
	managed, err := control.Managed(context.Background(), version.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The independently configured Binlog endpoint must be reachable for an
	// actual UUID query; metadata from the status DSN is not a substitute.
	var beforeCapture syncpipe.Checkpoint
	beforeCapture, err = f.state.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wrongCapture := &syncpipe.Capture{Store: f.state, Queue: f.queue, Config: syncpipe.CaptureConfig{Address: "127.0.0.1:1", User: os.Getenv("TICKETPULSE_CDC_USER"), Password: os.Getenv("TICKETPULSE_CDC_PASSWORD"), ServerID: 4031009}, Check: managed.Check}
	if err = wrongCapture.VerifySource(context.Background(), beforeCapture); !errors.Is(err, projection.ErrTargetMismatch) {
		t.Fatalf("unavailable Binlog endpoint=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	esDone, ckDone, cdcDone := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() {
		esDone <- (&syncpipe.Consumer{Store: f.state, Queue: f.queue, ES: f.es, Check: managed.Check}).Run(ctx)
	}()
	go func() {
		ckDone <- (&syncpipe.CKConsumer{Store: f.state, Queue: f.queue, CK: f.ck, Check: managed.Check}).Run(ctx)
	}()
	go func() {
		cdcDone <- (&syncpipe.Capture{Store: f.state, Queue: f.queue, Config: syncpipe.CaptureConfig{Address: os.Getenv("TICKETPULSE_CDC_ADDR"), User: os.Getenv("TICKETPULSE_CDC_USER"), Password: os.Getenv("TICKETPULSE_CDC_PASSWORD"), ServerID: 4031001}, Check: managed.Check}).Run(ctx)
	}()
	checkExited := func() {
		for name, done := range map[string]chan error{"es": esDone, "ck": ckDone, "cdc": cdcDone} {
			select {
			case err := <-done:
				t.Fatalf("managed %s exited unexpectedly: %v", name, err)
			default:
			}
		}
	}
	if _, err = projection.Resume(context.Background(), f.db, *gate.FreezeID); err != nil {
		t.Fatal(err)
	}
	updated, err := f.service.Comment(context.Background(), 1, ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: first.Ticket.ID, ExpectedVersion: 1}, Content: "managed resumed write"})
	if err != nil {
		t.Fatal(err)
	}
	m403aWait(t, "managed consumers apply current version", func() bool {
		checkExited()
		var e, c int
		_ = f.db.QueryRow("SELECT count(*) FROM sync_kafka_es_deliveries WHERE ticket_id=? AND ticket_version=2 AND status='applied'", first.Ticket.ID).Scan(&e)
		_ = f.db.QueryRow("SELECT count(*) FROM sync_kafka_ck_deliveries WHERE ticket_id=? AND ticket_version=2 AND status='applied'", first.Ticket.ID).Scan(&c)
		return e > 0 && c > 0
	})
	gate = m403aFreeze(t, f.fixture)
	if err = f.state.IssueWatermark(context.Background()); err != nil {
		t.Fatal(err)
	}
	var issued time.Time
	if err = f.db.QueryRow("SELECT issued_at FROM sync_source_watermark WHERE source_name='tickets-v1'").Scan(&issued); err != nil {
		t.Fatal(err)
	}
	m403aWait(t, "managed frozen progress", func() bool {
		checkExited()
		var n int
		_ = f.db.QueryRow("SELECT count(*) FROM sync_kafka_watermarks WHERE component IN ('es','ck') AND issued_at>=?", issued).Scan(&n)
		return n == 6
	})
	report, err := syncpipe.Reconcile(context.Background(), f.state, nil, f.es, f.ck, syncpipe.ReconcileOptions{BatchSize: 100})
	if err != nil || report.State != "consistent" {
		t.Fatalf("managed data=%s err=%v", report.State, err)
	}
	before := map[string][]syncpipe.PartitionProgress{}
	for _, component := range []string{"es", "ck"} {
		p, e := f.queue.Progress(context.Background(), component)
		if e != nil {
			t.Fatal(e)
		}
		before[component] = p
	}
	changedAt := time.Now().UTC()
	f.ckSQL(t, "RENAME TABLE ticket_events TO m403a_original_events")
	if err = f.ck.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	newIdentity, _, err := f.ck.Identity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	discovered := map[string]time.Time{}
	for name, done := range map[string]chan error{"es": esDone, "ck": ckDone, "cdc": cdcDone} {
		select {
		case err := <-done:
			if !errors.Is(err, projection.ErrTargetMismatch) {
				t.Fatalf("managed %s late identity error=%v", name, err)
			}
			discovered[name] = time.Now().UTC()
		case <-time.After(10 * time.Second):
			t.Fatalf("managed %s failed to discover UUID change", name)
		}
	}
	after := map[string][]syncpipe.PartitionProgress{}
	for _, component := range []string{"es", "ck"} {
		p, e := f.queue.Progress(context.Background(), component)
		if e != nil {
			t.Fatal(e)
		}
		after[component] = p
	}
	f.ckSQL(t, "DROP TABLE ticket_events")
	f.ckSQL(t, "RENAME TABLE m403a_original_events TO ticket_events")
	if err = managed.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = projection.Resume(context.Background(), f.db, *gate.FreezeID); err != nil {
		t.Fatal(err)
	}
	m403aSave(t, "managed-workers.json", map[string]any{"version": version, "resumed_ticket": updated.Ticket, "full_reconciliation": report, "identity_changed_at": changedAt, "identity_loss_discovered_at": discovered, "new_ck_identity": newIdentity, "progress_before": before, "progress_after": after, "original_groups": []string{f.queue.Group("es"), f.queue.Group("ck")}, "no_group_handoff": true, "unavailable_binlog_endpoint_rejected": true})
}

func TestM403AActiveIdentityReadinessAndSameKeyCursor(t *testing.T) {
	m403aOwned(t)
	f, _ := newM4(t)
	m403aInstall(t, f.fixture)
	ids := []int64{}
	for i := 0; i < 3; i++ {
		ids = append(ids, f.create(t, 1, request(fmt.Sprintf("m403a-target-%d", i))).Ticket.ID)
	}
	f.startCapture(t)
	f.startConsumer(t)
	f.startCK(t)
	for _, id := range ids {
		f.projected(t, id, 1)
		f.latest(t, id, 1)
	}
	gate := m403aFreeze(t, f.fixture)
	control := m403aControlFor(f)
	var active projection.Version
	f.await(t, "fresh existing enrollment", func() bool {
		var err error
		active, err = control.EnrollExisting(context.Background(), "existing_v1")
		return err == nil
	})
	managed, err := control.Managed(context.Background(), active.ID)
	if err != nil {
		t.Fatal(err)
	}
	readiness := httptest.NewServer(httpapi.NewWithOptions(f.service, f.store, nil, httpapi.Options{Readiness: managed.Check}))
	defer readiness.Close()
	status := func(want int) {
		res, e := http.Get(readiness.URL + "/readyz")
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			body, _ := io.ReadAll(res.Body)
			t.Fatalf("ready=%d want=%d body=%s", res.StatusCode, want, body)
		}
	}
	status(200)
	// Exercise real command wiring: an ACTIVE target starts the API; deliberately
	// wrong target names fail before listening or joining any consumer group.
	cliEnv := func(extra ...string) []string {
		values := []string{}
		for _, item := range os.Environ() {
			if !strings.HasPrefix(item, "TICKETPULSE_TARGET_VERSION=") && !strings.HasPrefix(item, "TICKETPULSE_SEARCH_INDEX=") && !strings.HasPrefix(item, "TICKETPULSE_CK_DATABASE=") && !strings.HasPrefix(item, "TICKETPULSE_CURSOR_KEY=") {
				values = append(values, item)
			}
		}
		values = append(values, "TICKETPULSE_DSN="+m403aDSN(t, f.name), "TICKETPULSE_SYNC_DSN="+m403aDSN(t, f.name), "TICKETPULSE_TARGET_VERSION="+active.ID, "TICKETPULSE_SEARCH_INDEX="+f.es.Index(), "TICKETPULSE_CK_DATABASE="+f.ck.Database(), "TICKETPULSE_CURSOR_KEY="+strings.Repeat("00", 32))
		return append(values, extra...)
	}
	listen, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listen.Addr().String()
	listen.Close()
	app := exec.Command(os.Getenv("TICKETPULSE_API_BINARY"), "-listen="+address, "-routing=../../configs/local.json")
	app.Env = cliEnv()
	app.Stdout, app.Stderr = io.Discard, io.Discard
	if err = app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Process.Kill()
	m403aWait(t, "managed API process readiness", func() bool {
		res, e := http.Get("http://" + address + "/readyz")
		if e != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == 200
	})
	app.Process.Signal(os.Interrupt)
	if err = app.Wait(); err != nil {
		t.Fatal(err)
	}
	rejectedCLI := []map[string]any{}
	for _, item := range []struct {
		binary string
		args   []string
		env    []string
		label  string
	}{
		{os.Getenv("TICKETPULSE_API_BINARY"), []string{"-listen=" + address, "-routing=../../configs/local.json"}, []string{"TICKETPULSE_SEARCH_INDEX=" + f.name + "_wrong"}, "api_wrong_es"},
		{os.Getenv("TICKETPULSE_API_BINARY"), []string{"-listen=" + address, "-routing=../../configs/local.json"}, []string{"TICKETPULSE_CK_DATABASE=" + f.name + "_wrong"}, "api_wrong_ck"},
		{os.Getenv("TICKETPULSE_SYNC_BINARY"), []string{"-mode=consume"}, []string{"TICKETPULSE_SEARCH_INDEX=" + f.name + "_wrong"}, "consumer_wrong_es"},
		{os.Getenv("TICKETPULSE_SYNC_BINARY"), []string{"-mode=ck"}, []string{"TICKETPULSE_CK_DATABASE=" + f.name + "_wrong"}, "consumer_wrong_ck"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		cmd := exec.CommandContext(ctx, item.binary, item.args...)
		cmd.Env = cliEnv(item.env...)
		output, e := cmd.CombinedOutput()
		cancel()
		if e == nil || !bytes.Contains(output, []byte("PROJECTION_TARGET_MISMATCH")) {
			t.Fatalf("%s=%v output=%s", item.label, e, output)
		}
		rejectedCLI = append(rejectedCLI, map[string]any{"case": item.label, "error_output": string(output), "rejected": true})
	}
	key := make([]byte, 32)
	s1, err := search.NewService(f.store, f.es, f.store, key, search.WithTargetVersion(active.CursorTarget(), managed.Check))
	if err != nil {
		t.Fatal(err)
	}
	query := search.Request{Sort: "id", Order: "asc", PageSize: 1}
	page1, err := s1.Query(context.Background(), 5, query)
	if err != nil || !page1.HasMore {
		t.Fatalf("first=%+v err=%v", page1, err)
	}
	query.Cursor = page1.NextCursor
	same, err := search.NewService(f.store, f.es, f.store, key, search.WithTargetVersion(active.CursorTarget(), managed.Check))
	if err != nil {
		t.Fatal(err)
	}
	page2, err := same.Query(context.Background(), 5, query)
	if err != nil || len(page2.Items) != 1 || page2.Items[0].ID != ids[1] {
		t.Fatalf("same-version=%+v err=%v", page2, err)
	}
	// Separate real test index/reader only; no persisted promotion, alias or
	// consumer takeover. The old cursor must be refused before querying it.
	otherES, err := search.NewElasticsearch(f.es.Endpoint(), f.name+"_cursor_v2")
	if err != nil {
		t.Fatal(err)
	}
	if err = otherES.EnsureIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m4HTTP(t, "DELETE", otherES.Endpoint()+"/"+otherES.Index(), nil, 200, 404) })
	otherUUID, exists, err := otherES.Identity(context.Background())
	if err != nil || !exists {
		t.Fatal("separate cursor target identity missing", err)
	}
	other, err := search.NewService(f.store, otherES, f.store, key, search.WithTargetVersion("test_v2:"+otherUUID, nil))
	if err != nil {
		t.Fatal(err)
	}
	versionHTTP := httptest.NewServer(httpapi.NewWithOptions(f.service, f.store, nil, httpapi.Options{Search: other}))
	defer versionHTTP.Close()
	req, _ := http.NewRequest("GET", versionHTTP.URL+"/api/v1/search/tickets?sort=id&order=asc&page_size=1&cursor="+url.QueryEscape(query.Cursor), nil)
	req.Header.Set("Authorization", "Bearer local-supervisor")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 409 || !bytes.Contains(body, []byte("SEARCH_CURSOR_TARGET_CHANGED")) {
		t.Fatalf("version HTTP=%d %s", res.StatusCode, body)
	}
	beforeOffsets := map[string][]syncpipe.PartitionProgress{}
	for _, component := range []string{"es", "ck"} {
		p, e := f.queue.Progress(context.Background(), component)
		if e != nil {
			t.Fatal(e)
		}
		beforeOffsets[component] = p
	}
	// Mutate only private runtime clients/metadata. All configured mismatches fail.
	wantedES, wantedCK := control.ES, control.CK
	control.ES, _ = search.NewElasticsearch(wantedES.Endpoint(), f.name+"_wrong")
	status(503)
	if _, err := control.Managed(context.Background(), active.ID); !errors.Is(err, projection.ErrTargetMismatch) {
		t.Fatal("ES startup mismatch accepted")
	}
	control.ES = wantedES
	control.CK, _ = analytics.NewClickHouse(wantedCK.Endpoint(), f.name+"_wrong")
	status(503)
	control.CK = wantedCK
	control.ES, _ = search.NewElasticsearch("http://127.0.0.1:1", wantedES.Index())
	status(503)
	control.ES = wantedES
	f.exec(t, "UPDATE sync_projection_progress_bindings SET version_id='existing_v1',binding=JSON_SET(binding,'$.status_component','es') WHERE component='ck'")
	status(503)
	binding, _ := json.Marshal(active.Contract.CKWriter)
	f.exec(t, "UPDATE sync_projection_progress_bindings SET binding=? WHERE component='ck'", binding)
	status(200)
	// Report corruption is detected even when targets themselves remain healthy.
	f.exec(t, "UPDATE sync_projection_versions SET report_sha256=? WHERE version_id=?", strings.Repeat("0", 64), active.ID)
	status(503)
	f.exec(t, "UPDATE sync_projection_versions SET report_sha256=? WHERE version_id=?", *active.ReportSHA256, active.ID)
	status(200)
	// A changed partition set cannot re-use the recorded group positions.
	originalQueue := control.Queue
	control.Queue, err = syncpipe.NewQueue(originalQueue.Brokers, originalQueue.Topic, 2)
	if err != nil {
		t.Fatal(err)
	}
	status(503)
	control.Queue.Close()
	control.Queue = originalQueue
	f.stopConsumer(t)
	f.stopCK(t)
	f.stopCapture(t)
	// Observe CK UUID loss while ES still matches. Restore the original table by
	// rename so this test can independently verify the later ES UUID failure.
	oldCK, _, err := f.ck.Identity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.ckSQL(t, "RENAME TABLE ticket_events TO m403a_saved_events")
	if err = f.ck.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	newCK, _, err := f.ck.Identity(context.Background())
	if err != nil || newCK.Events == oldCK.Events {
		t.Fatal("test did not change CK UUID")
	}
	status(503)
	if _, err = control.Managed(context.Background(), active.ID); !errors.Is(err, projection.ErrTargetMismatch) {
		t.Fatal("CK UUID mismatch accepted at startup")
	}
	f.ckSQL(t, "DROP TABLE ticket_events")
	f.ckSQL(t, "RENAME TABLE m403a_saved_events TO ticket_events")
	status(200)
	// Recreate the same index name in the private test cluster; UUID must change.
	oldUUID, _, err := f.es.Identity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m4HTTP(t, "DELETE", f.es.Endpoint()+"/"+f.es.Index(), nil, 200)
	if err = f.es.EnsureIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	newUUID, _, err := f.es.Identity(context.Background())
	if err != nil || newUUID == oldUUID {
		t.Fatal("test did not change ES UUID")
	}
	status(503)
	if _, err = control.Managed(context.Background(), active.ID); !errors.Is(err, projection.ErrTargetMismatch) {
		t.Fatal("new UUID accepted at startup")
	}
	// Consumer guard rejects before group join or downstream Ensure/offset work.
	if err = (&syncpipe.Consumer{Store: f.state, Queue: f.queue, ES: f.es, Check: managed.Check}).Run(context.Background()); !errors.Is(err, projection.ErrTargetMismatch) {
		t.Fatalf("ES guard=%v", err)
	}
	if err = (&syncpipe.CKConsumer{Store: f.state, Queue: f.queue, CK: f.ck, Check: managed.Check}).Run(context.Background()); !errors.Is(err, projection.ErrTargetMismatch) {
		t.Fatalf("CK guard=%v", err)
	}
	saved, err := (&projection.VersionStore{DB: f.db}).Get(context.Background(), active.ID)
	if err != nil || saved.Fingerprint() != active.Fingerprint() {
		t.Fatalf("durable version=%+v err=%v", saved, err)
	}
	if _, err = projection.Resume(context.Background(), f.db, *gate.FreezeID); err != nil {
		t.Fatal(err)
	}
	m403aSave(t, "target-and-cursor.json", map[string]any{"active": saved, "page1": page1, "page2": page2, "old_cursor_error": json.RawMessage(body), "same_key": true, "old_es_uuid": oldUUID, "new_es_uuid": newUUID, "old_ck": oldCK, "new_ck": newCK, "startup_mismatch_rejected": true, "consumer_guard_rejected": true, "initial_offsets": beforeOffsets, "real_cli_cases": rejectedCLI, "managed_api_ready": true, "report_hash_corruption_rejected": true, "partition_change_rejected": true, "separate_cursor_index": otherES.Index(), "separate_cursor_index_uuid": otherUUID})
}

func TestM403APreparedRequiresFreshFrozenContentAndPreservesOffsets(t *testing.T) {
	m403aOwned(t)
	f, r := newM4(t)
	m403aInstall(t, f.fixture)
	m4LoadGold(t, f)
	f.startCapture(t)
	j := m4Job(t, f, r, "prepare_gold", "both")
	j = m4Run(t, r, j.ID)
	control := m403aControlFor(f)
	if _, err := control.Prepare(context.Background(), "prepared_v1", j.ID); err == nil {
		t.Fatal("candidate prepared without durable freeze")
	}
	f.create(t, 1, request("m403a-stale-report"))
	gate := m403aFreeze(t, f.fixture)
	// Original job/report is still completed/consistent, but candidate lacks new source.
	if _, err := control.Prepare(context.Background(), "prepared_stale", j.ID); err == nil {
		t.Fatal("old consistent report accepted without current content")
	}
	j = m4Run(t, r, j.ID)
	candidateES, err := search.NewElasticsearch(f.es.Endpoint(), j.Index)
	if err != nil {
		t.Fatal(err)
	}
	candidateCK, err := analytics.NewClickHouse(f.ck.Endpoint(), j.Database)
	if err != nil {
		t.Fatal(err)
	}
	candidateControl := &syncpipe.ProjectionControl{Store: f.state, Queue: f.queue, ES: candidateES, CK: candidateCK}
	if _, err = candidateControl.EnrollExisting(context.Background(), "candidate_bypass"); err == nil || !strings.Contains(err.Error(), "cannot promote") {
		t.Fatalf("candidate enrollment bypass=%v", err)
	}
	beforeES, e := f.queue.Progress(context.Background(), "es")
	if e != nil {
		t.Fatal(e)
	}
	beforeCK, e := f.queue.Progress(context.Background(), "ck")
	if e != nil {
		t.Fatal(e)
	}
	version, err := control.Prepare(context.Background(), "prepared_v1", j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if version.Phase != projection.Prepared || version.Contract.HandoffVerified || version.Contract.Candidate == nil || version.FreezeRevision == nil || *version.FreezeRevision != gate.Revision {
		t.Fatalf("candidate=%+v", version)
	}
	if _, err = control.Managed(context.Background(), version.ID); !errors.Is(err, projection.ErrTargetMismatch) {
		t.Fatal("PREPARED became usable by API/standard consumer")
	}
	// Registration and associated metadata survive a real CLI crash atomically.
	// Block stdout after commit, then kill the operator before it can emit JSON.
	pipeRead, pipeWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	fillDone := make(chan struct{})
	go func() { pipeWrite.Write(bytes.Repeat([]byte("x"), 2<<20)); close(fillDone) }()
	time.Sleep(30 * time.Millisecond)
	operator := exec.Command(os.Getenv("TICKETPULSE_CONTROL_BINARY"), "-mode=prepare", "-version=prepared_crash", "-job="+j.ID)
	operator.Env = append(os.Environ(), "TICKETPULSE_CONTROL_DSN="+m403aDSN(t, f.name), "TICKETPULSE_SEARCH_INDEX="+f.es.Index(), "TICKETPULSE_CK_DATABASE="+f.ck.Database())
	operator.Stdout = pipeWrite
	operator.Stderr = io.Discard
	if err = operator.Start(); err != nil {
		t.Fatal(err)
	}
	defer operator.Process.Kill()
	versions := &projection.VersionStore{DB: f.db}
	m403aWait(t, "durable prepared version before CLI kill", func() bool { _, e := versions.Get(context.Background(), "prepared_crash"); return e == nil })
	if err = operator.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = operator.Wait(); err == nil {
		t.Fatal("candidate operator did not receive SIGKILL")
	}
	pipeRead.Close()
	pipeWrite.Close()
	<-fillDone
	crashVersion, err := versions.Get(context.Background(), "prepared_crash")
	if err != nil || crashVersion.Phase != projection.Prepared {
		t.Fatalf("crash record=%+v err=%v", crashVersion, err)
	}
	if *m403aGate(t, f.fixture).FreezeID != *gate.FreezeID {
		t.Fatal("candidate CLI crash cleared business freeze")
	}
	statusJob, err := r.Status(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(statusJob.NextOffsets, j.NextOffsets) {
		t.Fatal("prepare mutated candidate offsets")
	}
	// A registration crash before COMMIT leaves neither a version nor its log.
	cfg, _ := mysql.ParseDSN(m403aDSN(t, f.name))
	isolatedDB, err := mysqlstore.Open(context.Background(), cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := isolatedDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	copy := version
	copy.ID = "prepared_abort"
	if err = (&projection.VersionStore{DB: isolatedDB}).Insert(context.Background(), tx, copy); err != nil {
		t.Fatal(err)
	}
	var connectionID int64
	if err = tx.QueryRow("SELECT CONNECTION_ID()").Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	f.exec(t, fmt.Sprintf("KILL CONNECTION %d", connectionID))
	if err = tx.Commit(); err == nil {
		t.Fatal("killed registration transaction committed")
	}
	isolatedDB.Close()
	if _, err = versions.Get(context.Background(), copy.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("uncommitted version survived=%v", err)
	}
	var orphanLogs int
	if err = f.db.QueryRow("SELECT count(*) FROM sync_projection_control_log WHERE reference_id=?", copy.ID).Scan(&orphanLogs); err != nil || orphanLogs != 0 {
		t.Fatal("uncommitted registration left control log")
	}
	afterES, e := f.queue.Progress(context.Background(), "es")
	if e != nil {
		t.Fatal(e)
	}
	afterCK, e := f.queue.Progress(context.Background(), "ck")
	if e != nil {
		t.Fatal(e)
	}
	for i := range beforeES {
		if beforeES[i].Committed != afterES[i].Committed || beforeCK[i].Committed != afterCK[i].Committed {
			t.Fatal("prepare migrated a standard group offset")
		}
	}
	followCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Follow(followCtx, j.ID) }()
	f.await(t, "candidate follow owns lock", func() bool { job, e := r.Status(context.Background(), j.ID); return e == nil && job.WorkerActive })
	if _, err = control.Prepare(context.Background(), "prepared_busy", j.ID); err == nil {
		t.Fatal("active follow ownership was ignored")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("follow stop timeout")
	}
	var report syncpipe.ReconcileReport
	if err = json.Unmarshal(version.ValidationReport, &report); err != nil {
		t.Fatal(err)
	}
	if report.Source.Tickets != 11 || report.Source.Histories != 47 || report.DifferenceCount != 0 {
		t.Fatalf("fresh full content=%+v", report.Source)
	}
	if _, err = projection.Resume(context.Background(), f.db, *gate.FreezeID); err != nil {
		t.Fatal(err)
	}
	m403aSave(t, "prepared.json", map[string]any{"version": version, "old_report_rejected": true, "follow_lock_rejected": true, "handoff_verified": false, "candidate_positions_before": j.NextOffsets, "candidate_positions_after": statusJob.NextOffsets, "standard_es_before": beforeES, "standard_es_after": afterES, "standard_ck_before": beforeCK, "standard_ck_after": afterCK, "registration_survived_cli_sigkill": crashVersion, "uncommitted_registration_rolled_back": true, "orphan_registration_logs": orphanLogs, "candidate_enrollment_bypass_rejected": true})
}

func TestM403AFrozenCDCMarkersAndCheckpointsContinue(t *testing.T) {
	m403aOwned(t)
	f, _ := newM4(t)
	m403aInstall(t, f.fixture)
	first := f.create(t, 1, request("m403a-internal"))
	f.startCapture(t)
	f.startConsumer(t)
	f.startCK(t)
	f.projected(t, first.Ticket.ID, 1)
	f.latest(t, first.Ticket.ID, 1)
	gate := m403aFreeze(t, f.fixture)
	before, err := f.state.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := f.state.IssueWatermark(context.Background()); err != nil {
		t.Fatal(err)
	}
	var mark string
	var issuedAt time.Time
	if err = f.db.QueryRow("SELECT barrier_id,issued_at FROM sync_source_watermark WHERE source_name='tickets-v1'").Scan(&mark, &issuedAt); err != nil {
		t.Fatal(err)
	}
	f.await(t, "frozen CDC marker reaches all partitions and both groups", func() bool {
		var n int
		_ = f.db.QueryRow("SELECT count(*) FROM sync_kafka_watermarks WHERE issued_at>=? AND component IN ('es','ck')", issuedAt).Scan(&n)
		return n == int(2*f.queue.Partitions)
	})
	after, err := f.state.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.File == before.File && after.Pos <= before.Pos {
		t.Fatal("frozen CDC checkpoint did not progress")
	}
	if got := m403aGate(t, f.fixture); got.State != "FROZEN" || *got.FreezeID != *gate.FreezeID {
		t.Fatal("internal operations changed freeze")
	}
	f.stopCapture(t)
	report, err := syncpipe.Reconcile(context.Background(), f.state, nil, f.es, f.ck, syncpipe.ReconcileOptions{BatchSize: 100})
	if err != nil || report.State != "consistent" || report.DifferenceCount != 0 {
		t.Fatalf("frozen internal reconciliation=%s err=%v", report.State, err)
	}
	if _, err = projection.Resume(context.Background(), f.db, *gate.FreezeID); err != nil {
		t.Fatal(err)
	}
	m403aSave(t, "internal-progress.json", map[string]any{"gate": gate, "source_marker": mark, "checkpoint_before": before, "checkpoint_after": after, "full_reconciliation": report, "internal_progress_unfrozen": true})
}
