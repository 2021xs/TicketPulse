//go:build integration && m2 && m3 && m4 && m403b

package mysqlstore_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
)

func m403bOwned(t *testing.T) {
	t.Helper()
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c3" {
		for key, want := range map[string]string{"TICKETPULSE_ES_URL": "http://127.0.0.1:19209", "TICKETPULSE_CK_URL": "http://127.0.0.1:18132", "TICKETPULSE_KAFKA_BROKERS": "127.0.0.1:19108", "TICKETPULSE_CDC_ADDR": "127.0.0.1:33315"} {
			if os.Getenv(key) != want {
				t.Fatal("C3 handoff regression endpoint mismatch")
			}
		}
		return
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c2" {
		for key, want := range map[string]string{"TICKETPULSE_ES_URL": "http://127.0.0.1:19208", "TICKETPULSE_CK_URL": "http://127.0.0.1:18131", "TICKETPULSE_KAFKA_BROKERS": "127.0.0.1:19106", "TICKETPULSE_CDC_ADDR": "127.0.0.1:33314"} {
			if os.Getenv(key) != want {
				t.Fatal("C2 handoff regression endpoint mismatch")
			}
		}
		return
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c1" {
		for key, want := range map[string]string{"TICKETPULSE_ES_URL": "http://127.0.0.1:19207", "TICKETPULSE_CK_URL": "http://127.0.0.1:18130", "TICKETPULSE_KAFKA_BROKERS": "127.0.0.1:19104", "TICKETPULSE_CDC_ADDR": "127.0.0.1:33313"} {
			if os.Getenv(key) != want {
				t.Fatal("C1 handoff regression endpoint mismatch")
			}
		}
		return
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03b" {
		t.Fatal("M4-03B requires private suite")
	}
	for key, want := range map[string]string{"TICKETPULSE_ES_URL": "http://127.0.0.1:19206", "TICKETPULSE_CK_URL": "http://127.0.0.1:18129", "TICKETPULSE_KAFKA_BROKERS": "127.0.0.1:19102", "TICKETPULSE_CDC_ADDR": "127.0.0.1:33312"} {
		if os.Getenv(key) != want {
			t.Fatal("M4-03B private endpoint mismatch")
		}
	}
}
func m403bSave(t *testing.T, name string, v any) {
	t.Helper()
	dir := os.Getenv("TICKETPULSE_M403B_EVIDENCE_DIR")
	if dir == "" {
		t.Fatal("evidence path required")
	}
	dir = filepath.Join(dir, t.Name())
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	out, err := os.OpenFile(filepath.Join(dir, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err = enc.Encode(v); err != nil {
		t.Fatal(err)
	}
}
func m403bWait(t *testing.T, label string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal(label + " deadline")
}

type m403bFixture struct {
	f            *m3fixture
	r            *syncpipe.Rebuilder
	job          syncpipe.RebuildJob
	control      *syncpipe.HandoffControl
	version      projection.Version
	gate         projection.Gate
	h            projection.Handoff
	observations []map[string]any
}

func m403bNew(t *testing.T) *m403bFixture {
	return m403bNewWithAPIEndpoint(t, "")
}

func m403bNewWithAPIEndpoint(t *testing.T, readEndpoint string) *m403bFixture {
	return m403bNewWithWorkers(t, readEndpoint, nil)
}

func m403bNewWithWorkers(t *testing.T, readEndpoint string, start func(*m3fixture)) *m403bFixture {
	t.Helper()
	m403bOwned(t)
	f, r := newM4(t)
	if readEndpoint != "" {
		r.ESURL = readEndpoint
	}
	if err := projection.EnsureHandoffSchema(context.Background(), f.db); err != nil {
		t.Fatal(err)
	}
	f.store.RequireBusinessGate()
	m4LoadGold(t, f)
	if start == nil {
		f.startCapture(t)
		f.startConsumer(t)
		f.startCK(t)
	} else {
		start(f)
	}
	m403bWait(t, "legacy projection caught up", func() bool {
		es, e := f.queue.Progress(context.Background(), "es")
		ck, c := f.queue.Progress(context.Background(), "ck")
		if e != nil || c != nil {
			return false
		}
		for _, x := range append(es, ck...) {
			if x.Lag != 0 {
				return false
			}
		}
		var initialized bool
		f.db.QueryRow("SELECT EXISTS(SELECT 1 FROM sync_kafka_analytics_bootstrap WHERE pipeline='ck-v1')").Scan(&initialized)
		return initialized
	})
	job := m4Job(t, f, r, "handoff_gold", "both")
	job = m4Run(t, r, job.ID)
	g, err := projection.Freeze(context.Background(), f.db, "M4-03B isolated handoff")
	if err != nil {
		t.Fatal(err)
	}
	pc := &syncpipe.ProjectionControl{Store: f.state, Queue: f.queue, ES: f.es, CK: f.ck}
	if readEndpoint != "" {
		readES, e := search.NewElasticsearch(readEndpoint, f.es.Index())
		if e != nil {
			t.Fatal(e)
		}
		pc.ES = readES
	}
	m403bWait(t, "legacy freeze boundary", func() bool {
		es, e := f.queue.Progress(context.Background(), "es")
		ck, c := f.queue.Progress(context.Background(), "ck")
		if e != nil || c != nil {
			return false
		}
		for _, x := range append(es, ck...) {
			if x.Lag != 0 {
				return false
			}
		}
		return true
	})
	if _, err = pc.EnrollExisting(context.Background(), "active_original"); err != nil {
		t.Fatal(err)
	}
	version, err := pc.Prepare(context.Background(), "candidate_v1", job.ID)
	if err != nil {
		t.Fatal(err)
	}
	es, ck, err := syncpipe.CandidateTargets(context.Background(), f.db, version.ID)
	if err != nil {
		t.Fatal(err)
	}
	c := &syncpipe.HandoffControl{ProjectionControl: &syncpipe.ProjectionControl{Store: f.state, Queue: f.queue, ES: es, CK: ck}, CatchupTimeout: 15 * time.Second}
	x := &m403bFixture{f: f, r: r, job: job, control: c, version: version, gate: g}
	if readEndpoint != "" {
		f.es = pc.ES
	}
	c.Observe = func(stage string, v any) error {
		x.observations = append(x.observations, map[string]any{"stage": stage, "value": v, "at": time.Now().UTC()})
		return nil
	}
	// Delete only these version-specific test groups on fixture cleanup.
	t.Cleanup(func() {
		if os.Getenv("TICKETPULSE_RETAIN_C3_RESOURCES") == "true" {
			return
		}
		f.queue.Admin.DeleteGroups(context.Background(), projection.TargetGroup(f.queue.Topic, version.ID, "es"), projection.TargetGroup(f.queue.Topic, version.ID, "ck"))
	})
	return x
}
func (x *m403bFixture) pin(t *testing.T) {
	t.Helper()
	h, err := x.control.Begin(context.Background(), "handoff_v1", x.version.ID)
	if err != nil {
		t.Fatal(err)
	}
	x.h = h
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	h, err = x.control.Pin(ctx, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	x.h = h
}
func (x *m403bFixture) open(t *testing.T) {
	t.Helper()
	h, err := x.control.Initialize(context.Background(), x.h.ID)
	if err != nil {
		t.Fatal(err)
	}
	h, err = x.control.OpenConsumption(context.Background(), h.ID)
	if err != nil {
		t.Fatal(err)
	}
	x.h = h
}
func (x *m403bFixture) target(t *testing.T) *syncpipe.TargetConsumerConfig {
	t.Helper()
	c, err := x.control.ConsumerTarget(context.Background(), x.h.ID)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func (x *m403bFixture) run(t *testing.T, component string) (context.CancelFunc, <-chan error) {
	t.Helper()
	target := x.target(t)
	q := *x.f.queue
	q.Target = target
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	if component == "es" {
		go func() {
			e := (&syncpipe.Consumer{Store: x.f.state, Queue: &q, ES: x.control.ES, Target: target}).Run(ctx)
			t.Logf("candidate ES exited: %v", e)
			done <- e
		}()
	} else {
		go func() {
			e := (&syncpipe.CKConsumer{Store: x.f.state, Queue: &q, CK: x.control.CK, Target: target}).Run(ctx)
			t.Logf("candidate CK exited: %v", e)
			done <- e
		}()
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("candidate worker cleanup timeout")
		}
	})
	m403bWait(t, "candidate "+component+" running", func() bool {
		var n int
		x.f.db.QueryRow("SELECT COUNT(*) FROM sync_projection_worker_state WHERE version_id=? AND component=? AND state='running'", x.version.ID, component).Scan(&n)
		return n == 1
	})
	return cancel, done
}
func (x *m403bFixture) verify(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	h, err := x.control.Verify(ctx, x.h.ID)
	if err != nil {
		snapshot := map[string]any{"error": err.Error()}
		for _, table := range []string{"sync_projection_handoffs", "sync_projection_worker_state", "sync_projection_watermarks", "sync_projection_deliveries", "sync_projection_target_owners"} {
			rows, e := x.f.db.Query("SELECT * FROM " + table)
			if e != nil {
				snapshot[table] = e.Error()
				continue
			}
			cols, _ := rows.Columns()
			data := []map[string]string{}
			for rows.Next() {
				values := make([]sql.RawBytes, len(cols))
				args := make([]any, len(cols))
				for i := range values {
					args[i] = &values[i]
				}
				if e = rows.Scan(args...); e != nil {
					break
				}
				row := map[string]string{}
				for i, c := range cols {
					if c == "isolated_payload" {
						continue
					}
					row[c] = string(values[i])
				}
				data = append(data, row)
			}
			rows.Close()
			snapshot[table] = data
		}
		dir := filepath.Join(os.Getenv("TICKETPULSE_M403B_EVIDENCE_DIR"), t.Name())
		out, _ := os.OpenFile(filepath.Join(dir, "goroutines.txt"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if out != nil {
			pprof.Lookup("goroutine").WriteTo(out, 2)
			out.Close()
		}
		m403bSave(t, "verify-failure", snapshot)
		t.Logf("worker evidence=%v", snapshot["sync_projection_worker_state"])
		t.Fatal(err)
	}
	x.h = h
}

func TestM403BOfflineNewGroupsExactPositionsAndGold(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	for _, fn := range []func() error{func() error { _, e := x.r.Run(context.Background(), x.job.ID); return e }, func() error { _, e := x.r.Check(context.Background(), x.job.ID); return e }, func() error { return x.r.Follow(context.Background(), x.job.ID) }} {
		if e := fn(); e == nil || !strings.Contains(e.Error(), "SEALED") {
			t.Fatalf("sealed M4 accepted writer: %v", e)
		}
	}
	if _, err := projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); err == nil {
		t.Fatal("resume accepted before verified")
	}
	before, _ := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if len(before) != 0 {
		t.Fatal("new group unexpectedly preexists")
	}
	x.open(t)
	for _, component := range []string{"es", "ck"} {
		raw, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.Group(component))
		if e != nil {
			t.Fatal(e)
		}
		for part, n := range x.h.PinnedOffsets {
			o, ok := raw.Lookup(x.f.queue.Topic, part)
			if !ok || o.Err != nil || o.At != n {
				t.Fatal("initial position not exact", part, n, o)
			}
		}
	}
	if _, err := x.control.Initialize(context.Background(), x.h.ID); err == nil {
		t.Fatal("consuming allowed reset")
	}
	x.run(t, "es")
	x.run(t, "ck")
	x.verify(t)
	m4AssertProjection(t, x.f, x.job)
	gold := m4AssertGold(t, x.f, x.job)
	active, err := (&projection.VersionStore{DB: x.f.db}).Get(context.Background(), "active_original")
	if err != nil || active.Phase != projection.Active {
		t.Fatal("ACTIVE changed", err)
	}
	prepared, err := (&projection.VersionStore{DB: x.f.db}).Get(context.Background(), x.version.ID)
	if err != nil || prepared.Fingerprint() != x.version.Fingerprint() || prepared.Phase != projection.Prepared {
		t.Fatal("candidate contract mutated", err)
	}
	if _, err := (&syncpipe.ProjectionControl{Store: x.f.state, Queue: x.f.queue, ES: x.control.ES, CK: x.control.CK}).Managed(context.Background(), x.version.ID); err == nil {
		t.Fatal("candidate became API ACTIVE")
	}
	status := &syncpipe.TargetStatus{Target: x.target(t)}
	diagnostics, err := status.Diagnostics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); err != nil {
		t.Fatal(err)
	}
	x.f.actionHTTP(t, "local-device-agent", "comments", 4, 10, map[string]any{"content": "after VERIFIED candidate and ACTIVE follow"})
	m403bWait(t, "both targets after resume", func() bool {
		var d struct {
			Version int64 `json:"_version"`
		}
		json.Unmarshal(m4HTTP(t, "GET", fmt.Sprintf("%s/%s/_doc/4", x.control.ES.Endpoint(), x.control.ES.Index()), nil, 200), &d)
		var old struct {
			Version int64 `json:"_version"`
		}
		json.Unmarshal(m4HTTP(t, "GET", fmt.Sprintf("%s/%s/_doc/4", x.f.es.Endpoint(), x.f.es.Index()), nil, 200), &old)
		return d.Version == 11 && old.Version == 11 && m4CKSQL(t, x.control.CK, "SELECT max(version) FROM ticket_states WHERE ticket_id=4 FORMAT TabSeparated") == "11" && m4CKSQL(t, x.f.ck, "SELECT max(version) FROM ticket_states WHERE ticket_id=4 FORMAT TabSeparated") == "11"
	})
	// Full content equality continues after business resumes.
	report, err := syncpipe.Reconcile(context.Background(), x.f.state, nil, x.control.ES, x.control.CK, syncpipe.ReconcileOptions{})
	if err != nil || report.State != "consistent" {
		t.Fatal(report.State, err)
	}
	m403bSave(t, "protocol", x.observations)
	m403bSave(t, "handoff", x.h)
	m403bSave(t, "gold", gold)
	m403bSave(t, "target-status", diagnostics)
	m403bSave(t, "after-resume", report)
}

type m403bProcess struct {
	cmd  *exec.Cmd
	done chan error
	log  string
	once sync.Once
}

func (x *m403bFixture) process(t *testing.T, component string) *m403bProcess {
	t.Helper()
	mode := "consume"
	if component == "ck" {
		mode = "ck"
	}
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName = x.f.name
	dir := filepath.Join(os.Getenv("TICKETPULSE_M403B_EVIDENCE_DIR"), t.Name())
	os.MkdirAll(dir, 0700)
	logPath := filepath.Join(dir, component+fmt.Sprint(time.Now().UnixNano())+".log")
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Getenv("TICKETPULSE_SYNC_BINARY"), "-mode="+mode, "-handoff="+x.h.ID)
	cmd.Env = append(os.Environ(), "TICKETPULSE_SYNC_DSN="+cfg.FormatDSN(), "TICKETPULSE_ES_URL="+x.control.ES.Endpoint(), "TICKETPULSE_CK_URL="+x.control.CK.Endpoint(), "TICKETPULSE_SEARCH_INDEX="+x.control.ES.Index(), "TICKETPULSE_CK_DATABASE="+x.control.CK.Database())
	cmd.Stdout = file
	cmd.Stderr = file
	if err = cmd.Start(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	p := &m403bProcess{cmd: cmd, done: make(chan error, 1), log: logPath}
	go func() { e := cmd.Wait(); file.Close(); p.done <- e }()
	t.Cleanup(func() {
		p.once.Do(func() {
			cmd.Process.Signal(os.Interrupt)
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				cmd.Process.Kill()
				<-p.done
			}
		})
	})
	return p
}
func (p *m403bProcess) kill(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		if err := p.cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		if err := <-p.done; err == nil {
			t.Fatal("SIGKILL returned success")
		}
	})
}

func TestM403BPartialInitializationUnknownResponseAndResume(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	calls := 0
	x.control.Observe = func(stage string, v any) error {
		x.observations = append(x.observations, map[string]any{"stage": stage, "value": v})
		if stage == "after_initial_commit" {
			calls++
			if calls == 1 {
				return context.DeadlineExceeded
			}
		}
		return nil
	}
	if _, err := x.control.Initialize(context.Background(), x.h.ID); err == nil {
		t.Fatal("response loss injection succeeded")
	}
	saved, e := (&projection.HandoffStore{DB: x.f.db}).Get(context.Background(), x.h.ID)
	if e != nil || saved.Phase != projection.Pinned {
		t.Fatal("partial initialization lost pinned phase", e)
	}
	raw, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if e != nil || len(raw[x.f.queue.Topic]) != 1 {
		t.Fatal("real partial commit missing", e, raw)
	}
	m403bSave(t, "partial-before-retry", map[string]any{"handoff": saved, "committed": raw, "error": "context deadline exceeded after real broker response"})
	x.control.Observe = func(stage string, v any) error {
		x.observations = append(x.observations, map[string]any{"stage": stage, "value": v})
		return nil
	}
	x.open(t)
	x.run(t, "es")
	x.run(t, "ck")
	x.verify(t)
	m403bSave(t, "retry-protocol", x.observations)
	m403bSave(t, "recovered", x.h)
}

// This child is a real operator process against the parent's owned fixture.
// The pipe boundary is outside MySQL/Kafka: the parent SIGKILLs it before or
// after a real commit and then inspects the durable result.
func TestM403BHandoffCrashChild(t *testing.T) {
	stage := os.Getenv("TICKETPULSE_M403B_CHILD_STAGE")
	if stage == "" {
		t.Skip("child-only helper")
	}
	m403bOwned(t)
	cfg, e := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if e != nil {
		t.Fatal(e)
	}
	cfg.DBName = os.Getenv("TICKETPULSE_M403B_CHILD_SCHEMA")
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	if !strings.HasPrefix(cfg.DBName, "ticketpulse_test_") {
		t.Fatal("child requires owned fixture schema")
	}
	db, e := sql.Open("mysql", cfg.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var uuid string
	if e = db.QueryRow("SELECT @@server_uuid").Scan(&uuid); e != nil || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
		t.Fatal("child identity mismatch")
	}
	h, e := (&projection.HandoffStore{DB: db}).Get(context.Background(), "handoff_v1")
	if e != nil {
		t.Fatal(e)
	}
	es, ck, e := syncpipe.CandidateTargets(context.Background(), db, h.VersionID)
	if e != nil {
		t.Fatal(e)
	}
	q, e := syncpipe.ConnectQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	c := &syncpipe.HandoffControl{ProjectionControl: &syncpipe.ProjectionControl{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: q, ES: es, CK: ck}, CatchupTimeout: 15 * time.Second}
	c.Observe = func(actual string, v any) error {
		if actual == stage {
			path := os.Getenv("TICKETPULSE_M403B_CHILD_READY")
			b, _ := json.Marshal(v)
			if e := os.WriteFile(path, b, 0600); e != nil {
				return e
			}
			select {}
		}
		return nil
	}
	if strings.Contains(stage, "pin_commit") {
		_, e = c.Pin(context.Background(), h.ID)
	} else {
		_, e = c.Initialize(context.Background(), h.ID)
	}
	if e != nil {
		t.Fatal(e)
	}
}
func (x *m403bFixture) crashOperator(t *testing.T, stage string) {
	t.Helper()
	dir := filepath.Join(os.Getenv("TICKETPULSE_M403B_EVIDENCE_DIR"), t.Name())
	os.MkdirAll(dir, 0700)
	ready := filepath.Join(dir, stage+".ready.json")
	log, err := os.OpenFile(filepath.Join(dir, stage+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestM403BHandoffCrashChild$", "-test.v")
	command.Env = append(os.Environ(), "TICKETPULSE_M403B_CHILD_STAGE="+stage, "TICKETPULSE_M403B_CHILD_SCHEMA="+x.f.name, "TICKETPULSE_M403B_CHILD_READY="+ready)
	command.Stdout = log
	command.Stderr = log
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	defer command.Process.Kill()
	m403bWait(t, "operator boundary "+stage, func() bool { _, e := os.Stat(ready); return e == nil })
	if err = command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = command.Wait(); err == nil {
		t.Fatal("operator SIGKILL succeeded")
	}
	log.Close()
	h, err := (&projection.HandoffStore{DB: x.f.db}).Get(context.Background(), x.h.ID)
	if err != nil {
		t.Fatal(err)
	}
	x.h = h
	m403bSave(t, stage+"-durable", h)
}
func TestM403BSealAndInitialCommitSIGKILL(t *testing.T) {
	x := m403bNew(t)
	h, e := x.control.Begin(context.Background(), "handoff_v1", x.version.ID)
	if e != nil {
		t.Fatal(e)
	}
	x.h = h
	x.crashOperator(t, "before_pin_commit")
	if x.h.Phase != projection.Draining {
		t.Fatal("uncommitted seal survived")
	}
	x.crashOperator(t, "after_pin_commit")
	if x.h.Phase != projection.Pinned {
		t.Fatal("committed seal lost")
	}
	if e = x.r.Follow(context.Background(), x.job.ID); e == nil {
		t.Fatal("sealed follower restarted")
	}
	x.crashOperator(t, "after_initial_commit")
	raw, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if e != nil || len(raw[x.f.queue.Topic]) != 1 {
		t.Fatal("SIGKILL initial partial commit not retained", raw, e)
	}
	x.crashOperator(t, "after_offsets_ready_commit")
	if x.h.Phase != projection.OffsetsReady {
		t.Fatal("initialized position status lost after SIGKILL")
	}
	x.open(t)
	x.run(t, "es")
	x.run(t, "ck")
	x.verify(t)
	m403bSave(t, "final", x.h)
}
func TestM403BFollowerDrainsPersistentIntent(t *testing.T) {
	x := m403bNew(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	r := &syncpipe.Rebuilder{Store: x.f.state, Queue: x.f.queue, ESURL: x.f.es.Endpoint(), CKURL: x.f.ck.Endpoint()}
	go func() { done <- r.Follow(ctx, x.job.ID) }()
	m403bWait(t, "follower running", func() bool {
		j, e := r.Status(context.Background(), x.job.ID)
		return e == nil && j.Status == "following" && j.WorkerActive
	})
	if _, e := x.control.Begin(context.Background(), "handoff_v1", x.version.ID); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if e == nil || !strings.Contains(e.Error(), "SEALED") {
			t.Fatal("follower did not obey durable drain", e)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("follower drain deadline")
	}
	h, e := x.control.Pin(context.Background(), "handoff_v1")
	if e != nil {
		t.Fatal(e)
	}
	x.h = h
	m403bSave(t, "drained-and-pinned", h)
}
func TestM403BInitializationAlreadyDoneBeforeConsumerSIGKILL(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	h, e := x.control.Initialize(context.Background(), x.h.ID)
	if e != nil {
		t.Fatal(e)
	}
	x.h = h
	// Actual control CLI killed after read-only status output, before any worker.
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName = x.f.name
	cmd := exec.Command(os.Getenv("TICKETPULSE_CONTROL_BINARY"), "-mode=handoff-status", "-handoff="+h.ID)
	cmd.Env = append(os.Environ(), "TICKETPULSE_CONTROL_DSN="+cfg.FormatDSN())
	out, e := cmd.Output()
	if e != nil {
		t.Fatal(e)
	}
	var status projection.Handoff
	if e = json.Unmarshal(out, &status); e != nil || status.Phase != projection.OffsetsReady {
		t.Fatal(e)
	}
	h, e = x.control.OpenConsumption(context.Background(), h.ID)
	if e != nil {
		t.Fatal(e)
	}
	x.h = h
	p := x.process(t, "es")
	m403bWait(t, "writer registered after authorization", func() bool {
		o, e := projection.ReadOwner(context.Background(), x.f.db, projection.VersionTargetKey(x.version, "es"), "")
		return e == nil && o.PID != nil && *o.PID == int64(p.cmd.Process.Pid)
	})
	p.kill(t)
	if _, e = x.control.Initialize(context.Background(), h.ID); e == nil {
		t.Fatal("restart initialized offsets")
	}
	x.run(t, "es")
	x.run(t, "ck")
	x.verify(t)
	m403bSave(t, "recovered", x.h)
}

func TestM403BM4ProjectionBeforeNextOffsetFailure(t *testing.T) {
	x := m403bNew(t)
	event := x.f.event(t, 4, 8, "m403b-m4-next-failure")
	ref, e := x.f.queue.Publish(context.Background(), event)
	if e != nil {
		t.Fatal(e)
	}
	before := m4CKSQL(t, x.control.CK, "SELECT count() FROM ticket_events WHERE ticket_id=4 AND version=8 FORMAT TabSeparated")
	x.f.exec(t, fmt.Sprintf("CREATE TRIGGER m403b_m4_next BEFORE UPDATE ON sync_rebuild_offsets FOR EACH ROW BEGIN IF NEW.partition_id=%d AND NEW.next_offset=%d THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='M403B projection committed before next offset'; END IF; END", ref.Partition, ref.Offset+1))
	_, e = x.r.Check(context.Background(), x.job.ID)
	if e == nil || !strings.Contains(e.Error(), "projection committed") {
		t.Fatal("real next offset failure absent", e)
	}
	after := m4CKSQL(t, x.control.CK, "SELECT count() FROM ticket_events WHERE ticket_id=4 AND version=8 FORMAT TabSeparated")
	job, e := x.r.Status(context.Background(), x.job.ID)
	if e != nil || job.NextOffsets[ref.Partition] > ref.Offset || before == after {
		t.Fatal("projection/offset ordering failed", job.NextOffsets, e, before, after)
	}
	x.f.exec(t, "DROP TRIGGER m403b_m4_next")
	report, e := x.r.Check(context.Background(), x.job.ID)
	if e != nil || report.State != "consistent" {
		t.Fatal(report.State, e)
	}
	gold := m4AssertGold(t, x.f, x.job)
	m403bSave(t, "actual-replay", map[string]any{"record": ref, "physical_before": before, "physical_after_failure": after, "failed_next": job.NextOffsets, "report": report, "gold": gold})
}
func (p *m403bProcess) failedExit(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		select {
		case e := <-p.done:
			if e == nil {
				t.Fatal("faulted process succeeded")
			}
		case <-time.After(15 * time.Second):
			p.cmd.Process.Kill()
			<-p.done
			t.Fatal("faulted process failed to stop")
		}
	})
}
func (x *m403bFixture) processRunning(t *testing.T, p *m403bProcess, component string) {
	t.Helper()
	m403bWait(t, "process writer ready", func() bool {
		o, e := projection.ReadOwner(context.Background(), x.f.db, projection.VersionTargetKey(x.version, component), "")
		return e == nil && o.PID != nil && *o.PID == int64(p.cmd.Process.Pid)
	})
}
func TestM403BStandardProjectionProgressFailureAndSIGKILLRestart(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	es := x.process(t, "es")
	ck := x.process(t, "ck")
	x.processRunning(t, es, "es")
	x.processRunning(t, ck, "ck")
	x.verify(t)
	if _, e := projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e != nil {
		t.Fatal(e)
	}
	x.f.exec(t, "CREATE TRIGGER m403b_es_progress BEFORE UPDATE ON sync_projection_deliveries FOR EACH ROW BEGIN IF NEW.component='es' AND NEW.ticket_id=4 AND NEW.ticket_version=11 AND NEW.status IN ('applied','duplicate_or_old') THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='M403B ES write before progress'; END IF; END")
	current := x.f.actionHTTP(t, "local-device-agent", "comments", 4, 10, map[string]any{"content": "new source v11 with candidate progress failure"})
	es.failedExit(t)
	doc := func(index string) int64 {
		var d struct {
			Version int64 `json:"_version"`
		}
		json.Unmarshal(m4HTTP(t, "GET", fmt.Sprintf("%s/%s/_doc/4", x.control.ES.Endpoint(), index), nil, 200), &d)
		return d.Version
	}
	if doc(x.control.ES.Index()) != 11 {
		t.Fatal("candidate ES write did not persist before progress fault")
	}
	var ref syncpipe.RecordRef
	ref.Topic = x.f.queue.Topic
	if e := x.f.db.QueryRow("SELECT partition_id,record_offset FROM sync_projection_deliveries WHERE version_id=? AND component='es' AND ticket_id=4 AND ticket_version=11 AND status='processing'", x.version.ID).Scan(&ref.Partition, &ref.Offset); e != nil {
		t.Fatal(e)
	}
	raw, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if e != nil {
		t.Fatal(e)
	}
	o, ok := raw.Lookup(ref.Topic, ref.Partition)
	if !ok || o.At > ref.Offset {
		t.Fatal("failed progress offset committed")
	}
	x.f.exec(t, "DROP TRIGGER m403b_es_progress")
	es = x.process(t, "es")
	x.processRunning(t, es, "es")
	m403bWait(t, "ES replay terminal", func() bool {
		var s string
		x.f.db.QueryRow("SELECT status FROM sync_projection_deliveries WHERE version_id=? AND component='es' AND partition_id=? AND record_offset=?", x.version.ID, ref.Partition, ref.Offset).Scan(&s)
		return s == "duplicate_or_old"
	})
	x.f.exec(t, "CREATE TRIGGER m403b_ck_progress BEFORE UPDATE ON sync_projection_deliveries FOR EACH ROW BEGIN IF NEW.component='ck' AND NEW.ticket_id=4 AND NEW.ticket_version=12 AND NEW.status='applied' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='M403B CK write before progress'; END IF; END")
	current = x.f.actionHTTP(t, "local-device-agent", "comments", 4, current.Version, map[string]any{"content": "new v12 CK progress failure"})
	ck.failedExit(t)
	if m4CKSQL(t, x.control.CK, "SELECT max(version) FROM ticket_states WHERE ticket_id=4 FORMAT TabSeparated") != "12" {
		t.Fatal("both CK projections not committed")
	}
	var ckRef syncpipe.RecordRef
	ckRef.Topic = ref.Topic
	if e = x.f.db.QueryRow("SELECT partition_id,record_offset FROM sync_projection_deliveries WHERE version_id=? AND component='ck' AND ticket_id=4 AND ticket_version=12 AND status='processing'", x.version.ID).Scan(&ckRef.Partition, &ckRef.Offset); e != nil {
		t.Fatal(e)
	}
	raw, e = x.f.queue.Admin.FetchOffsets(context.Background(), x.h.CKGroup)
	if e != nil {
		t.Fatal(e)
	}
	o, ok = raw.Lookup(ref.Topic, ckRef.Partition)
	if !ok || o.At > ckRef.Offset {
		t.Fatal("failed CK progress committed")
	}
	physicalBefore := m4CKSQL(t, x.control.CK, "SELECT count() FROM ticket_events WHERE ticket_id=4 AND version=12 FORMAT TabSeparated")
	x.f.exec(t, "DROP TRIGGER m403b_ck_progress")
	ck = x.process(t, "ck")
	x.processRunning(t, ck, "ck")
	m403bWait(t, "CK replay committed", func() bool {
		raw, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.CKGroup)
		o, _ := raw.Lookup(ref.Topic, ckRef.Partition)
		return e == nil && o.At > ckRef.Offset
	})
	physicalAfter := m4CKSQL(t, x.control.CK, "SELECT count() FROM ticket_events WHERE ticket_id=4 AND version=12 FORMAT TabSeparated")
	if physicalBefore == physicalAfter {
		t.Fatal("CK physical replay did not happen")
	}
	// Kill a healthy process after commits; its replacement must retain and
	// advance current Kafka positions, rather than return to the M4 start.
	beforeKill, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if e != nil {
		t.Fatal(e)
	}
	es.kill(t)
	current = x.f.actionHTTP(t, "local-device-agent", "comments", 4, current.Version, map[string]any{"content": "while candidate ES killed"})
	es = x.process(t, "es")
	x.processRunning(t, es, "es")
	m403bWait(t, "post SIGKILL current version", func() bool {
		return doc(x.control.ES.Index()) == current.Version && doc(x.f.es.Index()) == current.Version
	})
	afterKill, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if e != nil {
		t.Fatal(e)
	}
	for part, b := range beforeKill[ref.Topic] {
		a, ok := afterKill.Lookup(ref.Topic, part)
		if !ok || a.At < b.At {
			t.Fatal("SIGKILL reset group commit")
		}
	}
	m403bWait(t, "new target CK after SIGKILL", func() bool {
		return m4CKSQL(t, x.control.CK, "SELECT max(version) FROM ticket_states WHERE ticket_id=4 FORMAT TabSeparated") == fmt.Sprint(current.Version)
	})
	report, e := syncpipe.Reconcile(context.Background(), x.f.state, nil, x.control.ES, x.control.CK, syncpipe.ReconcileOptions{})
	if e != nil || report.State != "consistent" {
		t.Fatal(report.State, e)
	}
	m403bSave(t, "progress-failure-and-recovery", map[string]any{"ES_record": ref, "CK_record": ckRef, "CK_physical_before": physicalBefore, "CK_physical_after": physicalAfter, "before_SIGKILL": beforeKill, "after_SIGKILL": afterKill, "report": report})
}

var _ = kadm.Offsets{}
var _ = kmsg.NewPtrLeaveGroupRequest

func TestM403BOffsetCommitFailureAndGroupRebalance(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	target := x.target(t)
	q := *x.f.queue
	q.Target = target
	var mu sync.Mutex
	events := []map[string]any{}
	blocked := make(chan syncpipe.RecordRef, 1)
	release := make(chan struct{})
	var injected atomic.Bool
	target.Observe = func(stage string, value any) {
		mu.Lock()
		events = append(events, map[string]any{"stage": stage, "value": value, "at": time.Now().UTC()})
		mu.Unlock()
		if stage == "before_record_commit" && injected.CompareAndSwap(false, true) {
			blocked <- value.(syncpipe.RecordRef)
			<-release
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&syncpipe.Consumer{Store: x.f.state, Queue: &q, ES: x.control.ES, Target: target}).Run(ctx)
	}()
	t.Cleanup(func() { cancel() })
	// A real record from retained source history is already fully present in
	// both candidate projections. Its duplicate must not alter Gold.
	event := x.f.event(t, 4, 8, "m403b-real-commit-fault")
	if _, e := x.f.queue.Publish(context.Background(), event); e != nil {
		t.Fatal(e)
	}
	var ref syncpipe.RecordRef
	select {
	case ref = <-blocked:
	case <-time.After(15 * time.Second):
		t.Fatal("record never reached commit boundary")
	}
	groups, e := x.f.queue.Admin.DescribeGroups(context.Background(), x.h.ESGroup)
	if e != nil {
		t.Fatal(e)
	}
	g := groups[x.h.ESGroup]
	if len(g.Members) != 1 {
		t.Fatal("candidate should have one writer member")
	}
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Group = x.h.ESGroup
	m := kmsg.NewLeaveGroupRequestMember()
	m.MemberID = g.Members[0].MemberID
	req.Members = append(req.Members, m)
	response, e := req.RequestWith(context.Background(), x.f.queue.Client)
	if e != nil || response.ErrorCode != 0 {
		t.Fatal("real coordinator LeaveGroup failed", e, response)
	}
	close(release)
	select {
	case e = <-done:
		if e == nil || !strings.Contains(e.Error(), "offset commit failed") {
			t.Fatal("record commit failure not observed", e)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("revoked consumer did not stop")
	}
	raw, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if e != nil {
		t.Fatal(e)
	}
	committed, _ := raw.Lookup(ref.Topic, ref.Partition)
	if committed.At > ref.Offset {
		t.Fatal("failed Kafka commit passed record")
	}
	var status string
	x.f.db.QueryRow("SELECT status FROM sync_projection_deliveries WHERE version_id=? AND component='es' AND partition_id=? AND record_offset=?", x.version.ID, ref.Partition, ref.Offset).Scan(&status)
	if status != "applied" && status != "duplicate_or_old" {
		t.Fatal("projection terminal row missing before commit fault", status)
	}
	x.run(t, "es")
	x.run(t, "ck")
	x.verify(t)
	mu.Lock()
	copyEvents := append([]map[string]any{}, events...)
	mu.Unlock()
	m403bSave(t, "real-commit-failure", map[string]any{"record": ref, "coordinator_response": response, "committed_before_replay": raw, "events": copyEvents, "handoff": x.h})
}
func TestM403BDuplicateCandidateProcessRejectedAndLockLoss(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	es := x.process(t, "es")
	ck := x.process(t, "ck")
	x.processRunning(t, es, "es")
	x.processRunning(t, ck, "ck")
	x.verify(t)
	duplicate := x.process(t, "es")
	duplicate.failedExit(t)
	log, e := os.ReadFile(duplicate.log)
	if e != nil || !strings.Contains(string(log), "PROJECTION_WRITER_OWNERSHIP") {
		t.Fatal("duplicate process not refused before projection", e, string(log))
	}
	owner, e := projection.ReadOwner(context.Background(), x.f.db, projection.VersionTargetKey(x.version, "es"), "")
	if e != nil || owner.ConnectionID == nil {
		t.Fatal(e)
	}
	if e = es.cmd.Process.Signal(syscall.SIGSTOP); e != nil {
		t.Fatal(e)
	}
	defer es.cmd.Process.Signal(syscall.SIGCONT)
	x.f.exec(t, fmt.Sprintf("KILL CONNECTION %d", *owner.ConnectionID))
	// Once the named lock vanishes, the live PID remains a persisted refusal
	// condition. A competing process cannot infer death from the free lock.
	competing := x.process(t, "es")
	competing.failedExit(t)
	if e = es.cmd.Process.Signal(syscall.SIGCONT); e != nil {
		t.Fatal(e)
	}
	es.failedExit(t)
	replacement := x.process(t, "es")
	x.processRunning(t, replacement, "es")
	before := owner.Epoch
	after, e := projection.ReadOwner(context.Background(), x.f.db, owner.Key, "")
	if e != nil || after.Epoch <= before {
		t.Fatal("owner epoch did not advance after proved death", e)
	}
	m403bSave(t, "writer-ownership", map[string]any{"before_connection_loss": owner, "replacement": after, "duplicate_log": string(log), "boundary": "named lock loss refuses live stale PID; remote/unknown writer never auto-preempted"})
}

func TestM403BCommittedOffsetMissingRangeAndActualTargetMismatch(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	target := x.target(t)
	q := *x.f.queue
	q.Target = target
	if e := (&syncpipe.Consumer{Store: x.f.state, Queue: &q, ES: x.f.es, Target: target}).Run(context.Background()); !errors.Is(e, projection.ErrTargetMismatch) {
		t.Fatal("actual wrong ES writer accepted", e)
	}
	if e := (&syncpipe.CKConsumer{Store: x.f.state, Queue: &q, CK: x.f.ck, Target: target}).Run(context.Background()); !errors.Is(e, projection.ErrTargetMismatch) {
		t.Fatal("actual wrong CK writer accepted", e)
	}
	ends, e := x.f.queue.Admin.ListEndOffsets(context.Background(), x.f.queue.Topic)
	if e != nil {
		t.Fatal(e)
	}
	end, _ := ends.Lookup(x.f.queue.Topic, 0)
	bad := kadm.Offsets{}
	bad.Add(kadm.Offset{Topic: x.f.queue.Topic, Partition: 0, At: end.Offset + 50, LeaderEpoch: -1})
	if e = x.f.queue.Admin.CommitAllOffsets(context.Background(), x.h.ESGroup, bad); e != nil {
		t.Fatal(e)
	}
	if _, w, e := target.Acquire(context.Background(), "es"); e == nil {
		w.Close()
		t.Fatal("out of range committed offset accepted")
	} else {
		m403bSave(t, "out-of-range", map[string]any{"error": e.Error(), "end": end.Offset, "committed": end.Offset + 50})
	}
	// Explicit test fault restoration is not a production initialization path.
	good := kadm.Offsets{}
	good.Add(kadm.Offset{Topic: x.f.queue.Topic, Partition: 0, At: x.h.PinnedOffsets[0], LeaderEpoch: -1})
	if e = x.f.queue.Admin.CommitAllOffsets(context.Background(), x.h.ESGroup, good); e != nil {
		t.Fatal(e)
	}
	topics := kadm.TopicsSet{}
	topics.Add(x.f.queue.Topic, 1)
	deleted, e := x.f.queue.Admin.DeleteOffsets(context.Background(), x.h.ESGroup, topics)
	if e != nil || deleted.Error() != nil {
		t.Fatal(e, deleted.Error())
	}
	if _, w, e := target.Acquire(context.Background(), "es"); e == nil {
		w.Close()
		t.Fatal("missing committed partition accepted")
	} else {
		m403bSave(t, "missing-commit", map[string]any{"error": e.Error(), "partition": 1})
	}
	// Direct client verifies AtCommitted itself, in addition to startup checks.
	// Use a direct group client to observe franz-go's AtCommitted error itself,
	// separately from our earlier startup and assignment validation failures.
	client, e := kgo.NewClient(kgo.SeedBrokers(q.Brokers...), kgo.ConsumerGroup(x.h.ESGroup), kgo.ConsumeTopics(q.Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtCommitted()), kgo.DisableAutoCommit())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	fetch := client.PollRecords(ctx, 16)
	for fetch.Err() == nil && ctx.Err() == nil {
		fetch = client.PollRecords(ctx, 16)
	}
	cancel()
	client.AllowRebalance()
	client.Close()
	if fetch.Err() == nil || !strings.Contains(fetch.Err().Error(), "no prior committed offset") {
		t.Fatal("AtCommitted client did not explicitly reject missing commit", fetch.Err())
	}
	m403bSave(t, "client-missing-offset", map[string]any{"error": fetch.Err().Error()})
	if _, e = x.control.Initialize(context.Background(), x.h.ID); e == nil {
		t.Fatal("CONSUMING reinitialization allowed")
	}
}
func TestM403BTopicPartitionAndTargetIdentityChanges(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	target := x.target(t)
	// Every mutation here targets this freshly allocated fixture.
	uuidBefore := x.version.Contract.ES.UUID
	m4HTTP(t, "DELETE", x.control.ES.Endpoint()+"/"+x.control.ES.Index(), nil, 200)
	if e := x.control.ES.EnsureIndex(context.Background()); e != nil {
		t.Fatal(e)
	}
	uuidAfter, _, e := x.control.ES.Identity(context.Background())
	if e != nil || uuidBefore == uuidAfter {
		t.Fatal("UUID injector did not change target")
	}
	if e = target.Check(context.Background()); !errors.Is(e, projection.ErrTargetMismatch) {
		t.Fatal("ES UUID change accepted", e)
	}
	m403bSave(t, "ES-UUID-refusal", map[string]any{"before": uuidBefore, "after": uuidAfter, "error": e.Error()})
	// Independent fixture for Topic partition mutation, so ES refusal cannot
	// accidentally serve as proof of Kafka identity rejection.
}
func TestM403BTopicIdentityAndPartitionRefusal(t *testing.T) {
	for _, kind := range []string{"partitions", "uuid"} {
		t.Run(kind, func(t *testing.T) {
			x := m403bNew(t)
			x.pin(t)
			x.open(t)
			target := x.target(t)
			x.f.stopCapture(t)
			x.f.stopConsumer(t)
			x.f.stopCK(t)
			if kind == "partitions" {
				reply, e := x.f.queue.Admin.CreatePartitions(context.Background(), 4, x.f.queue.Topic)
				if e != nil || reply.Error() != nil {
					t.Fatal(e, reply.Error())
				}
			} else {
				response, e := x.f.queue.Admin.DeleteTopics(context.Background(), x.f.queue.Topic)
				if e != nil || response[x.f.queue.Topic].Err != nil {
					t.Fatal(e)
				}
				m403bWait(t, "Topic deletion", func() bool {
					topics, e := x.f.queue.Admin.ListTopics(context.Background(), x.f.queue.Topic)
					return e == nil && (topics[x.f.queue.Topic].Err != nil || len(topics) == 0)
				})
				created, e := x.f.queue.Admin.CreateTopics(context.Background(), 3, 1, map[string]*string{"cleanup.policy": kadm.StringPtr("delete")}, x.f.queue.Topic)
				if e != nil || created[x.f.queue.Topic].Err != nil {
					t.Fatal(e, created[x.f.queue.Topic].Err)
				}
			}
			e := target.Check(context.Background())
			if !errors.Is(e, projection.ErrTargetMismatch) {
				t.Fatal("Topic mutation accepted", e)
			}
			m403bSave(t, "identity-refused", map[string]any{"kind": kind, "error": e.Error(), "prior_topic": x.version.Contract.Kafka})
		})
	}
}

// Wire fault forwards an actual request to the private broker and withholds
// only the real OffsetCommit response. It cannot manufacture a successful ACK.
type m403bCommitWire struct {
	net.Conn
	responseMode string
	correlation  atomic.Uint32
	pending      atomic.Bool
	buffer       []byte
	dropped      *atomic.Int32
}

func (c *m403bCommitWire) Write(data []byte) (int, error) {
	if len(data) >= 14 && int16(binary.BigEndian.Uint16(data[4:6])) == 8 {
		c.correlation.Store(binary.BigEndian.Uint32(data[8:12]))
		c.pending.Store(true)
	}
	return c.Conn.Write(data)
}
func (c *m403bCommitWire) Read(data []byte) (int, error) {
	if len(c.buffer) == 0 {
		header := make([]byte, 4)
		if _, e := io.ReadFull(c.Conn, header); e != nil {
			return 0, e
		}
		size := int(binary.BigEndian.Uint32(header))
		body := make([]byte, size)
		if _, e := io.ReadFull(c.Conn, body); e != nil {
			return 0, e
		}
		if len(body) >= 4 && c.pending.Load() && binary.BigEndian.Uint32(body[:4]) == c.correlation.Load() {
			c.pending.Store(false)
			c.dropped.Add(1)
			if c.responseMode == "timeout" {
				c.Conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
				_, e := c.Conn.Read(make([]byte, 1))
				return 0, e
			}
			c.Conn.Close()
			return 0, io.ErrUnexpectedEOF
		}
		c.buffer = append(header, body...)
	}
	n := copy(data, c.buffer)
	c.buffer = c.buffer[n:]
	return n, nil
}
func TestM403BRealKafkaCommitResponseTimeout(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	var dropped atomic.Int32
	client, e := kgo.NewClient(kgo.SeedBrokers(x.f.queue.Brokers...), kgo.RequestRetries(0), kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
		expectedBroker := "127.0.0.1:19102"
		if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c3" {
			expectedBroker = "127.0.0.1:19108"
		}
		if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c2" {
			expectedBroker = "127.0.0.1:19106"
		}
		if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c1" {
			expectedBroker = "127.0.0.1:19104"
		}
		if address != expectedBroker {
			return nil, fmt.Errorf("wire fault refused non-independent broker")
		}
		conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return &m403bCommitWire{Conn: conn, responseMode: "timeout", dropped: &dropped}, nil
	}))
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	faultQueue := *x.f.queue
	faultQueue.Client = client
	faultQueue.Admin = kadm.NewClient(client)
	pc := *x.control.ProjectionControl
	pc.Queue = &faultQueue
	c := &syncpipe.HandoffControl{ProjectionControl: &pc}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	h, e := c.Initialize(ctx, x.h.ID)
	cancel()
	if e == nil {
		t.Fatal("withheld response did not fail initialize")
	}
	if dropped.Load() == 0 {
		t.Fatal("wire fault did not intercept broker OffsetCommit response")
	}
	raw, err := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if err != nil || len(raw[x.f.queue.Topic]) == 0 {
		t.Fatal("unknown-outcome offset not retained", err, raw)
	}
	m403bSave(t, "real-timeout-before-retry", map[string]any{"error": e.Error(), "responses_withheld": dropped.Load(), "phase": h.Phase, "raw_readback": raw, "fault": "real broker response read and withheld on private 19102 socket"})
	x.open(t)
	x.run(t, "es")
	x.run(t, "ck")
	x.verify(t)
	m403bSave(t, "recovered", x.h)
}
func TestM403BKafkaInitialPartitionErrorsAndGroupNotEmpty(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	// Kafka's non-member OffsetCommit cannot initialize a Stable group. Use
	// the real coordinator response to verify top-level nil/partition errors.
	member, e := kgo.NewClient(kgo.SeedBrokers(x.f.queue.Brokers...), kgo.ConsumerGroup(x.h.ESGroup), kgo.ConsumeTopics(x.f.queue.Topic), kgo.DisableAutoCommit())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			member.PollRecords(ctx, 1)
		}
	}()
	m403bWait(t, "unapproved new group member", func() bool {
		groups, e := x.f.queue.Admin.DescribeGroups(context.Background(), x.h.ESGroup)
		return e == nil && groups[x.h.ESGroup].State == "Stable" && len(groups[x.h.ESGroup].Members) == 1
	})
	if _, e = x.control.Initialize(context.Background(), x.h.ID); e == nil || !strings.Contains(e.Error(), "NOT_EMPTY") {
		t.Fatal("initializer did not refuse group member", e)
	}
	offsets := kadm.Offsets{}
	for part, n := range x.h.PinnedOffsets {
		offsets.Add(kadm.Offset{Topic: x.f.queue.Topic, Partition: part, At: n, LeaderEpoch: -1})
	}
	responses, topError := x.f.queue.Admin.CommitOffsets(context.Background(), x.h.ESGroup, offsets)
	if topError != nil || responses.Error() == nil {
		t.Fatal("real per-partition Kafka error not observed", topError, responses)
	}
	m403bSave(t, "real-partition-errors", map[string]any{"top_level_error": fmt.Sprint(topError), "partition_error": responses.Error().Error(), "response": responses})
	cancel()
	<-done
	member.Close()
	x.open(t)
	m403bSave(t, "initialized-after-member-left", x.h)
}
func TestM403BNormalConsumerRebalancePreservesPositions(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	target := x.target(t)
	q := *x.f.queue
	q.Target = target
	var mu sync.Mutex
	events := []map[string]any{}
	target.Observe = func(stage string, v any) {
		mu.Lock()
		events = append(events, map[string]any{"stage": stage, "value": v})
		mu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&syncpipe.Consumer{Store: x.f.state, Queue: &q, ES: x.control.ES, Target: target}).Run(ctx)
	}()
	m403bWait(t, "single candidate group assigned", func() bool {
		groups, e := x.f.queue.Admin.DescribeGroups(context.Background(), x.h.ESGroup)
		return e == nil && groups[x.h.ESGroup].State == "Stable" && len(groups[x.h.ESGroup].Members) == 1
	})
	// A broker-only test member induces an actual rebalance and performs no
	// projection writes or commits. Normal duplicate writer admission is tested
	// separately and refused before it can join.
	peer, e := kgo.NewClient(kgo.SeedBrokers(x.f.queue.Brokers...), kgo.ConsumerGroup(x.h.ESGroup), kgo.ConsumeTopics(x.f.queue.Topic), kgo.DisableAutoCommit())
	if e != nil {
		t.Fatal(e)
	}
	peerCtx, peerCancel := context.WithCancel(context.Background())
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		for peerCtx.Err() == nil {
			peer.PollRecords(peerCtx, 1)
		}
	}()
	m403bWait(t, "two Kafka members balanced", func() bool {
		groups, e := x.f.queue.Admin.DescribeGroups(context.Background(), x.h.ESGroup)
		return e == nil && groups[x.h.ESGroup].State == "Stable" && len(groups[x.h.ESGroup].Members) == 2
	})
	before, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if e != nil {
		t.Fatal(e)
	}
	peerCancel()
	<-peerDone
	peer.Close()
	m403bWait(t, "candidate regained all partitions", func() bool {
		groups, e := x.f.queue.Admin.DescribeGroups(context.Background(), x.h.ESGroup)
		return e == nil && groups[x.h.ESGroup].State == "Stable" && len(groups[x.h.ESGroup].Members) == 1
	})
	x.run(t, "ck")
	x.verify(t)
	after, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if e != nil {
		t.Fatal(e)
	}
	for part, b := range before[x.f.queue.Topic] {
		a, _ := after.Lookup(x.f.queue.Topic, part)
		if a.At < b.At {
			t.Fatal("rebalance rewound group")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not exit")
	}
	mu.Lock()
	copied := append([]map[string]any{}, events...)
	mu.Unlock()
	revoked := false
	assigned := 0
	for _, v := range copied {
		if v["stage"] == "revoked" {
			revoked = true
		}
		if v["stage"] == "assigned" {
			assigned++
		}
	}
	if !revoked || assigned < 2 {
		t.Fatal("real callbacks missing", copied)
	}
	m403bSave(t, "rebalance", map[string]any{"events": copied, "before": before, "after": after, "handoff": x.h, "projection_writers": 1})
}

func TestM403BEmptyPartitionsOfflineCommitAtLogEnd(t *testing.T) {
	m403bOwned(t)
	f := newM2(t)
	if e := f.state.VerifyQueue(context.Background(), f.queue); e != nil {
		t.Fatal(e)
	}
	before, e := f.queue.Admin.ListEndOffsets(context.Background(), f.queue.Topic)
	if e != nil {
		t.Fatal(e)
	}
	group := projection.TargetGroup(f.queue.Topic, "empty_candidate", "es")
	defer f.queue.Admin.DeleteGroups(context.Background(), group)
	offsets := kadm.Offsets{}
	for part := int32(0); part < 3; part++ {
		end, ok := before.Lookup(f.queue.Topic, part)
		if !ok || end.Offset != 0 {
			t.Fatal("fixture partition not empty")
		}
		offsets.Add(kadm.Offset{Topic: f.queue.Topic, Partition: part, At: end.Offset, LeaderEpoch: -1})
	}
	responses, e := f.queue.Admin.CommitOffsets(context.Background(), group, offsets)
	if e != nil || responses.Error() != nil {
		t.Fatal(e, responses.Error())
	}
	raw, e := f.queue.Admin.FetchOffsets(context.Background(), group)
	if e != nil {
		t.Fatal(e)
	}
	for part := int32(0); part < 3; part++ {
		o, ok := raw.Lookup(f.queue.Topic, part)
		if !ok || o.At != 0 {
			t.Fatal("empty partition commit missing")
		}
	}
	client, e := kgo.NewClient(kgo.SeedBrokers(f.queue.Brokers...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(f.queue.Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtCommitted()), kgo.DisableAutoCommit())
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	fetch := client.PollRecords(ctx, 1)
	cancel()
	if fetch.NumRecords() != 0 {
		t.Fatal("empty topic produced record")
	}
	created := f.create(t, 1, request("m403b-empty-first"))
	event := syncpipe.Event{Schema: 1, Source: "snapshot", ID: "empty-first-event", TicketID: created.Ticket.ID, Version: 1, EventAt: created.Ticket.UpdatedAt, Document: search.FromTicket(created.Ticket)}
	ref, e := f.queue.Publish(context.Background(), event)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	fetch = client.PollRecords(ctx, 1)
	cancel()
	if fetch.NumRecords() != 1 || fetch.Records()[0].Offset != ref.Offset {
		t.Fatal("AtCommitted empty group missed first record", fetch.Err())
	}
	m403bSave(t, "empty-group", map[string]any{"before": before, "response": responses, "readback": raw, "first_record": ref})
}
func TestM403BVersionStateIsolationAndFailureRefusesVerify(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	// A valid transport envelope with an invalid ES field is quarantined only
	// in both targets' independent delivery namespaces.
	x.run(t, "es")
	x.run(t, "ck")
	payload := []byte(`{"schema":2,"kind":"ticket","source":"snapshot","event_id":"m403b-invalid-independent","ticket_id":4,"version":99,"event_at":"2026-11-01T00:00:00Z","document":{}}`)
	ref, e := x.f.queue.PublishBytes(context.Background(), payload, "m403b-invalid-independent")
	if e != nil {
		t.Fatal(e)
	}
	m403bWait(t, "both isolated records", func() bool {
		var n int
		x.f.db.QueryRow("SELECT COUNT(*) FROM sync_projection_deliveries WHERE version_id=? AND topic=? AND partition_id=? AND record_offset=? AND status='isolated'", x.version.ID, ref.Topic, ref.Partition, ref.Offset).Scan(&n)
		return n == 2
	})
	var legacyCount int
	if e = x.f.db.QueryRow("SELECT COUNT(*) FROM sync_kafka_es_deliveries WHERE topic=? AND partition_id=? AND record_offset=? AND status='isolated'", ref.Topic, ref.Partition, ref.Offset).Scan(&legacyCount); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	_, e = x.control.Verify(ctx, x.h.ID)
	cancel()
	if e == nil {
		t.Fatal("isolated candidate verified")
	}
	if _, e = projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e == nil {
		t.Fatal("isolated candidate resumed business")
	}
	h, e := (&projection.HandoffStore{DB: x.f.db}).Get(context.Background(), x.h.ID)
	if e != nil || h.Phase != projection.Consuming {
		t.Fatal(e)
	}
	// Read-only status objects select candidate data by version, while the
	// ordinary Search reader continues to use the original ACTIVE table.
	candidate, e := (&syncpipe.TargetStatus{Target: x.target(t)}).SearchFreshness(context.Background())
	if e != nil || candidate.State != "degraded" {
		t.Fatal("candidate freshness did not show isolation", candidate, e)
	}
	current, e := x.f.store.SearchFreshness(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	m403bSave(t, "independent-failure-state", map[string]any{"record": ref, "candidate": candidate, "legacy": current, "legacy_isolation_rows": legacyCount, "handoff": h, "count_candidate_isolated": 2})
}
func TestM403BVersionRecoveryNamespaceAndOldState(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	x.f.stopConsumer(t) // keep the original row fixed during namespace-only recovery
	target := x.target(t)
	store, lease, e := target.Acquire(context.Background(), "es")
	if e != nil {
		t.Fatal(e)
	}
	defer lease.Close()
	if e = store.ForConsumer("ck").Heartbeat(context.Background(), "ck", "running", nil, nil); !errors.Is(e, projection.ErrWriterOwnership) {
		t.Fatal("ES writer admitted CK progress scope", e)
	}
	wrong := *store
	wrong.VersionID = "active_original"
	if e = wrong.Heartbeat(context.Background(), "es", "running", nil, nil); !errors.Is(e, projection.ErrWriterOwnership) {
		t.Fatal("writer admitted another version progress scope", e)
	}
	// KeepAlive's compatibility signature returns only a stop function. A wrong
	// component must still be refused rather than refreshing the other writer's
	// row when its numeric owner epoch happens to equal ours.
	ckStore, ckLease, e := x.target(t).Acquire(context.Background(), "ck")
	if e != nil {
		t.Fatal(e)
	}
	defer ckLease.Close()
	if e = ckStore.Heartbeat(context.Background(), "ck", "running", nil, nil); e != nil {
		t.Fatal(e)
	}
	x.f.exec(t, "UPDATE sync_projection_worker_state SET heartbeat_at='2001-01-01 00:00:00' WHERE component='ck'")
	wrongBeat := store.KeepAlive(context.Background(), "ck")
	defer wrongBeat()
	time.Sleep(1200 * time.Millisecond)
	wrongBeat()
	var ckBeat time.Time
	if e = x.f.db.QueryRow("SELECT heartbeat_at FROM sync_projection_worker_state WHERE version_id=? AND component='ck'", x.version.ID).Scan(&ckBeat); e != nil || ckBeat.Year() != 2001 {
		t.Fatal("ES lease refreshed CK heartbeat", ckBeat, e)
	}
	event := x.f.event(t, 4, 8, "m403b-recover-same-physical-record")
	ref, e := x.f.queue.Publish(context.Background(), event)
	if e != nil {
		t.Fatal(e)
	}
	payload, _ := json.Marshal(event)
	problem := fmt.Errorf("independent recovery fixture")
	if e = store.Delivery(context.Background(), ref, event, "isolated", 1, problem, payload); e != nil {
		t.Fatal(e)
	}
	// Original ACTIVE remains separately marked. Candidate recovery cannot
	// alter it even when the transport record identity is exactly the same.
	if e = x.f.state.Delivery(context.Background(), ref, event, "isolated", 1, problem, payload); e != nil {
		t.Fatal(e)
	}
	got, e := store.ReplayPayload(context.Background(), ref)
	if e != nil || !reflect.DeepEqual(got, payload) {
		t.Fatal("version-scoped replay payload mismatch", e)
	}
	recovery, e := x.f.queue.PublishBytes(context.Background(), payload, "m403b-recovery-transport")
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Delivery(context.Background(), recovery, event, "applied", 1, nil, nil); e != nil {
		t.Fatal(e)
	}
	if e = store.RecordRecovery(context.Background(), ref, recovery); e != nil {
		t.Fatal(e)
	}
	var newState, oldState string
	x.f.db.QueryRow("SELECT status FROM sync_projection_deliveries WHERE version_id=? AND component='es' AND partition_id=? AND record_offset=?", x.version.ID, ref.Partition, ref.Offset).Scan(&newState)
	x.f.db.QueryRow("SELECT status FROM sync_kafka_es_deliveries WHERE partition_id=? AND record_offset=?", ref.Partition, ref.Offset).Scan(&oldState)
	if newState != "recovered" || oldState != "isolated" {
		t.Fatal("recovery crossed target version", newState, oldState)
	}
	// Stopped/failed candidate state does not alter ACTIVE's running heartbeat.
	if e = store.Heartbeat(context.Background(), "es", "failed", nil, problem); e != nil {
		t.Fatal(e)
	}
	fresh, e := x.f.store.SearchFreshness(context.Background())
	if e != nil || fresh.State != "stopped" {
		t.Fatal(fresh, e)
	}
	m403bSave(t, "recovery-isolation", map[string]any{"original": ref, "recovery": recovery, "candidate_original": newState, "active_original": oldState, "active_freshness": fresh, "wrong_component_keepalive_refused": true, "ck_heartbeat_after_refused_refresh": ckBeat})
}

func TestM403BMissingHandoffMetadataCannotResume(t *testing.T) {
	x := m403bNew(t)
	x.pin(t)
	x.f.exec(t, "RENAME TABLE sync_projection_handoffs TO sync_projection_handoffs_test_unavailable")
	_, e := projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID)
	if e == nil || !strings.Contains(e.Error(), "METADATA_UNAVAILABLE") {
		t.Fatal("missing handoff metadata downgraded resume", e)
	}
	if _, failure := x.r.Check(context.Background(), x.job.ID); failure == nil || !strings.Contains(failure.Error(), "METADATA_UNAVAILABLE") {
		t.Fatal("missing handoff metadata downgraded M4 admission", failure)
	}
	x.f.exec(t, "RENAME TABLE sync_projection_handoffs_test_unavailable TO sync_projection_handoffs")
	// Even when both admission and target-owner metadata become unavailable,
	// preserved installation provenance must prevent legacy fallback.
	x.f.exec(t, "RENAME TABLE sync_projection_handoffs TO sync_projection_handoffs_test_unavailable, sync_projection_target_owners TO sync_projection_target_owners_test_unavailable")
	_, m4Failure := x.r.Check(context.Background(), x.job.ID)
	if m4Failure == nil || !strings.Contains(m4Failure.Error(), "METADATA_UNAVAILABLE") {
		t.Fatal("missing handoff and owner metadata admitted sealed M4", m4Failure)
	}
	legacyFailure := (&syncpipe.Consumer{Store: x.f.state, Queue: x.f.queue, ES: x.control.ES}).Run(context.Background())
	if legacyFailure == nil || !strings.Contains(legacyFailure.Error(), "METADATA_UNAVAILABLE") {
		t.Fatal("missing owner metadata admitted unmanaged candidate writer", legacyFailure)
	}
	x.f.exec(t, "RENAME TABLE sync_projection_handoffs_test_unavailable TO sync_projection_handoffs, sync_projection_target_owners_test_unavailable TO sync_projection_target_owners")
	m403bSave(t, "resume-refused", map[string]any{"error": e.Error(), "restored_table": true, "missing_metadata_m4_error": m4Failure.Error(), "missing_metadata_legacy_consumer_error": legacyFailure.Error()})
}

func TestM403BCKIdentityAndRetainedOffsetsRefuseRecovery(t *testing.T) {
	t.Run("ck_uuid", func(t *testing.T) {
		x := m403bNew(t)
		x.pin(t)
		x.open(t)
		target := x.target(t)
		before, exists, err := x.control.CK.Identity(context.Background())
		if err != nil || !exists {
			t.Fatal("candidate CK identity unavailable", err)
		}
		m4CKSQL(t, x.control.CK, "RENAME TABLE ticket_states TO ticket_states_before_uuid_fault")
		// The retired table and every mutation remain inside this candidate's
		// freshly allocated database. Recreating the pair changes only its UUID.
		if err = x.control.CK.Ensure(context.Background()); err != nil {
			t.Fatal(err)
		}
		after, exists, err := x.control.CK.Identity(context.Background())
		if err != nil || !exists || after.States == before.States || after.Events != before.Events {
			t.Fatal("CK table UUID fault did not occur", before, after, err)
		}
		err = target.Check(context.Background())
		if !errors.Is(err, projection.ErrTargetMismatch) {
			t.Fatal("candidate CK UUID replacement accepted", err)
		}
		m403bSave(t, "ck-uuid-refused", map[string]any{"before": before, "after": after, "error": err.Error()})
	})
	for _, phase := range []string{"PINNED", "OFFSETS_READY", "CONSUMING"} {
		t.Run(phase, func(t *testing.T) {
			x := m403bNew(t)
			x.pin(t)
			var err error
			if phase != "PINNED" {
				x.h, err = x.control.Initialize(context.Background(), x.h.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if phase == "CONSUMING" {
				x.h, err = x.control.OpenConsumption(context.Background(), x.h.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			// Stop only this fixture's producers/workers before deleting its
			// retained prefix; no demo Topic or committed position is touched.
			x.f.stopCapture(t)
			x.f.stopConsumer(t)
			x.f.stopCK(t)
			event := x.f.event(t, 4, 8, "m403b-retention-prefix")
			ref, err := x.f.queue.Publish(context.Background(), event)
			if err != nil || ref.Offset < x.h.PinnedOffsets[ref.Partition] {
				t.Fatal("retained fault record missing", ref, err)
			}
			requested := kadm.Offsets{}
			requested.Add(kadm.Offset{Topic: ref.Topic, Partition: ref.Partition, At: ref.Offset + 1, LeaderEpoch: -1})
			deleted, err := x.f.queue.Admin.DeleteRecords(context.Background(), requested)
			if err != nil || deleted.Error() != nil {
				t.Fatal("private retained prefix fault failed", err, deleted.Error())
			}
			starts, err := x.f.queue.Admin.ListStartOffsets(context.Background(), ref.Topic)
			start, found := starts.Lookup(ref.Topic, ref.Partition)
			if err != nil || !found || start.Err != nil || start.Offset <= x.h.PinnedOffsets[ref.Partition] {
				t.Fatal("pinned offset was not expired by real Kafka", start, err)
			}
			switch phase {
			case "PINNED":
				_, err = x.control.Initialize(context.Background(), x.h.ID)
			case "OFFSETS_READY":
				_, err = x.control.OpenConsumption(context.Background(), x.h.ID)
			case "CONSUMING":
				target := x.target(t)
				_, lease, failure := target.Acquire(context.Background(), "es")
				err = failure
				if lease != nil {
					lease.Close()
				}
			}
			if err == nil || (!strings.Contains(err.Error(), "OFFSET") && !strings.Contains(err.Error(), "outside retained log")) {
				t.Fatal("expired handoff offset accepted", err)
			}
			h, failure := (&projection.HandoffStore{DB: x.f.db}).Get(context.Background(), x.h.ID)
			if failure != nil || h.Phase != phase {
				t.Fatal("retention fault advanced handoff phase", h.Phase, failure)
			}
			if _, failure = projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); failure == nil {
				t.Fatal("retention fault resumed business")
			}
			m403bSave(t, "retained-offset-refused", map[string]any{"phase": phase, "pinned": x.h.PinnedOffsets, "actual_start": starts, "delete_response": deleted, "error": err.Error(), "handoff": h})
		})
	}
}

func TestM403BOperatorCLIFullHandoffAndCandidateStatus(t *testing.T) {
	x := m403bNew(t)
	if os.Getenv("TICKETPULSE_CONTROL_BINARY") == "" || os.Getenv("TICKETPULSE_SYNC_BINARY") == "" {
		t.Fatal("built operator and worker binaries required")
	}
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = x.f.name
	env := append(os.Environ(), "TICKETPULSE_CONTROL_DSN="+cfg.FormatDSN(), "TICKETPULSE_SYNC_DSN="+cfg.FormatDSN(), "TICKETPULSE_SEARCH_INDEX="+x.control.ES.Index(), "TICKETPULSE_CK_DATABASE="+x.control.CK.Database())
	commands := []map[string]any{}
	control := func(mode string, extra ...string) []byte {
		t.Helper()
		args := []string{"-mode=" + mode, "-wait=20s"}
		args = append(args, extra...)
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Getenv("TICKETPULSE_CONTROL_BINARY"), args...)
		cmd.Env = env
		out, failure := cmd.CombinedOutput()
		commands = append(commands, map[string]any{"args": args, "output": string(out), "error": fmt.Sprint(failure), "at": time.Now().UTC()})
		if failure != nil {
			t.Fatalf("operator %s failed: %v %s", mode, failure, out)
		}
		return out
	}
	control("migrate-handoff") // repeat installation is non-destructive
	control("handoff-begin", "-handoff=handoff_v1", "-version="+x.version.ID)
	control("handoff-pin", "-handoff=handoff_v1")
	control("handoff-init", "-handoff=handoff_v1")
	ready := control("handoff-status", "-handoff=handoff_v1")
	if err = json.Unmarshal(ready, &x.h); err != nil || x.h.Phase != projection.OffsetsReady {
		t.Fatal("operator offset readiness missing", err)
	}
	// The public operator binary must enforce the same unfinished-handoff
	// resume guard as the transaction-level implementation.
	cmd := exec.Command(os.Getenv("TICKETPULSE_CONTROL_BINARY"), "-mode=resume", "-freeze-id="+*x.gate.FreezeID)
	cmd.Env = env
	out, failure := cmd.CombinedOutput()
	if failure == nil || !strings.Contains(string(out), "HANDOFF_INCOMPLETE") {
		t.Fatal("operator resumed incomplete handoff", failure, string(out))
	}
	commands = append(commands, map[string]any{"args": cmd.Args[1:], "output": string(out), "error": fmt.Sprint(failure)})
	opened := control("handoff-consume", "-handoff=handoff_v1")
	if err = json.Unmarshal(opened, &x.h); err != nil || x.h.Phase != projection.Consuming {
		t.Fatal("operator consumption authorization missing", err)
	}
	es, ck := x.process(t, "es"), x.process(t, "ck")
	x.processRunning(t, es, "es")
	x.processRunning(t, ck, "ck")
	verified := control("handoff-verify", "-handoff=handoff_v1")
	if err = json.Unmarshal(verified, &x.h); err != nil || x.h.Phase != projection.Verified {
		t.Fatal("operator verification missing", err)
	}
	for _, mode := range []string{"status", "analytics-status"} {
		cmd := exec.Command(os.Getenv("TICKETPULSE_SYNC_BINARY"), "-mode="+mode, "-handoff=handoff_v1")
		cmd.Env = env
		out, failure := cmd.CombinedOutput()
		if failure != nil {
			t.Fatal("candidate CLI status failed", mode, failure, string(out))
		}
		var value any
		if err = json.Unmarshal(out, &value); err != nil {
			t.Fatal("candidate CLI status is not JSON", err)
		}
		commands = append(commands, map[string]any{"binary": "ticketpulse-sync", "args": cmd.Args[1:], "output": value})
	}
	control("resume", "-freeze-id="+*x.gate.FreezeID)
	m403bSave(t, "cli-commands", commands)
	m403bSave(t, "handoff", x.h)
}

var _ = errors.Is
var _ = reflect.DeepEqual
var _ = sql.ErrNoRows
var _ analytics.Result
var _ search.Document
