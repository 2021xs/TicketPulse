//go:build integration && m2 && m3 && m4 && m403b && m403c1 && m403c2

package mysqlstore_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kadm"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/readruntime"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

const m403c2Key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

type m403c2Fixture struct {
	x               *m403bFixture
	c               *syncpipe.ReadSwitchControl
	scope           readruntime.Scope
	processes       []*m403c2Process
	progressBefore  map[string]any
	candidateES     *m403bProcess
	candidateCK     *m403bProcess
	originalWorkers []readruntime.Process
}
type m403c2Process struct {
	cmd      *exec.Cmd
	done     chan error
	once     sync.Once
	addr     string
	boot     string
	identity readruntime.Process
}

func m403c2Save(t *testing.T, name string, value any) {
	t.Helper()
	dir := filepath.Join(os.Getenv("TICKETPULSE_M403C2_EVIDENCE_DIR"), t.Name())
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err = enc.Encode(value); err != nil {
		t.Fatal(err)
	}
}
func m403c2New(t *testing.T) *m403c2Fixture { return m403c2NewAt(t, "") }
func m403c2NewAt(t *testing.T, endpoint string, prepare ...func(*m3fixture)) *m403c2Fixture {
	t.Helper()
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03c2" && os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03c3" {
		t.Fatal("C2 requires new private instances")
	}
	workers := []readruntime.Process{}
	x := m403bNewWithWorkers(t, endpoint, func(source *m3fixture) {
		for _, fn := range prepare {
			fn(source)
		}
		for _, mode := range []string{"capture", "consume", "ck"} {
			workers = append(workers, m403c2OriginalWorker(t, source, mode, endpoint))
		}
	})
	x.pin(t)
	x.open(t)
	esProcess := x.process(t, "es")
	x.processRunning(t, esProcess, "es")
	ckProcess := x.process(t, "ck")
	x.processRunning(t, ckProcess, "ck")
	x.verify(t)
	c := m403c1Install(t, x)
	if err := projection.EnsureReadRuntimeSchema(context.Background(), x.f.db); err != nil {
		t.Fatal(err)
	}
	g, err := projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID)
	if err != nil || g.State != "OPEN" {
		t.Fatalf("initial resume=%+v err=%v", g, err)
	}
	launcher, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	launcher, err = filepath.EvalSymlinks(launcher)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(launcher)
	host, _ := os.Hostname()
	lh, _ := readruntime.HashFile(launcher)
	scope := readruntime.Scope{Host: host, Root: root, Launcher: readruntime.Artifact{Path: launcher, SHA256: lh}, LaunchMode: "direct-no-restart", Exclusive: true, LaunchdLabels: []string{"net.ticketpulse.m403c2.disabled"}}
	for _, name := range []string{"ticketpulse-sync", "ticketpulse-control", "ticketpulse-rebuild"} {
		path := filepath.Join(root, name)
		hash, err := readruntime.HashFile(path)
		if err != nil {
			t.Fatal(err)
		}
		scope.OtherExecutables = append(scope.OtherExecutables, readruntime.Artifact{Path: path, SHA256: hash})
	}
	for _, name := range []string{os.Getenv("TICKETPULSE_API_BINARY"), os.Getenv("TICKETPULSE_LEGACY_API_BINARY")} {
		p, err := filepath.EvalSymlinks(name)
		if err != nil {
			t.Fatal(err)
		}
		h, _ := readruntime.HashFile(p)
		scope.Executables = append(scope.Executables, readruntime.Artifact{Path: p, SHA256: h})
	}
	value := &m403c2Fixture{x: x, c: c, scope: scope, candidateES: esProcess, candidateCK: ckProcess, originalWorkers: workers}
	value.progressBefore = value.progress(t)
	m403c2Save(t, "baseline", map[string]any{"B_handoff": x.h, "selection": mustSelection(t, x.f.db), "source_progress": value.progressBefore, "scope": scope})
	return value
}
func mustSelection(t *testing.T, db *sql.DB) projection.ReadSelection {
	t.Helper()
	s, e := (&projection.ReadControlStore{DB: db}).Selection(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func (f *m403c2Fixture) progress(t *testing.T) map[string]any {
	t.Helper()
	cp, e := f.x.f.state.Checkpoint(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	groups := map[string]any{}
	for label, group := range map[string]string{"A_es": f.x.f.queue.Group("es"), "A_ck": f.x.f.queue.Group("ck"), "B_es": f.x.h.ESGroup, "B_ck": f.x.h.CKGroup} {
		raw, e := f.x.f.queue.Admin.FetchOffsets(context.Background(), group)
		if e != nil {
			t.Fatal(e)
		}
		groups[label] = raw
	}
	for _, worker := range f.originalWorkers {
		actual, err := readruntime.Inspect(context.Background(), worker.PID, "")
		if err != nil || !readruntime.Same(worker, actual) {
			t.Fatalf("shared source propagation process changed: %v", err)
		}
	}
	return map[string]any{"cdc_checkpoint": cp, "groups": groups, "pinned": f.x.h.PinnedOffsets, "propagation_processes": f.originalWorkers}
}

func m403c2OriginalWorker(t *testing.T, f *m3fixture, mode string, endpoint ...string) readruntime.Process {
	t.Helper()
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName = f.name
	cmd := exec.Command(os.Getenv("TICKETPULSE_SYNC_BINARY"), "-mode="+mode, "-server-id=13002")
	cmd.Env = append(os.Environ(), "TICKETPULSE_SYNC_DSN="+cfg.FormatDSN(), "TICKETPULSE_SEARCH_INDEX="+f.es.Index(), "TICKETPULSE_CK_DATABASE="+f.ck.Database(), "TICKETPULSE_TARGET_VERSION=")
	if mode == "consume" && len(endpoint) > 0 && endpoint[0] != "" {
		cmd.Env = append(cmd.Env, "TICKETPULSE_ES_URL="+endpoint[0])
	}
	dir := filepath.Join(os.Getenv("TICKETPULSE_M403C2_EVIDENCE_DIR"), t.Name())
	os.MkdirAll(dir, 0700)
	file, err := os.OpenFile(filepath.Join(dir, "original-"+mode+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = file, file
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { e := cmd.Wait(); file.Close(); done <- e }()
	identity, err := readruntime.Inspect(context.Background(), cmd.Process.Pid, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		actual, err := readruntime.Inspect(context.Background(), identity.PID, "")
		if err != nil || !readruntime.Same(identity, actual) {
			select {
			case <-done:
				return
			default:
				t.Errorf("original worker identity unknown; no signal: %v", err)
				return
			}
		}
		cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			cmd.Process.Kill()
			<-done
		}
	})
	component := mode
	if mode == "capture" {
		component = "cdc"
	}
	if mode == "consume" {
		component = "es"
	}
	m403bWait(t, "original "+component+" process running", func() bool {
		select {
		case e := <-done:
			done <- e
			t.Fatalf("original worker exited: %v", e)
		default:
		}
		var state string
		f.db.QueryRow("SELECT state FROM sync_worker_state WHERE component=?", component).Scan(&state)
		return state == "running"
	})
	return identity
}
func (f *m403c2Fixture) startAPI(t *testing.T, version string, legacy bool, extra []string, overrides map[string]string) *m403c2Process {
	t.Helper()
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName = f.x.f.name
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	addr := listener.Addr().String()
	listener.Close()
	binary := os.Getenv("TICKETPULSE_API_BINARY")
	if legacy {
		binary = os.Getenv("TICKETPULSE_LEGACY_API_BINARY")
	}
	args := append([]string{"-listen=" + addr, "-routing=../../configs/local.json"}, extra...)
	cmd := exec.Command(binary, args...)
	targets := f.x.f.es.Index()
	space := f.x.f.ck.Database()
	if version == "candidate_v1" {
		targets = f.x.control.ES.Index()
		space = f.x.control.CK.Database()
	}
	values := map[string]string{"TICKETPULSE_DSN": cfg.FormatDSN(), "TICKETPULSE_SEARCH_INDEX": targets, "TICKETPULSE_CK_DATABASE": space, "TICKETPULSE_TARGET_VERSION": version, "TICKETPULSE_CURSOR_KEY": m403c2Key, "TICKETPULSE_REQUIRE_WRITE_GATE": "true"}
	for k, v := range overrides {
		values[k] = v
	}
	cmd.Env = os.Environ()
	for k, v := range values {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	dir := filepath.Join(os.Getenv("TICKETPULSE_M403C2_EVIDENCE_DIR"), t.Name())
	os.MkdirAll(dir, 0700)
	path := filepath.Join(dir, fmt.Sprintf("api-%d.log", time.Now().UnixNano()))
	log, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	cmd.Stdout, cmd.Stderr = log, log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	p := &m403c2Process{cmd: cmd, done: make(chan error, 1), addr: addr}
	go func() { err := cmd.Wait(); log.Close(); p.done <- err }()
	t.Cleanup(func() {
		p.once.Do(func() {
			cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-p.done:
			case <-time.After(8 * time.Second):
				cmd.Process.Kill()
				<-p.done
			}
		})
	})
	p.identity, e = readruntime.Inspect(context.Background(), cmd.Process.Pid, addr)
	if e != nil {
		t.Fatal(e)
	}
	f.processes = append(f.processes, p)
	return p
}
func (p *m403c2Process) ready(t *testing.T) {
	t.Helper()
	m403bWait(t, "real API ready", func() bool {
		select {
		case e := <-p.done:
			p.done <- e
			t.Fatalf("API exited: %v", e)
		default:
		}
		req, _ := http.NewRequest(http.MethodGet, "http://"+p.addr+"/readyz", nil)
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			return false
		}
		defer res.Body.Close()
		p.boot = res.Header.Get("X-TicketPulse-Boot-ID")
		return res.StatusCode == 200
	})
}
func (p *m403c2Process) stop(t *testing.T, kill bool) {
	t.Helper()
	p.once.Do(func() {
		if err := readruntime.Alive(context.Background(), p.identity); err != nil {
			t.Fatal(err)
		}
		var e error
		if kill {
			e = p.cmd.Process.Kill()
		} else {
			e = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		if e != nil {
			t.Fatal(e)
		}
		select {
		case err := <-p.done:
			if kill && err == nil {
				t.Fatal("SIGKILL expected nonzero exit")
			}
		case <-time.After(12 * time.Second):
			t.Fatal("API did not exit; no drain permission")
		}
		if e = readruntime.Exited(context.Background(), p.identity); e != nil {
			t.Fatal(e)
		}
	})
}
func (p *m403c2Process) failed(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		select {
		case e := <-p.done:
			if e == nil {
				t.Fatal("expected startup failure")
			}
		case <-time.After(12 * time.Second):
			t.Fatal("invalid API remained live")
		}
	})
}
func (f *m403c2Fixture) http(t *testing.T, p *m403c2Process, method, path, token string, body any, want int) ([]byte, http.Header) {
	t.Helper()
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	req, e := http.NewRequest(method, "http://"+p.addr+path, bytes.NewReader(data))
	if e != nil {
		t.Fatal(e)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	res, e := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	payload, _ := io.ReadAll(res.Body)
	if res.StatusCode != want {
		t.Fatalf("HTTP %s status=%d expected=%d response=%s", path, res.StatusCode, want, payload)
	}
	return payload, res.Header
}
func (f *m403c2Fixture) plan(t *testing.T, id string) projection.ReadSwitch {
	t.Helper()
	g, e := projection.Freeze(context.Background(), f.x.f.db, "C2 independent read promotion")
	if e != nil {
		t.Fatal(e)
	}
	f.x.gate = g
	o := m403c1Plan(t, f.c, id)
	if _, e = f.c.ConfigureReadDrain(context.Background(), id, f.scope); e != nil {
		t.Fatal(e)
	}
	m403c1Validate(t, f.c, id)
	return o
}
func (f *m403c2Fixture) drain(t *testing.T, id string) {
	t.Helper()
	e, err := f.c.ObserveReadDrain(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	m403c2Save(t, "drain-"+id, e)
}
func (f *m403c2Fixture) commit(t *testing.T, id string) projection.ReadSwitch {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	o, err := f.c.CommitReadSelection(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if o.Stage != projection.ReadCommitted {
		t.Fatalf("stage=%s", o.Stage)
	}
	return o
}
func (f *m403c2Fixture) probe(p *m403c2Process) syncpipe.ReadAPIProbe {
	return syncpipe.ReadAPIProbe{BootID: p.boot, Token: "local-supervisor", PermissionToken: "local-it-agent", Analytics: m4Window}
}
func (f *m403c2Fixture) observe(t *testing.T, id string, p *m403c2Process) projection.ReadSwitch {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	o, err := f.c.ObserveAPIReady(ctx, id, f.probe(p))
	if err != nil {
		t.Fatal(err)
	}
	if o.Stage != projection.ReadAPIReady {
		t.Fatal(o.Stage)
	}
	return o
}
func (f *m403c2Fixture) resume(t *testing.T, id string, p *m403c2Process) projection.Gate {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	g, err := f.c.ResumeReadSwitch(ctx, id, *f.x.gate.FreezeID, f.probe(p))
	if err != nil || g.State != "OPEN" {
		t.Fatalf("resume=%+v err=%v", g, err)
	}
	return g
}

func TestM403C2RealForwardSwitchNewBusinessAndIndependentTargets(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	first, _ := f.http(t, a, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2", "local-supervisor", nil, 200)
	var page search.Result
	json.Unmarshal(first, &page)
	if !page.HasMore || page.NextCursor == "" {
		t.Fatal("A did not serve cursor page")
	}
	gold, _ := f.http(t, a, http.MethodGet, "/api/v1/analytics?start=2026-11-01&end=2026-11-04", "local-supervisor", nil, 200)
	m403c2Gold(t, gold)
	// B VERIFIED is deliberately followed by a new HTTP mutation.
	f.http(t, a, http.MethodPost, "/api/v1/tickets/comments", "local-device-agent", ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: 4, ExpectedVersion: 10}, Content: "C2 new write after B VERIFIED"}, 200)
	f.plan(t, "forward_live")
	f.http(t, a, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2", "local-supervisor", nil, 503)
	f.http(t, a, http.MethodGet, "/api/v1/tickets/4", "local-alice", nil, 200)
	if _, err := f.c.CommitReadSelection(context.Background(), "forward_live"); err == nil {
		t.Fatal("live A permitted commit")
	}
	a.stop(t, false)
	f.drain(t, "forward_live")
	o := f.commit(t, "forward_live")
	repeat, err := f.c.CommitReadSelection(context.Background(), o.ID)
	if err != nil || repeat.Stage != projection.ReadCommitted || mustSelection(t, f.x.f.db).ReadRevision != 2 {
		t.Fatalf("commit retry changed revision: operation=%+v err=%v", repeat, err)
	}
	b := f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	ready := f.observe(t, o.ID, b)
	oldPath := "/api/v1/search/tickets?sort=id&order=asc&page_size=2&cursor=" + url.QueryEscape(page.NextCursor)
	old, _ := f.http(t, b, http.MethodGet, oldPath, "local-supervisor", nil, 409)
	if !bytes.Contains(old, []byte("SEARCH_CURSOR_TARGET_CHANGED")) {
		t.Fatal(string(old))
	}
	expired := m403c2Expire(t, page.NextCursor)
	f.http(t, b, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2&cursor="+url.QueryEscape(expired), "local-supervisor", nil, 400)
	nowGold, _ := f.http(t, b, http.MethodGet, "/api/v1/analytics?start=2026-11-01&end=2026-11-04", "local-supervisor", nil, 200)
	m403c2Gold(t, nowGold)
	if _, err = projection.Resume(context.Background(), f.x.f.db, *f.x.gate.FreezeID); err == nil {
		t.Fatal("ordinary Resume bypassed switch")
	}
	f.resume(t, o.ID, b)
	created, _ := f.http(t, b, http.MethodPost, "/api/v1/tickets", "local-alice", request("c2_after_resume"), 201)
	var made ticket.CreateResult
	json.Unmarshal(created, &made)
	f.http(t, b, http.MethodPost, "/api/v1/tickets/comments", "local-alice", ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: made.Ticket.ID, ExpectedVersion: 1}, Content: "both projections continue after resume"}, 200)
	m403bWait(t, "A B both current after new HTTP writes", func() bool {
		for _, target := range []struct {
			es *search.Elasticsearch
			ck *analytics.ClickHouse
		}{{f.x.f.es, f.x.f.ck}, {f.x.control.ES, f.x.control.CK}} {
			r, e := syncpipe.Reconcile(context.Background(), f.x.f.state, f.x.f.queue, target.es, target.ck, syncpipe.ReconcileOptions{BatchSize: 250, LagTimeout: 5 * time.Second})
			if e != nil || r.State != syncpipe.ReconcileConsistent {
				return false
			}
		}
		return true
	})
	reports := map[string]syncpipe.ReconcileReport{}
	for name, target := range map[string]struct {
		es *search.Elasticsearch
		ck *analytics.ClickHouse
	}{"A": {f.x.f.es, f.x.f.ck}, "B": {f.x.control.ES, f.x.control.CK}} {
		r, e := syncpipe.Reconcile(context.Background(), f.x.f.state, nil, target.es, target.ck, syncpipe.ReconcileOptions{BatchSize: 250, MetricsOrganizationID: 1, MetricsRequest: m4Window})
		if e != nil || r.State != syncpipe.ReconcileConsistent {
			t.Fatalf("%s audit=%+v err=%v", name, r, e)
		}
		reports[name] = r
	}
	if f.x.version.Phase != projection.Prepared {
		t.Fatal("candidate registration changed")
	}
	status, err := f.c.ReadSwitchStatus(context.Background(), o.ID)
	if err != nil || status.Operation.Stage != projection.ReadResumed {
		t.Fatal(err)
	}
	m403c2Save(t, "forward-complete", map[string]any{"committed": o, "ready": ready, "final": status, "A_B_reconcile": reports, "old_cursor_response": json.RawMessage(old), "A_api_gold": json.RawMessage(gold), "B_api_gold": json.RawMessage(nowGold), "created_after_resume": made.Ticket, "progress_before": f.progressBefore, "progress_after": f.progress(t)})
}
func m403c2Gold(t *testing.T, raw []byte) {
	t.Helper()
	var result analytics.Result
	json.Unmarshal(raw, &result)
	gold, err := os.ReadFile("../../tests/analytics/prd_gold.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected struct {
		Daily    []analytics.Day    `json:"daily"`
		Overview analytics.Overview `json:"overview"`
	}
	json.Unmarshal(gold, &expected)
	if !reflect.DeepEqual(result.Daily, expected.Daily) || !reflect.DeepEqual(result.Overview, expected.Overview) {
		t.Fatalf("Gold differs actual=%s", raw)
	}
}
func m403c2Expire(t *testing.T, token string) string {
	t.Helper()
	key, _ := hex.DecodeString(m403c2Key)
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	raw, _ := base64.RawURLEncoding.DecodeString(token)
	n := aead.NonceSize()
	data, err := aead.Open(nil, raw[:n], raw[n:], []byte("ticketpulse-search-v1"))
	if err != nil {
		t.Fatal(err)
	}
	var c map[string]any
	json.Unmarshal(data, &c)
	c["e"] = time.Now().Add(-time.Minute).Unix()
	data, _ = json.Marshal(c)
	nonce := make([]byte, n)
	rand.Read(nonce)
	return base64.RawURLEncoding.EncodeToString(aead.Seal(nonce, nonce, data, []byte("ticketpulse-search-v1")))
}

func TestM403C2TwoPortsAndUnawareLegacyAPIMustActuallyExit(t *testing.T) {
	f := m403c2New(t)
	a1 := f.startAPI(t, "active_original", false, nil, nil)
	a1.ready(t)
	a2 := f.startAPI(t, "active_original", true, nil, nil)
	a2.ready(t)
	if a2.boot != "" {
		t.Fatal("legacy binary unexpectedly understands C2")
	}
	f.plan(t, "two_apis")
	inventory, err := f.c.ConfigureReadDrain(context.Background(), "two_apis", f.scope)
	if err != nil || len(inventory.APIs) != 2 {
		t.Fatalf("inventory=%+v err=%v", inventory, err)
	}
	f.http(t, a1, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2", "local-supervisor", nil, 503)
	f.http(t, a2, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2", "local-supervisor", nil, 200)
	a1.stop(t, false)
	if _, err = f.c.ObserveReadDrain(context.Background(), "two_apis"); err == nil {
		t.Fatal("one port stopped allowed second legacy process")
	}
	if _, err = f.c.CommitReadSelection(context.Background(), "two_apis"); err == nil {
		t.Fatal("legacy process still alive allowed commit")
	}
	a2.stop(t, false)
	f.drain(t, "two_apis")
	o := f.commit(t, "two_apis")
	b := f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	f.observe(t, o.ID, b)
	f.resume(t, o.ID, b)
	m403c2Save(t, "two-port-legacy-complete", map[string]any{"inventory": inventory, "old_ports": []string{a1.addr, a2.addr}, "committed": o, "final_selection": mustSelection(t, f.x.f.db)})
}
func TestM403C2StartupExpectationsAndReadyProcessDeath(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	f.plan(t, "ready_restart")
	a.stop(t, false)
	f.drain(t, "ready_restart")
	f.commit(t, "ready_restart")
	wrong := f.startAPI(t, "active_original", false, nil, nil)
	wrong.failed(t)
	unmanaged := f.startAPI(t, "candidate_v1", false, nil, map[string]string{"TICKETPULSE_TARGET_VERSION": ""})
	unmanaged.failed(t)
	mismatch := f.startAPI(t, "candidate_v1", false, nil, map[string]string{"TICKETPULSE_CK_DATABASE": f.x.f.ck.Database()})
	mismatch.failed(t)
	g, _ := projection.ReadGate(context.Background(), f.x.f.db)
	if g.State != "FROZEN" {
		t.Fatal("startup failure resumed business")
	}
	b := f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	f.observe(t, "ready_restart", b)
	b.stop(t, true)
	if _, err := f.c.ResumeReadSwitch(context.Background(), "ready_restart", *f.x.gate.FreezeID, f.probe(b)); err == nil {
		t.Fatal("dead API_READY allowed Resume")
	}
	restart := f.startAPI(t, "candidate_v1", false, nil, nil)
	restart.ready(t)
	if restart.boot == b.boot {
		t.Fatal("boot ID reused")
	}
	if _, err := f.c.ResumeReadSwitch(context.Background(), "ready_restart", *f.x.gate.FreezeID, f.probe(restart)); err == nil {
		t.Fatal("new boot bypassed reobservation")
	}
	o := f.observe(t, "ready_restart", restart)
	f.resume(t, o.ID, restart)
	// A binary from C1 still ignores selector B; it is explicitly blocked by
	// the host inventory, not falsely claimed to obey the new metadata.
	legacy := f.startAPI(t, "active_original", true, nil, nil)
	legacy.ready(t)
	if _, err := f.c.ObserveAPIReady(context.Background(), o.ID, f.probe(restart)); err == nil {
		t.Fatal("legacy extra process should not be accepted as ready scope")
	}
	legacy.stop(t, true)
	m403c2Save(t, "ready-process-recovery", map[string]any{"old_boot": b.boot, "new_boot": restart.boot, "operation": o, "gate": g})
}
func TestM403C2ConcurrentIntentAndSubmittedRetry(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	g, e := projection.Freeze(context.Background(), f.x.f.db, "concurrent C2")
	if e != nil {
		t.Fatal(e)
	}
	f.x.gate = g
	var wins atomic.Int64
	var mu sync.Mutex
	winner := ""
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("concurrent_%d", i)
			_, err := f.c.BeginReadSwitch(context.Background(), id, projection.ReadPromote, "candidate_v1", 1)
			if err == nil {
				wins.Add(1)
				mu.Lock()
				winner = id
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("CAS winners=%d", wins.Load())
	}
	if _, e = f.c.ConfigureReadDrain(context.Background(), winner, f.scope); e != nil {
		t.Fatal(e)
	}
	m403c1Validate(t, f.c, winner)
	a.stop(t, false)
	f.drain(t, winner)
	f.commit(t, winner)
	for i := 0; i < 3; i++ {
		o, e := f.c.CommitReadSelection(context.Background(), winner)
		if e != nil || o.Stage != projection.ReadCommitted {
			t.Fatal(e)
		}
	}
	if mustSelection(t, f.x.f.db).ReadRevision != 2 {
		t.Fatal("retry increased revision")
	}
	m403c2Save(t, "concurrent-intent", map[string]any{"winner": winner, "wins": wins.Load(), "selection": mustSelection(t, f.x.f.db)})
}

type m403c2Wire struct {
	net.Conn
	enabled *atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *m403c2Wire) Write(data []byte) (int, error) {
	if w.enabled.Load() {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	return w.Conn.Write(data)
}
func TestM403C2InFlightSearchDrainAndShutdownTimeout(t *testing.T) {
	for _, forced := range []bool{false, true} {
		t.Run(fmt.Sprint(forced), func(t *testing.T) {
			var armed atomic.Bool
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			upstream, _ := url.Parse(os.Getenv("TICKETPULSE_ES_URL"))
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if armed.Load() && strings.HasSuffix(r.URL.Path, "/_search") {
					select {
					case entered <- struct{}{}:
					default:
					}
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				}
				req, _ := http.NewRequestWithContext(r.Context(), r.Method, upstream.String()+r.URL.RequestURI(), r.Body)
				req.Header = r.Header.Clone()
				res, e := http.DefaultClient.Do(req)
				if e != nil {
					return
				}
				defer res.Body.Close()
				for k, v := range res.Header {
					w.Header()[k] = v
				}
				w.WriteHeader(res.StatusCode)
				io.Copy(w, res.Body)
			}))
			defer proxy.Close()
			f := m403c2NewAt(t, proxy.URL)
			extra := []string{}
			if forced {
				extra = []string{"-shutdown-timeout=100ms"}
			}
			a := f.startAPI(t, "active_original", false, extra, map[string]string{"TICKETPULSE_ES_URL": proxy.URL})
			a.ready(t)
			armed.Store(true)
			done := make(chan error, 1)
			go func() {
				req, _ := http.NewRequest(http.MethodGet, "http://"+a.addr+"/api/v1/search/tickets?sort=id&order=asc&page_size=2", nil)
				req.Header.Set("Authorization", "Bearer local-supervisor")
				res, e := http.DefaultClient.Do(req)
				if e == nil {
					io.Copy(io.Discard, res.Body)
					res.Body.Close()
				}
				done <- e
			}()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("search did not pass admission and reach real ES proxy")
			}
			g, e := projection.Freeze(context.Background(), f.x.f.db, "in-flight search")
			if e != nil {
				t.Fatal(e)
			}
			f.x.gate = g
			m403c1Plan(t, f.c, "in_flight")
			if _, e = f.c.ConfigureReadDrain(context.Background(), "in_flight", f.scope); e != nil {
				t.Fatal(e)
			}
			a.cmd.Process.Signal(syscall.SIGTERM)
			time.Sleep(150 * time.Millisecond)
			if !forced {
				if _, e = f.c.ObserveReadDrain(context.Background(), "in_flight"); e == nil {
					t.Fatal("in-flight request counted drained before exit")
				}
			}
			releaseOnce.Do(func() { close(release) })
			<-done
			a.once.Do(func() {
				select {
				case <-a.done:
				case <-time.After(10 * time.Second):
					a.cmd.Process.Kill()
					<-a.done
				}
			})
			if e = readruntime.Exited(context.Background(), a.identity); e != nil {
				t.Fatal(e)
			}
			m403c1Validate(t, f.c, "in_flight")
			f.drain(t, "in_flight")
			o := f.commit(t, "in_flight")
			boot, e := (&projection.ReadControlStore{DB: f.x.f.db}).APIBoot(context.Background(), a.boot)
			if e != nil {
				t.Fatal(e)
			}
			var shutdown struct {
				Complete bool `json:"shutdown_complete"`
				Forced   bool `json:"forced_connections_closed"`
			}
			if json.Unmarshal(boot.Shutdown, &shutdown) != nil || shutdown.Complete == forced || shutdown.Forced != forced {
				t.Fatalf("actual shutdown timeout evidence differs: %s", boot.Shutdown)
			}
			m403c2Save(t, "inflight-drained", map[string]any{"forced": forced, "proxy_is_original_enrolled_endpoint": proxy.URL, "boot": boot, "committed": o})
		})
	}
}

func TestM403C2ControlCrashChild(t *testing.T) {
	stage := os.Getenv("TICKETPULSE_M403C2_CRASH_STAGE")
	if stage == "" {
		t.Skip("only invoked by parent crash matrix")
	}
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName = os.Getenv("TICKETPULSE_M403C2_CRASH_SCHEMA")
	db, err := mysqlstore.Open(context.Background(), cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var uuid string
	if err = db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
		t.Fatal("child source identity differs")
	}
	q, err := syncpipe.ConnectStatusQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	c := &syncpipe.ReadSwitchControl{ProjectionControl: &syncpipe.ProjectionControl{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: q}, Observe: func(actual string, value any) error {
		if actual == stage {
			raw, _ := json.Marshal(value)
			if err := os.WriteFile(os.Getenv("TICKETPULSE_M403C2_CRASH_READY"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			select {}
		}
		return nil
	}}
	probe := syncpipe.ReadAPIProbe{BootID: os.Getenv("TICKETPULSE_M403C2_CRASH_BOOT"), Token: "local-supervisor", PermissionToken: "local-it-agent", Analytics: m4Window}
	id := os.Getenv("TICKETPULSE_M403C2_CRASH_OPERATION")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	if strings.Contains(stage, "api_drain_plan") {
		var scope readruntime.Scope
		if json.Unmarshal([]byte(os.Getenv("TICKETPULSE_M403C2_CRASH_SCOPE")), &scope) != nil {
			t.Fatal("crash inventory invalid")
		}
		_, err = c.ConfigureReadDrain(ctx, id, scope)
	} else if strings.Contains(stage, "api_drained") {
		_, err = c.ObserveReadDrain(ctx, id)
	} else if strings.Contains(stage, "read_commit") {
		_, err = c.CommitReadSelection(ctx, id)
	} else if strings.Contains(stage, "api_ready") {
		_, err = c.ObserveAPIReady(ctx, id, probe)
	} else {
		_, err = c.ResumeReadSwitch(ctx, id, os.Getenv("TICKETPULSE_M403C2_CRASH_FREEZE"), probe)
	}
	t.Fatalf("crash boundary was not reached: %s err=%v", stage, err)
}
func (f *m403c2Fixture) crash(t *testing.T, stage, id string, api *m403c2Process) {
	t.Helper()
	binary, _ := os.Executable()
	dir := filepath.Join(os.Getenv("TICKETPULSE_M403C2_EVIDENCE_DIR"), t.Name())
	os.MkdirAll(dir, 0700)
	ready := filepath.Join(dir, stage+"-boundary.json")
	log, err := os.OpenFile(filepath.Join(dir, stage+"-child.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestM403C2ControlCrashChild$", "-test.v")
	boot := ""
	if api != nil {
		boot = api.boot
	}
	cmd.Env = append(os.Environ(), "TICKETPULSE_M403C2_CRASH_STAGE="+stage, "TICKETPULSE_M403C2_CRASH_READY="+ready, "TICKETPULSE_M403C2_CRASH_SCHEMA="+f.x.f.name, "TICKETPULSE_M403C2_CRASH_OPERATION="+id, "TICKETPULSE_M403C2_CRASH_BOOT="+boot, "TICKETPULSE_M403C2_CRASH_FREEZE="+*f.x.gate.FreezeID)
	scopeJSON, _ := json.Marshal(f.scope)
	cmd.Env = append(cmd.Env, "TICKETPULSE_M403C2_CRASH_SCOPE="+string(scopeJSON))
	cmd.Stdout, cmd.Stderr = log, log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { e := cmd.Wait(); log.Close(); done <- e }()
	deadline := time.Now().Add(50 * time.Second)
	reached := false
	for time.Now().Before(deadline) {
		if _, err = os.Stat(ready); err == nil {
			reached = true
			break
		}
		select {
		case e := <-done:
			t.Fatalf("crash child exited early %s: %v", stage, e)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !reached {
		cmd.Process.Kill()
		<-done
		t.Fatalf("crash boundary missing %s", stage)
	}
	identity, err := readruntime.Inspect(context.Background(), cmd.Process.Pid, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil {
		t.Fatal("child SIGKILL returned success")
	}
	if err = readruntime.Exited(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	status, err := f.c.ReadSwitchStatus(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	m403c2Save(t, stage+"-persistent", status)
}
func TestM403C2CommitReadyResumeSIGKILLDurableRecovery(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	f.plan(t, "crash_steps")
	a.stop(t, false)
	f.drain(t, "crash_steps")
	f.crash(t, "before_read_commit", "crash_steps", nil)
	s := mustSelection(t, f.x.f.db)
	if s.VersionID != "active_original" || s.ReadRevision != 1 {
		t.Fatal("uncommitted selector leaked")
	}
	f.crash(t, "after_read_commit", "crash_steps", nil)
	s = mustSelection(t, f.x.f.db)
	if s.VersionID != "candidate_v1" || s.ReadRevision != 2 {
		t.Fatal("committed selector was lost")
	}
	if _, err := f.c.CommitReadSelection(context.Background(), "crash_steps"); err != nil {
		t.Fatal(err)
	}
	b := f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	f.crash(t, "before_api_ready_commit", "crash_steps", b)
	o, _ := (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "crash_steps")
	if o.Stage != projection.ReadCommitted {
		t.Fatal("API_READY leaked before commit")
	}
	f.crash(t, "after_api_ready_commit", "crash_steps", b)
	o, _ = (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "crash_steps")
	if o.Stage != projection.ReadAPIReady {
		t.Fatal("API_READY commit lost")
	}
	f.crash(t, "before_read_resume_commit", "crash_steps", b)
	g, _ := projection.ReadGate(context.Background(), f.x.f.db)
	if g.State != "FROZEN" {
		t.Fatal("uncommitted Resume leaked")
	}
	f.crash(t, "after_read_resume_commit", "crash_steps", b)
	g, _ = projection.ReadGate(context.Background(), f.x.f.db)
	o, _ = (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "crash_steps")
	if g.State != "OPEN" || o.Stage != projection.ReadResumed || mustSelection(t, f.x.f.db).ReadRevision != 2 {
		t.Fatal("atomic Resume lost")
	}
	retry, err := f.c.ResumeReadSwitch(context.Background(), o.ID, *f.x.gate.FreezeID, f.probe(b))
	if err != nil || retry.Revision != g.Revision {
		t.Fatal("unknown Resume retry changed durable facts")
	}
	m403c2Save(t, "crash-recovery-complete", map[string]any{"operation": o, "gate": g, "selection": s})
}
func TestM403C2CommitRechecksIdentityOffsetsIsolationAndContent(t *testing.T) {
	for _, fault := range []string{"es_uuid", "ck_uuid", "missing_offset", "offset_range", "isolation", "es_content", "source_boundary", "owner_epoch"} {
		t.Run(fault, func(t *testing.T) {
			f := m403c2New(t)
			a := f.startAPI(t, "active_original", false, nil, nil)
			a.ready(t)
			f.plan(t, "commit_refusal")
			a.stop(t, false)
			f.drain(t, "commit_refusal")
			original, err := (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "commit_refusal")
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "es_uuid":
				m4HTTP(t, http.MethodDelete, f.x.control.ES.Endpoint()+"/"+f.x.control.ES.Index(), nil, 200)
				if err = f.x.control.ES.EnsureIndex(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "ck_uuid":
				if _, err = f.x.control.CK.Query(context.Background(), "DROP TABLE ticket_states"); err != nil {
					t.Fatal(err)
				}
				if err = f.x.control.CK.Ensure(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "missing_offset":
				f.candidateES.kill(t)
				m403bWait(t, "fault group has no member", func() bool {
					groups, e := f.x.f.queue.Admin.DescribeGroups(context.Background(), f.x.h.ESGroup)
					return e == nil && len(groups[f.x.h.ESGroup].Members) == 0
				})
				if _, err = f.x.f.queue.Admin.DeleteOffsets(context.Background(), f.x.h.ESGroup, kadm.TopicsSet{f.x.f.queue.Topic: map[int32]struct{}{1: {}}}); err != nil {
					t.Fatal(err)
				}
				raw, e := f.x.f.queue.Admin.FetchOffsets(context.Background(), f.x.h.ESGroup)
				if e != nil {
					t.Fatal(e)
				}
				offset, exists := raw.Lookup(f.x.f.queue.Topic, 1)
				if exists && offset.At >= 0 {
					t.Fatal("private offset deletion did not actually remove commit")
				}
			case "offset_range":
				f.candidateES.kill(t)
				m403bWait(t, "range fault group has no member", func() bool {
					groups, e := f.x.f.queue.Admin.DescribeGroups(context.Background(), f.x.h.ESGroup)
					return e == nil && len(groups[f.x.h.ESGroup].Members) == 0
				})
				ends, err := f.x.f.queue.Admin.ListEndOffsets(context.Background(), f.x.f.queue.Topic)
				if err != nil {
					t.Fatal(err)
				}
				end, _ := ends.Lookup(f.x.f.queue.Topic, 1)
				_, err = f.x.f.queue.Admin.CommitOffsets(context.Background(), f.x.h.ESGroup, kadm.Offsets{f.x.f.queue.Topic: map[int32]kadm.Offset{1: {Topic: f.x.f.queue.Topic, Partition: 1, At: end.Offset + 1000000, LeaderEpoch: -1}}})
				if err != nil {
					t.Fatal(err)
				}
				raw, e := f.x.f.queue.Admin.FetchOffsets(context.Background(), f.x.h.ESGroup)
				if e != nil {
					t.Fatal(e)
				}
				actual, ok := raw.Lookup(f.x.f.queue.Topic, 1)
				if !ok || actual.At != end.Offset+1000000 {
					t.Fatal("range fault not actually committed")
				}
			case "isolation":
				f.x.f.db.Exec(`INSERT INTO sync_projection_deliveries(version_id,component,topic,partition_id,record_offset,event_id,status,attempts,first_seen_at,last_seen_at) VALUES(?,'es',?,0,999999999,'c2-terminal-isolation','isolated',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, f.x.version.ID, f.x.f.queue.Topic)
			case "es_content":
				m4HTTP(t, http.MethodDelete, fmt.Sprintf("%s/%s/_doc/1", f.x.control.ES.Endpoint(), f.x.control.ES.Index()), nil, 200)
				m4HTTP(t, http.MethodPost, f.x.control.ES.Endpoint()+"/"+f.x.control.ES.Index()+"/_refresh", nil, 200)
			case "source_boundary":
				f.x.f.db.Exec("UPDATE tickets SET title='private deliberately bypassed freeze source' WHERE id=1")
			case "owner_epoch":
				f.x.f.db.Exec("UPDATE sync_projection_target_owners SET owner_epoch=owner_epoch+1 WHERE version_id=? AND component='es'", f.x.version.ID)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			_, err = f.c.CommitReadSelection(ctx, original.ID)
			cancel()
			if err == nil {
				t.Fatalf("%s allowed historical proof to commit", fault)
			}
			s := mustSelection(t, f.x.f.db)
			g, _ := projection.ReadGate(context.Background(), f.x.f.db)
			if s.VersionID != "active_original" || s.ReadRevision != 1 || g.State != "FROZEN" {
				t.Fatal("refused commit changed selection/freeze")
			}
			m403c2Save(t, "commit-refused", map[string]any{"fault": fault, "error": err.Error(), "selection": s, "gate": g, "historical_proof_sha256": original.ProofSHA256})
		})
	}
}

func (f *m403c2Fixture) cli(t *testing.T, mode, id string, api *m403c2Process, extra ...string) []byte {
	t.Helper()
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName = f.x.f.name
	args := append([]string{"-mode=" + mode, "-operation=" + id, "-wait=50s"}, extra...)
	if api != nil {
		args = append(args, "-api-boot="+api.boot, "-probe-start=2026-11-01", "-probe-end=2026-11-04")
	}
	cmd := exec.Command(os.Getenv("TICKETPULSE_CONTROL_BINARY"), args...)
	cmd.Env = append(os.Environ(), "TICKETPULSE_CONTROL_DSN="+cfg.FormatDSN(), "TICKETPULSE_READ_PROBE_TOKEN=local-supervisor", "TICKETPULSE_READ_PERMISSION_TOKEN=local-it-agent")
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("control %s failed: %v %s", mode, err, data)
	}
	m403c2Save(t, "cli-"+mode, json.RawMessage(data))
	return data
}
func TestM403C2OperatorCLIAndControlMetadataLossRefuseStartup(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	g, err := projection.Freeze(context.Background(), f.x.f.db, "C2 CLI")
	if err != nil {
		t.Fatal(err)
	}
	f.x.gate = g
	f.cli(t, "read-begin", "cli_switch", nil, "-version=candidate_v1", "-read-revision=1")
	scopeFile := filepath.Join(f.scope.Root, fmt.Sprintf("scope-%d.json", time.Now().UnixNano()))
	raw, _ := json.Marshal(f.scope)
	if err = os.WriteFile(scopeFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.cli(t, "read-switch-drain-plan", "cli_switch", nil, "-api-scope="+scopeFile)
	f.cli(t, "read-validate", "cli_switch", nil)
	a.stop(t, false)
	f.cli(t, "read-switch-drain-observe", "cli_switch", nil)
	f.cli(t, "read-switch-commit", "cli_switch", nil)
	b := f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	f.cli(t, "read-switch-observe-ready", "cli_switch", b)
	f.cli(t, "read-switch-resume", "cli_switch", b, "-freeze-id="+*g.FreezeID)
	f.cli(t, "read-switch-status", "cli_switch", nil)
	b.stop(t, false)
	// An enabled selector cannot be removed and replaced by legacy environment
	// expectations. Test only this new random schema and a real API startup.
	if _, err = f.x.f.db.Exec("DELETE FROM sync_projection_read_selection"); err != nil {
		t.Fatal(err)
	}
	missing := f.startAPI(t, "candidate_v1", false, nil, nil)
	missing.failed(t)
	if _, err = f.c.ReadSwitchStatus(context.Background(), "cli_switch"); err == nil {
		t.Fatal("missing enabled selector downgraded")
	}
	m403c2Save(t, "metadata-startup-rejected", map[string]any{"read_revision_before_delete": 2, "new_API_failed": true})
}
func TestM403C2UnknownScopeAndRestartInventoryRemainFrozen(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	g, err := projection.Freeze(context.Background(), f.x.f.db, "unknown restart scope")
	if err != nil {
		t.Fatal(err)
	}
	f.x.gate = g
	m403c1Plan(t, f.c, "unknown_scope")
	invalid := f.scope
	invalid.LaunchMode = "unknown-supervisor"
	if _, err = f.c.ConfigureReadDrain(context.Background(), "unknown_scope", invalid); err == nil {
		t.Fatal("unproven restart policy accepted")
	}
	invalid = f.scope
	invalid.LaunchdLabels = []string{"com.apple.Finder"}
	if _, err = f.c.ConfigureReadDrain(context.Background(), "unknown_scope", invalid); err == nil {
		t.Fatal("loaded launchd entry accepted")
	}
	if _, err = f.c.ConfigureReadDrain(context.Background(), "unknown_scope", f.scope); err != nil {
		t.Fatal(err)
	}
	m403c1Validate(t, f.c, "unknown_scope")
	// A new binary respects PLANNED and refuses boot; a legacy binary remains
	// uncontrolled by metadata and is caught by the actual host scan.
	planned := f.startAPI(t, "active_original", false, nil, nil)
	planned.failed(t)
	a.stop(t, false)
	f.drain(t, "unknown_scope")
	legacy := f.startAPI(t, "active_original", true, nil, nil)
	legacy.ready(t)
	if _, err = f.c.CommitReadSelection(context.Background(), "unknown_scope"); err == nil {
		t.Fatal("legacy reentry accepted")
	}
	legacy.stop(t, true)
	current, _ := projection.ReadGate(context.Background(), f.x.f.db)
	if current.State != "FROZEN" {
		t.Fatal("unknown supervisor resumed")
	}
	m403c2Save(t, "uncontrolled-restart-blocked", map[string]any{"gate": current, "selection": mustSelection(t, f.x.f.db), "loaded_label_rejected": true, "PLANNED_boot_refused": true})
}

func TestM403C2SelectedDataAndStatusRemainBWhenAIsUnavailable(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	f.plan(t, "same_scope")
	a.stop(t, false)
	f.drain(t, "same_scope")
	f.commit(t, "same_scope")
	b := f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	m4HTTP(t, http.MethodDelete, f.x.f.es.Endpoint()+"/"+f.x.f.es.Index(), nil, 200)
	if _, err := f.x.f.ck.Query(context.Background(), "DROP TABLE ticket_states"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"sync_kafka_es_deliveries", "sync_kafka_ck_deliveries"} {
		if _, err := f.x.f.db.Exec("INSERT INTO "+table+"(topic,partition_id,record_offset,event_id,status,attempts,first_seen_at,last_seen_at) VALUES(?,0,999999999,'private-old-status-gap','isolated',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))", f.x.f.queue.Topic); err != nil {
			t.Fatal(err)
		}
	}
	searched, headers := f.http(t, b, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=100", "local-supervisor", nil, 200)
	var results search.Result
	json.Unmarshal(searched, &results)
	if len(results.Items) != 10 || results.Freshness.VersionID != "candidate_v1" || results.Freshness.State == "degraded" || results.Freshness.ConsumerGroup != f.x.h.ESGroup {
		t.Fatalf("B data/status not isolated: %s", searched)
	}
	stats, _ := f.http(t, b, http.MethodGet, "/api/v1/analytics?start=2026-11-01&end=2026-11-04", "local-supervisor", nil, 200)
	m403c2Gold(t, stats)
	var analysis analytics.Result
	json.Unmarshal(stats, &analysis)
	if analysis.Sync.VersionID != "candidate_v1" || analysis.Sync.ConsumerGroup != f.x.h.CKGroup || analysis.Sync.Isolated != 0 {
		t.Fatalf("B CK state miswired: %s", stats)
	}
	o := f.observe(t, "same_scope", b)
	m403c2Save(t, "same-target-data-status", map[string]any{"B_search": json.RawMessage(searched), "B_analytics": json.RawMessage(stats), "headers": headers, "operation": o, "A_faults": []string{"index absent", "states table absent", "legacy isolated status"}})
}
func TestM403C2APIReadyDestinationChangeMustRefuseResume(t *testing.T) {
	for _, fault := range []string{"es_uuid", "ck_database_uuid", "missing_offset", "isolation"} {
		t.Run(fault, func(t *testing.T) {
			f := m403c2New(t)
			a := f.startAPI(t, "active_original", false, nil, nil)
			a.ready(t)
			f.plan(t, "ready_fault")
			a.stop(t, false)
			f.drain(t, "ready_fault")
			f.commit(t, "ready_fault")
			b := f.startAPI(t, "candidate_v1", false, nil, nil)
			b.ready(t)
			f.observe(t, "ready_fault", b)
			switch fault {
			case "es_uuid":
				m4HTTP(t, http.MethodDelete, f.x.control.ES.Endpoint()+"/"+f.x.control.ES.Index(), nil, 200)
				if err := f.x.control.ES.EnsureIndex(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "ck_database_uuid":
				if _, err := f.x.control.CK.QueryGlobal(context.Background(), "DROP DATABASE "+f.x.control.CK.Database()); err != nil {
					t.Fatal(err)
				}
				if err := f.x.control.CK.Ensure(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "missing_offset":
				f.candidateES.kill(t)
				m403bWait(t, "ready-fault group empty", func() bool {
					groups, e := f.x.f.queue.Admin.DescribeGroups(context.Background(), f.x.h.ESGroup)
					return e == nil && len(groups[f.x.h.ESGroup].Members) == 0
				})
				if _, err := f.x.f.queue.Admin.DeleteOffsets(context.Background(), f.x.h.ESGroup, kadm.TopicsSet{f.x.f.queue.Topic: map[int32]struct{}{2: {}}}); err != nil {
					t.Fatal(err)
				}
			case "isolation":
				if _, err := f.x.f.queue.PublishBytes(context.Background(), []byte(`{"schema":2,"kind":"ticket","source":"snapshot","event_id":"c2-ready-invalid-event","ticket_id":4,"version":99,"event_at":"2026-11-01T00:00:00Z","document":{}}`), "c2-ready-invalid-event"); err != nil {
					t.Fatal(err)
				}
				m403bWait(t, "real candidate Kafka terminal isolation", func() bool {
					var n int
					f.x.f.db.QueryRow("SELECT COUNT(*) FROM sync_projection_deliveries WHERE version_id=? AND component='es' AND status='isolated'", f.x.version.ID).Scan(&n)
					return n > 0
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, err := f.c.ResumeReadSwitch(ctx, "ready_fault", *f.x.gate.FreezeID, f.probe(b))
			cancel()
			if err == nil {
				t.Fatal("historical API_READY allowed changed destination to resume")
			}
			g, _ := projection.ReadGate(context.Background(), f.x.f.db)
			if g.State != "FROZEN" {
				t.Fatal("fault resumed business")
			}
			m403c2Save(t, "ready-refused", map[string]any{"fault": fault, "error": err.Error(), "gate": g})
		})
	}
}
func TestM403C2RealPrivateRestartEntryBlocksDrainUntilDisabled(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	g, err := projection.Freeze(context.Background(), f.x.f.db, "private restart admission")
	if err != nil {
		t.Fatal(err)
	}
	f.x.gate = g
	m403c1Plan(t, f.c, "restart_entry")
	label := fmt.Sprintf("net.ticketpulse.m403c2.%d", time.Now().UnixNano())
	path := filepath.Join(f.scope.Root, label+".plist")
	plist := []byte("<?xml version=\"1.0\"?><!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\"><plist version=\"1.0\"><dict><key>Label</key><string>" + label + "</string><key>ProgramArguments</key><array><string>/bin/sleep</string><string>60</string></array><key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>")
	if err = os.WriteFile(path, plist, 0600); err != nil {
		t.Fatal(err)
	}
	domain := "gui/" + strconv.Itoa(os.Getuid())
	job := domain + "/" + label
	if out, err := exec.Command("launchctl", "bootstrap", domain, path).CombinedOutput(); err != nil {
		t.Fatalf("private launchd start: %v %s", err, out)
	}
	var stopped bool
	t.Cleanup(func() {
		if !stopped {
			exec.Command("launchctl", "bootout", job).Run()
		}
	})
	scope := f.scope
	scope.LaunchdLabels = []string{label}
	if _, err = f.c.ConfigureReadDrain(context.Background(), "restart_entry", scope); err == nil {
		t.Fatal("real loaded restart entry accepted")
	}
	if out, err := exec.Command("launchctl", "print", job).CombinedOutput(); err != nil || !bytes.Contains(out, []byte(label)) {
		t.Fatalf("private restart identity unknown: %v", err)
	}
	if out, err := exec.Command("launchctl", "bootout", job).CombinedOutput(); err != nil {
		t.Fatalf("private restart stop: %v %s", err, out)
	}
	stopped = true
	hash, _ := readruntime.HashFile(path)
	scope.DisabledArtifacts = []readruntime.Artifact{{Path: path, SHA256: hash}}
	proof, err := f.c.ConfigureReadDrain(context.Background(), "restart_entry", scope)
	if err != nil {
		t.Fatal(err)
	}
	a.stop(t, false)
	f.drain(t, "restart_entry")
	m403c2Save(t, "private-restart-disabled", map[string]any{"private_label": label, "loaded_entry_refused": true, "scope_after_explicit_bootout": proof})
}

func TestM403C2APIDiesAtFinalResumeBoundaryKeepsFreeze(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	f.plan(t, "last_resume_gate")
	a.stop(t, false)
	f.drain(t, "last_resume_gate")
	f.commit(t, "last_resume_gate")
	b := f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	f.observe(t, "last_resume_gate", b)
	f.c.Observe = func(stage string, value any) error {
		if stage == "before_read_resume_commit" {
			b.stop(t, true)
		}
		return nil
	}
	if _, err := f.c.ResumeReadSwitch(context.Background(), "last_resume_gate", *f.x.gate.FreezeID, f.probe(b)); err == nil {
		t.Fatal("dead API at final transaction boundary permitted Resume")
	}
	f.c.Observe = nil
	g, err := projection.ReadGate(context.Background(), f.x.f.db)
	if err != nil || g.State != "FROZEN" {
		t.Fatal("uncommitted OPEN leaked after API death")
	}
	o, err := (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "last_resume_gate")
	if err != nil || o.Stage != projection.ReadAPIReady {
		t.Fatal("uncommitted RESUMED leaked")
	}
	restarted := f.startAPI(t, "candidate_v1", false, nil, nil)
	restarted.ready(t)
	f.observe(t, o.ID, restarted)
	f.resume(t, o.ID, restarted)
	m403c2Save(t, "final-resume-process-death", map[string]any{"gate_after_refusal": g, "stage_after_refusal": o.Stage, "old_boot": b.boot, "new_boot": restarted.boot, "reobserved_and_resumed": true})
}

func TestM403C2DrainInventorySIGKILLRecovery(t *testing.T) {
	f := m403c2New(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	g, err := projection.Freeze(context.Background(), f.x.f.db, "drain crash")
	if err != nil {
		t.Fatal(err)
	}
	f.x.gate = g
	m403c1Plan(t, f.c, "drain_crash")
	f.crash(t, "before_api_drain_plan_commit", "drain_crash", nil)
	var n int
	if err = f.x.f.db.QueryRow("SELECT COUNT(*) FROM sync_projection_read_execution WHERE operation_id='drain_crash'").Scan(&n); err != nil || n != 0 {
		t.Fatal("inventory leaked before commit")
	}
	f.crash(t, "after_api_drain_plan_commit", "drain_crash", nil)
	if _, err = f.c.ConfigureReadDrain(context.Background(), "drain_crash", f.scope); err != nil {
		t.Fatal(err)
	}
	a.stop(t, false)
	f.crash(t, "before_api_drained_commit", "drain_crash", nil)
	e, err := (&projection.ReadControlStore{DB: f.x.f.db}).Execution(context.Background(), "drain_crash")
	if err != nil || len(e.Drained) != 0 {
		t.Fatal("drained leaked before commit")
	}
	f.crash(t, "after_api_drained_commit", "drain_crash", nil)
	e, err = (&projection.ReadControlStore{DB: f.x.f.db}).Execution(context.Background(), "drain_crash")
	if err != nil || len(e.Drained) == 0 {
		t.Fatal("drained commit lost")
	}
	m403c1Validate(t, f.c, "drain_crash")
	f.commit(t, "drain_crash")
	b := f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	f.observe(t, "drain_crash", b)
	f.resume(t, "drain_crash", b)
	m403c2Save(t, "drain-crash-recovered", map[string]any{"execution": e, "selection": mustSelection(t, f.x.f.db)})
}
