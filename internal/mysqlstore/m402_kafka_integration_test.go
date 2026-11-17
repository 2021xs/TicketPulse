//go:build integration && m2 && m3 && m4 && m402

package mysqlstore_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

const m402Bootstrap = "127.0.0.1:19094,127.0.0.1:19096,127.0.0.1:19098"

// Fault helpers are deliberately restricted to the round's exact owned cluster.
// No PID, endpoint or topic can be supplied by a test caller.
func m402Root(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil || os.Getenv("TICKETPULSE_KAFKA_BROKERS") != m402Bootstrap {
		t.Fatal("M4-02 faults require the private three-node bootstrap list", err)
	}
	data, err := os.ReadFile(filepath.Join(root, ".local/m4-02/kafka/cluster.json"))
	var marker struct {
		Project string `json:"project"`
		Runtime string `json:"runtime"`
		ID      string `json:"cluster_id"`
	}
	if err != nil || json.Unmarshal(data, &marker) != nil || marker.Project != root || marker.Runtime != filepath.Join(root, ".local/m4-02/kafka") || marker.ID == "" || marker.ID != os.Getenv("TICKETPULSE_M402_CLUSTER_ID") {
		t.Fatal("M4-02 cluster ownership/identity mismatch", err)
	}
	return root
}

func m402Ready(t *testing.T) {
	t.Helper()
	root := m402Root(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "python3", "-c", "import sys;sys.path.insert(0,'scripts');import m402_environment as e;e.kafka_start()")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("owned cluster readiness: %v %s", err, output)
	}
}

type m402Log struct {
	mu    sync.Mutex
	path  string
	acks  map[syncpipe.RecordRef]string
	phase atomic.Value
}

func m402NewLog(t *testing.T) *m402Log {
	t.Helper()
	m402Root(t)
	dir := os.Getenv("TICKETPULSE_M402_EVIDENCE_DIR")
	if dir == "" {
		t.Fatal("explicit new M4-02 evidence directory required")
	}
	dir = filepath.Join(dir, t.Name())
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "timeline.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	l := &m402Log{path: path, acks: map[syncpipe.RecordRef]string{}}
	l.phase.Store("setup")
	l.add("start", map[string]any{"test": t.Name(), "cluster_id": os.Getenv("TICKETPULSE_M402_CLUSTER_ID"), "bootstrap": m402Bootstrap})
	return l
}
func (l *m402Log) add(kind string, value any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(map[string]any{"at": time.Now().UTC(), "phase": l.phase.Load(), "kind": kind, "data": value}); err != nil {
		panic(err)
	}
}
func (l *m402Log) save(t *testing.T, name string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(l.path), name+".json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
func m402Hash(data []byte) string { v := sha256.Sum256(data); return hex.EncodeToString(v[:]) }
func (l *m402Log) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	var event syncpipe.Event
	_ = json.Unmarshal(r.Value, &event)
	value := map[string]any{"record": syncpipe.Reference(r), "event_id": event.ID, "ticket_id": event.TicketID, "version": event.Version, "kind": event.Kind, "source": event.Source, "commit": event.Commit, "payload_sha256": m402Hash(r.Value), "confirmed": err == nil}
	if err != nil {
		value["error"] = err.Error()
		value["error_type"] = fmt.Sprintf("%T", err)
	} else {
		l.mu.Lock()
		l.acks[syncpipe.Reference(r)] = m402Hash(r.Value)
		l.mu.Unlock()
	}
	l.add("produce-result", value)
}
func (l *m402Log) OnBrokerE2E(meta kgo.BrokerMetadata, key int16, span kgo.BrokerE2E) {
	if span.Err() == nil && key != 0 {
		return
	}
	value := map[string]any{"node_id": meta.NodeID, "api_key": key, "duration_ms": float64(span.DurationE2E().Microseconds()) / 1000, "bytes_written": span.BytesWritten, "bytes_read": span.BytesRead}
	if span.Err() != nil {
		value["error"] = span.Err().Error()
		value["error_type"] = fmt.Sprintf("%T", span.Err())
	}
	l.add("broker-request", value)
}
func m402Observe(t *testing.T, q *syncpipe.Queue, l *m402Log) {
	t.Helper()
	// Clone franz-go's actual input options; only add read-only diagnostics.
	// This avoids a second producer implementation/configuration in the harness.
	opts := append([]kgo.Opt{}, q.Client.Opts()...)
	opts = append(opts, kgo.WithHooks(l))
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	q.Client.Close()
	q.Client = client
	q.Admin = kadm.NewClient(client)
}
func m402Wait(t *testing.T, label string, seconds int, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("M4-02 deadline: " + label)
}
func m402Metadata(q *syncpipe.Queue) (kadm.TopicDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	topics, err := q.Admin.ListTopics(ctx, q.Topic)
	if err != nil {
		return kadm.TopicDetail{}, err
	}
	d, ok := topics[q.Topic]
	if !ok || d.Err != nil {
		return d, fmt.Errorf("topic unavailable: %v", d.Err)
	}
	return d, nil
}
func m402ISR(t *testing.T, q *syncpipe.Queue, n int) kadm.TopicDetail {
	t.Helper()
	var d kadm.TopicDetail
	m402Wait(t, fmt.Sprintf("all partitions ISR=%d", n), 65, func() bool {
		var err error
		d, err = m402Metadata(q)
		if err != nil || len(d.Partitions) != int(q.Partitions) {
			return false
		}
		for _, p := range d.Partitions {
			if p.Leader < 0 || len(p.ISR) != n {
				return false
			}
		}
		return true
	})
	return d
}
func m402Control(t *testing.T, l *m402Log, action string, node int32, hard bool) {
	t.Helper()
	root := m402Root(t)
	if node < 401 || node > 403 {
		t.Fatal("refusing non-owned Kafka node")
	}
	args := []string{"scripts/m402_environment.py", action, "--node", fmt.Sprint(node)}
	if hard {
		args = append(args, "--sigkill")
	}
	l.add("fault-command-start", map[string]any{"command": append([]string{"python3"}, args...), "node_id": node})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "python3", args...)
	command.Dir = root
	output, err := command.CombinedOutput()
	l.add("fault-command-end", map[string]any{"node_id": node, "action": action, "output": string(output), "error": fmt.Sprint(err)})
	if err != nil {
		t.Fatalf("owned node fault failed: %v %s", err, output)
	}
}
func m402Restore(t *testing.T, l *m402Log) {
	t.Helper()
	t.Cleanup(func() {
		for n := int32(401); n <= 403; n++ {
			m402Control(t, l, "node-start", n, false)
		}
		m402Ready(t)
	})
}
func m402CLI(t *testing.T, l *m402Log, label string) {
	t.Helper()
	root := m402Root(t)
	code := "import sys;sys.path.insert(0,'scripts');import m402_environment as e;r=e.cli('kafka-metadata-quorum.sh',['--bootstrap-server',e.BOOTSTRAP,'describe','--status'],8,False);print(r.stdout+r.stderr);sys.exit(r.returncode)"
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "python3", "-c", code)
	command.Dir = root
	output, err := command.CombinedOutput()
	l.save(t, label+"-quorum", map[string]any{"output": string(output), "error": fmt.Sprint(err), "at": time.Now().UTC()})
}
func m402Snapshot(t *testing.T, f *m3fixture, l *m402Log, label string) {
	t.Helper()
	snapshot := map[string]any{"at": time.Now().UTC(), "database": f.name, "topic": f.queue.Topic}
	d, err := m402Metadata(f.queue)
	snapshot["topic_metadata"] = d
	snapshot["metadata_error"] = fmt.Sprint(err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	configs, err := f.queue.Admin.DescribeTopicConfigs(ctx, f.queue.Topic, "__consumer_offsets")
	cancel()
	snapshot["topic_configs"] = configs
	snapshot["topic_configs_error"] = fmt.Sprint(err)
	for _, component := range []string{"es", "ck"} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		p, e := f.queue.Progress(ctx, component)
		cancel()
		snapshot[component+"_progress"] = p
		snapshot[component+"_progress_error"] = fmt.Sprint(e)
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		g, e := f.queue.Admin.DescribeGroups(ctx, f.queue.Group(component))
		cancel()
		snapshot[component+"_group"] = g
		snapshot[component+"_group_error"] = fmt.Sprint(e)
	}
	cp, err := f.state.Checkpoint(context.Background())
	snapshot["checkpoint"] = cp
	snapshot["checkpoint_error"] = fmt.Sprint(err)
	snapshot["worker_states"] = m4Rows(t, f, "SELECT component,state,heartbeat_at,last_error FROM sync_worker_state ORDER BY component")
	snapshot["pipeline_identity"] = m4Rows(t, f, "SELECT * FROM sync_kafka_pipeline")
	snapshot["watermarks"] = m4Rows(t, f, "SELECT * FROM sync_kafka_watermarks ORDER BY component,partition_id")
	l.save(t, label, snapshot)
	l.add("snapshot", map[string]any{"label": label})
}

func m402BrokerEvidence(t *testing.T, l *m402Log, topic string) []string {
	t.Helper()
	root := m402Root(t)
	lines := []string{}
	for node := 401; node <= 403; node++ {
		path := filepath.Join(root, ".local/m4-02/kafka", fmt.Sprintf("node-%d/logs/server.log", node))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, topic) && (strings.Contains(line, "NotEnoughReplicas") || strings.Contains(line, "insufficient")) {
				lines = append(lines, fmt.Sprintf("node-%d: %s", node, line))
			}
		}
	}
	l.save(t, "broker-minisr-errors", lines)
	return lines
}

type m402Worker struct {
	command  *exec.Cmd
	done     chan error
	log      *os.File
	mode     string
	stopped  bool
	recorder *m402Log
}

func m402Start(t *testing.T, f *m3fixture, l *m402Log, mode, label string) *m402Worker {
	t.Helper()
	binary := os.Getenv("TICKETPULSE_SYNC_BINARY")
	if binary == "" || !strings.HasPrefix(binary, filepath.Join(m402Root(t), ".local/m4-02/")) {
		t.Fatal("owned sync binary required")
	}
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = f.name
	file, err := os.OpenFile(filepath.Join(filepath.Dir(l.path), label+".log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-mode="+mode, "-server-id=340204")
	command.Dir = m402Root(t)
	command.Env = append(os.Environ(), "TICKETPULSE_SYNC_DSN="+cfg.FormatDSN(), "TICKETPULSE_SEARCH_INDEX="+f.name+"_tickets_v1", "TICKETPULSE_CK_DATABASE="+f.name+"_analytics_v1")
	command.Stdout = file
	command.Stderr = file
	if err := command.Start(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	w := &m402Worker{command: command, done: make(chan error, 1), log: file, mode: mode, recorder: l}
	go func() { w.done <- command.Wait() }()
	l.add("worker-started", map[string]any{"mode": mode, "pid": command.Process.Pid, "log": label + ".log"})
	t.Cleanup(func() { w.stop(t) })
	return w
}
func (w *m402Worker) stop(t *testing.T) {
	t.Helper()
	if w.stopped {
		return
	}
	w.stopped = true
	_ = w.command.Process.Signal(os.Interrupt)
	select {
	case err := <-w.done:
		w.recorder.add("worker-stopped", map[string]any{"mode": w.mode, "pid": w.command.Process.Pid, "error": fmt.Sprint(err)})
	case <-time.After(14 * time.Second):
		_ = w.command.Process.Kill()
		<-w.done
		t.Error("owned sync stop exceeded deadline")
	}
	w.log.Close()
}
func m402Caught(t *testing.T, f *m3fixture) {
	t.Helper()
	// The all-partition source marker is ordered after every preceding business
	// transaction. Lag=0 against an old Kafka end alone cannot prove capture.
	if err := f.state.IssueWatermark(context.Background()); err != nil {
		t.Fatal(err)
	}
	var boundary time.Time
	if err := f.db.QueryRow("SELECT issued_at FROM sync_source_watermark WHERE source_name='tickets-v1'").Scan(&boundary); err != nil {
		t.Fatal(err)
	}
	m402Wait(t, "both consumer groups caught up", 75, func() bool {
		for _, c := range []string{"es", "ck"} {
			var confirmed int
			// The running capture replaces its last marker every two seconds. A
			// newer committed source marker is also proof of this ordered boundary.
			if err := f.db.QueryRow("SELECT count(*) FROM sync_kafka_watermarks WHERE component=? AND issued_at>=?", c, boundary).Scan(&confirmed); err != nil || confirmed != int(f.queue.Partitions) {
				return false
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			p, err := f.queue.Progress(ctx, c)
			cancel()
			if err != nil || len(p) != int(f.queue.Partitions) {
				return false
			}
			for _, v := range p {
				if v.End == 0 || v.Lag != 0 {
					return false
				}
			}
		}
		return true
	})
}
func m402HTTPCreate(t *testing.T, f *m3fixture, l *m402Log, label string) ticket.Ticket {
	t.Helper()
	started := time.Now().UTC()
	var r ticket.CreateResult
	if err := json.Unmarshal(f.call(t, "local-alice", "POST", "/tickets", request("m402-"+label), 201), &r); err != nil {
		t.Fatal(err)
	}
	l.add("http-write", map[string]any{"route": "create", "status": 201, "ticket_id": r.Ticket.ID, "version": r.Ticket.Version, "started_at": started, "duration_ms": float64(time.Since(started).Microseconds()) / 1000})
	return r.Ticket
}
func m402HTTPAction(t *testing.T, f *m3fixture, l *m402Log, current ticket.Ticket, action string, fields map[string]any) ticket.Ticket {
	t.Helper()
	started := time.Now().UTC()
	r := f.actionHTTP(t, "local-it-agent", action, current.ID, current.Version, fields)
	l.add("http-write", map[string]any{"route": action, "status": 200, "ticket_id": r.ID, "version": r.Version, "started_at": started, "duration_ms": float64(time.Since(started).Microseconds()) / 1000})
	return r
}
func m402Resolve(t *testing.T, f *m3fixture, l *m402Log, current ticket.Ticket) ticket.Ticket {
	t.Helper()
	current = m402HTTPAction(t, f, l, current, "claim", nil)
	current = m402HTTPAction(t, f, l, current, "start", nil)
	return m402HTTPAction(t, f, l, current, "resolve", map[string]any{"resolution_note": "isolated Kafka fault verification completed"})
}

type m402RawRecord struct {
	Ref      syncpipe.RecordRef `json:"record"`
	ID       string             `json:"event_id"`
	TicketID int64              `json:"ticket_id"`
	Version  int64              `json:"version"`
	Kind     string             `json:"kind"`
	Source   string             `json:"source"`
	Commit   *syncpipe.Position `json:"commit,omitempty"`
	SHA      string             `json:"payload_sha256"`
}

func m402ReadAll(t *testing.T, q *syncpipe.Queue) []m402RawRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ends, err := q.Admin.ListEndOffsets(ctx, q.Topic)
	if err != nil {
		t.Fatal(err)
	}
	starts := map[int32]kgo.Offset{}
	var total int64
	for p := int32(0); p < q.Partitions; p++ {
		e, _ := ends.Lookup(q.Topic, p)
		if e.Err != nil {
			t.Fatal(e.Err)
		}
		starts[p] = kgo.NewOffset().At(0)
		total += e.Offset
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(q.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{q.Topic: starts}), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result := []m402RawRecord{}
	for int64(len(result)) < total {
		fetch := client.PollRecords(ctx, 500)
		if err := fetch.Err(); err != nil {
			t.Fatal("raw Kafka read", err)
		}
		for _, r := range fetch.Records() {
			end, _ := ends.Lookup(q.Topic, r.Partition)
			if r.Offset >= end.Offset {
				continue
			}
			var e syncpipe.Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				t.Fatal(err)
			}
			result = append(result, m402RawRecord{syncpipe.Reference(r), e.ID, e.TicketID, e.Version, e.Kind, e.Source, e.Commit, m402Hash(r.Value)})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Ref.Partition != result[j].Ref.Partition {
			return result[i].Ref.Partition < result[j].Ref.Partition
		}
		return result[i].Ref.Offset < result[j].Ref.Offset
	})
	next := map[int32]int64{}
	for _, r := range result {
		if r.Ref.Offset != next[r.Ref.Partition] {
			t.Fatalf("raw Kafka offset hole: %+v expected=%d", r, next[r.Ref.Partition])
		}
		next[r.Ref.Partition]++
	}
	return result
}
func m402Audit(t *testing.T, f *m3fixture, l *m402Log, label string) {
	t.Helper()
	m402Caught(t, f)
	j := syncpipe.RebuildJob{Target: "both", Index: f.name + "_tickets_v1", Database: f.name + "_analytics_v1"}
	m4AssertProjection(t, f, j)
	l.mu.Lock()
	confirmed := make(map[syncpipe.RecordRef]string, len(l.acks))
	for ref, hash := range l.acks {
		confirmed[ref] = hash
	}
	l.mu.Unlock()
	raw := m402ReadAll(t, f.queue)
	l.save(t, label+"-kafka-records", raw)
	retained := map[syncpipe.RecordRef]string{}
	for _, r := range raw {
		retained[r.Ref] = r.SHA
	}
	acks := []map[string]any{}
	for ref, hash := range confirmed {
		if retained[ref] != hash {
			t.Fatalf("confirmed Kafka record lost or changed: %+v", ref)
		}
		acks = append(acks, map[string]any{"record": ref, "sha256": hash})
	}
	l.save(t, label+"-ack-integrity", map[string]any{"confirmed_records_checked": len(acks), "all_confirmed_retained_with_same_payload": true, "acks": acks})
	for _, c := range []string{"es", "ck"} {
		rows := m4Rows(t, f, "SELECT partition_id,record_offset,ticket_id,ticket_version,status,attempts,last_error FROM sync_kafka_"+c+"_deliveries ORDER BY partition_id,record_offset")
		l.save(t, label+"-"+c+"-deliveries", rows)
		seen := map[syncpipe.RecordRef]bool{}
		for _, r := range rows {
			p := int32(r["partition_id"].(int64))
			n := r["record_offset"].(int64)
			s := r["status"].(string)
			if s != "applied" && s != "duplicate_or_old" {
				t.Fatalf("unexpected disposition: %v", r)
			}
			seen[syncpipe.RecordRef{Topic: f.queue.Topic, Partition: p, Offset: n}] = true
		}
		for _, r := range raw {
			if !seen[r.Ref] {
				t.Fatalf("%s delivery offset omitted: %+v", c, r.Ref)
			}
		}
	}
	l.save(t, label+"-source-tickets", m4Rows(t, f, "SELECT * FROM tickets ORDER BY id"))
	l.save(t, label+"-source-histories", m4Rows(t, f, "SELECT * FROM ticket_histories ORDER BY ticket_id,version"))
	l.save(t, label+"-es", json.RawMessage(m4HTTP(t, http.MethodGet, os.Getenv("TICKETPULSE_ES_URL")+"/"+j.Index+"/_search?size=1000&sort=id:asc", nil, 200)))
	l.save(t, label+"-ck", map[string]any{"events": strings.Split(f.ckSQL(t, "SELECT * FROM ticket_events ORDER BY ticket_id,version FORMAT JSONEachRow"), "\n"), "states": strings.Split(f.ckSQL(t, "SELECT * FROM ticket_states ORDER BY ticket_id,version FORMAT JSONEachRow"), "\n")})
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	report, err := syncpipe.Reconcile(ctx, f.state, f.queue, f.es, f.ck, syncpipe.ReconcileOptions{BatchSize: 100, LagTimeout: 25 * time.Second, OrganizationID: 1, MetricsRequest: m4Window})
	l.save(t, label+"-reconcile", map[string]any{"report": report, "error": fmt.Sprint(err)})
	if err != nil || report.State != syncpipe.ReconcileConsistent {
		t.Fatalf("full reconciliation state=%s err=%v", report.State, err)
	}
	l.add("correctness-verified", map[string]any{"label": label, "raw_records": len(raw), "confirmed_records": len(acks), "source_tickets": report.Source.Tickets, "source_histories": report.Source.Histories, "state": report.State, "independent_all_fields_checked": true})
}

func TestM402ReplicaFaultMatrixAtoEAndH(t *testing.T) {
	m402Ready(t)
	f, _ := newM4(t)
	l := m402NewLog(t)
	m402Observe(t, f.queue, l)
	m402Restore(t, l)
	m4LoadGold(t, f)
	var bArmed, dArmed atomic.Bool
	bResult := make(chan map[string]any, 1)
	dResult := make(chan map[string]any, 1)
	captureCtx, captureCancel := context.WithCancel(context.Background())
	t.Cleanup(captureCancel)
	captureDone := make(chan error, 1)
	capture := &syncpipe.Capture{Store: f.state, Queue: f.queue, Config: syncpipe.CaptureConfig{Address: os.Getenv("TICKETPULSE_CDC_ADDR"), User: os.Getenv("TICKETPULSE_CDC_USER"), Password: os.Getenv("TICKETPULSE_CDC_PASSWORD"), ServerID: 340202}}
	capture.AfterPublished = func(e syncpipe.Event, ref syncpipe.RecordRef) error {
		if e.Source != "binlog" {
			return nil
		}
		if e.Version == 1 && bArmed.CompareAndSwap(true, false) {
			d, err := m402Metadata(f.queue)
			if err != nil {
				return err
			}
			leader := d.Partitions[ref.Partition].Leader
			at := time.Now().UTC()
			l.add("B-confirmed-before-leader-kill", map[string]any{"record": ref, "leader": leader, "ticket_id": e.TicketID})
			m402Control(t, l, "node-stop", leader, true)
			bResult <- map[string]any{"record": ref, "old_leader": leader, "kill_requested_at": at, "ticket_id": e.TicketID}
		}
		if e.Version == 2 && dArmed.CompareAndSwap(true, false) {
			d, err := m402Metadata(f.queue)
			if err != nil {
				return err
			}
			leader := d.Partitions[ref.Partition].Leader
			cp, err := f.state.Checkpoint(context.Background())
			if err != nil {
				return err
			}
			at := time.Now().UTC()
			l.add("D-first-event-confirmed", map[string]any{"record": ref, "checkpoint": cp, "transaction_commit": e.Commit, "surviving_node": leader})
			for n := int32(401); n <= 403; n++ {
				if n != leader {
					m402Control(t, l, "node-stop", n, true)
				}
			}
			dResult <- map[string]any{"record": ref, "checkpoint": cp, "transaction_commit": *e.Commit, "surviving_node": leader, "kill_requested_at": at}
		}
		return nil
	}
	go func() { captureDone <- capture.Run(captureCtx) }()
	m402Wait(t, "real capture started", 20, func() bool {
		var state string
		_ = f.db.QueryRow("SELECT state FROM sync_worker_state WHERE component='cdc'").Scan(&state)
		return state == "running"
	})
	es1 := m402Start(t, f, l, "consume", "es-primary")
	ck1 := m402Start(t, f, l, "ck", "ck-primary")
	_ = es1
	_ = ck1
	m402Caught(t, f)
	m402ISR(t, f.queue, 3)
	l.phase.Store("A")
	for i := 0; i < 6; i++ {
		m402Resolve(t, f, l, m402HTTPCreate(t, f, l, fmt.Sprintf("A-%d", i)))
	}
	m402Caught(t, f)
	l.save(t, "A-gold", f.gold(t))
	m402Snapshot(t, f, l, "A-baseline")
	m402CLI(t, l, "A-baseline")
	m402Audit(t, f, l, "A")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	internals, err := f.queue.Admin.ListTopics(ctx, "__consumer_offsets")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range internals["__consumer_offsets"].Partitions {
		if len(p.Replicas) != 3 || len(p.ISR) != 3 {
			t.Fatal("internal offsets not RF3/ISR3", p)
		}
	}
	l.save(t, "offsets-topic-metadata", internals)
	ctx, cancel = context.WithTimeout(context.Background(), 4*time.Second)
	configs, err := f.queue.Admin.DescribeBrokerConfigs(ctx, 401, 402, 403)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	l.save(t, "effective-broker-configs", configs)
	l.phase.Store("B")
	bArmed.Store(true)
	current := m402HTTPCreate(t, f, l, "B-leader-ack")
	var b map[string]any
	select {
	case b = <-bResult:
	case <-time.After(30 * time.Second):
		t.Fatal("leader kill did not follow real ACK")
	}
	old := b["old_leader"].(int32)
	ref := b["record"].(syncpipe.RecordRef)
	m402Wait(t, "new partition leader after SIGKILL", 40, func() bool {
		d, e := m402Metadata(f.queue)
		return e == nil && d.Partitions[ref.Partition].Leader >= 0 && d.Partitions[ref.Partition].Leader != old && len(d.Partitions[ref.Partition].ISR) == 2
	})
	b["new_leader_observed_at"] = time.Now().UTC()
	d, _ := m402Metadata(f.queue)
	b["new_leader"] = d.Partitions[ref.Partition].Leader
	m402Resolve(t, f, l, current)
	for i := 0; i < 3; i++ {
		m402Resolve(t, f, l, m402HTTPCreate(t, f, l, fmt.Sprintf("B-during-outage-%d", i)))
	}
	m402Caught(t, f)
	b["both_groups_caught_up_at"] = time.Now().UTC()
	l.save(t, "B-result", b)
	m402Snapshot(t, f, l, "B-one-node-dead")
	m402CLI(t, l, "B-one-node-dead")
	m402Audit(t, f, l, "B")
	m402Control(t, l, "node-start", old, false)
	m402ISR(t, f.queue, 3)
	l.phase.Store("C")
	d, _ = m402Metadata(f.queue)
	p := d.Partitions[ref.Partition]
	var follower int32
	for _, n := range p.Replicas {
		if n != p.Leader {
			follower = n
			break
		}
	}
	cStart := time.Now().UTC()
	m402Control(t, l, "node-stop", follower, false)
	m402ISR(t, f.queue, 2)
	for i := 0; i < 5; i++ {
		m402Resolve(t, f, l, m402HTTPCreate(t, f, l, fmt.Sprintf("C-follower-down-%d", i)))
	}
	m402Caught(t, f)
	m402Snapshot(t, f, l, "C-follower-dead")
	l.save(t, "C-result", map[string]any{"follower": follower, "selected_partition_leader": p.Leader, "started_at": cStart, "all_writes_and_consumers_caught_up_at": time.Now().UTC()})
	m402Audit(t, f, l, "C")
	m402Control(t, l, "node-start", follower, false)
	m402ISR(t, f.queue, 3)
	first := m402HTTPCreate(t, f, l, "D-multi-event-first")
	second := m402HTTPCreate(t, f, l, "D-multi-event-second")
	m402Caught(t, f)
	l.phase.Store("D")
	dArmed.Store(true)
	tx, err := f.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []ticket.Ticket{first, second} {
		after := v
		after.Version = 2
		after.Title = "M402 same committed transaction " + v.TicketNo
		after.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
		oldState, _ := json.Marshal(v.State())
		newState, _ := json.Marshal(after.State())
		if _, err = tx.Exec("UPDATE tickets SET title=?,version=2,updated_at=? WHERE id=?", after.Title, after.UpdatedAt, v.ID); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec("INSERT INTO ticket_histories(organization_id,ticket_id,action,operator_id,version,before_state,after_state,responsible_team_id,occurred_at) VALUES(1,?,'assignee_transferred',5,2,?,?,?,?)", v.ID, oldState, newState, after.CurrentTeamID, after.UpdatedAt); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var outage map[string]any
	select {
	case outage = <-dResult:
	case <-time.After(35 * time.Second):
		t.Fatal("two-node kill did not follow first transaction event ACK")
	}
	var captureError error
	select {
	case captureError = <-captureDone:
	case <-time.After(25 * time.Second):
		t.Fatal("producer did not stop confirmation at its bounded timeout")
	}
	if captureError == nil || !strings.Contains(captureError.Error(), "publication not confirmed") {
		t.Fatal("expected explicit unconfirmed publication", captureError)
	}
	cp, err := f.state.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	before := outage["checkpoint"].(syncpipe.Checkpoint)
	commit := outage["transaction_commit"].(syncpipe.Position)
	if cp.Position != before.Position || cp.File != commit.File || cp.Pos >= commit.Pos {
		t.Fatalf("checkpoint passed partially ACKed transaction: before=%+v after=%+v commit=%+v", before, cp, commit)
	}
	outage["producer_failure_observed_at"] = time.Now().UTC()
	outage["producer_error"] = captureError.Error()
	outage["checkpoint_after_failure"] = cp
	outage["controller_majority_lost_too"] = true
	extra := m402HTTPCreate(t, f, l, "D-http-while-two-dead")
	outage["http_while_two_dead"] = map[string]any{"status": 201, "ticket_id": extra.ID}
	outage["checkpoint_after_http"], _ = f.state.Checkpoint(context.Background())
	l.save(t, "D-result", outage)
	m402CLI(t, l, "D-two-nodes-dead")
	l.phase.Store("E-H")
	recovery := time.Now().UTC()
	survivor := outage["surviving_node"].(int32)
	for n := int32(401); n <= 403; n++ {
		if n != survivor {
			m402Control(t, l, "node-start", n, false)
		}
	}
	m402Ready(t)
	es2 := m402Start(t, f, l, "consume", "es-recovery-second-member")
	ck2 := m402Start(t, f, l, "ck", "ck-recovery-second-member")
	cdc := m402Start(t, f, l, "capture", "cdc-restarted-from-checkpoint")
	_ = cdc
	m402Wait(t, "two stable members in both real consumer groups", 70, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		groups, e := f.queue.Admin.DescribeGroups(ctx, f.queue.Group("es"), f.queue.Group("ck"))
		if e != nil {
			return false
		}
		for _, c := range []string{"es", "ck"} {
			g := groups[f.queue.Group(c)]
			if g.Err != nil || g.State != "Stable" || len(g.Members) != 2 {
				return false
			}
		}
		return true
	})
	m402Caught(t, f)
	m402ISR(t, f.queue, 3)
	recoveredAt := time.Now().UTC()
	m402Snapshot(t, f, l, "H-two-members-recovered")
	m402CLI(t, l, "E-H-recovered")
	for _, id := range []int64{first.ID, second.ID, extra.ID} {
		detail, e := f.service.Detail(context.Background(), 5, id)
		if e != nil {
			t.Fatal(e)
		}
		m402Resolve(t, f, l, detail.Ticket)
	}
	m402Caught(t, f)
	es2.stop(t)
	ck2.stop(t)
	m402Caught(t, f)
	l.save(t, "E-result", map[string]any{"recovery_started_at": recovery, "all_partitions_ISR3_and_groups_caught_up_at": recoveredAt, "followup_writes_caught_up_at": time.Now().UTC(), "gold": f.gold(t)})
	m402Audit(t, f, l, "E-final")
	m402Snapshot(t, f, l, "E-final")
	// Replay of the first already-confirmed v2 is allowed and actually occurs.
	raw := m402ReadAll(t, f.queue)
	count := 0
	for _, record := range raw {
		if (record.TicketID == first.ID || record.TicketID == second.ID) && record.Version == 2 && record.Source == "binlog" {
			count++
		}
	}
	if count < 3 {
		t.Fatalf("partial transaction did not replay first event: count=%d", count)
	}
	l.save(t, "partial-transaction-replay", map[string]any{"two_business_events": 2, "physical_binlog_records_for_versions": count, "checkpoint_did_not_cross_unconfirmed_transaction": true, "all_business_facts_canonical": true})
}

func TestM402MinISRWithControllerQuorumAvailable(t *testing.T) {
	m402Ready(t)
	f := newM3(t)
	l := m402NewLog(t)
	m402Restore(t, l)
	q, err := syncpipe.NewQueue(strings.Split(m402Bootstrap, ","), f.queue.Topic+".minisr_control", 1)
	if err != nil {
		t.Fatal(err)
	}
	q.ReplicationFactor = 2
	q.MinInSyncReplicas = 2
	t.Cleanup(q.Close)
	t.Cleanup(func() { _ = q.DeleteTestTopic(context.Background()) })
	m402Observe(t, q, l)
	m402Wait(t, "minISR control Topic leaders ready", 25, func() bool { return q.Ensure(context.Background()) == nil })
	q.ReplicationFactor = 3
	if mismatch := q.Ensure(context.Background()); mismatch == nil || !strings.Contains(mismatch.Error(), "replication factor differs") {
		t.Fatalf("existing replica mismatch accepted: %v", mismatch)
	}
	q.ReplicationFactor = 2
	q.MinInSyncReplicas = 1
	if mismatch := q.Ensure(context.Background()); mismatch == nil || !strings.Contains(mismatch.Error(), "min.insync.replicas") {
		t.Fatalf("existing minISR mismatch accepted: %v", mismatch)
	}
	q.MinInSyncReplicas = 2
	d := m402ISR(t, q, 2)
	current := f.create(t, 1, request("m402-minisr-control"))
	event := syncpipe.Event{Schema: 1, ID: "minisr-first-confirmed", Source: "snapshot", TicketID: current.Ticket.ID, Version: 1, EventAt: current.Ticket.UpdatedAt, Document: search.FromTicket(current.Ticket)}
	confirmed, err := q.Publish(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	p := d.Partitions[0]
	var follower int32
	for _, n := range p.Replicas {
		if n != p.Leader {
			follower = n
		}
	}
	l.phase.Store("D-minISR-control")
	m402Control(t, l, "node-stop", follower, true)
	m402ISR(t, q, 1)
	m402CLI(t, l, "controller-quorum-still-available")
	event.ID = "minisr-unconfirmed"
	start := time.Now().UTC()
	_, err = q.Publish(context.Background(), event)
	// The producer retries the Broker's NOT_ENOUGH_REPLICAS and can surface
	// its finite delivery timeout. Verify the actual Broker cause independently.
	brokerErrors := m402BrokerEvidence(t, l, q.Topic)
	if err == nil || len(brokerErrors) == 0 {
		t.Fatalf("expected observed Broker minISR2 rejection with quorum alive: %v raw=%v", err, brokerErrors)
	}
	l.save(t, "minisr-rejection", map[string]any{"topic": q.Topic, "replication_factor": 2, "min_insync_replicas": 2, "actual_ISR": 1, "leader": p.Leader, "stopped_follower": follower, "controller_voters_remaining": 2, "confirmed_before": confirmed, "error": err.Error(), "started_at": start, "failed_at": time.Now().UTC()})
	m402Control(t, l, "node-start", follower, false)
	m402ISR(t, q, 2)
	event.ID = "minisr-recovered"
	recovered, err := q.Publish(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	l.save(t, "retained-after-recovery", map[string]any{"records": m402ReadAll(t, q), "recovered_ack": recovered, "confirmed_before_payload": m402Hash(mustM402Read(t, q, confirmed))})
}
func mustM402Read(t *testing.T, q *syncpipe.Queue, ref syncpipe.RecordRef) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	b, e := q.ReadRecord(ctx, ref)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

func TestM402DownstreamWriteBeforeOffsetAndVersionReplayFG(t *testing.T) {
	m402Ready(t)
	f, _ := newM4(t)
	l := m402NewLog(t)
	m402Observe(t, f.queue, l)
	m402Restore(t, l)
	m4LoadGold(t, f)
	cdc := m402Start(t, f, l, "capture", "cdc")
	_ = cdc
	es := m402Start(t, f, l, "consume", "es-initial")
	ck := m402Start(t, f, l, "ck", "ck-initial")
	m402Caught(t, f)
	l.save(t, "initial-gold", f.gold(t))
	l.phase.Store("F")
	f.exec(t, "CREATE TRIGGER m402_es_precommit BEFORE UPDATE ON sync_kafka_es_deliveries FOR EACH ROW BEGIN IF NEW.ticket_id=4 AND NEW.ticket_version=11 AND NEW.status IN ('applied','duplicate_or_old') THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='M402 ES committed but progress not saved'; END IF; END")
	t.Cleanup(func() { _, _ = f.db.Exec("DROP TRIGGER IF EXISTS m402_es_precommit") })
	current := f.actionHTTP(t, "local-device-agent", "comments", 4, 10, map[string]any{"content": "M402 ES acknowledged projection before progress failure"})
	m402Wait(t, "ES write succeeded v11", 25, func() bool { return m402ESVersion(f, 4) == 11 })
	var ref syncpipe.RecordRef
	ref.Topic = f.queue.Topic
	m402Wait(t, "failed ES durable progress row", 25, func() bool {
		return f.db.QueryRow("SELECT partition_id,record_offset FROM sync_kafka_es_deliveries WHERE ticket_id=4 AND ticket_version=11 AND status='processing'").Scan(&ref.Partition, &ref.Offset) == nil
	})
	es.stop(t)
	m402Uncommitted(t, f.queue, "es", ref)
	m402Snapshot(t, f, l, "F-after-ES-write-before-offset")
	current = f.actionHTTP(t, "local-device-agent", "comments", 4, current.Version, map[string]any{"content": "newer source version arrives before delayed ES replay"})
	// A real source-derived external-version write simulates another snapshot
	// writer getting the new version into ES before the old uncommitted record.
	old, err := f.es.Put(context.Background(), search.FromTicket(current))
	if err != nil || old {
		t.Fatal("newer version precondition", old, err)
	}
	f.exec(t, "DROP TRIGGER m402_es_precommit")
	es = m402Start(t, f, l, "consume", "es-replay")
	_ = es
	m402Caught(t, f)
	var status string
	f.db.QueryRow("SELECT status FROM sync_kafka_es_deliveries WHERE partition_id=? AND record_offset=?", ref.Partition, ref.Offset).Scan(&status)
	if status != "duplicate_or_old" || m402ESVersion(f, 4) != 12 {
		t.Fatal("ES replay overwrote new version", status)
	}
	l.save(t, "F-result", map[string]any{"uncommitted_record": ref, "ES_written_version_before_failure": 11, "new_version_before_replay": 12, "replay_disposition": status, "current_external_version": m402ESVersion(f, 4), "gold": f.gold(t)})
	l.phase.Store("G")
	f.exec(t, "CREATE TRIGGER m402_ck_precommit BEFORE UPDATE ON sync_kafka_ck_deliveries FOR EACH ROW BEGIN IF NEW.ticket_id=4 AND NEW.ticket_version=13 AND NEW.status='applied' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='M402 both CK writes committed but progress not saved'; END IF; END")
	t.Cleanup(func() { _, _ = f.db.Exec("DROP TRIGGER IF EXISTS m402_ck_precommit") })
	current = f.actionHTTP(t, "local-device-agent", "comments", 4, current.Version, map[string]any{"content": "M402 CK writes before offset interruption"})
	m402Wait(t, "CK both writes succeeded v13", 25, func() bool {
		v, err := f.ck.Query(context.Background(), "SELECT count() FROM ticket_events WHERE ticket_id=4 AND version=13 FORMAT TabSeparated")
		s, e := f.ck.Query(context.Background(), "SELECT max(version) FROM ticket_states WHERE ticket_id=4 FORMAT TabSeparated")
		return err == nil && e == nil && strings.TrimSpace(string(v)) != "0" && strings.TrimSpace(string(s)) == "13"
	})
	var ckRef syncpipe.RecordRef
	ckRef.Topic = f.queue.Topic
	m402Wait(t, "failed CK durable progress row", 25, func() bool {
		return f.db.QueryRow("SELECT partition_id,record_offset FROM sync_kafka_ck_deliveries WHERE ticket_id=4 AND ticket_version=13 AND status='processing'").Scan(&ckRef.Partition, &ckRef.Offset) == nil
	})
	ck.stop(t)
	m402Uncommitted(t, f.queue, "ck", ckRef)
	physicalBefore := f.ckSQL(t, "SELECT count() FROM ticket_events WHERE ticket_id=4 AND version=13 FORMAT TabSeparated")
	m402Snapshot(t, f, l, "G-after-CK-writes-before-offset")
	current = f.actionHTTP(t, "local-device-agent", "comments", 4, current.Version, map[string]any{"content": "newer source version before delayed CK replay"})
	newEvent := f.event(t, 4, current.Version, "m402-newer-CK-projection")
	fact, state, err := syncpipe.AnalysisProjection(newEvent)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ck.InsertFact(context.Background(), *fact); err != nil {
		t.Fatal(err)
	}
	if err = f.ck.InsertCurrent(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	f.exec(t, "DROP TRIGGER m402_ck_precommit")
	ck = m402Start(t, f, l, "ck", "ck-replay")
	_ = ck
	m402Caught(t, f)
	physicalAfter := f.ckSQL(t, "SELECT count() FROM ticket_events WHERE ticket_id=4 AND version=13 FORMAT TabSeparated")
	if physicalBefore == physicalAfter {
		t.Fatal("CK precommit interruption did not cause physical replay")
	}
	// Old-version late arrival and a duplicate first resolution come from the
	// original immutable SQL histories, including the historical event team.
	for _, version := range []int64{4, 8, 8, 13} {
		e := f.event(t, 4, version, fmt.Sprintf("m402-late-replay-%d-%d", version, time.Now().UnixNano()))
		if _, err = f.queue.Publish(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	m402Caught(t, f)
	if m402ESVersion(f, 4) != 14 || f.ckSQL(t, "SELECT max(version) FROM ticket_states WHERE ticket_id=4 FORMAT TabSeparated") != "14" {
		t.Fatal("late replay downgraded current version")
	}
	l.save(t, "G-result", map[string]any{"uncommitted_record": ckRef, "physical_v13_before": physicalBefore, "physical_v13_after_replay": physicalAfter, "current_version": 14, "canonical_v13_facts": f.ckSQL(t, "SELECT uniqExact(tuple(organization_id,ticket_id,version)) FROM ticket_events WHERE ticket_id=4 AND version=13 FORMAT TabSeparated"), "gold": f.gold(t)})
	m402Audit(t, f, l, "FG-final")
	m402Snapshot(t, f, l, "FG-final")
}
func m402ESVersion(f *m3fixture, id int64) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/%s_tickets_v1/_doc/%d", os.Getenv("TICKETPULSE_ES_URL"), f.name, id), nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	defer res.Body.Close()
	var d struct {
		Version int64 `json:"_version"`
	}
	_ = json.NewDecoder(res.Body).Decode(&d)
	return d.Version
}
func m402Uncommitted(t *testing.T, q *syncpipe.Queue, c string, ref syncpipe.RecordRef) {
	t.Helper()
	p, e := q.Progress(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	for _, v := range p {
		if v.Partition == ref.Partition && v.Committed > ref.Offset {
			t.Fatalf("%s committed beyond unwritten progress: %+v ref=%+v", c, v, ref)
		}
	}
}

func TestM402TopicIdentityChangeRejectedI(t *testing.T) {
	m402Ready(t)
	f, r := newM4(t)
	l := m402NewLog(t)
	m402Restore(t, l)
	m4LoadGold(t, f)
	f.startCapture(t)
	j := m4Job(t, f, r, "m402_identity", "both")
	j = m4Run(t, r, j.ID)
	f.stopCapture(t)
	before, err := f.state.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	original, err := m402Metadata(f.queue)
	if err != nil {
		t.Fatal(err)
	}
	l.save(t, "before-topic-replacement", map[string]any{"checkpoint": before, "metadata": original, "job": j, "records": m402ReadAll(t, f.queue)})
	l.phase.Store("I")
	if err = f.queue.DeleteTestTopic(context.Background()); err != nil {
		t.Fatal(err)
	}
	m402Wait(t, "old topic absent", 20, func() bool {
		d, e := f.queue.Admin.ListTopics(context.Background(), f.queue.Topic)
		v, ok := d[f.queue.Topic]
		return e == nil && (!ok || v.Err != nil)
	})
	m402Wait(t, "replacement Topic leaders ready", 25, func() bool { return f.queue.Ensure(context.Background()) == nil })
	changed, err := m402Metadata(f.queue)
	if err != nil || changed.ID == original.ID {
		t.Fatal("actual Topic identity did not change", err)
	}
	errorsByWorker := map[string]string{}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	workers := map[string]func(context.Context) error{"capture": (&syncpipe.Capture{Store: f.state, Queue: f.queue}).Run, "es": (&syncpipe.Consumer{Store: f.state, Queue: f.queue, ES: f.es}).Run, "ck": (&syncpipe.CKConsumer{Store: f.state, Queue: f.queue, CK: f.ck}).Run}
	for name, run := range workers {
		e := run(ctx)
		if e == nil || !strings.Contains(e.Error(), "Kafka topic identity/config differs") {
			t.Fatalf("%s silently resumed wrong Topic: %v", name, e)
		}
		errorsByWorker[name] = e.Error()
	}
	_, err = r.Run(ctx, j.ID)
	if err == nil || !strings.Contains(err.Error(), "REBUILD_KAFKA_CHANGED") {
		t.Fatal("M4 did not reject stale partition coordinates", err)
	}
	errorsByWorker["rebuild"] = err.Error()
	after, e := f.state.Checkpoint(context.Background())
	if e != nil || before != after {
		t.Fatal("identity fault moved checkpoint", before, after, e)
	}
	l.save(t, "I-result", map[string]any{"old_topic_id": original.ID.String(), "new_topic_id": changed.ID.String(), "errors": errorsByWorker, "checkpoint_before": before, "checkpoint_after": after, "new_metadata": changed})
	m402Snapshot(t, f, l, "I-replacement-rejected")
}

func TestM402RebuildIncrementAndMarkersAfterBrokerFaultJ(t *testing.T) {
	m402Ready(t)
	f, r := newM4(t)
	l := m402NewLog(t)
	m402Observe(t, f.queue, l)
	m402Restore(t, l)
	m4LoadGold(t, f)
	cdc := m402Start(t, f, l, "capture", "cdc")
	_ = cdc
	es := m402Start(t, f, l, "consume", "es-baseline")
	ck := m402Start(t, f, l, "ck", "ck-baseline")
	m402Caught(t, f)
	es.stop(t)
	ck.stop(t)
	before := map[string][]syncpipe.PartitionProgress{}
	for _, c := range []string{"es", "ck"} {
		before[c], _ = f.queue.Progress(context.Background(), c)
	}
	j := m4Job(t, f, r, "m402_broker_rebuild", "both")
	snapshotReady := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	r.AfterBatch = func(progress syncpipe.RebuildJob) error {
		if progress.Applied == progress.Total {
			once.Do(func() { close(snapshotReady) })
			<-release
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	runDone := make(chan error, 1)
	var finished syncpipe.RebuildJob
	go func() { var e error; finished, e = r.Run(ctx, j.ID); runDone <- e }()
	select {
	case <-snapshotReady:
	case <-time.After(40 * time.Second):
		t.Fatal("frozen rebuild snapshot not applied")
	}
	l.phase.Store("J")
	current := f.actionHTTP(t, "local-device-agent", "comments", 4, 10, map[string]any{"content": "M402 change after frozen snapshot"})
	newTicket := m402Resolve(t, f, l, m402HTTPCreate(t, f, l, "J-after-snapshot"))
	_ = newTicket
	// Obtain a real ACK of an immutable increment, then kill its actual leader.
	event := f.event(t, 4, current.Version, "m402-J-confirmed-increment")
	ref, err := f.queue.Publish(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	d, err := m402Metadata(f.queue)
	if err != nil {
		t.Fatal(err)
	}
	leader := d.Partitions[ref.Partition].Leader
	m402Snapshot(t, f, l, "J-before-leader-kill")
	m402Control(t, l, "node-stop", leader, true)
	close(release)
	current = f.actionHTTP(t, "local-device-agent", "comments", 4, current.Version, map[string]any{"content": "M402 continuing HTTP while rebuild leader recovers"})
	select {
	case err = <-runDone:
	case <-time.After(70 * time.Second):
		t.Fatal("M4 broker recovery deadline")
	}
	r.AfterBatch = nil
	if err != nil {
		l.save(t, "J-first-bounded-failure", map[string]any{"job": finished, "error": err.Error()})
		// M4 retains its explicit failed job and independent cursor. Restore
		// the real Broker before requesting an operator-style bounded resume.
		m402Control(t, l, "node-start", leader, false)
		m402Ready(t)
		m402ISR(t, f.queue, 3)
		finished = m4Run(t, r, j.ID)
	}
	if finished.Status != "completed" || finished.Report == nil || finished.Report.State != "consistent" {
		t.Fatal("rebuild did not finish consistently", finished)
	}
	m402Control(t, l, "node-start", leader, false)
	m402ISR(t, f.queue, 3)
	finished = m4Run(t, r, j.ID)
	m4AssertProjection(t, f, finished)
	gold := m4AssertGold(t, f, finished)
	markers := m4Rows(t, f, "SELECT partition_id,next_offset,barrier_id FROM sync_rebuild_offsets WHERE job_id=? ORDER BY partition_id", j.ID)
	if len(markers) != 3 {
		t.Fatal("missing rebuild partitions")
	}
	for _, row := range markers {
		if row["barrier_id"] != finished.BarrierID {
			t.Fatalf("mismatched final partition marker: %v final=%s", row, finished.BarrierID)
		}
	}
	after := map[string][]syncpipe.PartitionProgress{}
	for _, c := range []string{"es", "ck"} {
		after[c], _ = f.queue.Progress(context.Background(), c)
		for i, p := range before[c] {
			if after[c][i].Committed != p.Committed {
				t.Fatal("M4 altered independent live group offset", c, p, after[c][i])
			}
		}
	}
	l.save(t, "J-result", map[string]any{"confirmed_increment": ref, "killed_leader": leader, "job": finished, "markers": markers, "live_groups_before": before, "live_groups_after": after, "live_group_commits_unchanged": true, "latest_http_version": current.Version, "gold": gold, "records": m402ReadAll(t, f.queue)})
	m4Evidence(t, f, finished, "m402-broker-increment-recovered", map[string]any{"gold": gold, "confirmed_increment": ref, "partition_markers": markers})
	l.add("J-verified", map[string]any{"state": finished.Report.State, "all_three_partition_markers": true, "independent_projection_all_fields_checked": true})
}
