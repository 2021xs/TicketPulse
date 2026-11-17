//go:build integration && m2

package mysqlstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"ticketpulse/internal/search"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kgo"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/syncpipe/linkerrors"
	"ticketpulse/internal/ticket"
)

func TestM2KafkaCutoverRetainsLegacyBoundaryAndConcurrentWrites(t *testing.T) {
	m := newM2(t)
	original := m.create(t, 1, request("before-kafka-cutover"))
	boundary, err := syncpipe.CurrentPosition(context.Background(), m.db)
	if err != nil {
		t.Fatal(err)
	}
	m.exec(t, "INSERT INTO sync_cdc_checkpoint(source_name,server_uuid,binlog_file,binlog_pos,bootstrap_complete,updated_at) VALUES('tickets-v1',?,?,?,TRUE,UTC_TIMESTAMP(6))", boundary.ServerUUID, boundary.File, boundary.Pos)
	_, err = syncpipe.Bootstrap(context.Background(), m.state, m.queue)
	if err == nil || !strings.Contains(err.Error(), "KAFKA_MIGRATION_REQUIRED") {
		t.Fatal("legacy boundary bypassed", err)
	}
	// Real HTTP changes after the stopped legacy boundary are still captured.
	var created ticket.CreateResult
	json.Unmarshal(m.call(t, "local-alice", "POST", "/tickets", request("during-kafka-cutover"), 201), &created)
	current := m.actionHTTP(t, "local-it-agent", "claim", created.Ticket.ID, 1, nil)
	binaryPath := os.Getenv("TICKETPULSE_SYNC_BINARY")
	if binaryPath == "" {
		binaryPath = "../../bin/ticketpulse-sync"
	}
	binary, err := filepath.Abs(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-mode=migrate-kafka")
	command.Dir = "../.."
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = m.name
	command.Env = append(os.Environ(), "TICKETPULSE_SYNC_DSN="+cfg.FormatDSN(), "TICKETPULSE_SEARCH_INDEX="+m.name+"_tickets_v1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("real cutover CLI failed: %v %s", err, output)
	}
	migrated, err := m.state.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if migrated.Position != boundary || !migrated.BootstrapComplete {
		t.Fatal("cutover jumped legacy Binlog boundary", migrated, boundary)
	}
	repeat := exec.Command(binary, "-mode=migrate-kafka")
	repeat.Dir = command.Dir
	repeat.Env = command.Env
	if output, err := repeat.CombinedOutput(); err != nil {
		t.Fatalf("repeat migration failed: %v %s", err, output)
	}
	repeated, err := m.state.Checkpoint(context.Background())
	if err != nil || repeated != migrated {
		t.Fatal("repeat migration moved checkpoint", repeated, migrated, err)
	}
	current = m.actionHTTP(t, "local-it-agent", "start", current.ID, current.Version, nil)
	m.startCapture(t)
	m.startConsumer(t)
	m.projected(t, original.Ticket.ID, 1)
	m.projected(t, current.ID, 3)
	var legacy syncpipe.Position
	m.db.QueryRow("SELECT server_uuid,binlog_file,binlog_pos FROM sync_cdc_checkpoint WHERE source_name='tickets-v1'").Scan(&legacy.ServerUUID, &legacy.File, &legacy.Pos)
	if legacy != boundary {
		t.Fatal("legacy rollback checkpoint was rewritten")
	}
	detail, err := m.service.Detail(context.Background(), 1, current.ID)
	if err != nil || len(detail.History) != 3 || detail.Ticket.Status != "in_progress" {
		t.Fatal(detail, err)
	}
	t.Logf("RUNTIME real cutover CLI: old boundary=%+v preserved; original id=%d, during-cutover id=%d latest=v3, complete history=3; post-snapshot increment captured", boundary, original.Ticket.ID, current.ID)
}

func TestM2KafkaPartialPublicationAndCheckpointReplay(t *testing.T) {
	m := newM2(t)
	first := m.create(t, 1, request("kafka-partial-first"))
	second := m.create(t, 1, request("kafka-partial-second"))
	if _, err := syncpipe.Bootstrap(context.Background(), m.state, m.queue); err != nil {
		t.Fatal(err)
	}
	m.startConsumer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	var published syncpipe.RecordRef
	var interruptedCheckpoint syncpipe.Checkpoint
	var interruptedCommit syncpipe.Position
	var count int
	capture := &syncpipe.Capture{Store: m.state, Queue: m.queue, Config: syncpipe.CaptureConfig{Address: os.Getenv("TICKETPULSE_CDC_ADDR"), User: os.Getenv("TICKETPULSE_CDC_USER"), Password: os.Getenv("TICKETPULSE_CDC_PASSWORD"), ServerID: 11002}, AfterPublished: func(e syncpipe.Event, ref syncpipe.RecordRef) error {
		if e.Version == 2 {
			count++
			published = ref
			interruptedCheckpoint, _ = m.state.Checkpoint(context.Background())
			interruptedCommit = *e.Commit
			return errors.New("injected after first real Kafka publish before checkpoint")
		}
		return nil
	}}
	go func() { done <- capture.Run(ctx) }()
	m.wait(t, "fault capture startup", func() bool {
		var state string
		m.db.QueryRow("SELECT state FROM sync_worker_state WHERE component='cdc'").Scan(&state)
		return state == "running"
	})
	before, err := m.state.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := m.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []ticket.Ticket{first.Ticket, second.Ticket} {
		after := r
		after.Version = 2
		after.Title = "Kafka partial transaction " + r.TicketNo
		after.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
		state, _ := json.Marshal(after.State())
		old, _ := json.Marshal(r.State())
		if _, err := tx.Exec("UPDATE tickets SET title=?,version=2,updated_at=? WHERE id=?", after.Title, after.UpdatedAt, r.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("INSERT INTO ticket_histories(organization_id,ticket_id,action,operator_id,version,before_state,after_state,responsible_team_id,occurred_at) VALUES(1,?,'assignee_transferred',5,2,?,?,?,?)", r.ID, old, state, after.CurrentTeamID, after.UpdatedAt); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil || count != 1 {
			t.Fatal(err, count)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("publication boundary did not interrupt")
	}
	after, err := m.state.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.Position != interruptedCheckpoint.Position || after.File != interruptedCommit.File || after.Pos >= interruptedCommit.Pos {
		t.Fatalf("checkpoint passed a partially published transaction: before=%+v after=%+v", before, after)
	}
	m.startCapture(t)
	m.projected(t, first.Ticket.ID, 2)
	m.projected(t, second.Ticket.ID, 2)
	m.wait(t, "safe replay of first publication", func() bool {
		var copies int
		m.db.QueryRow("SELECT COUNT(*) FROM sync_kafka_es_deliveries WHERE ticket_id=? AND ticket_version=2 AND status IN ('applied','duplicate_or_old')", first.Ticket.ID).Scan(&copies)
		return copies == 2
	})
	t.Logf("RUNTIME real first publish=%+v; checkpoint remained %s:%d; same transaction second output resumed; first version safely duplicated", published, before.File, before.Pos)
}

func TestM2KafkaPartitionsFailureHoleAndGroupRebalance(t *testing.T) {
	m := newM2(t)
	positions := map[int32][]syncpipe.RecordRef{}
	for i := 0; i < 24; i++ {
		r := m.create(t, 1, request(fmt.Sprintf("kafka-partition-%d", i)))
		e := syncpipe.Event{Schema: 1, ID: fmt.Sprintf("fixed-key-%d", i), Source: "snapshot", TicketID: r.Ticket.ID, Version: 1, EventAt: r.Ticket.UpdatedAt, Document: search.FromTicket(r.Ticket)}
		ref, err := m.queue.Publish(context.Background(), e)
		if err != nil {
			t.Fatal(err)
		}
		positions[ref.Partition] = append(positions[ref.Partition], ref)
		e.ID += "-duplicate"
		same, err := m.queue.Publish(context.Background(), e)
		if err != nil || same.Partition != ref.Partition {
			t.Fatal("same ticket key moved partition", same, ref, err)
		}
		positions[same.Partition] = append(positions[same.Partition], same)
	}
	if len(positions) != int(m.queue.Partitions) {
		t.Fatalf("sample did not cover all configured partitions: %+v", positions)
	}
	var blocked int32
	for p, records := range positions {
		if len(records) >= 2 {
			blocked = p
			break
		}
	}
	var release atomic.Bool
	release.Store(false)
	var mutex sync.Mutex
	processed := map[syncpipe.RecordRef]int{}
	process := func(ctx context.Context, r *kgo.Record) error {
		if r.Partition == blocked && !release.Load() {
			return linkerrors.Transient("injected_partition_outage", errors.New("only this partition unavailable"))
		}
		mutex.Lock()
		processed[syncpipe.Reference(r)]++
		mutex.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.queue.Consume(ctx, "es", 50*time.Millisecond, process) }()
	m.wait(t, "other partitions progress while failure hole held", func() bool {
		progress, err := m.queue.Progress(context.Background(), "es")
		if err != nil {
			return false
		}
		others := 0
		for _, p := range progress {
			if p.Partition == blocked {
				if p.Committed != 0 {
					return false
				}
			} else if p.Lag == 0 && p.End > 0 {
				others++
			}
		}
		return others == int(m.queue.Partitions)-1
	})
	progress, _ := m.queue.Progress(context.Background(), "es")
	// A second group member triggers a real Kafka rebalance while the first drains.
	secondCtx, secondCancel := context.WithCancel(context.Background())
	defer secondCancel()
	secondDone := make(chan error, 1)
	go func() { secondDone <- m.queue.Consume(secondCtx, "es", 50*time.Millisecond, process) }()
	m.wait(t, "two real group members after rebalance", func() bool {
		groups, err := m.queue.Admin.DescribeGroups(context.Background(), m.queue.Group("es"))
		if err != nil {
			return false
		}
		group := groups[m.queue.Group("es")]
		return group.Err == nil && group.State == "Stable" && len(group.Members) == 2
	})
	release.Store(true)
	m.wait(t, "all group offsets caught up after recovery/rebalance", func() bool {
		p, err := m.queue.Progress(context.Background(), "es")
		if err != nil {
			return false
		}
		for _, v := range p {
			if v.Lag != 0 {
				return false
			}
		}
		return true
	})
	cancel()
	secondCancel()
	<-done
	<-secondDone
	mutex.Lock()
	for _, refs := range positions {
		for _, ref := range refs {
			if processed[ref] == 0 {
				t.Errorf("record omitted across rebalance: %+v", ref)
			}
		}
	}
	mutex.Unlock()
	// New instance of the same group must resume, never begin from zero.
	restartCtx, restartCancel := context.WithCancel(context.Background())
	restartDone := make(chan error, 1)
	var replayed atomic.Int64
	go func() {
		restartDone <- m.queue.Consume(restartCtx, "es", 50*time.Millisecond, func(context.Context, *kgo.Record) error { replayed.Add(1); return nil })
	}()
	time.Sleep(400 * time.Millisecond)
	restartCancel()
	<-restartDone
	if replayed.Load() != 0 {
		t.Fatal("committed group restarted from old offsets", replayed.Load())
	}
	t.Logf("RUNTIME partition %d blocked at offset 0 while other partitions drained: %+v; two group members rebalance/recovery omitted no records; restarted committed group read=0", blocked, progress)
}
