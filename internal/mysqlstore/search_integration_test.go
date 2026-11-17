//go:build integration && m2

package mysqlstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kadm"

	assets "ticketpulse/db"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

type m2fixture struct {
	*fixture
	state          *syncpipe.StateStore
	queue          *syncpipe.Queue
	es             *search.Elasticsearch
	search         *search.Service
	http           *httptest.Server
	captureCancel  context.CancelFunc
	captureDone    chan error
	consumerCancel context.CancelFunc
	consumerDone   chan error
}

func newM2(t *testing.T) *m2fixture {
	t.Helper()
	f := newFixture(t)
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = f.name
	cfg.MultiStatements = true
	init, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := init.Exec(assets.SearchSyncMigration + assets.AnalyticsSyncMigration + assets.KafkaSyncMigration); err != nil {
		init.Close()
		t.Fatal(err)
	}
	init.Close()
	q, err := syncpipe.ConnectQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), f.name)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Opt in only for fresh C3 random fixtures. A small sparse index avoids
	// allocating 60 MiB per three-partition Topic during repeated acceptance.
	// Queue.Ensure still validates the normal durability and retention contract.
	if os.Getenv("TICKETPULSE_TEST_SMALL_KAFKA_INDEX") == "true" {
		if os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03c3" || !strings.HasPrefix(f.name, "ticketpulse_test_") || q.Topic != "ticketpulse."+f.name+".tickets.v1" {
			t.Fatal("small Kafka index is allowed only for a new private C3 fixture")
		}
		configs := map[string]*string{"cleanup.policy": kadm.StringPtr("delete"), "retention.ms": kadm.StringPtr("-1"), "retention.bytes": kadm.StringPtr("-1"), "min.insync.replicas": kadm.StringPtr("1"), "unclean.leader.election.enable": kadm.StringPtr("false"), "max.message.bytes": kadm.StringPtr("524288"), "segment.index.bytes": kadm.StringPtr("65536"), "file.delete.delay.ms": kadm.StringPtr("1000")}
		created, err := q.Admin.CreateTopics(ctx, q.Partitions, q.ReplicationFactor, configs, q.Topic)
		if err != nil {
			t.Fatal(err)
		}
		if result, exists := created[q.Topic]; !exists || result.Err != nil {
			t.Fatalf("new private Topic creation failed: %+v", created)
		}
	}
	if err := q.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	index := f.name + "_tickets_v1"
	es, err := search.NewElasticsearch(os.Getenv("TICKETPULSE_ES_URL"), index)
	if err != nil {
		t.Fatal(err)
	}
	state := &syncpipe.StateStore{DB: f.db, Schema: f.name}
	key := make([]byte, 32)
	rand.Read(key)
	service, err := search.NewService(f.store, es, f.store, key)
	if err != nil {
		t.Fatal(err)
	}
	m := &m2fixture{fixture: f, state: state, queue: q, es: es, search: service}
	m.http = httptest.NewServer(httpapi.NewWithOptions(f.service, f.store, nil, httpapi.Options{SessionTTL: time.Hour, Search: service, SearchOptions: f.store}))
	t.Cleanup(func() {
		m.stopCapture(t)
		m.stopConsumer(t)
		m.http.Close()
		if os.Getenv("TICKETPULSE_RETAIN_C3_RESOURCES") == "true" {
			q.Close()
			return
		}
		q.DeleteTestTopic(context.Background())
		q.Close()
		req, _ := http.NewRequest(http.MethodDelete, os.Getenv("TICKETPULSE_ES_URL")+"/"+index, nil)
		res, err := http.DefaultClient.Do(req)
		if err == nil {
			res.Body.Close()
		}
	})
	return m
}
func (m *m2fixture) startCapture(t *testing.T) {
	t.Helper()
	if m.captureCancel != nil {
		t.Fatal("capture already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.captureCancel = cancel
	m.captureDone = make(chan error, 1)
	c := &syncpipe.Capture{Store: m.state, Queue: m.queue, Config: syncpipe.CaptureConfig{Address: os.Getenv("TICKETPULSE_CDC_ADDR"), User: os.Getenv("TICKETPULSE_CDC_USER"), Password: os.Getenv("TICKETPULSE_CDC_PASSWORD"), ServerID: 11001}}
	go func() { m.captureDone <- c.Run(ctx) }()
	m.wait(t, "capture running", func() bool {
		var state string
		_ = m.db.QueryRow("SELECT state FROM sync_worker_state WHERE component='cdc'").Scan(&state)
		return state == "running"
	})
}
func (m *m2fixture) stopCapture(t *testing.T) {
	t.Helper()
	if m.captureCancel == nil {
		return
	}
	m.captureCancel()
	select {
	case err := <-m.captureDone:
		if err != nil && !strings.Contains(err.Error(), "canceled") {
			t.Logf("capture stop: %v", err)
		}
	case <-time.After(12 * time.Second):
		t.Error("capture stop timeout")
	}
	m.captureCancel = nil
}
func (m *m2fixture) startConsumer(t *testing.T) {
	t.Helper()
	if m.consumerCancel != nil {
		t.Fatal("consumer already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.consumerCancel = cancel
	m.consumerDone = make(chan error, 1)
	c := &syncpipe.Consumer{Store: m.state, Queue: m.queue, ES: m.es, RedeliveryDelay: 100 * time.Millisecond}
	go func() { m.consumerDone <- c.Run(ctx) }()
	m.wait(t, "consumer running", func() bool {
		var state string
		_ = m.db.QueryRow("SELECT state FROM sync_worker_state WHERE component='es'").Scan(&state)
		return state == "running"
	})
}
func (m *m2fixture) stopConsumer(t *testing.T) {
	t.Helper()
	if m.consumerCancel == nil {
		return
	}
	m.consumerCancel()
	select {
	case err := <-m.consumerDone:
		if err != nil && !strings.Contains(err.Error(), "canceled") {
			t.Logf("consumer stop: %v", err)
		}
	case <-time.After(12 * time.Second):
		t.Error("consumer stop timeout")
	}
	m.consumerCancel = nil
}
func (m *m2fixture) wait(t *testing.T, label string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		select {
		case err := <-m.captureDone:
			t.Fatalf("%s capture exited: %v", label, err)
		default:
		}
		select {
		case err := <-m.consumerDone:
			t.Fatalf("%s consumer exited: %v", label, err)
		default:
		}
		time.Sleep(30 * time.Millisecond)
	}
	status, _ := syncpipe.ReadStatus(context.Background(), m.state, m.queue)
	t.Fatalf("%s timed out; status=%+v", label, status)
}
func (m *m2fixture) projected(t *testing.T, id, version int64) {
	t.Helper()
	m.wait(t, "ES projection", func() bool {
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s_tickets_v1/_doc/%d", os.Getenv("TICKETPULSE_ES_URL"), m.name, id), nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer res.Body.Close()
		var doc struct {
			Version int64 `json:"_version"`
		}
		json.NewDecoder(res.Body).Decode(&doc)
		return res.StatusCode == 200 && doc.Version == version
	})
	if err := m.es.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func (m *m2fixture) call(t *testing.T, token, method, path string, body any, status int) []byte {
	t.Helper()
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	req, _ := http.NewRequest(method, m.http.URL+"/api/v1"+path, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	if res.StatusCode != status {
		t.Fatalf("%s %s status=%d want=%d body=%s", method, path, res.StatusCode, status, out)
	}
	return out
}
func (m *m2fixture) actionHTTP(t *testing.T, token, action string, id, version int64, fields map[string]any) ticket.Ticket {
	t.Helper()
	body := map[string]any{"ticket_id": id, "expected_version": version}
	for k, v := range fields {
		body[k] = v
	}
	out := m.call(t, token, "POST", "/tickets/"+action, body, 200)
	var result ticket.ActionResult
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatal(err)
	}
	return result.Ticket
}
func (m *m2fixture) query(t *testing.T, token, path string) search.Result {
	t.Helper()
	var result search.Result
	json.Unmarshal(m.call(t, token, "GET", "/search/tickets?"+path, nil, 200), &result)
	return result
}

func TestM2RealCDCWorkflowPermissionsAndRecovery(t *testing.T) {
	m := newM2(t)
	old := m.create(t, 1, request("m2-existing"))
	m.startCapture(t)
	m.startConsumer(t)
	m.projected(t, old.Ticket.ID, 1)
	if _, err := syncpipe.Snapshot(context.Background(), m.state, m.queue); err != nil {
		t.Fatal(err)
	}
	m.projected(t, old.Ticket.ID, 1)
	var created ticket.CreateResult
	json.Unmarshal(m.call(t, "local-alice", "POST", "/tickets", ticket.CreateRequest{RequestID: "m2-http", Title: "M2 network workflow", Description: "Database commit captured through real binlog", Category: "network", Priority: "high"}, 201), &created)
	id := created.Ticket.ID
	m.projected(t, id, 1)
	current := m.actionHTTP(t, "local-it-agent", "claim", id, 1, nil)
	m.projected(t, id, 2)
	current = m.actionHTTP(t, "local-it-agent", "start", id, current.Version, nil)
	m.projected(t, id, 3)
	current = m.actionHTTP(t, "local-it-agent", "comments", id, current.Version, map[string]any{"content": "Comment unindexedmarker"})
	m.projected(t, id, 4)
	if got := m.query(t, "local-it-agent", "q=unindexedmarker"); len(got.Items) != 0 {
		t.Fatal("comment text was indexed")
	}
	// Pause only the consumer: ES stays team A, MySQL commits team B. Authorization must be immediate.
	m.stopConsumer(t)
	current = m.actionHTTP(t, "local-supervisor", "transfer-team", id, current.Version, map[string]any{"team_id": 20, "reason": "设备团队接手"})
	if current.Status != "pending" || current.AssigneeID != nil || current.InitialTeamID != 10 {
		t.Fatal(current)
	}
	got := m.query(t, "local-it-agent", "ticket_no="+current.TicketNo)
	if len(got.Items) != 0 || got.Total != nil || got.HasMore || got.NextCursor != "" {
		t.Fatalf("stale index leaked result/count/cursor: %+v", got)
	}
	m.call(t, "local-it-agent", "GET", fmt.Sprintf("/tickets/%d", id), nil, 403)
	m.startConsumer(t)
	m.projected(t, id, 5)
	got = m.query(t, "local-device-agent", "ticket_no="+current.TicketNo)
	if len(got.Items) != 1 || got.Items[0].CurrentTeamID != 20 {
		t.Fatal(got)
	}
	current = m.actionHTTP(t, "local-device-agent", "claim", id, 5, nil)
	current = m.actionHTTP(t, "local-device-agent", "start", id, 6, nil)
	current = m.actionHTTP(t, "local-device-agent", "resolve", id, 7, map[string]any{"resolution_note": "首次解决说明"})
	first := *current.FirstResolvedAt
	current = m.actionHTTP(t, "local-alice", "reopen", id, 8, nil)
	current = m.actionHTTP(t, "local-device-agent", "resolve", id, 9, map[string]any{"resolution_note": "再次解决说明"})
	current = m.actionHTTP(t, "local-alice", "close", id, 10, nil)
	m.projected(t, id, 11)
	if current.ReopenCount != 1 || !current.FirstResolvedAt.Equal(first) || current.ResolutionNote == nil || *current.ResolutionNote != "再次解决说明" {
		t.Fatal(current)
	}
	var detail ticket.Detail
	json.Unmarshal(m.call(t, "local-alice", "GET", fmt.Sprintf("/tickets/%d", id), nil, 200), &detail)
	if len(detail.History) != 11 || len(detail.Comments) != 1 {
		t.Fatal(detail)
	}
	// Duplicate and late old event over the actual MQ; same ID document remains at v11.
	cp, _ := m.state.Checkpoint(context.Background())
	newer := syncpipe.Event{Schema: 1, ID: "test-duplicate", Source: "binlog", TicketID: id, Version: 11, EventAt: current.UpdatedAt, Commit: &cp.Position, Document: search.FromTicket(current)}
	seq, err := m.queue.Publish(context.Background(), newer)
	if err != nil {
		t.Fatal(err)
	}
	oldEvent := newer
	oldEvent.ID = "test-old"
	oldEvent.Version = 1
	oldEvent.Document = search.FromTicket(created.Ticket)
	oldSeq, err := m.queue.Publish(context.Background(), oldEvent)
	if err != nil {
		t.Fatal(err)
	}
	m.wait(t, "old/duplicate no-op", func() bool {
		var count int
		_ = m.db.QueryRow("SELECT COUNT(*) FROM sync_kafka_es_deliveries WHERE ((partition_id=? AND record_offset=?) OR (partition_id=? AND record_offset=?)) AND status='duplicate_or_old'", seq.Partition, seq.Offset, oldSeq.Partition, oldSeq.Offset).Scan(&count)
		return count == 2
	})
	m.projected(t, id, 11)
	// A persistent malformed event must not be claimed as projected and can be replayed.
	poison, err := m.queue.PublishBytes(context.Background(), []byte(`{"schema":99,"event_id":"poison"}`), "poison")
	if err != nil {
		t.Fatal(err)
	}
	m.wait(t, "isolation", func() bool {
		var count int
		_ = m.db.QueryRow("SELECT COUNT(*) FROM sync_kafka_es_deliveries WHERE partition_id=? AND record_offset=? AND status='isolated' AND isolated_payload IS NOT NULL", poison.Partition, poison.Offset).Scan(&count)
		return count == 1
	})
	replay, err := syncpipe.Replay(context.Background(), m.state, m.queue, poison)
	if err != nil {
		t.Fatal(err)
	}
	m.wait(t, "replay recorded", func() bool {
		var count int
		_ = m.db.QueryRow("SELECT COUNT(*) FROM sync_kafka_es_deliveries WHERE partition_id=? AND record_offset=? AND status='isolated'", replay.Partition, replay.Offset).Scan(&count)
		return count == 1
	})
	// A permanent ES block is isolated; correcting the environment and replaying restores it.
	indexURL := os.Getenv("TICKETPULSE_ES_URL") + "/" + m.name + "_tickets_v1/_settings"
	settings := func(blocked bool) {
		body, _ := json.Marshal(map[string]any{"index.blocks.write": blocked})
		req, _ := http.NewRequest(http.MethodPut, indexURL, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatal("index settings failed")
		}
	}
	settings(true)
	recoveryTicket := m.create(t, 1, request("m2-isolation-recovery"))
	var isolatedSequence syncpipe.RecordRef
	isolatedSequence.Topic = m.queue.Topic
	m.wait(t, "real ES permanent failure isolated", func() bool {
		return m.db.QueryRow("SELECT partition_id,record_offset FROM sync_kafka_es_deliveries WHERE ticket_id=? AND status='isolated'", recoveryTicket.Ticket.ID).Scan(&isolatedSequence.Partition, &isolatedSequence.Offset) == nil
	})
	settings(false)
	if _, err := syncpipe.Replay(context.Background(), m.state, m.queue, isolatedSequence); err != nil {
		t.Fatal(err)
	}
	m.projected(t, recoveryTicket.Ticket.ID, 1)
	m.wait(t, "isolated event recovered", func() bool {
		var state string
		_ = m.db.QueryRow("SELECT status FROM sync_kafka_es_deliveries WHERE partition_id=? AND record_offset=?", isolatedSequence.Partition, isolatedSequence.Offset).Scan(&state)
		return state == "recovered"
	})
	// Restart capture and consumer with outstanding/new changes, no HTTP-side sender exists.
	m.stopCapture(t)
	m.stopConsumer(t)
	current = m.actionHTTP(t, "local-alice", "comments", id, 11, map[string]any{"content": "恢复后评论"})
	m.startCapture(t)
	m.startConsumer(t)
	m.projected(t, id, 12)
	// Direct SQL change proves this is Binlog CDC, independently of any HTTP callback.
	if _, err := m.db.Exec("UPDATE tickets SET title='Direct SQL binlogmarker',version=version+1,updated_at=UTC_TIMESTAMP(6) WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	m.projected(t, id, 13)
	if got := m.query(t, "local-alice", "q=binlogmarker"); len(got.Items) != 1 {
		t.Fatal(got)
	}
	// Current team/user/member facts override indexed visibility even when their changes are not projected.
	m.db.Exec("UPDATE teams SET active=FALSE WHERE id=20")
	if got := m.query(t, "local-device-agent", "ticket_no="+current.TicketNo); len(got.Items) != 0 || got.Total != nil {
		t.Fatal(got)
	}
	m.db.Exec("UPDATE teams SET active=TRUE WHERE id=20")
	m.db.Exec("UPDATE users SET active=FALSE WHERE id=4")
	m.call(t, "local-device-agent", "GET", "/search/tickets?q=binlogmarker", nil, 401)
	m.db.Exec("UPDATE users SET active=TRUE WHERE id=4")
	m.db.Exec("UPDATE team_members SET active=FALSE WHERE team_id=20 AND user_id=4")
	if got := m.query(t, "local-device-agent", "q=binlogmarker"); len(got.Items) != 0 {
		t.Fatal(got)
	}
	status, err := syncpipe.ReadStatus(context.Background(), m.state, m.queue)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RUNTIME workflow id=%d first_resolved_at=%s version=13 histories-before-direct-SQL=12 status=%+v", id, first.Format(time.RFC3339Nano), status)
}

func TestM2IndependentSearchFixtureAndStablePagination(t *testing.T) {
	m := newM2(t)
	// Expected IDs are derived from this fixed table, not the production query builder.
	input := []struct {
		title, description, category, priority string
		team                                   int64
	}{
		{"Copper network issue", "needle alpha", "network", "urgent", 10},
		{"Copper device issue", "needle beta", "device", "high", 20},
		{"Copper software issue", "different description", "software", "medium", 10},
		{"Other request", "needle delta", "other", "low", 90},
		{"Copper account issue", "needle echo", "account", "high", 10},
	}
	ids := []int64{}
	for i, in := range input {
		r := ticket.CreateRequest{RequestID: fmt.Sprintf("fixed-%d", i), Title: in.title, Description: in.description, Category: in.category, Priority: in.priority}
		item := m.create(t, 1, r)
		ids = append(ids, item.Ticket.ID)
		_, err := m.db.Exec("UPDATE tickets SET created_at='2026-11-01 00:00:00',updated_at='2026-11-03 00:00:00' WHERE id=?", item.Ticket.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	m.startCapture(t)
	m.startConsumer(t)
	for _, id := range ids {
		m.projected(t, id, 1)
	}
	cases := []struct {
		query    string
		expected []int64
	}{
		{"q=Copper&sort=id&order=asc", []int64{ids[0], ids[1], ids[2], ids[4]}},
		{"q=needle&sort=id&order=asc", []int64{ids[0], ids[1], ids[3], ids[4]}},
		{"q=Copper&priority=urgent&priority=high&current_team_id=10&sort=id&order=asc", []int64{ids[0], ids[4]}},
		{"category=device&category=account&status=pending&sort=id&order=asc", []int64{ids[1], ids[4]}},
		{"created_from=2026-11-01T00:00:00Z&created_to=2026-11-02T00:00:00Z&sort=id&order=asc", ids},
		{"created_to=2026-11-01T00:00:00Z&sort=id&order=asc", []int64{}},
		{"updated_from=2026-11-03T00:00:00Z&updated_to=2026-11-04T00:00:00Z&sort=id&order=asc", ids},
		{"assignee_id=0&sort=id&order=asc", ids},
		{"sort=priority&order=desc", []int64{ids[0], ids[4], ids[1], ids[2], ids[3]}},
		{"sort=updated_at&order=desc", []int64{ids[4], ids[3], ids[2], ids[1], ids[0]}},
	}
	for _, tc := range cases {
		got := m.query(t, "local-supervisor", tc.query)
		actual := []int64{}
		for _, item := range got.Items {
			actual = append(actual, item.ID)
		}
		if !slices.Equal(actual, tc.expected) {
			t.Errorf("%s IDs=%v want=%v", tc.query, actual, tc.expected)
		}
		if got.Total != nil {
			t.Error("total exposed")
		}
	}
	actual := []int64{}
	path := "sort=updated_at&order=desc&page_size=2"
	for {
		got := m.query(t, "local-alice", path)
		for _, item := range got.Items {
			actual = append(actual, item.ID)
		}
		if !got.HasMore {
			break
		}
		path = "sort=updated_at&order=desc&page_size=2&cursor=" + got.NextCursor
	}
	expected := []int64{ids[4], ids[3], ids[2], ids[1], ids[0]}
	if !slices.Equal(actual, expected) {
		t.Fatalf("stable pages=%v want=%v", actual, expected)
	}
	m.call(t, "local-alice", "GET", "/search/tickets?role=admin", nil, 400)
	m.call(t, "local-alice", "GET", "/search/tickets?sort=title", nil, 400)
	m.call(t, "local-alice", "GET", "/search/tickets?page_size=101", nil, 400)
	t.Logf("RUNTIME independent fixture=%v stable pages=%v", ids, actual)
}

func TestM2SnapshotOverlapAndTransactionPublication(t *testing.T) {
	m := newM2(t)
	first := m.create(t, 1, request("overlap-existing"))
	// Install a boundary before snapshot, then concurrently commit business changes.
	p, err := syncpipe.CurrentPosition(context.Background(), m.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.state.SaveCheckpoint(context.Background(), p, false); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			_, err := m.service.Create(context.Background(), 1, request(fmt.Sprintf("overlap-%d", i)))
			if err != nil {
				t.Error(err)
			}
		}
	}()
	if _, err := syncpipe.Snapshot(context.Background(), m.state, m.queue); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	m.startCapture(t)
	m.startConsumer(t)
	var ids []int64
	rows, err := m.db.Query("SELECT id FROM tickets ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		m.projected(t, id, 1)
	}
	// Row writes made in an open transaction are not published before COMMIT.
	tx, err := m.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE tickets SET title='uncommitted marker',version=2,updated_at=UTC_TIMESTAMP(6) WHERE id=?", first.Ticket.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	m.projected(t, first.Ticket.ID, 1)
	tx.Rollback()
	m.projected(t, first.Ticket.ID, 1)
	// Version 2 is committed by a real HTTP action, complete audit included in one event.
	m.actionHTTP(t, "local-it-agent", "claim", first.Ticket.ID, 1, nil)
	m.projected(t, first.Ticket.ID, 2)
	m.wait(t, "history published once", func() bool {
		var partition int32
		var offset int64
		if err := m.db.QueryRow("SELECT partition_id,record_offset FROM sync_kafka_es_deliveries WHERE ticket_id=? AND ticket_version=2 ORDER BY id DESC LIMIT 1", first.Ticket.ID).Scan(&partition, &offset); err != nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		payload, err := m.queue.ReadRecord(ctx, syncpipe.RecordRef{Topic: m.queue.Topic, Partition: partition, Offset: offset})
		if err != nil {
			return false
		}
		var e syncpipe.Event
		json.Unmarshal(payload, &e)
		return e.TicketID == first.Ticket.ID && e.Version == 2 && e.Source == "binlog" && e.History != nil && e.History.Action == "claimed" && e.EventType == "claimed"
	})
	before, _ := m.state.Checkpoint(context.Background())
	if _, err := syncpipe.Snapshot(context.Background(), m.state, m.queue); err != nil {
		t.Fatal(err)
	}
	after, _ := m.state.Checkpoint(context.Background())
	if before != after {
		t.Fatal("snapshot moved CDC progress")
	}
	t.Logf("RUNTIME overlap imported IDs=%v committed-history-preserved=true", ids)
}

func TestM2ESUnavailableRetryAndProgressBeforeAck(t *testing.T) {
	m := newM2(t)
	m.startCapture(t)
	// This proxy simulates ES network failure while the target is the real ES index.
	// Native ES shutdown/restart is additionally verified by the process smoke.
	if err := m.es.EnsureIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	failed := true
	var mutex sync.Mutex
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		offline := failed
		mutex.Unlock()
		if r.Method == "PUT" && strings.Contains(r.URL.Path, "/_doc/") && offline {
			http.Error(w, "unavailable", 503)
			return
		}
		req, _ := http.NewRequest(r.Method, os.Getenv("TICKETPULSE_ES_URL")+r.URL.RequestURI(), r.Body)
		req.Header = r.Header
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, "proxy failed", 503)
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
	proxyES, _ := search.NewElasticsearch(proxy.URL, m.name+"_tickets_v1")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- (&syncpipe.Consumer{Store: m.state, Queue: m.queue, ES: proxyES, RedeliveryDelay: 100 * time.Millisecond}).Run(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })
	item := m.create(t, 1, request("es-outage"))
	id := item.Ticket.ID
	m.wait(t, "durable retry", func() bool {
		var count int
		_ = m.db.QueryRow("SELECT COUNT(*) FROM sync_kafka_es_deliveries WHERE ticket_id=? AND status='retrying'", id).Scan(&count)
		return count == 1
	})
	var failedRef syncpipe.RecordRef
	failedRef.Topic = m.queue.Topic
	m.db.QueryRow("SELECT partition_id,record_offset FROM sync_kafka_es_deliveries WHERE ticket_id=? AND status='retrying'", id).Scan(&failedRef.Partition, &failedRef.Offset)
	progress, err := m.queue.Progress(context.Background(), "es")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range progress {
		if p.Partition == failedRef.Partition && p.Committed > failedRef.Offset {
			t.Fatal("offset committed before ES success")
		}
	}
	mutex.Lock()
	failed = false
	mutex.Unlock()
	m.projected(t, id, 1)
	m.wait(t, "offset after ES", func() bool {
		progress, err := m.queue.Progress(context.Background(), "es")
		if err != nil {
			return false
		}
		for _, p := range progress {
			if p.Partition == failedRef.Partition {
				return p.Committed > failedRef.Offset
			}
		}
		return false
	})
	// Stop at an idle durable boundary. A separate partition's periodic source
	// marker can still be in CommitRecords after the ticket partition drains.
	// Stop capture and drain all partitions before cancelling this direct worker.
	m.stopCapture(t)
	m.wait(t, "all partitions drained before direct consumer stop", func() bool {
		progress, err := m.queue.Progress(context.Background(), "es")
		if err != nil {
			return false
		}
		for _, p := range progress {
			if p.Lag != 0 {
				return false
			}
		}
		return true
	})
	cancel()
	if err := <-done; err != nil && !strings.Contains(err.Error(), "canceled") {
		t.Fatal(err)
	}
	done = make(chan error, 1)
	done <- nil
	// An injected progress-record failure occurs after ES success: it must still leave the MQ unacked.
	m.startCapture(t)
	m.exec(t, "CREATE TRIGGER fail_m2_delivery BEFORE INSERT ON sync_kafka_es_deliveries FOR EACH ROW BEGIN IF NEW.ticket_id IS NOT NULL AND NEW.ticket_version=2 AND NEW.status IN ('applied','duplicate_or_old') THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected progress failure'; END IF; END")
	cctx, ccancel := context.WithCancel(context.Background())
	defer ccancel()
	end := make(chan error, 1)
	go func() { end <- (&syncpipe.Consumer{Store: m.state, Queue: m.queue, ES: m.es}).Run(cctx) }()
	next := m.actionHTTP(t, "local-it-agent", "claim", id, 1, nil)
	m.projected(t, id, next.Version)
	select {
	case err := <-end:
		if err == nil {
			t.Fatal("progress failure returned success")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop at injected progress failure")
	}
	var pendingRef syncpipe.RecordRef
	pendingRef.Topic = m.queue.Topic
	m.db.QueryRow("SELECT partition_id,record_offset FROM sync_kafka_es_deliveries WHERE ticket_id=? AND ticket_version=2", id).Scan(&pendingRef.Partition, &pendingRef.Offset)
	progress, err = m.queue.Progress(context.Background(), "es")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range progress {
		if p.Partition == pendingRef.Partition && p.Committed > pendingRef.Offset {
			t.Fatal("lost uncommitted record after ES success")
		}
	}
	m.db.Exec("DROP TRIGGER fail_m2_delivery")
	// Restart waits for real Kafka redelivery from the unacknowledged message.
	m.startConsumer(t)
	m.projected(t, id, 2)
	m.wait(t, "duplicate replay ack", func() bool {
		var count int
		_ = m.db.QueryRow("SELECT COUNT(*) FROM sync_kafka_es_deliveries WHERE ticket_id=? AND ticket_version=2 AND status='duplicate_or_old'", id).Scan(&count)
		return count == 1
	})
	t.Log("RUNTIME ES failure retries and durable progress failure both retained unacknowledged messages")
}
