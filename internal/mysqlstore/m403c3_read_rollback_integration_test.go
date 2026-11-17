//go:build integration && m2 && m3 && m4 && m403b && m403c1 && m403c2 && m403c3

package mysqlstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/readruntime"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

func c3Save(t *testing.T, name string, value any) {
	t.Helper()
	dir := filepath.Join(os.Getenv("TICKETPULSE_M403C3_EVIDENCE_DIR"), t.Name())
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	e := json.NewEncoder(f)
	e.SetIndent("", "  ")
	if err = e.Encode(value); err != nil {
		t.Fatal(err)
	}
}

func c3StopLegacy(t *testing.T, f *m403c2Fixture, component string) {
	t.Helper()
	index := 1
	if component == "ck" {
		index = 2
	}
	p := f.originalWorkers[index]
	actual, e := readruntime.Inspect(context.Background(), p.PID, "")
	if e != nil || !readruntime.Same(p, actual) {
		t.Fatal("legacy identity before stop differs", e)
	}
	if e = syscall.Kill(p.PID, syscall.SIGTERM); e != nil {
		t.Fatal(e)
	}
	m403bWait(t, "legacy PID actually exits", func() bool { return readruntime.Exited(context.Background(), p) == nil })
	group := f.x.f.queue.Group(component)
	m403bWait(t, "legacy group has no live member", func() bool {
		g, e := f.x.f.queue.Admin.DescribeGroups(context.Background(), group)
		return e == nil && len(g[group].Members) == 0
	})
}

func TestM403C3LegacyFaultMatrixRefusesRollbackKeepsB(t *testing.T) {
	for _, fault := range []string{"stopped_writer", "missing_offset", "offset_outside_range", "retention_start_passed", "topic_uuid", "partition_set", "es_uuid", "ck_uuid", "isolation", "missing_content", "business_conflict", "unknown_process", "freeze_revision", "read_revision"} {
		t.Run(fault, func(t *testing.T) {
			f := c3New(t)
			b, _ := c3Forward(t, f)
			c3Plan(t, f, "rollback_fault")
			proof := c3Proof(t, f, "rollback_fault")
			b.stop(t, false)
			f.drain(t, "rollback_fault")
			q := f.x.f.queue
			db := f.x.f.db
			switch fault {
			case "stopped_writer":
				c3StopLegacy(t, f, "es")
			case "missing_offset":
				c3StopLegacy(t, f, "es")
				if _, e := q.Admin.DeleteOffsets(context.Background(), q.Group("es"), kadm.TopicsSet{q.Topic: {1: {}}}); e != nil {
					t.Fatal(e)
				}
				raw, e := q.Admin.FetchOffsets(context.Background(), q.Group("es"))
				if e != nil {
					t.Fatal(e)
				}
				o, ok := raw.Lookup(q.Topic, 1)
				if ok && o.At >= 0 {
					t.Fatal("offset not actually removed")
				}
				c3Save(t, "raw-missing-offset", raw)
			case "offset_outside_range":
				c3StopLegacy(t, f, "es")
				ends, e := q.Admin.ListEndOffsets(context.Background(), q.Topic)
				if e != nil {
					t.Fatal(e)
				}
				end, _ := ends.Lookup(q.Topic, 1)
				res, e := q.Admin.CommitOffsets(context.Background(), q.Group("es"), kadm.Offsets{q.Topic: {1: {Topic: q.Topic, Partition: 1, At: end.Offset + 1000000, LeaderEpoch: -1}}})
				if e != nil {
					t.Fatal(e)
				}
				if v, ok := res.Lookup(q.Topic, 1); !ok || v.Err != nil {
					t.Fatal("fault commit failed", res)
				}
				raw, e := q.Admin.FetchOffsets(context.Background(), q.Group("es"))
				if e != nil {
					t.Fatal(e)
				}
				o, ok := raw.Lookup(q.Topic, 1)
				if !ok || o.At != end.Offset+1000000 {
					t.Fatal("fault offset not persisted")
				}
				c3Save(t, "raw-outside-offset", raw)
			case "retention_start_passed":
				c3StopLegacy(t, f, "es")
				before, e := q.Admin.FetchOffsets(context.Background(), q.Group("es"))
				if e != nil {
					t.Fatal(e)
				}
				old, ok := before.Lookup(q.Topic, 1)
				if !ok || old.Err != nil || old.At < 0 {
					t.Fatal("private retention fixture has no committed offset")
				}
				if e = f.x.f.state.IssueWatermark(context.Background()); e != nil {
					t.Fatal(e)
				}
				m403bWait(t, "private new marker extends retained log", func() bool {
					ends, e := q.Admin.ListEndOffsets(context.Background(), q.Topic)
					end, ok := ends.Lookup(q.Topic, 1)
					return e == nil && ok && end.Err == nil && end.Offset > old.At
				})
				deleted, e := q.Admin.DeleteRecords(context.Background(), kadm.Offsets{q.Topic: {1: {Topic: q.Topic, Partition: 1, At: old.At + 1}}})
				if e != nil {
					t.Fatal(e)
				}
				if part, ok := deleted.Lookup(q.Topic, 1); !ok || part.Err != nil || part.LowWatermark < old.At+1 {
					t.Fatal("private retention injection failed", deleted)
				}
				d, e := f.c.ResolveReadDestination(context.Background(), "active_original")
				if e != nil {
					t.Fatal(e)
				}
				_, _, e = d.RawOffsetProgress(context.Background(), "es")
				if e == nil || !strings.Contains(e.Error(), "READ_OFFSET_OUT_OF_RANGE") {
					t.Fatal("legacy retained range violation was not rejected", e)
				}
				raw, e2 := q.Admin.FetchOffsets(context.Background(), q.Group("es"))
				starts, e3 := q.Admin.ListStartOffsets(context.Background(), q.Topic)
				if e2 != nil || e3 != nil {
					t.Fatal(e2, e3)
				}
				c3Save(t, "raw-retention-expired", map[string]any{"before": before, "after": raw, "log_start": starts, "delete_records_response": deleted, "error": e.Error()})
			case "topic_uuid":
				res, e := q.Admin.DeleteTopics(context.Background(), q.Topic)
				if e != nil || res[q.Topic].Err != nil {
					t.Fatal(e)
				}
				m403bWait(t, "topic gone", func() bool {
					x, e := q.Admin.ListTopics(context.Background(), q.Topic)
					return e == nil && x[q.Topic].Err != nil
				})
				if e = q.Ensure(context.Background()); e != nil {
					t.Fatal(e)
				}
			case "partition_set":
				req := kmsg.NewPtrCreatePartitionsRequest()
				req.Topics = append(req.Topics, kmsg.CreatePartitionsRequestTopic{Topic: q.Topic, Count: 4})
				resp, e := req.RequestWith(context.Background(), q.Client)
				if e != nil || len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
					t.Fatal("partition fault failed", resp, e)
				}
			case "es_uuid":
				m4HTTP(t, http.MethodDelete, f.x.f.es.Endpoint()+"/"+f.x.f.es.Index(), nil, 200)
				if e := f.x.f.es.EnsureIndex(context.Background()); e != nil {
					t.Fatal(e)
				}
			case "ck_uuid":
				if _, e := f.x.f.ck.Query(context.Background(), "DROP TABLE ticket_states"); e != nil {
					t.Fatal(e)
				}
				if e := f.x.f.ck.Ensure(context.Background()); e != nil {
					t.Fatal(e)
				}
			case "isolation":
				if _, e := db.Exec(`INSERT INTO sync_kafka_es_deliveries(topic,partition_id,record_offset,event_id,status,attempts,first_seen_at,last_seen_at) VALUES(?,0,999999999,'private-C3-isolation','isolated',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, q.Topic); e != nil {
					t.Fatal(e)
				}
			case "missing_content":
				m4HTTP(t, http.MethodDelete, fmt.Sprintf("%s/%s/_doc/1", f.x.f.es.Endpoint(), f.x.f.es.Index()), nil, 200)
				m4HTTP(t, http.MethodPost, f.x.f.es.Endpoint()+"/"+f.x.f.es.Index()+"/_refresh", nil, 200)
			case "business_conflict":
				if _, e := f.x.f.ck.Query(context.Background(), "INSERT INTO ticket_states SELECT * REPLACE(toUInt64(current_team_id+1) AS current_team_id) FROM ticket_states WHERE ticket_id=1 LIMIT 1"); e != nil {
					t.Fatal(e)
				}
			case "unknown_process":
				if _, e := db.Exec("DELETE FROM sync_projection_legacy_boots WHERE component='es'"); e != nil {
					t.Fatal(e)
				}
			case "freeze_revision":
				if _, e := db.Exec("UPDATE sync_business_write_gate SET revision=revision+1 WHERE gate_id=1"); e != nil {
					t.Fatal(e)
				}
			case "read_revision":
				if _, e := db.Exec("UPDATE sync_projection_read_switches SET expected_read_revision=expected_read_revision+1 WHERE operation_id='rollback_fault'"); e != nil {
					t.Fatal(e)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			_, err := f.c.CommitReadSelection(ctx, "rollback_fault")
			cancel()
			if err == nil {
				t.Fatalf("%s allowed rollback", fault)
			}
			// Inspect the raw selector even when deliberately damaged operation
			// metadata makes the ordinary resolver correctly fail closed.
			var s projection.ReadSelection
			if e := db.QueryRow("SELECT version_id,contract_sha256,handoff_id,read_revision,current_operation_id,updated_at FROM sync_projection_read_selection WHERE selector_id=1").Scan(&s.VersionID, &s.ContractSHA256, &s.HandoffID, &s.ReadRevision, &s.CurrentOperationID, &s.UpdatedAt); e != nil {
				t.Fatal(e)
			}
			g, e := projection.ReadGate(context.Background(), db)
			if e != nil || s.VersionID != "candidate_v1" || s.ReadRevision != 2 || g.State != "FROZEN" {
				t.Fatal("refusal changed selection", s, g, e)
			}
			c3Save(t, "rollback-refused", map[string]any{"fault": fault, "error": err.Error(), "selection": s, "gate": g, "historical_proof": proof.ProofSHA256})
		})
	}
}
func c3New(t *testing.T) *m403c2Fixture {
	t.Helper()
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03c3" {
		t.Fatal("new C3 instances required")
	}
	f := m403c2NewAt(t, "", func(s *m3fixture) {
		for _, fn := range []func(context.Context, *sql.DB) error{projection.EnsureReadControlSchema, projection.EnsureReadRuntimeSchema, projection.EnsureReadRollbackSchema} {
			if err := fn(context.Background(), s.db); err != nil {
				t.Fatal(err)
			}
		}
	})
	c3Save(t, "baseline", map[string]any{"A_B_contracts": []projection.Version{mustVersion(t, f, "active_original"), f.x.version}, "B_handoff": f.x.h, "initial_selection": mustSelection(t, f.x.f.db), "progress": f.progress(t)})
	return f
}
func mustVersion(t *testing.T, f *m403c2Fixture, id string) projection.Version {
	t.Helper()
	v, e := (&projection.VersionStore{DB: f.x.f.db}).Get(context.Background(), id)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func c3Forward(t *testing.T, f *m403c2Fixture) (*m403c2Process, string) {
	t.Helper()
	active := mustVersion(t, f, "active_original")
	a := f.startAPI(t, "active_original", false, nil, map[string]string{"TICKETPULSE_ES_URL": active.Contract.ES.Endpoint, "TICKETPULSE_CK_URL": active.Contract.CK.Endpoint})
	a.ready(t)
	first, _ := f.http(t, a, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2", "local-supervisor", nil, 200)
	var page search.Result
	if err := json.Unmarshal(first, &page); err != nil {
		t.Fatal(err)
	}
	f.plan(t, "forward")
	a.stop(t, false)
	f.drain(t, "forward")
	f.commit(t, "forward")
	b := f.startAPI(t, "candidate_v1", false, nil, map[string]string{"TICKETPULSE_ES_URL": f.x.version.Contract.ES.Endpoint, "TICKETPULSE_CK_URL": f.x.version.Contract.CK.Endpoint})
	b.ready(t)
	f.observe(t, "forward", b)
	f.resume(t, "forward", b)
	return b, page.NextCursor
}
func c3Plan(t *testing.T, f *m403c2Fixture, id string) projection.ReadSwitch {
	t.Helper()
	g, e := projection.Freeze(context.Background(), f.x.f.db, "C3 new rollback boundary")
	if e != nil {
		t.Fatal(e)
	}
	f.x.gate = g
	s := mustSelection(t, f.x.f.db)
	if s.CurrentOperationID == nil {
		t.Fatal("no committed parent")
	}
	o, e := f.c.BeginReadSwitch(context.Background(), id, projection.ReadRollback, "active_original", s.ReadRevision, *s.CurrentOperationID)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.c.ConfigureReadDrain(context.Background(), id, f.scope); e != nil {
		t.Fatal(e)
	}
	return o
}
func c3Proof(t *testing.T, f *m403c2Fixture, id string) projection.ReadSwitch {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	o, e := f.c.ValidateReadSwitchDestination(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	return o
}
func c3Both(t *testing.T, f *m403c2Fixture) map[string]syncpipe.ReconcileReport {
	t.Helper()
	reports := map[string]syncpipe.ReconcileReport{}
	m403bWait(t, "C3 both projections complete", func() bool {
		for name, target := range map[string]struct {
			es *search.Elasticsearch
			ck *analytics.ClickHouse
		}{"A": {f.x.f.es, f.x.f.ck}, "B": {f.x.control.ES, f.x.control.CK}} {
			r, e := syncpipe.Reconcile(context.Background(), f.x.f.state, nil, target.es, target.ck, syncpipe.ReconcileOptions{BatchSize: 250, MetricsOrganizationID: 1, MetricsRequest: m4Window})
			if e != nil || r.State != syncpipe.ReconcileConsistent || r.DifferenceCount != 0 {
				return false
			}
			reports[name] = r
		}
		return true
	})
	return reports
}
func TestM403C3RealForwardRollbackRevisionCursorAndBothTargets(t *testing.T) {
	c3Roundtrip(t, false)
}

// Invoked only by the identity-checked private live driver after acceptance.
// It intentionally keeps the launcher and all five propagation/API children
// running; its ready record, rather than a blocked test's exit, is observed by
// a separate process. Ordinary C3 test runs skip this service fixture.
func TestM403C3RetainedPrivateRoundtrip(t *testing.T) {
	if os.Getenv("TICKETPULSE_C3_RETAIN_LIVE") != "true" {
		t.Skip("explicit private live driver only")
	}
	c3Roundtrip(t, true)
}

func c3Roundtrip(t *testing.T, retain bool) {
	t.Helper()
	f := c3New(t)
	before := f.progress(t)
	b, oldA := c3Forward(t, f)
	second, _ := f.http(t, b, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2", "local-supervisor", nil, 200)
	var pageB search.Result
	json.Unmarshal(second, &pageB)
	created, _ := f.http(t, b, http.MethodPost, "/api/v1/tickets", "local-alice", request("c3_b_service"), 201)
	var made ticket.CreateResult
	json.Unmarshal(created, &made)
	f.http(t, b, http.MethodPost, "/api/v1/tickets/comments", "local-alice", ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: made.Ticket.ID, ExpectedVersion: 1}, Content: "B service new comment"}, 200)
	f.http(t, b, http.MethodPost, "/api/v1/tickets/transfer-team", "local-supervisor", ticket.TeamTransferRequest{ActionRequest: ticket.ActionRequest{TicketID: made.Ticket.ID, ExpectedVersion: 2}, TeamID: 20, Reason: "C3 transfer after forward promotion"}, 200)
	f.http(t, b, http.MethodGet, fmt.Sprintf("/api/v1/tickets/%d", made.Ticket.ID), "local-it-agent", nil, 403)
	f.http(t, b, http.MethodGet, fmt.Sprintf("/api/v1/tickets/%d", made.Ticket.ID), "local-device-agent", nil, 200)
	beforeRollback := c3Both(t, f)
	c3Plan(t, f, "rollback")
	proof := c3Proof(t, f, "rollback")
	if _, e := f.c.CommitReadSelection(context.Background(), "rollback"); e == nil {
		t.Fatal("live B allowed rollback")
	}
	f.http(t, b, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc", "local-supervisor", nil, 503)
	b.stop(t, false)
	f.drain(t, "rollback")
	committed := f.commit(t, "rollback")
	if committed.RollbackOf == nil || *committed.RollbackOf != "forward" {
		t.Fatal("rollback linkage absent")
	}
	for i := 0; i < 3; i++ {
		if _, e := f.c.CommitReadSelection(context.Background(), "rollback"); e != nil {
			t.Fatal(e)
		}
	}
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	ready := f.observe(t, "rollback", a)
	for _, cursor := range []string{oldA, pageB.NextCursor} {
		f.http(t, a, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2&cursor="+url.QueryEscape(cursor), "local-supervisor", nil, 409)
	}
	f.http(t, a, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2&cursor="+url.QueryEscape(m403c2Expire(t, oldA)), "local-supervisor", nil, 400)
	newPage, _ := f.http(t, a, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2", "local-supervisor", nil, 200)
	var fresh search.Result
	json.Unmarshal(newPage, &fresh)
	f.http(t, a, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=2&cursor="+url.QueryEscape(fresh.NextCursor), "local-supervisor", nil, 200)
	gold, _ := f.http(t, a, http.MethodGet, "/api/v1/analytics?start=2026-11-01&end=2026-11-04", "local-supervisor", nil, 200)
	c3Gold(t, gold, 1)
	f.resume(t, "rollback", a)
	f.http(t, a, http.MethodPost, "/api/v1/tickets/comments", "local-device-agent", ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: made.Ticket.ID, ExpectedVersion: 3}, Content: "A service after safe rollback"}, 200)
	f.http(t, a, http.MethodPost, "/api/v1/tickets", "local-alice", request("c3_a_service_again"), 201)
	reports := c3Both(t, f)
	selection := mustSelection(t, f.x.f.db)
	if selection.VersionID != "active_original" || selection.ReadRevision != 3 || selection.HandoffID != nil {
		t.Fatal(selection)
	}
	h, e := (&projection.HandoffStore{DB: f.x.f.db}).Get(context.Background(), f.x.h.ID)
	if e != nil || !reflect.DeepEqual(h, f.x.h) {
		t.Fatal("B handoff changed", e)
	}
	c3Save(t, "roundtrip-complete", map[string]any{"revisions": []int{1, 2, 3}, "rollback_fresh_proof": proof, "committed": committed, "ready": ready, "selection": selection, "A_B_before_rollback": beforeRollback, "A_B_after_resume": reports, "progress_before": before, "progress_after": f.progress(t), "old_A_cursor_rejected": true, "B_cursor_rejected": true, "new_A_paging_pass": true, "gold": json.RawMessage(gold), "handoff_unchanged": true})
	if retain {
		c3RetainLive(t, f, a, reports)
	}
}

func c3RetainLive(t *testing.T, f *m403c2Fixture, a *m403c2Process, reports map[string]syncpipe.ReconcileReport) {
	t.Helper()
	if os.Getenv("TICKETPULSE_RETAIN_C3_RESOURCES") != "true" || os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03c3" {
		t.Fatal("explicit C3 private retention required")
	}
	launcher, err := readruntime.Inspect(context.Background(), os.Getpid(), "")
	if err != nil {
		t.Fatal(err)
	}
	children := []readruntime.Process{a.identity}
	children = append(children, f.originalWorkers...)
	for _, child := range []*m403bProcess{f.candidateES, f.candidateCK} {
		p, e := readruntime.Inspect(context.Background(), child.cmd.Process.Pid, "")
		if e != nil {
			t.Fatal(e)
		}
		children = append(children, p)
	}
	status, err := f.c.ReadSwitchStatus(context.Background(), "rollback")
	if err != nil || status.Operation.Stage != projection.ReadResumed || status.Gate.State != "OPEN" {
		t.Fatal("live fixture is not restored", err)
	}
	c3Save(t, "retained-live", map[string]any{"launcher": launcher, "processes": children, "api": a.addr, "api_boot_id": a.boot, "schema": f.x.f.name, "topic": f.x.f.queue.Topic, "contracts": []projection.Version{mustVersion(t, f, "active_original"), f.x.version}, "handoff": f.x.h, "selection": status.Selection, "gate": status.Gate, "status": status, "scope": f.scope, "A_B": reports, "progress": f.progress(t), "manual_shutdown_only": true})
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(stop)
	// No restart, rollback, retirement or offset operation happens here. The
	// ordinary fixture cleanup is reached only after an explicit parent stop.
	<-stop
}

func TestM403C3RollbackCrashChild(t *testing.T) {
	stage := os.Getenv("TICKETPULSE_C3_CRASH_STAGE")
	if stage == "" {
		t.Skip("parent invokes real crash child")
	}
	cfg, e := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if e != nil {
		t.Fatal(e)
	}
	cfg.DBName = os.Getenv("TICKETPULSE_C3_CRASH_SCHEMA")
	db, e := mysqlstore.Open(context.Background(), cfg.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var uuid string
	if e = db.QueryRow("SELECT @@server_uuid").Scan(&uuid); e != nil || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
		t.Fatal("child identity mismatch")
	}
	q, e := syncpipe.ConnectStatusQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	c := &syncpipe.ReadSwitchControl{ProjectionControl: &syncpipe.ProjectionControl{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: q}, Observe: func(actual string, value any) error {
		if actual == stage {
			data, _ := json.Marshal(value)
			if e := os.WriteFile(os.Getenv("TICKETPULSE_C3_CRASH_READY"), data, 0600); e != nil {
				t.Fatal(e)
			}
			select {}
		}
		return nil
	}}
	id := os.Getenv("TICKETPULSE_C3_CRASH_ID")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	if strings.Contains(stage, "plan_commit") {
		_, e = c.BeginReadSwitch(ctx, id, projection.ReadRollback, "active_original", 2, "forward")
	} else if strings.Contains(stage, "marker_commit") || strings.Contains(stage, "proof_") {
		_, e = c.ValidateReadSwitchDestination(ctx, id)
	} else if strings.Contains(stage, "read_commit") {
		_, e = c.CommitReadSelection(ctx, id)
	} else {
		probe := syncpipe.ReadAPIProbe{BootID: os.Getenv("TICKETPULSE_C3_CRASH_BOOT"), Token: "local-supervisor", PermissionToken: "local-it-agent", Analytics: m4Window}
		if strings.Contains(stage, "api_ready") {
			_, e = c.ObserveAPIReady(ctx, id, probe)
		} else {
			_, e = c.ResumeReadSwitch(ctx, id, os.Getenv("TICKETPULSE_C3_CRASH_FREEZE"), probe)
		}
	}
	t.Fatalf("boundary not reached %s: %v", stage, e)
}
func c3Crash(t *testing.T, f *m403c2Fixture, stage, id string, api *m403c2Process) {
	t.Helper()
	binary, _ := os.Executable()
	dir := filepath.Join(os.Getenv("TICKETPULSE_M403C3_EVIDENCE_DIR"), t.Name())
	os.MkdirAll(dir, 0700)
	ready := filepath.Join(dir, stage+"-boundary.json")
	log, e := os.OpenFile(filepath.Join(dir, stage+"-child.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(binary, "-test.run=^TestM403C3RollbackCrashChild$", "-test.v")
	boot := ""
	if api != nil {
		boot = api.boot
	}
	cmd.Env = append(os.Environ(), "TICKETPULSE_C3_CRASH_STAGE="+stage, "TICKETPULSE_C3_CRASH_READY="+ready, "TICKETPULSE_C3_CRASH_SCHEMA="+f.x.f.name, "TICKETPULSE_C3_CRASH_ID="+id, "TICKETPULSE_C3_CRASH_BOOT="+boot, "TICKETPULSE_C3_CRASH_FREEZE="+*f.x.gate.FreezeID)
	cmd.Stdout, cmd.Stderr = log, log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { e := cmd.Wait(); log.Close(); done <- e }()
	deadline := time.Now().Add(50 * time.Second)
	reached := false
	for time.Now().Before(deadline) {
		if _, e = os.Stat(ready); e == nil {
			reached = true
			break
		}
		select {
		case e := <-done:
			t.Fatalf("child early %s: %v", stage, e)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !reached {
		cmd.Process.Kill()
		<-done
		t.Fatal("boundary timeout", stage)
	}
	identity, e := readruntime.Inspect(context.Background(), cmd.Process.Pid, "")
	if e != nil {
		t.Fatal(e)
	}
	if e = cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e == nil {
		t.Fatal("SIGKILL child successful exit")
	}
	if e = readruntime.Exited(context.Background(), identity); e != nil {
		t.Fatal(e)
	}
	s := mustSelection(t, f.x.f.db)
	g, e := projection.ReadGate(context.Background(), f.x.f.db)
	if e != nil {
		t.Fatal(e)
	}
	o, err := (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), id)
	c3Save(t, stage+"-persistent", map[string]any{"selection": s, "gate": g, "operation": o, "operation_found": err == nil, "killed_process": identity})
}

func TestM403C3RollbackSIGKILLDurableStagesAndBootRecovery(t *testing.T) {
	f := c3New(t)
	b, _ := c3Forward(t, f)
	g, e := projection.Freeze(context.Background(), f.x.f.db, "C3 rollback SIGKILL")
	if e != nil {
		t.Fatal(e)
	}
	f.x.gate = g
	c3Crash(t, f, "before_plan_commit", "rollback_crash", nil)
	if _, e := (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "rollback_crash"); e != sql.ErrNoRows {
		t.Fatal("uncommitted plan survived", e)
	}
	c3Crash(t, f, "after_plan_commit", "rollback_crash", nil)
	if _, e = f.c.ConfigureReadDrain(context.Background(), "rollback_crash", f.scope); e != nil {
		t.Fatal(e)
	}
	c3Crash(t, f, "before_marker_commit", "rollback_crash", nil)
	o, e := (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "rollback_crash")
	if e != nil || o.MarkerID != nil {
		t.Fatal("marker committed too early", e)
	}
	c3Crash(t, f, "after_marker_commit", "rollback_crash", nil)
	o, e = (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "rollback_crash")
	if e != nil || o.MarkerID == nil {
		t.Fatal(e)
	}
	marker := *o.MarkerID
	c3Crash(t, f, "before_proof_commit", "rollback_crash", nil)
	o, _ = (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "rollback_crash")
	if o.ProofSHA256 != nil || *o.MarkerID != marker {
		t.Fatal("proof or marker inconsistent")
	}
	c3Crash(t, f, "after_proof_commit", "rollback_crash", nil)
	o, _ = (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "rollback_crash")
	if o.ProofSHA256 == nil || *o.MarkerID != marker {
		t.Fatal("proof missing")
	}
	if _, e = f.c.CommitReadSelection(context.Background(), "rollback_crash"); e == nil {
		t.Fatal("live B allowed rollback")
	}
	b.stop(t, true)
	f.drain(t, "rollback_crash")
	c3Crash(t, f, "before_read_commit", "rollback_crash", nil)
	if mustSelection(t, f.x.f.db).ReadRevision != 2 {
		t.Fatal("early commit")
	}
	c3Crash(t, f, "after_read_commit", "rollback_crash", nil)
	if mustSelection(t, f.x.f.db).ReadRevision != 3 {
		t.Fatal("commit absent")
	}
	if _, e = f.c.CommitReadSelection(context.Background(), "rollback_crash"); e != nil {
		t.Fatal(e)
	}
	bad := f.startAPI(t, "active_original", false, nil, map[string]string{"TICKETPULSE_CK_DATABASE": f.x.control.CK.Database()})
	bad.failed(t)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	c3Crash(t, f, "before_api_ready_commit", "rollback_crash", a)
	c3Crash(t, f, "after_api_ready_commit", "rollback_crash", a)
	a.stop(t, true)
	if _, e = f.c.ResumeReadSwitch(context.Background(), "rollback_crash", *g.FreezeID, f.probe(a)); e == nil {
		t.Fatal("dead API resumed")
	}
	a = f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	if _, e = f.c.ResumeReadSwitch(context.Background(), "rollback_crash", *g.FreezeID, f.probe(a)); e == nil {
		t.Fatal("new boot inherited old ready")
	}
	f.observe(t, "rollback_crash", a)
	c3Crash(t, f, "before_read_resume_commit", "rollback_crash", a)
	state, _ := projection.ReadGate(context.Background(), f.x.f.db)
	if state.State != "FROZEN" {
		t.Fatal("early OPEN")
	}
	c3Crash(t, f, "after_read_resume_commit", "rollback_crash", a)
	f.resume(t, "rollback_crash", a)
	f.http(t, a, http.MethodPost, "/api/v1/tickets", "local-alice", request("c3_crash_recovered"), 201)
	reports := c3Both(t, f)
	status, e := f.c.ReadSwitchStatus(context.Background(), "rollback_crash")
	if e != nil || status.Operation.Stage != projection.ReadResumed {
		t.Fatal(e)
	}
	c3Save(t, "crash-recovered", map[string]any{"final": status, "A_B": reports, "marker_retained": marker, "controller_SIGKILL_boundaries": 12, "A_API_restarted": true})
}

func TestM403C3CancelledRollbackRequiresHealthyCurrentB(t *testing.T) {
	f := c3New(t)
	b, _ := c3Forward(t, f)
	c3Plan(t, f, "cancel_rollback")
	c3Proof(t, f, "cancel_rollback")
	m4HTTP(t, http.MethodDelete, fmt.Sprintf("%s/%s/_doc/1", f.x.f.es.Endpoint(), f.x.f.es.Index()), nil, 200)
	m4HTTP(t, http.MethodPost, f.x.f.es.Endpoint()+"/"+f.x.f.es.Index()+"/_refresh", nil, 200)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	_, e := f.c.ValidateReadSwitchDestination(ctx, "cancel_rollback")
	cancel()
	if e == nil {
		t.Fatal("damaged A proof succeeded")
	}
	o, e := f.c.CancelPlannedReadSwitch(context.Background(), "cancel_rollback")
	if e != nil || o.Stage != projection.ReadCancelled {
		t.Fatal(e)
	}
	if _, e = f.c.CancelPlannedReadSwitch(context.Background(), "cancel_rollback"); e != nil {
		t.Fatal(e)
	}
	if _, e = projection.Resume(context.Background(), f.x.f.db, *f.x.gate.FreezeID); e == nil {
		t.Fatal("ordinary Resume bypassed B health proof")
	}
	if _, e = f.c.ResumeCancelledReadSwitch(context.Background(), "cancel_rollback", *f.x.gate.FreezeID, syncpipe.ReadAPIProbe{BootID: "missing-boot", Token: "local-supervisor", Analytics: m4Window}); e == nil {
		t.Fatal("unknown current API opened cancelled gate")
	}
	stillFrozen, e := projection.ReadGate(context.Background(), f.x.f.db)
	if e != nil || stillFrozen.State != "FROZEN" {
		t.Fatal("refused cancellation Resume changed gate", stillFrozen, e)
	}
	oldBoot := b.boot
	b.stop(t, true)
	if _, e = f.c.ResumeCancelledReadSwitch(context.Background(), "cancel_rollback", *f.x.gate.FreezeID, f.probe(b)); e == nil {
		t.Fatal("dead current B API opened cancelled gate")
	}
	b = f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	g, e := f.c.ResumeCancelledReadSwitch(context.Background(), "cancel_rollback", *f.x.gate.FreezeID, f.probe(b))
	if e != nil || g.State != "OPEN" {
		t.Fatal(e)
	}
	again, e := f.c.ResumeCancelledReadSwitch(context.Background(), "cancel_rollback", *f.x.gate.FreezeID, f.probe(b))
	if e != nil || !reflect.DeepEqual(again, g) {
		t.Fatal("cancel Resume outcome retry differs", again, e)
	}
	f.http(t, b, http.MethodPost, "/api/v1/tickets", "local-alice", request("c3_cancel_b_available"), 201)
	s := mustSelection(t, f.x.f.db)
	if s.VersionID != "candidate_v1" || s.ReadRevision != 2 {
		t.Fatal(s)
	}
	c3Save(t, "cancelled-safe-resume", map[string]any{"operation": o, "selection": s, "gate": g, "A_damage_not_promoted": true, "B_actual_API_checked": true, "unknown_boot_refused": true, "dead_B_boot_refused": oldBoot, "healthy_B_boot": b.boot, "repeat_resume_revision_unchanged": true})
}

func TestM403C3CommittedRollbackExplicitReverseRecovery(t *testing.T) {
	f := c3New(t)
	b, _ := c3Forward(t, f)
	c3Plan(t, f, "rollback_unready")
	c3Proof(t, f, "rollback_unready")
	b.stop(t, false)
	f.drain(t, "rollback_unready")
	f.commit(t, "rollback_unready")
	bad := f.startAPI(t, "active_original", false, nil, map[string]string{"TICKETPULSE_ES_URL": f.x.control.ES.Endpoint(), "TICKETPULSE_SEARCH_INDEX": f.x.control.ES.Index()})
	bad.failed(t)
	if _, e := f.c.CancelPlannedReadSwitch(context.Background(), "rollback_unready"); e == nil {
		t.Fatal("cancelled committed switch")
	}
	s := mustSelection(t, f.x.f.db)
	temporary, e := f.c.BeginReadSwitch(context.Background(), "recovery_cancelled", projection.ReadPromote, "candidate_v1", s.ReadRevision, "rollback_unready")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.c.ConfigureReadDrain(context.Background(), temporary.ID, f.scope); e != nil {
		t.Fatal(e)
	}
	if _, e = f.c.CancelPlannedReadSwitch(context.Background(), temporary.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = f.c.CancelPlannedReadSwitch(context.Background(), temporary.ID); e != nil {
		t.Fatal("explicit unfinished recovery cancellation retry", e)
	}
	if _, e = projection.Resume(context.Background(), f.x.f.db, *f.x.gate.FreezeID); e == nil {
		t.Fatal("cancelling a recovery opened its unfinished predecessor")
	}
	if _, e = f.c.ResumeCancelledReadSwitch(context.Background(), temporary.ID, *f.x.gate.FreezeID, f.probe(b)); e == nil {
		t.Fatal("cancelled recovery bypassed committed predecessor")
	}
	o, e := f.c.BeginReadSwitch(context.Background(), "recover_to_b", projection.ReadPromote, "candidate_v1", s.ReadRevision, "rollback_unready")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.c.ConfigureReadDrain(context.Background(), o.ID, f.scope); e != nil {
		t.Fatal(e)
	}
	c3Proof(t, f, o.ID)
	f.drain(t, o.ID)
	f.commit(t, o.ID)
	b = f.startAPI(t, "candidate_v1", false, nil, nil)
	b.ready(t)
	f.observe(t, o.ID, b)
	f.resume(t, o.ID, b)
	f.http(t, b, http.MethodPost, "/api/v1/tickets", "local-alice", request("c3_explicit_inverse_recovery"), 201)
	reports := c3Both(t, f)
	s = mustSelection(t, f.x.f.db)
	if s.ReadRevision != 4 || s.VersionID != "candidate_v1" {
		t.Fatal(s)
	}
	prior, e := (&projection.ReadControlStore{DB: f.x.f.db}).Operation(context.Background(), "rollback_unready")
	if e != nil || prior.Stage != projection.ReadCommitted || prior.SupersededBy == nil || *prior.SupersededBy != o.ID {
		t.Fatal(prior, e)
	}
	c3Save(t, "explicit-committed-recovery", map[string]any{"revisions": []int{1, 2, 3, 4}, "prior": prior, "selection": s, "A_B": reports, "automatic_reversion": false, "cancelled_recovery_retry_kept_predecessor_frozen": true})
}

func TestM403C3OperatorCLIRollbackAndCancelledResume(t *testing.T) {
	f := c3New(t)
	b, _ := c3Forward(t, f)
	c3CLI(t, f, "migrate-read-rollback", "install_015_retry", nil)
	g, err := projection.Freeze(context.Background(), f.x.f.db, "C3 explicit operator rollback CLI")
	if err != nil {
		t.Fatal(err)
	}
	f.x.gate = g
	c3CLI(t, f, "read-begin", "cli_cancelled", nil, "-direction=ROLLBACK", "-rollback-of=forward", "-version=active_original", "-read-revision=2")
	scopeFile := filepath.Join(f.scope.Root, fmt.Sprintf("c3-scope-%d.json", time.Now().UnixNano()))
	raw, _ := json.Marshal(f.scope)
	if err = os.WriteFile(scopeFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	c3CLI(t, f, "read-switch-drain-plan", "cli_cancelled", nil, "-api-scope="+scopeFile)
	c3CLI(t, f, "read-cancel", "cli_cancelled", nil)
	c3CLI(t, f, "read-switch-cancel-resume", "cli_cancelled", b, "-freeze-id="+*g.FreezeID)
	g, err = projection.Freeze(context.Background(), f.x.f.db, "C3 CLI second fresh rollback freeze")
	if err != nil {
		t.Fatal(err)
	}
	f.x.gate = g
	c3CLI(t, f, "read-begin", "cli_rollback", nil, "-direction=ROLLBACK", "-rollback-of=forward", "-version=active_original", "-read-revision=2")
	c3CLI(t, f, "read-switch-drain-plan", "cli_rollback", nil, "-api-scope="+scopeFile)
	c3CLI(t, f, "read-validate", "cli_rollback", nil)
	b.stop(t, false)
	c3CLI(t, f, "read-switch-drain-observe", "cli_rollback", nil)
	c3CLI(t, f, "read-switch-commit", "cli_rollback", nil)
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	c3CLI(t, f, "read-switch-observe-ready", "cli_rollback", a)
	c3CLI(t, f, "read-switch-resume", "cli_rollback", a, "-freeze-id="+*g.FreezeID)
	c3CLI(t, f, "read-switch-status", "cli_rollback", nil)
	s := mustSelection(t, f.x.f.db)
	if s.VersionID != "active_original" || s.ReadRevision != 3 {
		t.Fatal(s)
	}
	c3Save(t, "operator-cli-complete", map[string]any{"selection": s, "A_B": c3Both(t, f), "cancelled_resume_used_explicit_health_checks": true, "migration_retry_safe": true})
}

func c3CLI(t *testing.T, f *m403c2Fixture, mode, id string, api *m403c2Process, extra ...string) []byte {
	t.Helper()
	cfg, _ := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	cfg.DBName = f.x.f.name
	args := append([]string{"-mode=" + mode, "-operation=" + id, "-wait=50s"}, extra...)
	if api != nil {
		args = append(args, "-api-boot="+api.boot, "-probe-start=2026-11-01", "-probe-end=2026-11-04")
	}
	cmd := exec.Command(os.Getenv("TICKETPULSE_CONTROL_BINARY"), args...)
	cmd.Env = append(os.Environ(), "TICKETPULSE_CONTROL_DSN="+cfg.FormatDSN(), "TICKETPULSE_READ_PROBE_TOKEN=local-supervisor", "TICKETPULSE_READ_PERMISSION_TOKEN=local-it-agent")
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("C3 control %s failed: %v %s", mode, err, raw)
	}
	c3Save(t, "cli-"+mode+"-"+id, json.RawMessage(raw))
	return raw
}

func TestM403C3PartialRollbackMetadataRefusesReadAndResume(t *testing.T) {
	f := c3New(t)
	b, _ := c3Forward(t, f)
	c3Plan(t, f, "metadata_rollback")
	if err := projection.EnsureReadRollbackSchema(context.Background(), f.x.f.db); err != nil {
		t.Fatal("015 retry", err)
	}
	if _, err := f.x.f.db.Exec("DELETE FROM sync_projection_control_log WHERE operation='install_read_rollback'"); err != nil {
		t.Fatal(err)
	}
	if _, err := projection.ReadRollbackInstalled(context.Background(), f.x.f.db); err == nil {
		t.Fatal("lost 015 provenance silently downgraded")
	}
	if _, err := (&projection.ReadControlStore{DB: f.x.f.db}).Selection(context.Background()); err == nil {
		t.Fatal("corrupted 015 allowed selected read")
	}
	if _, err := projection.Resume(context.Background(), f.x.f.db, *f.x.gate.FreezeID); err == nil {
		t.Fatal("corrupted 015 allowed resume")
	}
	f.http(t, b, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc", "local-supervisor", nil, 503)
	if err := projection.EnsureReadRollbackSchema(context.Background(), f.x.f.db); err != nil {
		t.Fatal("explicit metadata repair", err)
	}
	if _, err := f.c.CancelPlannedReadSwitch(context.Background(), "metadata_rollback"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.x.f.db.Exec("ALTER TABLE sync_projection_read_switches DROP COLUMN superseded_by"); err != nil {
		t.Fatal(err)
	}
	if err := projection.EnsureReadRollbackSchema(context.Background(), f.x.f.db); err == nil {
		t.Fatal("partial 015 silently auto-repaired")
	}
	if _, err := f.c.ResumeCancelledReadSwitch(context.Background(), "metadata_rollback", *f.x.gate.FreezeID, f.probe(b)); err == nil {
		t.Fatal("partial metadata allowed cancelled Resume")
	}
	g, err := projection.ReadGate(context.Background(), f.x.f.db)
	if err != nil || g.State != "FROZEN" {
		t.Fatal(g, err)
	}
	c3Save(t, "partial-metadata-refused", map[string]any{"lost_provenance_refused": true, "partial_column_refused": true, "explicit_install_retry_safe": true, "gate": g})
}

func TestM403C3ConcurrentReverseIntentsAndAuditTampering(t *testing.T) {
	f := c3New(t)
	b, _ := c3Forward(t, f)
	g, e := projection.Freeze(context.Background(), f.x.f.db, "C3 concurrent reverse intents")
	if e != nil {
		t.Fatal(e)
	}
	f.x.gate = g
	start := make(chan struct{})
	wins := make(chan projection.ReadSwitch, 8)
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func(n int) {
			defer group.Done()
			<-start
			direction := projection.ReadRollback
			if n%2 == 1 {
				direction = projection.ReadPromote
			}
			for retry := 0; retry < 30; retry++ {
				o, e := f.c.BeginReadSwitch(context.Background(), fmt.Sprintf("reverse_%d", n), direction, "active_original", 2, "forward")
				if e == nil {
					wins <- o
					return
				}
				if !strings.Contains(e.Error(), "already running or lock failed") {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}
	close(start)
	group.Wait()
	close(wins)
	var winner []projection.ReadSwitch
	for o := range wins {
		winner = append(winner, o)
	}
	if len(winner) != 1 || winner[0].Direction != projection.ReadRollback {
		t.Fatal("ambiguous concurrent winner", winner)
	}
	id := winner[0].ID
	if _, e = f.c.BeginReadSwitch(context.Background(), id, projection.ReadRollback, "active_original", 2, "forward"); e != nil {
		t.Fatal("same intent retry", e)
	}
	if _, e = f.c.ConfigureReadDrain(context.Background(), id, f.scope); e != nil {
		t.Fatal(e)
	}
	c3Proof(t, f, id)
	b.stop(t, false)
	f.drain(t, id)
	f.commit(t, id)
	for i := 0; i < 3; i++ {
		if _, e = f.c.BeginReadSwitch(context.Background(), id, projection.ReadRollback, "active_original", 2, "forward"); e != nil {
			t.Fatal(e)
		}
	}
	s := mustSelection(t, f.x.f.db)
	if s.ReadRevision != 3 {
		t.Fatal(s)
	}
	if _, e = f.x.f.db.Exec("UPDATE sync_projection_read_switches SET rollback_of=NULL WHERE operation_id=?", id); e != nil {
		t.Fatal(e)
	}
	if _, e = (&projection.ReadControlStore{DB: f.x.f.db}).Selection(context.Background()); e == nil {
		t.Fatal("removed reverse audit link allowed selection")
	}
	c3Save(t, "concurrent-and-tamper-refused", map[string]any{"intents": 8, "winner": winner[0], "selection_before_fault": s, "reverse_link_corruption_refused": true})
}

func TestM403C3ActualKafkaReplayPreservesGoldAndHistory(t *testing.T) {
	f := c3New(t)
	b, _ := c3Forward(t, f)
	q := f.x.f.queue
	captured := map[int64][]byte{}
	for part := int32(0); part < q.Partitions; part++ {
		ends, e := q.Admin.ListEndOffsets(context.Background(), q.Topic)
		if e != nil {
			t.Fatal(e)
		}
		end, _ := ends.Lookup(q.Topic, part)
		for offset := int64(0); offset < end.Offset; offset++ {
			data, e := q.ReadRecord(context.Background(), syncpipe.RecordRef{Topic: q.Topic, Partition: part, Offset: offset})
			if e != nil {
				t.Fatal(e)
			}
			var event syncpipe.Event
			if json.Unmarshal(data, &event) == nil && event.TicketID == 4 && event.History != nil && event.History.Action == ticket.ActionResolved {
				captured[event.History.Version] = data
			}
			if len(captured) >= 2 {
				break
			}
		}
		if len(captured) >= 2 {
			break
		}
	}
	if len(captured) < 2 {
		t.Fatal("real first and repeated resolution events unavailable")
	}
	refs := []syncpipe.RecordRef{}
	for version, data := range captured {
		for i := 0; i < 2; i++ {
			ref, e := q.PublishBytes(context.Background(), data, fmt.Sprintf("c3-replay-%d-%d", version, i))
			if e != nil {
				t.Fatal(e)
			}
			refs = append(refs, ref)
		}
	}
	for _, ref := range refs {
		m403bWait(t, "both replay groups commit actual messages", func() bool {
			for _, id := range []string{q.Group("es"), q.Group("ck"), f.x.h.ESGroup, f.x.h.CKGroup} {
				raw, e := q.Admin.FetchOffsets(context.Background(), id)
				if e != nil {
					return false
				}
				offset, ok := raw.Lookup(q.Topic, ref.Partition)
				if !ok || offset.Err != nil || offset.At <= ref.Offset {
					return false
				}
			}
			return true
		})
	}
	before := c3Both(t, f)
	c3Plan(t, f, "replay_rollback")
	c3Proof(t, f, "replay_rollback")
	b.stop(t, false)
	f.drain(t, "replay_rollback")
	f.commit(t, "replay_rollback")
	a := f.startAPI(t, "active_original", false, nil, nil)
	a.ready(t)
	f.observe(t, "replay_rollback", a)
	f.resume(t, "replay_rollback", a)
	gold, _ := f.http(t, a, http.MethodGet, "/api/v1/analytics?start=2026-11-01&end=2026-11-04", "local-supervisor", nil, 200)
	m403c2Gold(t, gold)
	reports := c3Both(t, f)
	versions := []int64{}
	for version := range captured {
		versions = append(versions, version)
	}
	c3Save(t, "actual-replay-idempotence", map[string]any{"replayed_ticket_id": 4, "resolution_versions": versions, "published_replay_records": refs, "A_B_before": before, "A_B_after": reports, "gold": json.RawMessage(gold), "normalized_history_unchanged": true})
}

func TestM403C3BInFlightReadSIGKILLDrainThenRollback(t *testing.T) {
	requestEntered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var hold atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hold.Load() && strings.HasSuffix(r.URL.Path, "/_search") {
			once.Do(func() { close(requestEntered) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		req, e := http.NewRequestWithContext(r.Context(), r.Method, os.Getenv("TICKETPULSE_ES_URL")+r.URL.RequestURI(), r.Body)
		if e != nil {
			return
		}
		req.Header = r.Header.Clone()
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			return
		}
		defer res.Body.Close()
		for k, values := range res.Header {
			for _, v := range values {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(res.StatusCode)
		io.Copy(w, res.Body)
	}))
	defer proxy.Close()
	f := m403c2NewAt(t, proxy.URL, func(s *m3fixture) {
		for _, fn := range []func(context.Context, *sql.DB) error{projection.EnsureReadControlSchema, projection.EnsureReadRuntimeSchema, projection.EnsureReadRollbackSchema} {
			if e := fn(context.Background(), s.db); e != nil {
				t.Fatal(e)
			}
		}
	})
	b, _ := c3Forward(t, f)
	hold.Store(true)
	response := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, "http://"+b.addr+"/api/v1/search/tickets?sort=id&order=asc", nil)
		req.Header.Set("Authorization", "Bearer local-supervisor")
		res, e := http.DefaultClient.Do(req)
		if e == nil {
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}
		response <- e
	}()
	select {
	case <-requestEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("B in-flight request never reached ES")
	}
	c3Plan(t, f, "inflight_reverse")
	if _, e := f.c.ObserveReadDrain(context.Background(), "inflight_reverse"); e == nil {
		t.Fatal("live request considered drained")
	}
	b.stop(t, true)
	hold.Store(false)
	close(release)
	select {
	case <-response:
	case <-time.After(5 * time.Second):
		t.Fatal("killed request remained")
	}
	c3Proof(t, f, "inflight_reverse")
	f.drain(t, "inflight_reverse")
	f.commit(t, "inflight_reverse")
	active := mustVersion(t, f, "active_original")
	a := f.startAPI(t, "active_original", false, nil, map[string]string{"TICKETPULSE_ES_URL": active.Contract.ES.Endpoint, "TICKETPULSE_CK_URL": active.Contract.CK.Endpoint})
	a.ready(t)
	f.observe(t, "inflight_reverse", a)
	f.resume(t, "inflight_reverse", a)
	c3Save(t, "inflight-SIGKILL-drained", map[string]any{"B_process": b.identity, "actual_exit": true, "selection": mustSelection(t, f.x.f.db), "A_B": c3Both(t, f)})
}
func c3Gold(t *testing.T, raw []byte, newPending int64) {
	t.Helper()
	var actual analytics.Result
	if err := json.Unmarshal(raw, &actual); err != nil {
		t.Fatal(err)
	}
	data, e := os.ReadFile("../../tests/analytics/prd_gold.json")
	if e != nil {
		t.Fatal(e)
	}
	var gold struct {
		Daily    []analytics.Day    `json:"daily"`
		Overview analytics.Overview `json:"overview"`
	}
	if e = json.Unmarshal(data, &gold); e != nil {
		t.Fatal(e)
	}
	gold.Overview.Backlog += newPending
	if !reflect.DeepEqual(actual.Daily, gold.Daily) || !reflect.DeepEqual(actual.Overview, gold.Overview) {
		t.Fatalf("Gold plus known new pending differs: %s", raw)
	}
}
