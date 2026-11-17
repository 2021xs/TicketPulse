//go:build integration && m2 && m3 && m4 && m403b && m403c1

package mysqlstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kadm"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/syncpipe"
)

func m403c1Owned(t *testing.T) {
	t.Helper()
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03c1" && os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03c2" && os.Getenv("TICKETPULSE_ISOLATED_SUITE") != "m4-03c3" {
		t.Fatal("C1 requires the new private instance")
	}
	m403bOwned(t)
}
func m403c1Save(t *testing.T, name string, v any) {
	t.Helper()
	dir := os.Getenv("TICKETPULSE_M403C1_EVIDENCE_DIR")
	if dir == "" {
		t.Fatal("C1 evidence directory required")
	}
	dir = filepath.Join(dir, t.Name())
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	file, e := os.OpenFile(filepath.Join(dir, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if e = enc.Encode(v); e != nil {
		t.Fatal(e)
	}
}
func m403c1Control(x *m403bFixture) *syncpipe.ReadSwitchControl {
	return &syncpipe.ReadSwitchControl{ProjectionControl: &syncpipe.ProjectionControl{Store: x.f.state, Queue: x.f.queue, ES: x.f.es, CK: x.f.ck}}
}
func m403c1Install(t *testing.T, x *m403bFixture) *syncpipe.ReadSwitchControl {
	t.Helper()
	if e := projection.EnsureReadControlSchema(context.Background(), x.f.db); e != nil {
		t.Fatal(e)
	}
	c := m403c1Control(x)
	s, e := c.InitializeReadSelection(context.Background(), "active_original")
	if e != nil || s.VersionID != "active_original" || s.ReadRevision != 1 || s.HandoffID != nil {
		t.Fatalf("initial=%+v err=%v", s, e)
	}
	return c
}
func m403c1Verified(t *testing.T) (*m403bFixture, *syncpipe.ReadSwitchControl) {
	t.Helper()
	m403c1Owned(t)
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	x.run(t, "es")
	x.run(t, "ck")
	x.verify(t)
	return x, m403c1Install(t, x)
}
func m403c1Plan(t *testing.T, c *syncpipe.ReadSwitchControl, id string) projection.ReadSwitch {
	t.Helper()
	o, e := c.BeginReadSwitch(context.Background(), id, projection.ReadPromote, "candidate_v1", 1)
	if e != nil || o.Stage != projection.ReadPlanned {
		t.Fatalf("plan=%+v err=%v", o, e)
	}
	return o
}
func m403c1Validate(t *testing.T, c *syncpipe.ReadSwitchControl, id string) projection.ReadSwitch {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	o, e := c.ValidateReadSwitchDestination(ctx, id)
	if e != nil || o.ProofSHA256 == nil || o.Stage != projection.ReadPlanned {
		t.Fatalf("proof=%+v err=%v", o, e)
	}
	return o
}
func m403c1AssertStillOriginal(t *testing.T, x *m403bFixture) {
	t.Helper()
	s, e := (&projection.ReadControlStore{DB: x.f.db}).Selection(context.Background())
	if e != nil || s.VersionID != "active_original" || s.ReadRevision != 1 {
		t.Fatal("C1 changed read selection", s, e)
	}
	active, e := (&projection.VersionStore{DB: x.f.db}).Get(context.Background(), "active_original")
	candidate, e2 := (&projection.VersionStore{DB: x.f.db}).Get(context.Background(), x.version.ID)
	if e != nil || e2 != nil || active.Phase != projection.Active || candidate.Phase != projection.Prepared || candidate.Fingerprint() != x.version.Fingerprint() {
		t.Fatal("C1 changed registration contracts")
	}
	h, e := (&projection.HandoffStore{DB: x.f.db}).Get(context.Background(), x.h.ID)
	if e != nil || !reflect.DeepEqual(h, x.h) {
		t.Fatal("C1 changed B handoff", e)
	}
}

func TestM403C1ExplicitInstallInitializeAndResolve(t *testing.T) {
	m403c1Owned(t)
	x := m403bNew(t)
	c := m403c1Control(x)
	if _, e := c.InitializeReadSelection(context.Background(), "active_original"); e == nil {
		t.Fatal("initialized without 013")
	}
	if e := projection.EnsureReadControlSchema(context.Background(), x.f.db); e != nil {
		t.Fatal(e)
	}
	enabled, e := projection.ReadControlEnabled(context.Background(), x.f.db)
	if e != nil || enabled {
		t.Fatal("install incorrectly enables read selection")
	}
	if _, e = c.InitializeReadSelection(context.Background(), x.version.ID); e == nil {
		t.Fatal("candidate used as initial selection")
	}
	c = m403c1Install(t, x)
	before, e := c.InitializeReadSelection(context.Background(), "active_original")
	if e != nil {
		t.Fatal(e)
	}
	again, e := c.InitializeReadSelection(context.Background(), "active_original")
	if e != nil || !reflect.DeepEqual(before, again) {
		t.Fatal("initialization is not idempotent", e)
	}
	legacy, e := c.ResolveReadDestination(context.Background(), "active_original")
	if e != nil || legacy.StateNamespace != "legacy" || legacy.Groups["es"] != x.f.queue.Group("es") {
		t.Fatal(legacy, e)
	}
	if _, e = c.ResolveReadDestination(context.Background(), x.version.ID); e == nil {
		t.Fatal("unverified B accepted as read destination")
	}
	x.pin(t)
	x.open(t)
	x.run(t, "es")
	x.run(t, "ck")
	x.verify(t)
	d, e := c.ResolveReadDestination(context.Background(), x.version.ID)
	if e != nil || d.Handoff == nil || d.StateNamespace != x.version.ID || d.Groups["es"] == legacy.Groups["es"] {
		t.Fatal(d, e)
	}
	m403c1AssertStillOriginal(t, x)
	m403c1Save(t, "install-initialize-and-targets", map[string]any{"initial": before, "legacy": legacy, "candidate": d})
}

func TestM403C1PlannedCASResumeAndExplicitCancellation(t *testing.T) {
	x, c := m403c1Verified(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := []projection.ReadSwitch{}
	errors := []string{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o, e := c.BeginReadSwitch(context.Background(), fmt.Sprintf("attempt_%d", i), projection.ReadPromote, x.version.ID, 1)
			mu.Lock()
			defer mu.Unlock()
			if e == nil {
				successes = append(successes, o)
			} else {
				errors = append(errors, e.Error())
			}
		}(i)
	}
	wg.Wait()
	if len(successes) != 1 {
		t.Fatal("concurrent intents admitted", len(successes), errors)
	}
	o := successes[0]
	same, e := c.BeginReadSwitch(context.Background(), o.ID, projection.ReadPromote, x.version.ID, 1)
	if e != nil || same.Revision != o.Revision {
		t.Fatal("same intent retry differs", e)
	}
	if _, e = c.BeginReadSwitch(context.Background(), o.ID, projection.ReadRollback, x.version.ID, 1); e == nil {
		t.Fatal("changed direction reused operation ID")
	}
	if _, e = projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e == nil || !strings.Contains(e.Error(), "READ_SWITCH") {
		t.Fatal("PLANNED resume allowed", e)
	}
	status, e := c.ReadSwitchStatus(context.Background(), o.ID)
	if e != nil || status.Selection.CurrentOperationID == nil {
		t.Fatal(status, e)
	}
	cancelled, e := c.CancelPlannedReadSwitch(context.Background(), o.ID)
	if e != nil || cancelled.Stage != projection.ReadCancelled {
		t.Fatal(cancelled, e)
	}
	retry, e := c.CancelPlannedReadSwitch(context.Background(), o.ID)
	if e != nil || retry.Revision != cancelled.Revision {
		t.Fatal("cancel not idempotent", e)
	}
	// An earlier cancellation log cannot hide a later manual stage rewrite.
	x.f.exec(t, "UPDATE sync_projection_read_switches SET direction='ROLLBACK' WHERE operation_id='"+o.ID+"'")
	if _, e = projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e == nil {
		t.Fatal("modified terminal intent matched obsolete cancel log")
	}
	x.f.exec(t, "UPDATE sync_projection_read_switches SET direction='PROMOTE' WHERE operation_id='"+o.ID+"'")
	s, e := (&projection.ReadControlStore{DB: x.f.db}).Selection(context.Background())
	if e != nil || s.CurrentOperationID != nil || s.ReadRevision != 1 {
		t.Fatal(s, e)
	}
	g, e := projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID)
	if e != nil || g.State != "OPEN" {
		t.Fatal("safe cancellation did not permit original resume", e)
	}
	if _, e = c.CancelPlannedReadSwitch(context.Background(), o.ID); e == nil {
		t.Fatal("old cancellation accepted after freeze ended")
	}
	m403c1AssertStillOriginal(t, x)
	m403c1Save(t, "planned-cas-cancelled", map[string]any{"winner": o, "losers": errors, "status": status, "cancelled": cancelled, "gate_after_resume": g})
}

func TestM403C1NewFreezeExactMarkerAfterVerifiedWritesAndGold(t *testing.T) {
	x, c := m403c1Verified(t)
	old := x.h
	if _, e := projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e != nil {
		t.Fatal(e)
	}
	x.f.actionHTTP(t, "local-device-agent", "comments", 4, 10, map[string]any{"content": "C1 write after B VERIFIED"})
	g, e := projection.Freeze(context.Background(), x.f.db, "C1 fresh promotion proof")
	if e != nil {
		t.Fatal(e)
	}
	x.gate = g
	if g.FreezeID == nil || *g.FreezeID == old.FreezeID || g.Revision == old.FreezeRevision {
		t.Fatal("freeze was not new")
	}
	o := m403c1Plan(t, c, "fresh_after_business")
	if o.ProofSHA256 != nil {
		t.Fatal("old VERIFIED became fresh proof")
	}
	verified, e := x.control.Verify(context.Background(), old.ID)
	if e != nil || verified.VerificationMarker == nil || *verified.VerificationMarker != *old.VerificationMarker {
		t.Fatal("B terminal observation changed", e)
	}
	o = m403c1Validate(t, c, o.ID)
	var proof syncpipe.ReadSwitchProof
	if e = json.Unmarshal(o.Proof, &proof); e != nil {
		t.Fatal(e)
	}
	if proof.MarkerID == *old.VerificationMarker || proof.FreezeID != *g.FreezeID || proof.Report.Source.Tickets != 10 || proof.Report.Source.Histories != 47 || proof.Report.State != "consistent" {
		t.Fatal("proof used historical boundary", proof)
	}
	for _, component := range []string{"es", "ck"} {
		if len(proof.MarkerRecords[component]) != 3 {
			t.Fatal("not all partitions")
		}
		for i, record := range proof.MarkerRecords[component] {
			if proof.Before[component].Progress[i].Committed <= record.RecordOffset {
				t.Fatal("marker not actually committed")
			}
		}
	}
	m4AssertProjection(t, x.f, x.job)
	gold := m4AssertGold(t, x.f, x.job)
	m403c1AssertStillOriginal(t, x)
	firstMarker := *o.MarkerID
	again := m403c1Validate(t, c, o.ID)
	if *again.MarkerID != firstMarker || again.Revision <= o.Revision {
		t.Fatal("retry invented a marker or reused stale proof")
	}
	if _, e = projection.Resume(context.Background(), x.f.db, *g.FreezeID); e == nil {
		t.Fatal("proof wrongly allowed business resume")
	}
	m403c1Save(t, "fresh-proof", o)
	m403c1Save(t, "repeat-live-proof", again)
	m403c1Save(t, "old-B-proof", old)
	m403c1Save(t, "gold", gold)
}

func TestM403C1MissingCandidateAndLegacyOffsetsRejected(t *testing.T) {
	m403c1Owned(t)
	x := m403bNew(t)
	x.pin(t)
	x.open(t)
	x.run(t, "ck")
	worker := x.process(t, "es")
	m403bWait(t, "candidate ES process running", func() bool {
		var n int
		x.f.db.QueryRow("SELECT COUNT(*) FROM sync_projection_worker_state WHERE version_id=? AND component='es' AND state='running'", x.version.ID).Scan(&n)
		return n == 1
	})
	x.verify(t)
	c := m403c1Install(t, x)
	o := m403c1Plan(t, c, "missing_offset")
	raw, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
	if e != nil {
		t.Fatal(e)
	}
	worker.once.Do(func() {
		worker.cmd.Process.Signal(os.Interrupt)
		select {
		case <-worker.done:
		case <-time.After(5 * time.Second):
			worker.cmd.Process.Kill()
			<-worker.done
		}
	})
	m403bWait(t, "candidate group empty", func() bool {
		groups, e := x.f.queue.Admin.DescribeGroups(context.Background(), x.h.ESGroup)
		return e == nil && len(groups[x.h.ESGroup].Members) == 0
	})
	response, e := x.f.queue.Admin.DeleteOffsets(context.Background(), x.h.ESGroup, kadm.TopicsSet{x.f.queue.Topic: {1: {}}})
	if e != nil || response.Error() != nil {
		t.Fatal("private offset deletion rejected", e, response.Error())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	_, e = c.ValidateReadSwitchDestination(ctx, o.ID)
	cancel()
	if e == nil || !strings.Contains(e.Error(), "READ_OFFSET_MISSING") {
		t.Fatal("candidate missing offset accepted", e)
	}
	candidateError := e.Error()
	// Read-only check of a unique empty private group exercises legacy fallback refusal.
	legacy, e := c.ResolveReadDestination(context.Background(), "active_original")
	if e != nil {
		t.Fatal(e)
	}
	legacy.Groups["es"] = x.f.queue.Topic + ".es.c1_missing"
	if _, _, e = legacy.RawOffsetProgress(context.Background(), "es"); e == nil || !strings.Contains(e.Error(), "READ_OFFSET_MISSING") {
		t.Fatal("empty group silently fell back", e)
	}
	m403c1Save(t, "missing-offset-refused", map[string]any{"operation": o, "before": raw, "delete_response": response, "candidate_error": candidateError, "legacy_error": e.Error()})
}

func TestM403C1OnePartitionCannotConfirmMarker(t *testing.T) {
	x, c := m403c1Verified(t)
	o := m403c1Plan(t, c, "partial_marker")
	// Only this new random schema: reject marker progress on ES partition 2.
	trigger := `CREATE TRIGGER c1_hold_marker BEFORE INSERT ON sync_projection_deliveries FOR EACH ROW BEGIN IF NEW.version_id='candidate_v1' AND NEW.component='es' AND NEW.partition_id=2 AND NEW.ticket_id IS NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='C1 isolated partition delay'; END IF; END`
	if _, e := x.f.db.Exec(trigger); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	_, e := c.ValidateReadSwitchDestination(ctx, o.ID)
	cancel()
	if e == nil {
		t.Fatal("partial marker accepted")
	}
	firstError := e.Error()
	saved, e := (&projection.ReadControlStore{DB: x.f.db}).Operation(context.Background(), o.ID)
	if e != nil || saved.MarkerID == nil || saved.ProofSHA256 != nil {
		t.Fatal("partial proof persisted", saved, e)
	}
	var confirmed, missing int
	x.f.db.QueryRow(`SELECT COUNT(DISTINCT partition_id) FROM sync_projection_deliveries WHERE version_id=? AND component='es' AND event_id=? AND status='applied'`, x.version.ID, *saved.MarkerID).Scan(&confirmed)
	x.f.db.QueryRow(`SELECT COUNT(*) FROM sync_projection_deliveries WHERE version_id=? AND component='es' AND event_id=? AND partition_id=2 AND status='applied'`, x.version.ID, *saved.MarkerID).Scan(&missing)
	if confirmed != 2 || missing != 0 {
		t.Fatal("fault did not isolate exactly one partition", confirmed, missing)
	}
	if _, e = x.f.db.Exec("DROP TRIGGER c1_hold_marker"); e != nil {
		t.Fatal(e)
	}
	x.run(t, "es")
	recovered := m403c1Validate(t, c, o.ID)
	if *recovered.MarkerID != *saved.MarkerID {
		t.Fatal("recovery invented marker")
	}
	m403c1Save(t, "partial-marker-refused-and-recovered", map[string]any{"error": firstError, "partial": saved, "ES_confirmed_partitions": confirmed, "missing_partition_applied": missing, "recovered": recovered})
}

func TestM403C1LagZeroIsolationAndFailedProofInvalidation(t *testing.T) {
	x, c := m403c1Verified(t)
	o := m403c1Plan(t, c, "isolation_guard")
	good := m403c1Validate(t, c, o.ID)
	// A real malformed private Kafka message creates terminal isolation.
	ref, e := x.f.queue.PublishBytes(context.Background(), []byte(`{"schema":2,"kind":"ticket","source":"snapshot","event_id":"c1-invalid-proof","ticket_id":4,"version":99,"event_at":"2026-11-01T00:00:00Z","document":{}}`), "c1-invalid-proof")
	if e != nil {
		t.Fatal(e)
	}
	m403bWait(t, "real isolated candidate record committed", func() bool {
		var n int
		x.f.db.QueryRow("SELECT COUNT(*) FROM sync_projection_deliveries WHERE version_id=? AND component='es' AND topic=? AND partition_id=? AND record_offset=? AND status='isolated'", x.version.ID, ref.Topic, ref.Partition, ref.Offset).Scan(&n)
		raw, e := x.f.queue.Admin.FetchOffsets(context.Background(), x.h.ESGroup)
		offset, ok := raw.Lookup(ref.Topic, ref.Partition)
		return e == nil && ok && offset.At > ref.Offset && n == 1
	})
	d, e := c.ResolveReadDestination(context.Background(), x.version.ID)
	if e != nil {
		t.Fatal(e)
	}
	raw, positions, e := d.RawOffsetProgress(context.Background(), "es")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, e = c.ValidateReadSwitchDestination(ctx, o.ID)
	cancel()
	if e == nil || !strings.Contains(e.Error(), "READ_UNRESOLVED_DELIVERIES") {
		t.Fatal("isolated lag=0 accepted", e)
	}
	failed, e2 := (&projection.ReadControlStore{DB: x.f.db}).Operation(context.Background(), o.ID)
	if e2 != nil || failed.ProofSHA256 != nil || failed.LastError == nil || *failed.MarkerID != *good.MarkerID {
		t.Fatal("failed revalidation retained unconditional proof", failed, e2)
	}
	m403c1Save(t, "isolation-refusal", map[string]any{"before_good": good, "failed": failed, "positions": positions, "raw": raw, "isolated_record": ref, "error": e.Error()})
}

func TestM403C1MetadataLossAndCancelMismatchFailClosed(t *testing.T) {
	x, c := m403c1Verified(t)
	o := m403c1Plan(t, c, "metadata_guard")
	_, e := x.f.db.Exec("UPDATE sync_business_write_gate SET revision=revision+1 WHERE gate_id=1")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.CancelPlannedReadSwitch(context.Background(), o.ID); e == nil {
		t.Fatal("cancel accepted mismatched freeze")
	}
	if _, e = projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e == nil {
		t.Fatal("resume ignored freeze mismatch")
	}
	_, e = x.f.db.Exec("UPDATE sync_business_write_gate SET revision=revision-1 WHERE gate_id=1")
	if e != nil {
		t.Fatal(e)
	}
	_, e = x.f.db.Exec("DELETE FROM sync_projection_read_switches WHERE operation_id=?", o.ID)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e == nil {
		t.Fatal("missing referenced operation allowed Resume")
	}
	_, e = x.f.db.Exec("DELETE FROM sync_projection_read_selection")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.ReadSwitchStatus(context.Background(), ""); e == nil {
		t.Fatal("enabled missing selector downgraded")
	}
	if _, e = projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e == nil {
		t.Fatal("enabled missing selector allowed Resume")
	}
	m403c1Save(t, "metadata-refused", map[string]any{"operation_before": o, "last_resume_error": e.Error()})
}

func TestM403C1ReadProofCrashChild(t *testing.T) {
	stage := os.Getenv("TICKETPULSE_C1_CHILD_STAGE")
	if stage == "" {
		t.Skip("child-only helper")
	}
	m403c1Owned(t)
	cfg, e := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if e != nil {
		t.Fatal(e)
	}
	cfg.DBName = os.Getenv("TICKETPULSE_C1_CHILD_SCHEMA")
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	if !strings.HasPrefix(cfg.DBName, "ticketpulse_test_") {
		t.Fatal("child requires owned random schema")
	}
	db, e := sql.Open("mysql", cfg.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var uuid string
	if e = db.QueryRow("SELECT @@server_uuid").Scan(&uuid); e != nil || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
		t.Fatal("child source mismatch")
	}
	q, e := syncpipe.ConnectStatusQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	c := &syncpipe.ReadSwitchControl{ProjectionControl: &syncpipe.ProjectionControl{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: q}}
	c.Observe = func(actual string, v any) error {
		if actual == stage {
			data, _ := json.Marshal(v)
			if e := os.WriteFile(os.Getenv("TICKETPULSE_C1_CHILD_READY"), data, 0600); e != nil {
				return e
			}
			select {}
		}
		return nil
	}
	_, e = c.ValidateReadSwitchDestination(context.Background(), os.Getenv("TICKETPULSE_C1_CHILD_OPERATION"))
	if e != nil {
		t.Fatal(e)
	}
}
func m403c1Crash(t *testing.T, x *m403bFixture, operation, stage string) {
	t.Helper()
	dir := filepath.Join(os.Getenv("TICKETPULSE_M403C1_EVIDENCE_DIR"), t.Name())
	os.MkdirAll(dir, 0700)
	ready := filepath.Join(dir, stage+".ready.json")
	log, e := os.OpenFile(filepath.Join(dir, stage+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer log.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestM403C1ReadProofCrashChild$", "-test.v")
	cmd.Env = append(os.Environ(), "TICKETPULSE_C1_CHILD_STAGE="+stage, "TICKETPULSE_C1_CHILD_SCHEMA="+x.f.name, "TICKETPULSE_C1_CHILD_READY="+ready, "TICKETPULSE_C1_CHILD_OPERATION="+operation)
	cmd.Stdout = log
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer cmd.Process.Kill()
	m403bWait(t, "C1 crash boundary "+stage, func() bool { _, e := os.Stat(ready); return e == nil })
	if e = cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	if e = cmd.Wait(); e == nil {
		t.Fatal("SIGKILL returned success")
	}
}
func TestM403C1MarkerAndProofSIGKILLRecovery(t *testing.T) {
	x, c := m403c1Verified(t)
	o := m403c1Plan(t, c, "crash_boundaries")
	for _, stage := range []string{"before_marker_commit", "after_marker_commit", "before_proof_save", "before_proof_commit", "after_proof_commit"} {
		m403c1Crash(t, x, o.ID, stage)
		saved, e := (&projection.ReadControlStore{DB: x.f.db}).Operation(context.Background(), o.ID)
		if e != nil {
			t.Fatal(e)
		}
		if stage == "before_marker_commit" && saved.MarkerID != nil {
			t.Fatal("uncommitted marker survived")
		}
		if stage != "before_marker_commit" && saved.MarkerID == nil {
			t.Fatal("committed marker lost")
		}
		if stage == "after_proof_commit" && saved.ProofSHA256 == nil {
			t.Fatal("committed proof lost")
		}
		if _, e = projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e == nil {
			t.Fatal("crash removed durable PLANNED")
		}
		m403c1Save(t, stage+"-persistent", saved)
	}
	saved := m403c1Validate(t, c, o.ID)
	m403c1AssertStillOriginal(t, x)
	m403c1Save(t, "recovered-proof", saved)
}

func TestM403C1UnenabledSchemaKeepsOriginalResume(t *testing.T) {
	m403c1Owned(t)
	x := m403bNew(t)
	// This test uses no B handoff and deliberately leaves 013 unenabled.
	for _, installed := range []bool{false, true} {
		if installed {
			if e := projection.EnsureReadControlSchema(context.Background(), x.f.db); e != nil {
				t.Fatal(e)
			}
		}
		g, e := projection.ReadGate(context.Background(), x.f.db)
		if e != nil {
			t.Fatal(e)
		}
		if g.State != "FROZEN" {
			g, e = projection.Freeze(context.Background(), x.f.db, "unenabled compatibility")
			if e != nil {
				t.Fatal(e)
			}
		}
		resumed, e := projection.Resume(context.Background(), x.f.db, *g.FreezeID)
		if e != nil || resumed.State != "OPEN" {
			t.Fatal("unenabled 013 broke original resume", e)
		}
	}
	m403c1Save(t, "unenabled-compatible", map[string]any{"013_initialized": false, "original_resume": true})
}

func TestM403C1TargetUUIDAndPartitionChangesRejectProof(t *testing.T) {
	for _, kind := range []string{"es_uuid", "ck_uuid", "ck_database_uuid", "topic_uuid", "partitions"} {
		t.Run(kind, func(t *testing.T) {
			x, c := m403c1Verified(t)
			o := m403c1Plan(t, c, "identity_guard")
			before := x.version.Contract
			switch kind {
			case "es_uuid":
				m4HTTP(t, "DELETE", x.control.ES.Endpoint()+"/"+x.control.ES.Index(), nil, 200)
				if e := x.control.ES.EnsureIndex(context.Background()); e != nil {
					t.Fatal(e)
				}
				after, _, e := x.control.ES.Identity(context.Background())
				if e != nil || after == before.ES.UUID {
					t.Fatal("ES UUID not changed", e)
				}
			case "ck_uuid":
				m4CKSQL(t, x.control.CK, "RENAME TABLE ticket_states TO ticket_states_c1_retired")
				if e := x.control.CK.Ensure(context.Background()); e != nil {
					t.Fatal(e)
				}
				after, _, e := x.control.CK.Identity(context.Background())
				if e != nil || after.States == before.CK.StatesUUID {
					t.Fatal("CK UUID not changed", e)
				}
			case "partitions":
				reply, e := x.f.queue.Admin.CreatePartitions(context.Background(), 4, x.f.queue.Topic)
				if e != nil || reply.Error() != nil {
					t.Fatal(e, reply.Error())
				}
			case "ck_database_uuid":
				// Move the two tables through a private temporary Atomic database,
				// preserving table UUIDs while replacing only database identity.
				db := x.control.CK.Database()
				temporary := db + "_c1_uuid"
				for _, statement := range []string{"CREATE DATABASE " + temporary + " ENGINE=Atomic", "RENAME TABLE " + db + ".ticket_events TO " + temporary + ".ticket_events, " + db + ".ticket_states TO " + temporary + ".ticket_states", "DROP DATABASE " + db, "CREATE DATABASE " + db + " ENGINE=Atomic", "RENAME TABLE " + temporary + ".ticket_events TO " + db + ".ticket_events, " + temporary + ".ticket_states TO " + db + ".ticket_states", "DROP DATABASE " + temporary} {
					if _, e := x.control.CK.QueryGlobal(context.Background(), statement); e != nil {
						t.Fatal(e)
					}
				}
				after, exists, e := x.control.CK.Identity(context.Background())
				if e != nil || !exists || after.Events != before.CK.EventsUUID || after.States != before.CK.StatesUUID {
					t.Fatal("database UUID fault changed table identities", e)
				}
			case "topic_uuid":
				x.f.stopCapture(t)
				x.f.stopConsumer(t)
				x.f.stopCK(t)
				reply, e := x.f.queue.Admin.DeleteTopics(context.Background(), x.f.queue.Topic)
				if e != nil || reply[x.f.queue.Topic].Err != nil {
					t.Fatal(e)
				}
				m403bWait(t, "C1 private Topic deletion", func() bool {
					topics, e := x.f.queue.Admin.ListTopics(context.Background(), x.f.queue.Topic)
					return e == nil && (len(topics) == 0 || topics[x.f.queue.Topic].Err != nil)
				})
				created, e := x.f.queue.Admin.CreateTopics(context.Background(), 3, 1, map[string]*string{"cleanup.policy": kadm.StringPtr("delete")}, x.f.queue.Topic)
				if e != nil || created[x.f.queue.Topic].Err != nil {
					t.Fatal(e)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, e := c.ValidateReadSwitchDestination(ctx, o.ID)
			cancel()
			if e == nil {
				t.Fatal("changed identity accepted")
			}
			saved, e2 := (&projection.ReadControlStore{DB: x.f.db}).Operation(context.Background(), o.ID)
			if e2 != nil || saved.ProofSHA256 != nil || saved.MarkerID != nil {
				t.Fatal("invalid identity published proof/marker", saved, e2)
			}
			m403c1Save(t, "identity-refusal", map[string]any{"kind": kind, "prior_contract": before, "error": e.Error(), "operation": saved})
		})
	}
}

func TestM403C1ManualTerminalMutationDoesNotPermitResume(t *testing.T) {
	x, c := m403c1Verified(t)
	o := m403c1Plan(t, c, "terminal_tamper")
	// Fault injection in a new private schema: stage edits do not substitute
	// for the transactional cancellation audit and selector CAS.
	x.f.exec(t, "UPDATE sync_projection_read_switches SET stage='CANCELLED',cancelled_at=UTC_TIMESTAMP(6) WHERE operation_id='"+o.ID+"'")
	x.f.exec(t, "UPDATE sync_projection_read_selection SET current_operation_id=NULL")
	if _, e := projection.Resume(context.Background(), x.f.db, *x.gate.FreezeID); e == nil {
		t.Fatal("manual stage mutation enabled Resume")
	} else {
		m403c1Save(t, "terminal-edit-refused", map[string]any{"error": e.Error(), "operation_before": o})
	}
}

func TestM403C1OperatorCLIPlansProvesCancelsWithoutPromotion(t *testing.T) {
	x, c := m403c1Verified(t)
	cfg, e := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if e != nil {
		t.Fatal(e)
	}
	cfg.DBName = x.f.name
	outputs := map[string]string{}
	cli := func(label string, args ...string) {
		cmd := exec.Command(os.Getenv("TICKETPULSE_CONTROL_BINARY"), args...)
		cmd.Env = append(os.Environ(), "TICKETPULSE_CONTROL_DSN="+cfg.FormatDSN(), "TICKETPULSE_SEARCH_INDEX="+x.f.es.Index(), "TICKETPULSE_CK_DATABASE="+x.f.ck.Database())
		data, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("%s CLI: %v %s", label, e, data)
		}
		outputs[label] = string(data)
	}
	cli("migration", "-mode=migrate-read-control")
	cli("init", "-mode=read-init", "-version=active_original")
	cli("plan", "-mode=read-begin", "-operation=operator_plan", "-version=candidate_v1", "-read-revision=1")
	cli("proof", "-mode=read-validate", "-operation=operator_plan", "-wait=30s")
	cli("status", "-mode=read-status", "-operation=operator_plan")
	cli("cancel", "-mode=read-cancel", "-operation=operator_plan")
	status, e := c.ReadSwitchStatus(context.Background(), "operator_plan")
	if e != nil || status.Operation.Stage != projection.ReadCancelled || status.Selection.ReadRevision != 1 {
		t.Fatal(status, e)
	}
	m403c1AssertStillOriginal(t, x)
	m403c1Save(t, "CLI", outputs)
}
