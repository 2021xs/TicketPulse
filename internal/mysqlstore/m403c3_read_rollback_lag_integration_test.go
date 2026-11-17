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
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ticketpulse/internal/projection"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

// Fail only the new fixture's legacy document writes. All successful requests
// still reach the real Elasticsearch; the candidate keeps consuming normally.
func TestM403C3TemporaryLegacyLagRequiresNewProofAndIdempotentResume(t *testing.T) {
	var blockLegacy atomic.Bool
	var failedWrites atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blockLegacy.Load() && r.Method == http.MethodPut && strings.Contains(r.URL.Path, "_tickets_v1/_doc/") {
			failedWrites.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `{"error":{"type":"private_c3_legacy_write_unavailable"}}`)
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, os.Getenv("TICKETPULSE_ES_URL")+r.URL.RequestURI(), r.Body)
		if err != nil {
			http.Error(w, "private proxy request failed", http.StatusBadGateway)
			return
		}
		req.Header = r.Header.Clone()
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, "private upstream unavailable", http.StatusBadGateway)
			return
		}
		defer res.Body.Close()
		for name, values := range res.Header {
			w.Header()[name] = values
		}
		w.WriteHeader(res.StatusCode)
		io.Copy(w, res.Body)
	}))
	defer proxy.Close()
	f := m403c2NewAt(t, proxy.URL, func(s *m3fixture) {
		for _, install := range []func(context.Context, *sql.DB) error{projection.EnsureReadControlSchema, projection.EnsureReadRuntimeSchema, projection.EnsureReadRollbackSchema} {
			if err := install(context.Background(), s.db); err != nil {
				t.Fatal(err)
			}
		}
	})
	b, _ := c3Forward(t, f)
	before := f.progress(t)

	// A prior frozen proof is cancelled before B admits newer business writes.
	// It must never serve as permission for the subsequent rollback operation.
	c3Plan(t, f, "old_rollback_proof")
	oldProof := c3Proof(t, f, "old_rollback_proof")
	if _, err := f.c.CancelPlannedReadSwitch(context.Background(), oldProof.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.ResumeCancelledReadSwitch(context.Background(), oldProof.ID, oldProof.FreezeID, f.probe(b)); err != nil {
		t.Fatal(err)
	}

	blockLegacy.Store(true)
	defer blockLegacy.Store(false)
	created, _ := f.http(t, b, http.MethodPost, "/api/v1/tickets", "local-alice", request("c3_temporary_legacy_lag"), 201)
	var made ticket.CreateResult
	if err := json.Unmarshal(created, &made); err != nil {
		t.Fatal(err)
	}
	f.http(t, b, http.MethodPost, "/api/v1/tickets/comments", "local-alice", ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: made.Ticket.ID, ExpectedVersion: 1}, Content: "B remains usable while A is retrying"}, 200)
	f.http(t, b, http.MethodPost, "/api/v1/tickets/transfer-team", "local-supervisor", ticket.TeamTransferRequest{ActionRequest: ticket.ActionRequest{TicketID: made.Ticket.ID, ExpectedVersion: 2}, TeamID: 20, Reason: "private delayed legacy projection"}, 200)
	m403bWait(t, "real legacy ES document write failed", func() bool { return failedWrites.Load() > 0 })
	var candidate syncpipe.ReconcileReport
	m403bWait(t, "candidate contains all newer business facts", func() bool {
		r, err := syncpipe.Reconcile(context.Background(), f.x.f.state, nil, f.x.control.ES, f.x.control.CK, syncpipe.ReconcileOptions{BatchSize: 250, MetricsOrganizationID: 1, MetricsRequest: m4Window})
		if err != nil || r.State != syncpipe.ReconcileConsistent {
			return false
		}
		candidate = r
		return true
	})
	legacyRaw, err := f.x.f.queue.Admin.FetchOffsets(context.Background(), f.x.f.queue.Group("es"))
	if err != nil {
		t.Fatal(err)
	}
	ends, err := f.x.f.queue.Admin.ListEndOffsets(context.Background(), f.x.f.queue.Topic)
	if err != nil {
		t.Fatal(err)
	}
	lagged := false
	for part := int32(0); part < f.x.f.queue.Partitions; part++ {
		offset, exists := legacyRaw.Lookup(f.x.f.queue.Topic, part)
		end, found := ends.Lookup(f.x.f.queue.Topic, part)
		if !exists || !found || offset.Err != nil || end.Err != nil {
			t.Fatal("real legacy progress unavailable")
		}
		lagged = lagged || offset.At < end.Offset
	}
	if !lagged {
		t.Fatal("fault did not create real Kafka consumption lag")
	}
	legacy, err := syncpipe.Reconcile(context.Background(), f.x.f.state, nil, f.x.f.es, f.x.f.ck, syncpipe.ReconcileOptions{BatchSize: 250})
	if err != nil || legacy.DifferenceCount == 0 || legacy.State == syncpipe.ReconcileConsistent {
		t.Fatal("legacy projection was not actually stale", err, legacy.State)
	}
	f.http(t, b, http.MethodGet, "/api/v1/search/tickets?sort=id&order=asc&page_size=100", "local-supervisor", nil, 200)

	op := c3Plan(t, f, "lagged_rollback")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, proofErr := f.c.ValidateReadSwitchDestination(ctx, op.ID)
	cancel()
	if proofErr == nil {
		t.Fatal("stale A received a fresh rollback proof")
	}
	_, staleErr := f.c.CommitReadSelection(context.Background(), oldProof.ID)
	if staleErr == nil {
		t.Fatal("cancelled earlier proof committed a newer rollback")
	}
	_, revisionErr := f.c.BeginReadSwitch(context.Background(), "wrong_rollback_revision", projection.ReadRollback, "active_original", 1, "forward")
	if revisionErr == nil {
		t.Fatal("wrong expected read revision was accepted")
	}
	_, commitErr := f.c.CommitReadSelection(context.Background(), op.ID)
	if commitErr == nil {
		t.Fatal("unproved stale A was committed")
	}
	refused, err := f.c.ReadSwitchStatus(context.Background(), op.ID)
	if err != nil || refused.Selection.VersionID != "candidate_v1" || refused.Selection.ReadRevision != 2 || refused.Gate.State != "FROZEN" || refused.Operation.Stage != projection.ReadPlanned || refused.Operation.ProofSHA256 != nil {
		t.Fatal("failed proof changed the durable safe state", err, refused)
	}
	c3Save(t, "temporary-lag-refused", map[string]any{"candidate_reconcile": candidate, "legacy_reconcile": legacy, "raw_legacy_fetch_offsets": legacyRaw, "log_ends": ends, "failed_legacy_writes": failedWrites.Load(), "proof_error": proofErr.Error(), "commit_error": commitErr.Error(), "cancelled_proof_error": staleErr.Error(), "revision_error": revisionErr.Error(), "safe_state": refused})

	blockLegacy.Store(false)
	c3Both(t, f)
	proof := c3Proof(t, f, op.ID)
	if proof.MarkerID == nil || oldProof.MarkerID == nil || *proof.MarkerID == *oldProof.MarkerID || proof.FreezeID == oldProof.FreezeID {
		t.Fatal("rollback reused the earlier freeze/marker")
	}
	b.stop(t, false)
	f.drain(t, op.ID)
	f.commit(t, op.ID)
	for i := 0; i < 3; i++ {
		if _, err := f.c.CommitReadSelection(context.Background(), op.ID); err != nil {
			t.Fatal(err)
		}
	}
	active := mustVersion(t, f, "active_original")
	a := f.startAPI(t, "active_original", false, nil, map[string]string{"TICKETPULSE_ES_URL": active.Contract.ES.Endpoint, "TICKETPULSE_CK_URL": active.Contract.CK.Endpoint})
	a.ready(t)
	f.observe(t, op.ID, a)
	f.resume(t, op.ID, a)
	firstGate, err := projection.ReadGate(context.Background(), f.x.f.db)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		gate, err := f.c.ResumeReadSwitch(context.Background(), op.ID, proof.FreezeID, f.probe(a))
		if err != nil || gate.State != "OPEN" || gate.Revision != firstGate.Revision {
			t.Fatal("duplicate Resume changed the gate", err, gate)
		}
	}
	selection := mustSelection(t, f.x.f.db)
	if selection.VersionID != "active_original" || selection.ReadRevision != 3 {
		t.Fatal("retry added a read revision", selection)
	}
	f.http(t, a, http.MethodGet, fmt.Sprintf("/api/v1/tickets/%d", made.Ticket.ID), "local-it-agent", nil, 403)
	f.http(t, a, http.MethodGet, fmt.Sprintf("/api/v1/tickets/%d", made.Ticket.ID), "local-device-agent", nil, 200)
	f.http(t, a, http.MethodPost, "/api/v1/tickets/comments", "local-device-agent", ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: made.Ticket.ID, ExpectedVersion: 3}, Content: "A resumes after a newly verified rollback"}, 200)
	f.http(t, a, http.MethodPost, "/api/v1/tickets", "local-alice", request("c3_after_legacy_lag_recovery"), 201)
	reports := c3Both(t, f)
	gold, _ := f.http(t, a, http.MethodGet, "/api/v1/analytics?start=2026-11-01&end=2026-11-04", "local-supervisor", nil, 200)
	c3Gold(t, gold, 2)
	status, err := f.c.ReadSwitchStatus(context.Background(), op.ID)
	if err != nil || status.Operation.Stage != projection.ReadResumed {
		t.Fatal(err, status)
	}
	c3Save(t, "temporary-lag-recovered", map[string]any{"old_proof": oldProof, "new_proof": proof, "selection": selection, "gate": firstGate, "A_B": reports, "progress_before": before, "progress_after": f.progress(t), "gold": json.RawMessage(gold), "status": status, "duplicate_commit_retries": 3, "duplicate_resume_retries": 3, "revisions": []int{1, 2, 3}})
}
