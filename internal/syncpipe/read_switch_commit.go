package syncpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"ticketpulse/internal/projection"
	"time"
)

type ReadCommit struct {
	OperationID    string      `json:"operation_id"`
	RollbackOf     *string     `json:"rollback_of,omitempty"`
	VersionID      string      `json:"version_id"`
	ContractHash   string      `json:"contract_sha256"`
	HandoffID      string      `json:"handoff_id"`
	ReadRevision   int64       `json:"read_revision"`
	FreezeID       string      `json:"freeze_id"`
	FreezeRevision int64       `json:"freeze_revision"`
	ProofHash      string      `json:"proof_sha256"`
	CKDatabaseUUID string      `json:"ck_database_uuid"`
	Drained        ReadDrained `json:"api_drained"`
	CommittedAt    time.Time   `json:"committed_at"`
}

func (p *ReadSwitchControl) CommitReadSelection(ctx context.Context, id string) (projection.ReadSwitch, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	release, err := p.lock(ctx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	defer release()
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockGateExclusive(ctx, tx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	s, err := p.readStore().LockSelection(ctx, tx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	o, err := p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return o, err
	}
	if o.Stage == projection.ReadCommitted || o.Stage == projection.ReadAPIReady || o.Stage == projection.ReadResumed {
		if s.VersionID != o.ToVersionID || s.ContractSHA256 != o.ToContractSHA256 || s.ReadRevision != o.ExpectedReadRevision+1 || s.CurrentOperationID == nil || *s.CurrentOperationID != o.ID {
			return o, fmt.Errorf("READ_COMMIT_OUTCOME_AMBIGUOUS: persistent facts differ")
		}
		if _, err = p.readStore().LockExecution(ctx, tx, id); err != nil {
			return o, err
		}
		return o, nil
	}
	// Exclusive gate -> selector -> operation is the same order as Resume.
	// APIs have all exited, and CDC/internal progress do not lock this gate.
	if err = projection.MatchPlanned(g, s, o); err != nil {
		return o, err
	}
	if s.ReadRevision != 1 && o.RollbackOf == nil {
		return o, fmt.Errorf("READ_REVERSE_LINK_REQUIRED")
	}
	if len(o.Proof) == 0 || o.ProofSHA256 == nil {
		return o, fmt.Errorf("READ_FRESH_PROOF_REQUIRED")
	}
	e, err := p.readStore().LockExecution(ctx, tx, id)
	if err != nil {
		return o, err
	}
	if len(e.Drained) == 0 {
		return o, fmt.Errorf("READ_API_DRAIN_REQUIRED: explicit drain observation required")
	}
	drained, err := p.checkAPIDrained(ctx, o, e)
	if err != nil {
		return o, err
	}
	pending, e2 := activeReadOperationCount(ctx, tx)
	if e2 != nil || pending != 1 {
		return o, fmt.Errorf("READ_SWITCH_STATE_CHANGED: competing operation: %v", err)
	}
	d, err := p.ResolveReadDestination(ctx, o.ToVersionID)
	if err != nil {
		return o, err
	}
	if d.Version.Fingerprint() != o.ToContractSHA256 || d.CKDatabaseUUID != o.CKDatabaseUUID || o.Direction == projection.ReadRollback && d.Version.Phase != projection.Active || o.Direction == projection.ReadPromote && d.Handoff == nil {
		return o, fmt.Errorf("READ_DESTINATION_CHANGED")
	}
	from, err := (&projection.VersionStore{DB: p.Store.DB}).Get(ctx, o.FromVersionID)
	if err != nil || from.Fingerprint() != o.FromContractSHA256 || !reflect.DeepEqual(from.Contract.Source, d.Version.Contract.Source) || !reflect.DeepEqual(from.Contract.Kafka, d.Version.Contract.Kafka) {
		return o, fmt.Errorf("READ_SOURCE_CHANGED: %v", err)
	}
	var old ReadSwitchProof
	if json.Unmarshal(o.Proof, &old) != nil {
		return o, projection.ErrReadControl
	}
	live, err := p.proveReadDestination(ctx, d, o)
	if err != nil {
		return o, err
	}
	if live.Report.Source != old.Report.Source || live.Report.SourceAfter == nil || old.Report.SourceAfter == nil || *live.Report.SourceAfter != *old.Report.SourceAfter {
		return o, fmt.Errorf("READ_SOURCE_BOUNDARY_CHANGED: source differs from frozen proof")
	}
	for _, component := range []string{"es", "ck"} {
		if !sameReadWriter(old.After[component], live.Before[component]) {
			return o, fmt.Errorf("READ_WRITER_CHANGED: owner/member changed since fresh proof")
		}
		for i, position := range live.Before[component].Progress {
			if position.Committed < old.After[component].Progress[i].Committed {
				return o, fmt.Errorf("READ_OFFSET_REGRESSED")
			}
		}
	}
	if err = p.observe("before_read_commit_recheck", live); err != nil {
		return o, err
	}
	// Observer faults can run during validation. Recheck processes and dynamic
	// identities after the final complete audit, immediately before transaction.
	drained, err = p.checkAPIDrained(ctx, o, e)
	if err != nil {
		return o, err
	}
	if err = d.CheckIdentities(ctx); err != nil {
		return o, err
	}
	for _, component := range []string{"es", "ck"} {
		last, err := p.waitReadWriter(ctx, d, component)
		if err != nil {
			return o, err
		}
		if !sameReadWriter(live.After[component], last) {
			return o, fmt.Errorf("READ_WRITER_CHANGED")
		}
		for i, position := range last.Progress {
			if position.Committed < live.After[component].Progress[i].Committed {
				return o, fmt.Errorf("READ_OFFSET_REGRESSED")
			}
		}
		live.After[component] = last
	}
	live.ValidatedAt = time.Now().UTC()
	data, _ := json.Marshal(live)
	hash, err := projection.HashJSON(data)
	if err != nil {
		return o, err
	}
	var handoffID any
	handoffText := ""
	if d.Handoff != nil {
		handoffID = d.Handoff.ID
		handoffText = d.Handoff.ID
	}
	commit := ReadCommit{OperationID: o.ID, RollbackOf: o.RollbackOf, VersionID: o.ToVersionID, ContractHash: o.ToContractSHA256, HandoffID: handoffText, ReadRevision: o.ExpectedReadRevision + 1, FreezeID: o.FreezeID, FreezeRevision: o.FreezeRevision, ProofHash: hash, CKDatabaseUUID: o.CKDatabaseUUID, Drained: drained, CommittedAt: time.Now().UTC()}
	if err = executionEvidence(ctx, tx, id, "drained", "read_api_drained", drained); err != nil {
		return o, err
	}
	if err = executionEvidence(ctx, tx, id, "committed", "read_switch_committed", commit); err != nil {
		return o, err
	}
	if err = readCAS(tx.ExecContext(ctx, `UPDATE sync_projection_read_switches SET stage='COMMITTED',proof=?,proof_sha256=?,validated_at=?,operation_revision=operation_revision+1,last_error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE operation_id=? AND stage='PLANNED' AND operation_revision=?`, data, hash, live.ValidatedAt, id, o.Revision)); err != nil {
		return o, err
	}
	if err = readCAS(tx.ExecContext(ctx, `UPDATE sync_projection_read_selection SET version_id=?,contract_sha256=?,handoff_id=?,read_revision=read_revision+1,updated_at=UTC_TIMESTAMP(6) WHERE selector_id=1 AND version_id=? AND contract_sha256=? AND read_revision=? AND current_operation_id=?`, o.ToVersionID, o.ToContractSHA256, handoffID, o.FromVersionID, o.FromContractSHA256, o.ExpectedReadRevision, id)); err != nil {
		return o, err
	}
	if err = p.observe("before_read_commit", commit); err != nil {
		return o, err
	}
	if err = tx.Commit(); err != nil {
		return o, fmt.Errorf("read commit outcome unknown; reload selector and operation before retry: %w", err)
	}
	if err = p.observe("after_read_commit", commit); err != nil {
		return o, err
	}
	return p.readStore().Operation(ctx, id)
}
