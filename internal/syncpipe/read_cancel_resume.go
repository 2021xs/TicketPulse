package syncpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/readruntime"
)

// A cancelled reverse intent never opens the gate. This explicit recovery
// rechecks the still-selected API, writers, raw positions and full facts.
func (p *ReadSwitchControl) ResumeCancelledReadSwitch(ctx context.Context, id, freeze string, probe ReadAPIProbe) (projection.Gate, error) {
	release, err := p.lock(ctx)
	if err != nil {
		return projection.Gate{}, err
	}
	defer release()
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.Gate{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockGateExclusive(ctx, tx)
	if err != nil {
		return g, err
	}
	s, err := p.readStore().ShareSelection(ctx, tx)
	if err != nil {
		return g, err
	}
	o, err := p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return g, err
	}
	if o.Stage != projection.ReadCancelled || o.RollbackOf == nil || o.FreezeID != freeze || s.VersionID != o.FromVersionID || s.ContractSHA256 != o.FromContractSHA256 || s.ReadRevision != o.ExpectedReadRevision || !reflect.DeepEqual(s.CurrentOperationID, o.RollbackOf) {
		return g, fmt.Errorf("READ_CANCEL_RESUME_MISMATCH")
	}
	if g.State == "OPEN" {
		var data []byte
		if err = tx.QueryRowContext(ctx, `SELECT details FROM sync_projection_control_log WHERE operation='read_cancel_resumed' AND reference_id=?`, id).Scan(&data); err != nil {
			return g, fmt.Errorf("READ_CANCEL_RESUME_OUTCOME_AMBIGUOUS: %w", err)
		}
		var value struct {
			FreezeID     string          `json:"freeze_id"`
			ReadRevision int64           `json:"read_revision"`
			Gate         projection.Gate `json:"gate"`
		}
		if json.Unmarshal(data, &value) != nil || value.FreezeID != freeze || value.ReadRevision != s.ReadRevision || value.Gate.Revision != g.Revision || g.Revision != o.FreezeRevision+1 {
			return g, fmt.Errorf("READ_CANCEL_RESUME_OUTCOME_AMBIGUOUS")
		}
		return g, nil
	}
	if g.State != "FROZEN" || g.FreezeID == nil || *g.FreezeID != freeze || g.Revision != o.FreezeRevision {
		return g, fmt.Errorf("READ_CANCEL_RESUME_FREEZE_CHANGED")
	}
	// Downgrade to the same frozen SHARE gate used by ObserveAPIReady: real
	// HTTP probes read the gate while intent remains locked by selector/op.
	if err = tx.Rollback(); err != nil {
		return g, err
	}
	tx, err = p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return g, err
	}
	defer tx.Rollback()
	g, err = projection.LockFrozen(ctx, tx)
	if err != nil {
		return g, err
	}
	s, err = p.readStore().ShareSelection(ctx, tx)
	if err != nil {
		return g, err
	}
	o, err = p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return g, err
	}
	if g.FreezeID == nil || *g.FreezeID != freeze || g.Revision != o.FreezeRevision || o.Stage != projection.ReadCancelled || o.RollbackOf == nil || o.FreezeID != freeze || s.VersionID != o.FromVersionID || s.ContractSHA256 != o.FromContractSHA256 || s.ReadRevision != o.ExpectedReadRevision || !reflect.DeepEqual(s.CurrentOperationID, o.RollbackOf) {
		return g, fmt.Errorf("READ_CANCEL_RESUME_MISMATCH")
	}
	prior, err := p.readStore().LockOperation(ctx, tx, *o.RollbackOf)
	if err != nil || prior.Stage != projection.ReadResumed {
		return g, fmt.Errorf("READ_CANCEL_REQUIRES_COMMITTED_RECOVERY: prior switch not resumed")
	}
	d, err := p.ResolveReadDestination(ctx, s.VersionID)
	if err != nil {
		return g, err
	}
	shadow := o
	shadow.ToVersionID = s.VersionID
	shadow.ToContractSHA256 = s.ContractSHA256
	shadow.ExpectedReadRevision = s.ReadRevision - 1
	shadow.CKDatabaseUUID = d.CKDatabaseUUID
	ready, err := p.checkReadReadyMode(ctx, s, shadow, probe, false)
	if err != nil {
		return g, err
	}
	if pending, e := activeReadOperationCount(ctx, tx); e != nil || pending != 0 {
		return g, fmt.Errorf("READ_SWITCH_INCOMPLETE")
	}
	var pendingHandoffs int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_handoffs WHERE phase<>'VERIFIED'").Scan(&pendingHandoffs); err != nil || pendingHandoffs != 0 {
		return g, fmt.Errorf("HANDOFF_INCOMPLETE")
	}
	g, err = projection.LockGateExclusive(ctx, tx)
	if err != nil {
		return g, err
	}
	if err = readruntime.Alive(ctx, ready.API.Identity.Process); err != nil {
		return g, err
	}
	if err = projection.CheckReadResume(ctx, tx, g); err != nil && !errors.Is(err, projection.ErrReadCancelResume) {
		return g, err
	}
	if err = projection.LogReadControl(ctx, tx, "read_cancel_resume_checked", o.ID, ready); err != nil {
		return g, err
	}
	g, err = projection.OpenReadSwitchGate(ctx, tx, freeze)
	if err != nil {
		return g, err
	}
	if err = projection.LogReadControl(ctx, tx, "read_cancel_resumed", id, map[string]any{"freeze_id": freeze, "read_revision": s.ReadRevision, "boot_id": probe.BootID, "gate": g}); err != nil {
		return g, err
	}
	if err = p.observe("before_cancel_resume_commit", ready); err != nil {
		return g, err
	}
	if err = readruntime.Alive(ctx, ready.API.Identity.Process); err != nil {
		return g, err
	}
	if err = tx.Commit(); err != nil {
		return g, fmt.Errorf("cancel Resume outcome unknown; inspect gate: %w", err)
	}
	if err = p.observe("after_cancel_resume_commit", ready); err != nil {
		return g, err
	}
	return g, nil
}
