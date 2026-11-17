package syncpipe

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/readruntime"
	"time"
)

type DrainAPI struct {
	Identity projection.APIIdentity `json:"identity"`
	Legacy   bool                   `json:"legacy_without_read_selector"`
}
type ReadDrainScope struct {
	OperationID    string            `json:"operation_id"`
	FreezeID       string            `json:"freeze_id"`
	FreezeRevision int64             `json:"freeze_revision"`
	VersionID      string            `json:"version_id"`
	ReadRevision   int64             `json:"read_revision"`
	Scope          readruntime.Scope `json:"scope"`
	APIs           []DrainAPI        `json:"apis"`
	ObservedAt     time.Time         `json:"observed_at"`
	RecoveryNoAPI  bool              `json:"explicit_recovery_without_serving_api,omitempty"`
}
type ReadDrained struct {
	ReadDrainScope
	Exited     []readruntime.Process `json:"exited"`
	VerifiedAt time.Time             `json:"verified_at"`
}

func executionEvidence(ctx context.Context, tx *sql.Tx, id, column, logName string, value any) error {
	switch column {
	case "drain_scope", "drained", "committed", "readiness", "resumed":
	default:
		return fmt.Errorf("invalid execution evidence")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	hash, err := projection.HashJSON(data)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT IGNORE INTO sync_projection_read_execution(operation_id,revision,updated_at) VALUES(?,1,UTC_TIMESTAMP(6))`, id); err != nil {
		return err
	}
	if err = readCAS(tx.ExecContext(ctx, "UPDATE sync_projection_read_execution SET "+column+"=?,"+column+"_sha256=?,revision=revision+1,updated_at=UTC_TIMESTAMP(6) WHERE operation_id=?", data, hash, id)); err != nil {
		return err
	}
	return projection.LogReadControl(ctx, tx, logName, id, map[string]any{"evidence_sha256": hash, "evidence": value})
}
func (p *ReadSwitchControl) ConfigureReadDrain(ctx context.Context, id string, scope readruntime.Scope) (ReadDrainScope, error) {
	release, err := p.lock(ctx)
	if err != nil {
		return ReadDrainScope{}, err
	}
	defer release()
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ReadDrainScope{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return ReadDrainScope{}, err
	}
	s, err := p.readStore().LockSelection(ctx, tx)
	if err != nil {
		return ReadDrainScope{}, err
	}
	o, err := p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return ReadDrainScope{}, err
	}
	if err = projection.MatchPlanned(g, s, o); err != nil {
		return ReadDrainScope{}, err
	}
	var existing []byte
	err = tx.QueryRowContext(ctx, "SELECT drain_scope FROM sync_projection_read_execution WHERE operation_id=?", id).Scan(&existing)
	if err != nil && err != sql.ErrNoRows {
		return ReadDrainScope{}, err
	}
	if len(existing) > 0 {
		var saved ReadDrainScope
		if json.Unmarshal(existing, &saved) != nil || !reflect.DeepEqual(saved.Scope, scope) {
			return saved, fmt.Errorf("READ_API_PROCESS_BLOCKED: drain scope already differs")
		}
		_, e := p.readStore().LockExecution(ctx, tx, id)
		return saved, e
	}
	processes, err := readruntime.Scan(ctx, scope)
	if err != nil {
		return ReadDrainScope{}, err
	}
	result := ReadDrainScope{OperationID: id, FreezeID: o.FreezeID, FreezeRevision: o.FreezeRevision, VersionID: o.FromVersionID, ReadRevision: o.ExpectedReadRevision, Scope: scope, ObservedAt: time.Now().UTC()}
	if len(processes) == 0 {
		if o.RollbackOf == nil {
			return result, fmt.Errorf("READ_API_PROCESS_BLOCKED: no serving API inventoried")
		}
		prior, e := p.readStore().LockOperation(ctx, tx, *o.RollbackOf)
		if e != nil || prior.SupersededBy == nil || *prior.SupersededBy != o.ID || (prior.Stage != projection.ReadCommitted && prior.Stage != projection.ReadAPIReady) {
			return result, fmt.Errorf("READ_API_PROCESS_BLOCKED: no API only allowed for explicit committed recovery")
		}
		result.RecoveryNoAPI = true
	}
	var ids []string
	rows, err := tx.QueryContext(ctx, "SELECT boot_id FROM sync_projection_api_boots WHERE version_id=? AND read_revision=? ORDER BY boot_id", o.FromVersionID, o.ExpectedReadRevision)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return result, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	boots := map[int]projection.APIBoot{}
	for _, id := range ids {
		b, err := p.readStore().APIBootIn(ctx, tx, id)
		if err != nil {
			return result, err
		}
		if readruntime.Exited(ctx, b.Identity.Process) == nil {
			continue
		}
		boots[b.Identity.Process.PID] = b
	}
	for _, process := range processes {
		listeners, e := readruntime.Listeners(ctx, process.PID)
		if e != nil || len(listeners) != 1 {
			return result, fmt.Errorf("READ_API_PROCESS_BLOCKED: API must have one declared listener: %v", e)
		}
		process.Listen = listeners[0]
		if e = readruntime.Alive(ctx, process); e != nil {
			return result, e
		}
		api := DrainAPI{}
		if b, exists := boots[process.PID]; exists {
			if !readruntime.Same(b.Identity.Process, process) {
				return result, fmt.Errorf("READ_API_PROCESS_BLOCKED: registry differs from process")
			}
			api.Identity = b.Identity
			delete(boots, process.PID)
		} else {
			raw := make([]byte, 16)
			if _, e = rand.Read(raw); e != nil {
				return result, e
			}
			api.Legacy = true
			api.Identity = projection.APIIdentity{BootID: hex.EncodeToString(raw), Process: process, VersionID: o.FromVersionID, ContractHash: o.FromContractSHA256, ReadRevision: o.ExpectedReadRevision}
		}
		result.APIs = append(result.APIs, api)
	}
	if len(boots) != 0 {
		return result, fmt.Errorf("READ_API_PROCESS_BLOCKED: live registered API omitted from scope")
	}
	if err = executionEvidence(ctx, tx, id, "drain_scope", "read_api_drain_scope", result); err != nil {
		return result, err
	}
	if err = p.observe("before_api_drain_plan_commit", result); err != nil {
		return result, err
	}
	if err = tx.Commit(); err != nil {
		return result, fmt.Errorf("drain inventory outcome unknown; inspect durable execution: %w", err)
	}
	if err = p.observe("after_api_drain_plan_commit", result); err != nil {
		return result, err
	}
	return result, nil
}
func (p *ReadSwitchControl) checkAPIDrained(ctx context.Context, o projection.ReadSwitch, e projection.ReadExecution) (ReadDrained, error) {
	var scope ReadDrainScope
	if len(e.DrainScope) == 0 || json.Unmarshal(e.DrainScope, &scope) != nil || scope.OperationID != o.ID || scope.FreezeID != o.FreezeID || scope.FreezeRevision != o.FreezeRevision || scope.VersionID != o.FromVersionID || scope.ReadRevision != o.ExpectedReadRevision || len(scope.APIs) == 0 && !scope.RecoveryNoAPI {
		return ReadDrained{}, fmt.Errorf("READ_API_DRAIN_REQUIRED: durable process inventory missing/different")
	}
	if scope.RecoveryNoAPI {
		if o.RollbackOf == nil {
			return ReadDrained{}, fmt.Errorf("invalid recovery drain")
		}
		prior, err := p.readStore().Operation(ctx, *o.RollbackOf)
		if err != nil || prior.SupersededBy == nil || *prior.SupersededBy != o.ID || (prior.Stage != projection.ReadCommitted && prior.Stage != projection.ReadAPIReady) {
			return ReadDrained{}, fmt.Errorf("recovery predecessor differs")
		}
	}
	processes, err := readruntime.Scan(ctx, scope.Scope)
	if err != nil {
		return ReadDrained{}, err
	}
	if len(processes) != 0 {
		return ReadDrained{}, fmt.Errorf("READ_API_DRAIN_REQUIRED: %d controlled API processes still exist", len(processes))
	}
	proof := ReadDrained{ReadDrainScope: scope, VerifiedAt: time.Now().UTC()}
	for _, api := range scope.APIs {
		if err = readruntime.Exited(ctx, api.Identity.Process); err != nil {
			return proof, err
		}
		proof.Exited = append(proof.Exited, api.Identity.Process)
	}
	rows, err := p.Store.DB.QueryContext(ctx, "SELECT boot_id FROM sync_projection_api_boots WHERE version_id=? AND read_revision=?", o.FromVersionID, o.ExpectedReadRevision)
	if err != nil {
		return proof, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return proof, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return proof, err
	}
	for _, id := range ids {
		b, err := p.readStore().APIBoot(ctx, id)
		if err != nil {
			return proof, err
		}
		if err = readruntime.Exited(ctx, b.Identity.Process); err != nil {
			return proof, err
		}
	}
	return proof, nil
}
func (p *ReadSwitchControl) ObserveReadDrain(ctx context.Context, id string) (ReadDrained, error) {
	release, err := p.lock(ctx)
	if err != nil {
		return ReadDrained{}, err
	}
	defer release()
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ReadDrained{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return ReadDrained{}, err
	}
	s, err := p.readStore().LockSelection(ctx, tx)
	if err != nil {
		return ReadDrained{}, err
	}
	o, err := p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return ReadDrained{}, err
	}
	if err = projection.MatchPlanned(g, s, o); err != nil {
		return ReadDrained{}, err
	}
	e, err := p.readStore().LockExecution(ctx, tx, id)
	if err != nil {
		return ReadDrained{}, err
	}
	evidence, err := p.checkAPIDrained(ctx, o, e)
	if err != nil {
		return evidence, err
	}
	if err = executionEvidence(ctx, tx, id, "drained", "read_api_drained", evidence); err != nil {
		return evidence, err
	}
	if err = p.observe("before_api_drained_commit", evidence); err != nil {
		return evidence, err
	}
	if err = tx.Commit(); err != nil {
		return evidence, fmt.Errorf("drain observation outcome unknown; reload durable execution: %w", err)
	}
	if err = p.observe("after_api_drained_commit", evidence); err != nil {
		return evidence, err
	}
	return evidence, nil
}
