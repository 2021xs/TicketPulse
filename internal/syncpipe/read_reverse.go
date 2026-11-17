package syncpipe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"ticketpulse/internal/projection"
)

func activeReadOperationCount(ctx context.Context, tx *sql.Tx) (int, error) {
	installed, err := projection.ReadRollbackInstalled(ctx, tx)
	if err != nil {
		return 0, err
	}
	query := "SELECT COUNT(*) FROM sync_projection_read_switches WHERE stage NOT IN ('CANCELLED','RESUMED')"
	if installed {
		query += " AND superseded_by IS NULL"
	}
	var n int
	err = tx.QueryRowContext(ctx, query).Scan(&n)
	return n, err
}

func (p *ReadSwitchControl) beginReadSwitch(ctx context.Context, id, direction, destination string, revision int64, reverseOf []string) (projection.ReadSwitch, error) {
	if !projection.ValidID(id) || !projection.ValidID(destination) || (direction != projection.ReadPromote && direction != projection.ReadRollback) || revision < 1 || len(reverseOf) > 1 {
		return projection.ReadSwitch{}, fmt.Errorf("invalid read operation intent")
	}
	var parent *string
	if len(reverseOf) == 1 {
		if !projection.ValidID(reverseOf[0]) {
			return projection.ReadSwitch{}, fmt.Errorf("invalid reverse link")
		}
		parent = &reverseOf[0]
	}
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
	enabled, err := projection.ReadControlEnabled(ctx, tx)
	if err != nil || !enabled {
		return projection.ReadSwitch{}, fmt.Errorf("read control is not legally initialized: %v", err)
	}
	s, err := p.readStore().LockSelection(ctx, tx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	existing, e := p.readStore().LockOperation(ctx, tx, id)
	if e == nil {
		if existing.Direction != direction || existing.ToVersionID != destination || existing.ExpectedReadRevision != revision || !reflect.DeepEqual(existing.RollbackOf, parent) {
			return existing, fmt.Errorf("read operation ID is immutable")
		}
		if existing.Stage != projection.ReadPlanned {
			return existing, nil
		}
		return existing, projection.MatchPlanned(g, s, existing)
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return existing, e
	}
	if g.State != "FROZEN" || g.FreezeID == nil || s.ReadRevision != revision || s.VersionID == destination {
		return existing, fmt.Errorf("READ_SWITCH_CONFLICT: freeze/read revision differs")
	}
	pending, err := activeReadOperationCount(ctx, tx)
	if err != nil {
		return existing, err
	}
	var prior *projection.ReadSwitch
	if s.CurrentOperationID != nil {
		value, e := p.readStore().LockOperation(ctx, tx, *s.CurrentOperationID)
		if e != nil {
			return value, e
		}
		prior = &value
	}
	if parent == nil {
		if revision != 1 || direction != projection.ReadPromote || pending != 0 || prior != nil {
			return existing, fmt.Errorf("READ_SWITCH_CONFLICT: explicit reverse reference required")
		}
		if err = projection.CheckReadResume(ctx, tx, g); err != nil {
			return existing, err
		}
	} else {
		installed, e := projection.ReadRollbackInstalled(ctx, tx)
		if e != nil || !installed {
			return existing, fmt.Errorf("reverse control requires completed 015: %v", e)
		}
		if prior == nil || prior.ID != *parent || prior.ExpectedReadRevision+1 != revision || prior.ToVersionID != s.VersionID || prior.FromVersionID != destination || prior.SupersededBy != nil {
			return existing, fmt.Errorf("READ_REVERSE_LINK_MISMATCH")
		}
		if prior.Stage == projection.ReadResumed {
			if pending != 0 {
				return existing, fmt.Errorf("READ_SWITCH_CONFLICT: active operation")
			}
		} else if (prior.Stage != projection.ReadCommitted && prior.Stage != projection.ReadAPIReady) || pending != 1 || *g.FreezeID != prior.FreezeID || g.Revision != prior.FreezeRevision {
			return existing, fmt.Errorf("READ_REVERSE_FREEZE_MISMATCH")
		}
	}
	from, e := (&projection.VersionStore{DB: p.Store.DB}).Get(ctx, s.VersionID)
	if e != nil || from.Fingerprint() != s.ContractSHA256 {
		return existing, fmt.Errorf("source contract changed: %v", e)
	}
	d, err := p.ResolveReadDestination(ctx, destination)
	if err != nil {
		return existing, err
	}
	if from.Contract.Source != d.Version.Contract.Source || !reflect.DeepEqual(from.Contract.Kafka, d.Version.Contract.Kafka) {
		return existing, fmt.Errorf("different read source/topic")
	}
	if direction == projection.ReadRollback && (from.Phase != projection.Prepared || d.Version.Phase != projection.Active) || direction == projection.ReadPromote && (from.Phase != projection.Active || d.Version.Phase != projection.Prepared) {
		return existing, fmt.Errorf("read direction does not match registration scope")
	}
	o := projection.ReadSwitch{ID: id, RollbackOf: parent, Direction: direction, FromVersionID: from.ID, ToVersionID: d.Version.ID, FromContractSHA256: from.Fingerprint(), ToContractSHA256: d.Version.Fingerprint(), CKDatabaseUUID: d.CKDatabaseUUID, ExpectedReadRevision: revision, FreezeID: *g.FreezeID, FreezeRevision: g.Revision, Stage: projection.ReadPlanned, Revision: 1}
	if err = o.Validate(); err != nil {
		return o, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sync_projection_read_switches(operation_id,direction,from_version_id,to_version_id,from_contract_sha256,to_contract_sha256,ck_database_uuid,expected_read_revision,freeze_id,freeze_revision,stage,operation_revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,'PLANNED',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, o.ID, o.Direction, o.FromVersionID, o.ToVersionID, o.FromContractSHA256, o.ToContractSHA256, o.CKDatabaseUUID, o.ExpectedReadRevision, o.FreezeID, o.FreezeRevision)
	if err != nil {
		return o, err
	}
	if parent != nil {
		if err = readCAS(tx.ExecContext(ctx, "UPDATE sync_projection_read_switches SET rollback_of=? WHERE operation_id=?", *parent, id)); err != nil {
			return o, err
		}
		if prior.Stage != projection.ReadResumed {
			if err = readCAS(tx.ExecContext(ctx, "UPDATE sync_projection_read_switches SET superseded_by=?,operation_revision=operation_revision+1,updated_at=UTC_TIMESTAMP(6) WHERE operation_id=? AND superseded_by IS NULL AND stage IN ('COMMITTED','API_READY')", id, prior.ID)); err != nil {
				return o, err
			}
			if err = projection.LogReadControl(ctx, tx, "read_switch_reverse_planned", prior.ID, map[string]any{"reverse_operation_id": id, "freeze_id": o.FreezeID, "freeze_revision": o.FreezeRevision}); err != nil {
				return o, err
			}
		}
	}
	if err = readCAS(tx.ExecContext(ctx, `UPDATE sync_projection_read_selection SET current_operation_id=?,updated_at=UTC_TIMESTAMP(6) WHERE selector_id=1 AND version_id=? AND read_revision=? AND current_operation_id <=> ?`, id, s.VersionID, revision, s.CurrentOperationID)); err != nil {
		return o, err
	}
	if err = projection.LogReadControl(ctx, tx, "read_switch_planned", id, o); err != nil {
		return o, err
	}
	if err = p.observe("before_plan_commit", o); err != nil {
		return o, err
	}
	if err = tx.Commit(); err != nil {
		return o, fmt.Errorf("plan outcome unknown; inspect persistent read operation: %w", err)
	}
	if err = p.observe("after_plan_commit", o); err != nil {
		return o, err
	}
	return p.readStore().Operation(ctx, id)
}
