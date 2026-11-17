package projection

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	assets "ticketpulse/db"
)

func sameOptionalID(a, b *string) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// Installation is explicit. A partial or lost installation cannot downgrade
// an enabled reverse protocol to the C1/C2 compatibility path.
func ReadRollbackInstalled(ctx context.Context, q rowQuery) (bool, error) {
	var installed, columns, tables int
	err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_control_log WHERE operation='install_read_rollback'").Scan(&installed)
	if err != nil && !MissingTable(err) {
		return false, err
	}
	if err = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='sync_projection_read_switches' AND column_name IN ('rollback_of','superseded_by')").Scan(&columns); err != nil {
		return false, err
	}
	if err = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='sync_projection_legacy_boots'").Scan(&tables); err != nil {
		return false, err
	}
	if installed == 0 && columns == 0 && tables == 0 {
		return false, nil
	}
	if installed != 1 || columns != 2 || tables != 1 {
		return false, fmt.Errorf("%w: incomplete 015 installation", ErrReadControl)
	}
	return true, nil
}

func EnsureReadRollbackSchema(ctx context.Context, db *sql.DB) error {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_control_log WHERE operation='install_read_runtime'").Scan(&count); err != nil || count != 1 {
		return fmt.Errorf("015 requires explicit 014 installation: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='sync_projection_read_switches' AND column_name IN ('rollback_of','superseded_by')").Scan(&count); err != nil {
		return err
	}
	statements := strings.Split(assets.ProjectionReadRollbackMigration, ";")
	if count == 0 {
		if _, err := db.ExecContext(ctx, statements[0]); err != nil {
			return err
		}
	} else if count != 2 {
		return fmt.Errorf("partial 015 columns require explicit repair")
	}
	for _, statement := range statements[1:] {
		if strings.TrimSpace(statement) != "" {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sync_projection_control_log(operation,reference_id,details,occurred_at) SELECT 'install_read_rollback','015_projection_read_rollback',JSON_OBJECT('schema',15),UTC_TIMESTAMP(6) WHERE NOT EXISTS(SELECT 1 FROM sync_projection_control_log existing WHERE existing.operation='install_read_rollback')`); err != nil {
		return err
	}
	_, err := ReadRollbackInstalled(ctx, db)
	return err
}

// Validate the entire committed revision chain, independent of the current
// PLANNED pointer. Cancelled intents never erase the previous committed head.
func readSelectionChain(ctx context.Context, q rowQuery, s ReadSelection) (string, string, error) {
	reader, ok := q.(interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	})
	if !ok {
		return "", "", ErrReadControl
	}
	rows, err := reader.QueryContext(ctx, "SELECT operation_id FROM sync_projection_read_switches ORDER BY expected_read_revision,operation_id")
	if err != nil {
		return "", "", err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return "", "", err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", "", err
	}
	var committed []ReadSwitch
	var current *ReadSwitch
	for _, id := range ids {
		o, e := readSwitch(ctx, q, id, "")
		if e != nil {
			return "", "", e
		}
		if s.CurrentOperationID != nil && *s.CurrentOperationID == id {
			value := o
			current = &value
		}
		if o.Stage == ReadCommitted || o.Stage == ReadAPIReady || o.Stage == ReadResumed {
			committed = append(committed, o)
		}
	}
	if int64(len(committed)) != s.ReadRevision-1 {
		return "", "", fmt.Errorf("%w: revision chain has a gap or fork", ErrReadControl)
	}
	initialID, initialHash := s.VersionID, s.ContractSHA256
	if len(committed) > 0 {
		initialID, initialHash = committed[0].FromVersionID, committed[0].FromContractSHA256
	}
	initial, e := readVersion(ctx, q, initialID)
	if e != nil || initial.Phase != Active || initial.Fingerprint() != initialHash {
		return "", "", fmt.Errorf("%w: original ACTIVE differs", ErrReadControl)
	}
	version, hash := initialID, initialHash
	for i, o := range committed {
		if o.ExpectedReadRevision != int64(i+1) || o.FromVersionID != version || o.FromContractSHA256 != hash {
			return "", "", fmt.Errorf("%w: discontinuous read history", ErrReadControl)
		}
		if _, e = checkExecutionStage(ctx, q, o); e != nil {
			return "", "", e
		}
		from, e1 := readVersion(ctx, q, o.FromVersionID)
		to, e2 := readVersion(ctx, q, o.ToVersionID)
		if e1 != nil || e2 != nil || from.Fingerprint() != o.FromContractSHA256 || to.Fingerprint() != o.ToContractSHA256 {
			return "", "", ErrReadControl
		}
		if o.RollbackOf == nil {
			if o.Direction != ReadPromote || o.ExpectedReadRevision != 1 || from.Phase != Active || to.Phase != Prepared {
				return "", "", fmt.Errorf("%w: unlinked transition", ErrReadControl)
			}
		} else {
			prior, e := readSwitch(ctx, q, *o.RollbackOf, "")
			if e != nil || prior.ExpectedReadRevision+1 != o.ExpectedReadRevision || prior.FromVersionID != o.ToVersionID || prior.ToVersionID != o.FromVersionID || prior.FromContractSHA256 != o.ToContractSHA256 || prior.ToContractSHA256 != o.FromContractSHA256 {
				return "", "", fmt.Errorf("%w: reverse link differs", ErrReadControl)
			}
			if (o.Direction == ReadRollback && (from.Phase != Prepared || to.Phase != Active)) || (o.Direction == ReadPromote && (from.Phase != Active || to.Phase != Prepared)) {
				return "", "", ErrReadControl
			}
			if prior.Stage != ReadResumed && (prior.SupersededBy == nil || *prior.SupersededBy != o.ID) {
				return "", "", fmt.Errorf("%w: unfinished prior operation is not explicitly superseded", ErrReadControl)
			}
			if prior.Stage != ReadResumed {
				var records int
				if err = q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_projection_control_log WHERE operation='read_switch_reverse_planned' AND reference_id=? AND JSON_UNQUOTE(JSON_EXTRACT(details,'$.reverse_operation_id'))=? AND JSON_UNQUOTE(JSON_EXTRACT(details,'$.freeze_id'))=? AND JSON_EXTRACT(details,'$.freeze_revision')=?`, prior.ID, o.ID, o.FreezeID, o.FreezeRevision).Scan(&records); err != nil || records != 1 {
					return "", "", fmt.Errorf("%w: reverse supersession audit missing", ErrReadControl)
				}
			}
		}
		if i < len(committed)-1 && o.Stage != ReadResumed {
			if o.SupersededBy == nil || *o.SupersededBy != committed[i+1].ID {
				return "", "", ErrReadControl
			}
		}
		version, hash = o.ToVersionID, o.ToContractSHA256
	}
	if version != s.VersionID || hash != s.ContractSHA256 {
		return "", "", fmt.Errorf("%w: selector differs from committed head", ErrReadControl)
	}
	if s.CurrentOperationID != nil {
		if current == nil {
			return "", "", ErrReadControl
		}
		if current.Stage == ReadPlanned {
			if current.FromVersionID != s.VersionID || current.FromContractSHA256 != s.ContractSHA256 || current.ExpectedReadRevision != s.ReadRevision {
				return "", "", ErrReadControl
			}
		} else if len(committed) == 0 || current.ID != committed[len(committed)-1].ID || current.Stage == ReadCancelled {
			return "", "", ErrReadControl
		}
	}
	if s.ReadRevision > 1 && s.CurrentOperationID == nil {
		return "", "", fmt.Errorf("%w: committed head reference missing", ErrReadControl)
	}
	v, e := readVersion(ctx, q, s.VersionID)
	if e != nil {
		return "", "", e
	}
	if v.Phase == Active && s.HandoffID != nil || v.Phase == Prepared && s.HandoffID == nil {
		return "", "", ErrReadControl
	}
	if s.HandoffID != nil {
		if e = checkReadHandoff(ctx, q, v, *s.HandoffID); e != nil {
			return "", "", e
		}
	}
	return initialID, initialHash, nil
}

func checkReadHandoff(ctx context.Context, q rowQuery, v Version, id string) error {
	h, err := readHandoff(ctx, q, id, "")
	if err != nil {
		return err
	}
	reader, ok := q.(interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	})
	if !ok {
		return ErrReadControl
	}
	rows, err := reader.QueryContext(ctx, `SELECT component,partition_id,initial_offset,confirmed_at,verification_offset FROM sync_projection_handoff_partitions WHERE handoff_id=? ORDER BY component,partition_id`, h.ID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var part HandoffPartition
		if err = rows.Scan(&part.Component, &part.Partition, &part.InitialOffset, &part.ConfirmedAt, &part.VerificationOffset); err != nil {
			rows.Close()
			return err
		}
		h.Partitions = append(h.Partitions, part)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if err = h.Validate(v); err != nil || h.Phase != Verified {
		return fmt.Errorf("%w: selected handoff invalid: %v", ErrReadControl, err)
	}
	return nil
}
