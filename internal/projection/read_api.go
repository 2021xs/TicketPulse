package projection

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"ticketpulse/internal/readruntime"
	"time"
)

type APIIdentity struct {
	BootID         string              `json:"boot_id"`
	Process        readruntime.Process `json:"process"`
	VersionID      string              `json:"version_id"`
	ContractHash   string              `json:"contract_sha256"`
	ReadRevision   int64               `json:"read_revision"`
	HandoffID      *string             `json:"handoff_id"`
	CKDatabaseUUID string              `json:"ck_database_uuid"`
	StateNamespace string              `json:"state_namespace"`
	Groups         map[string]string   `json:"groups"`
}
type APIBoot struct {
	Identity     APIIdentity     `json:"identity"`
	IdentityHash string          `json:"identity_sha256"`
	State        string          `json:"state"`
	HeartbeatAt  time.Time       `json:"heartbeat_at"`
	Shutdown     json.RawMessage `json:"shutdown"`
	ExitedAt     *time.Time      `json:"exited_at"`
}

func readAPIBoot(ctx context.Context, q rowQuery, id string) (b APIBoot, err error) {
	var raw, shutdown []byte
	var version string
	var revision int64
	err = q.QueryRowContext(ctx, `SELECT identity,identity_sha256,version_id,read_revision,state,heartbeat_at,shutdown,exited_at FROM sync_projection_api_boots WHERE boot_id=?`, id).Scan(&raw, &b.IdentityHash, &version, &revision, &b.State, &b.HeartbeatAt, &shutdown, &b.ExitedAt)
	if err != nil {
		return b, fmt.Errorf("%w: API boot: %v", ErrReadControl, err)
	}
	b.Shutdown = shutdown
	hash, e := HashJSON(raw)
	if e != nil || hash != b.IdentityHash || json.Unmarshal(raw, &b.Identity) != nil || b.Identity.BootID != id || b.Identity.VersionID != version || b.Identity.ReadRevision != revision || !readTokenPattern.MatchString(id) {
		return b, fmt.Errorf("%w: corrupted API identity", ErrReadControl)
	}
	return b, nil
}
func (s *ReadControlStore) APIBoot(ctx context.Context, id string) (APIBoot, error) {
	return readAPIBoot(ctx, s.DB, id)
}
func (s *ReadControlStore) APIBootIn(ctx context.Context, tx *sql.Tx, id string) (APIBoot, error) {
	return readAPIBoot(ctx, tx, id)
}
func (s *ReadControlStore) RegisterAPI(ctx context.Context, i APIIdentity) (APIBoot, error) {
	if !readTokenPattern.MatchString(i.BootID) || i.Process.PID < 2 || i.Process.Listen == "" || i.CKDatabaseUUID == "" {
		return APIBoot{}, fmt.Errorf("invalid API identity")
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return APIBoot{}, err
	}
	defer tx.Rollback()
	selected, err := readSelection(ctx, tx, " FOR SHARE")
	if err != nil {
		return APIBoot{}, err
	}
	if err = CheckReadAdmission(ctx, tx, selected); err != nil {
		return APIBoot{}, err
	}
	if selected.VersionID != i.VersionID || selected.ContractSHA256 != i.ContractHash || selected.ReadRevision != i.ReadRevision {
		return APIBoot{}, fmt.Errorf("READ_SELECTION_CHANGED: startup configuration differs")
	}
	raw, _ := json.Marshal(i)
	hash, _ := HashJSON(raw)
	if _, err = tx.ExecContext(ctx, `INSERT INTO sync_projection_api_boots(boot_id,version_id,read_revision,identity,identity_sha256,state,heartbeat_at) VALUES(?,?,?,?,?,'STARTING',UTC_TIMESTAMP(6))`, i.BootID, i.VersionID, i.ReadRevision, raw, hash); err != nil {
		return APIBoot{}, err
	}
	if err = logControl(ctx, tx, "read_api_starting", i.BootID, i); err != nil {
		return APIBoot{}, err
	}
	if err = tx.Commit(); err != nil {
		return APIBoot{}, fmt.Errorf("API boot registration unknown; inspect boot before retry: %w", err)
	}
	return s.APIBoot(ctx, i.BootID)
}
func (s *ReadControlStore) APIHeartbeat(ctx context.Context, id, state string, shutdown any) error {
	if state != "RUNNING" && state != "DRAINING" && state != "EXITED" {
		return fmt.Errorf("invalid API state")
	}
	var data any
	if shutdown != nil {
		raw, err := json.Marshal(shutdown)
		if err != nil {
			return err
		}
		data = raw
	}
	result, err := s.DB.ExecContext(ctx, `UPDATE sync_projection_api_boots SET state=?,heartbeat_at=UTC_TIMESTAMP(6),shutdown=COALESCE(?,shutdown),exited_at=IF(?='EXITED',UTC_TIMESTAMP(6),exited_at) WHERE boot_id=? AND state<>'EXITED'`, state, data, state, id)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("API heartbeat update failed: %v", err)
	}
	return nil
}

// SELECT FOR SHARE serializes admission with PLANNED/COMMITTED. It is released
// before the backend query: admitted requests are drained by process exit.
func CheckReadAdmission(ctx context.Context, q rowQuery, s ReadSelection) error {
	var pending int
	installed, err := ReadRollbackInstalled(ctx, q)
	if err != nil {
		return err
	}
	query := "SELECT COUNT(*) FROM sync_projection_read_switches WHERE stage NOT IN ('CANCELLED','RESUMED')"
	if installed {
		query += " AND superseded_by IS NULL"
	}
	if err := q.QueryRowContext(ctx, query).Scan(&pending); err != nil {
		return fmt.Errorf("%w: read operation metadata unavailable", ErrReadControl)
	}
	g, err := readGate(ctx, q, "")
	if err != nil || !g.Valid() {
		return fmt.Errorf("%w: business gate invalid", ErrReadControl)
	}
	if s.CurrentOperationID == nil {
		if pending != 0 {
			return fmt.Errorf("%w: pending operation reference missing", ErrReadControl)
		}
		return nil
	}
	o, err := readSwitch(ctx, q, *s.CurrentOperationID, "")
	if err != nil {
		return err
	}
	switch o.Stage {
	case ReadPlanned:
		return fmt.Errorf("READ_SWITCH_MAINTENANCE: PLANNED denies new projection reads and API boots")
	case ReadCommitted, ReadAPIReady:
		if s.VersionID != o.ToVersionID || s.ContractSHA256 != o.ToContractSHA256 || s.ReadRevision != o.ExpectedReadRevision+1 {
			return fmt.Errorf("%w: selected version does not match committed operation", ErrReadControl)
		}
		if pending != 1 || g.State != "FROZEN" || g.FreezeID == nil || *g.FreezeID != o.FreezeID || g.Revision != o.FreezeRevision {
			return fmt.Errorf("%w: committed read/freeze boundary differs", ErrReadControl)
		}
	case ReadResumed:
		if s.VersionID != o.ToVersionID || s.ContractSHA256 != o.ToContractSHA256 || s.ReadRevision != o.ExpectedReadRevision+1 {
			return fmt.Errorf("%w: selected version does not match resumed operation", ErrReadControl)
		}
		if pending != 0 {
			return fmt.Errorf("%w: competing read operation", ErrReadControl)
		}
	default:
		return fmt.Errorf("%w: illegal read admission stage", ErrReadControl)
	}
	return nil
}
func (s *ReadControlStore) CheckAPI(ctx context.Context, expected ReadSelection, bootID string) error {
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	enabled, err := ReadControlEnabled(ctx, tx)
	if err != nil || !enabled {
		return fmt.Errorf("%w: read control no longer enabled", ErrReadControl)
	}
	actual, err := readSelection(ctx, tx, " FOR SHARE")
	if err != nil {
		return err
	}
	if actual.VersionID != expected.VersionID || actual.ContractSHA256 != expected.ContractSHA256 || actual.ReadRevision != expected.ReadRevision {
		return fmt.Errorf("READ_SELECTION_CHANGED: stale API read target")
	}
	if err = CheckReadAdmission(ctx, tx, actual); err != nil {
		return err
	}
	if bootID != "" {
		b, err := readAPIBoot(ctx, tx, bootID)
		if err != nil {
			return err
		}
		if b.State != "RUNNING" || b.Identity.VersionID != actual.VersionID || b.Identity.ContractHash != actual.ContractSHA256 || b.Identity.ReadRevision != actual.ReadRevision || time.Since(b.HeartbeatAt) > 10*time.Second {
			return fmt.Errorf("READ_API_UNAVAILABLE: boot no longer RUNNING/current")
		}
	}
	return tx.Commit()
}
func LockGateExclusive(ctx context.Context, tx *sql.Tx) (Gate, error) {
	return readGate(ctx, tx, " FOR UPDATE")
}
func OpenReadSwitchGate(ctx context.Context, tx *sql.Tx, freezeID string) (Gate, error) {
	if _, err := tx.ExecContext(ctx, `UPDATE sync_business_write_gate SET state='OPEN',revision=revision+1,freeze_id=NULL,reason='',frozen_at=NULL,updated_at=UTC_TIMESTAMP(6) WHERE gate_id=1 AND state='FROZEN' AND freeze_id=?`, freezeID); err != nil {
		return Gate{}, err
	}
	return readGate(ctx, tx, "")
}
