package projection

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	assets "ticketpulse/db"
)

var ErrReadControl = errors.New("READ_CONTROL_UNAVAILABLE")
var ErrReadCancelResume = errors.New("READ_CANCEL_RESUME_REQUIRED")
var readTokenPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
var readUUIDPattern = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)

const (
	ReadPromote   = "PROMOTE"
	ReadRollback  = "ROLLBACK"
	ReadPlanned   = "PLANNED"
	ReadCancelled = "CANCELLED"
	ReadCommitted = "COMMITTED"
	ReadAPIReady  = "API_READY"
	ReadResumed   = "RESUMED"
)

// Registration phase and Contract remain unchanged. Read revision advances only
// through an audited atomic read-selection transaction.
type ReadSelection struct {
	VersionID          string    `json:"version_id"`
	ContractSHA256     string    `json:"contract_sha256"`
	HandoffID          *string   `json:"handoff_id"`
	ReadRevision       int64     `json:"read_revision"`
	CurrentOperationID *string   `json:"current_operation_id"`
	UpdatedAt          time.Time `json:"updated_at"`
}

func (s ReadSelection) Validate() error {
	if !ValidID(s.VersionID) || !hashPattern.MatchString(s.ContractSHA256) || s.ReadRevision < 1 || s.HandoffID != nil && !ValidID(*s.HandoffID) || s.CurrentOperationID != nil && !ValidID(*s.CurrentOperationID) {
		return fmt.Errorf("%w: malformed read selection", ErrReadControl)
	}
	return nil
}

type ReadSwitch struct {
	ID                   string          `json:"operation_id"`
	RollbackOf           *string         `json:"rollback_of,omitempty"`
	SupersededBy         *string         `json:"superseded_by,omitempty"`
	Direction            string          `json:"direction"`
	FromVersionID        string          `json:"from_version_id"`
	ToVersionID          string          `json:"to_version_id"`
	FromContractSHA256   string          `json:"from_contract_sha256"`
	ToContractSHA256     string          `json:"to_contract_sha256"`
	CKDatabaseUUID       string          `json:"ck_database_uuid"`
	ExpectedReadRevision int64           `json:"expected_read_revision"`
	FreezeID             string          `json:"freeze_id"`
	FreezeRevision       int64           `json:"freeze_revision"`
	Stage                string          `json:"stage"`
	Revision             int64           `json:"operation_revision"`
	MarkerID             *string         `json:"marker_id"`
	MarkerIssuedAt       *time.Time      `json:"marker_issued_at"`
	Proof                json.RawMessage `json:"proof"`
	ProofSHA256          *string         `json:"proof_sha256"`
	ValidatedAt          *time.Time      `json:"validated_at"`
	LastError            *string         `json:"last_error"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
	CancelledAt          *time.Time      `json:"cancelled_at"`
}

func (o ReadSwitch) Terminal() bool { return o.Stage == ReadCancelled || o.Stage == ReadResumed }
func (o ReadSwitch) Validate() error {
	if o.RollbackOf != nil && (!ValidID(*o.RollbackOf) || *o.RollbackOf == o.ID) || o.SupersededBy != nil && (!ValidID(*o.SupersededBy) || *o.SupersededBy == o.ID || (o.Stage != ReadCommitted && o.Stage != ReadAPIReady)) {
		return fmt.Errorf("%w: malformed reverse-operation linkage", ErrReadControl)
	}
	if !ValidID(o.ID) || !ValidID(o.FromVersionID) || !ValidID(o.ToVersionID) || o.FromVersionID == o.ToVersionID || !hashPattern.MatchString(o.FromContractSHA256) || !hashPattern.MatchString(o.ToContractSHA256) || !readUUIDPattern.MatchString(o.CKDatabaseUUID) || o.CKDatabaseUUID == "00000000-0000-0000-0000-000000000000" || (o.Direction != ReadPromote && o.Direction != ReadRollback) || o.ExpectedReadRevision < 1 || !readTokenPattern.MatchString(o.FreezeID) || o.FreezeRevision < 1 || o.Revision < 1 {
		return fmt.Errorf("%w: malformed read operation", ErrReadControl)
	}
	switch o.Stage {
	case ReadPlanned, ReadCancelled, ReadCommitted, ReadAPIReady, ReadResumed:
	default:
		return fmt.Errorf("%w: unknown read stage", ErrReadControl)
	}
	if (o.MarkerID == nil) != (o.MarkerIssuedAt == nil) || o.MarkerID != nil && (!readTokenPattern.MatchString(*o.MarkerID) || o.MarkerIssuedAt.IsZero()) || (o.Stage == ReadCancelled) != (o.CancelledAt != nil) {
		return fmt.Errorf("%w: inconsistent marker/cancel metadata", ErrReadControl)
	}
	if len(o.Proof) == 0 {
		if o.ProofSHA256 != nil || o.ValidatedAt != nil {
			return fmt.Errorf("%w: partial proof metadata", ErrReadControl)
		}
	} else {
		if o.ProofSHA256 == nil || o.ValidatedAt == nil || o.MarkerID == nil {
			return fmt.Errorf("%w: incomplete proof", ErrReadControl)
		}
		h, err := HashJSON(o.Proof)
		if err != nil || h != *o.ProofSHA256 {
			return fmt.Errorf("%w: proof hash differs", ErrReadControl)
		}
		var p struct {
			OperationID    string          `json:"operation_id"`
			FreezeID       string          `json:"freeze_id"`
			FreezeRevision int64           `json:"freeze_revision"`
			FromVersionID  string          `json:"from_version_id"`
			ToVersionID    string          `json:"to_version_id"`
			FromHash       string          `json:"from_contract_sha256"`
			ToHash         string          `json:"to_contract_sha256"`
			ReadRevision   int64           `json:"read_revision"`
			RollbackOf     *string         `json:"rollback_of,omitempty"`
			Marker         string          `json:"marker_id"`
			CKDatabaseUUID string          `json:"ck_database_uuid"`
			MarkerIssuedAt time.Time       `json:"marker_issued_at"`
			ValidatedAt    time.Time       `json:"validated_at"`
			Report         json.RawMessage `json:"reconcile_report"`
			ReportSHA256   string          `json:"reconcile_report_sha256"`
		}
		if json.Unmarshal(o.Proof, &p) != nil || p.OperationID != o.ID || p.FreezeID != o.FreezeID || p.FreezeRevision != o.FreezeRevision || p.FromVersionID != o.FromVersionID || p.ToVersionID != o.ToVersionID || p.FromHash != o.FromContractSHA256 || p.ToHash != o.ToContractSHA256 || p.ReadRevision != o.ExpectedReadRevision || p.Marker != *o.MarkerID || p.CKDatabaseUUID != o.CKDatabaseUUID {
			return fmt.Errorf("%w: proof belongs to another operation", ErrReadControl)
		}
		if !sameOptionalID(p.RollbackOf, o.RollbackOf) {
			return fmt.Errorf("%w: reverse proof link differs", ErrReadControl)
		}
		reportHash, err := HashJSON(p.Report)
		var report struct {
			State           string           `json:"state"`
			DifferenceCount int64            `json:"difference_count"`
			SourceChanged   bool             `json:"source_changed"`
			SourceAfter     *json.RawMessage `json:"source_after"`
			ES              *json.RawMessage `json:"es"`
			CK              *json.RawMessage `json:"ck"`
		}
		if err != nil || reportHash != p.ReportSHA256 || json.Unmarshal(p.Report, &report) != nil || report.State != "consistent" || report.DifferenceCount != 0 || report.SourceChanged || report.SourceAfter == nil || report.ES == nil || report.CK == nil || !p.MarkerIssuedAt.Equal(*o.MarkerIssuedAt) || p.ValidatedAt.IsZero() || p.ValidatedAt.Before(p.MarkerIssuedAt) || p.ValidatedAt.Sub(*o.ValidatedAt) > time.Microsecond || o.ValidatedAt.Sub(p.ValidatedAt) > time.Microsecond {
			return fmt.Errorf("%w: proof report/times differ", ErrReadControl)
		}
	}
	return nil
}

type ReadControlStore struct{ DB *sql.DB }

// Installation does not enable read selection. Keep provenance in the existing
// 011 log so loss of a 013 table cannot silently disable an enabled feature.
func EnsureReadControlSchema(ctx context.Context, db *sql.DB) error {
	var prerequisites int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN ('sync_business_write_gate','sync_projection_versions','sync_projection_control_log','sync_projection_handoffs')").Scan(&prerequisites); err != nil {
		return err
	}
	if prerequisites != 4 {
		return fmt.Errorf("013 requires explicitly installed 011/012")
	}
	for _, statement := range strings.Split(assets.ProjectionReadControlMigration, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	_, err := db.ExecContext(ctx, `INSERT INTO sync_projection_control_log(operation,reference_id,details,occurred_at) SELECT 'install_read_control','013_projection_read_control',JSON_OBJECT('schema',13),UTC_TIMESTAMP(6) WHERE NOT EXISTS(SELECT 1 FROM sync_projection_control_log existing WHERE existing.operation='install_read_control')`)
	return err
}

func ReadControlEnabled(ctx context.Context, q rowQuery) (bool, error) {
	var enabled int
	err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_control_log WHERE operation='enable_read_control'").Scan(&enabled)
	if err != nil && !MissingTable(err) {
		return false, fmt.Errorf("%w: %v", ErrReadControl, err)
	}
	if enabled > 0 {
		return true, nil
	}
	// An uninitialized installation has two empty tables. Nonempty control
	// records without enable provenance are damaged metadata, never legacy.
	for _, table := range []string{"sync_projection_read_selection", "sync_projection_read_switches", "sync_projection_read_execution", "sync_projection_api_boots"} {
		var count int
		err = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
		if MissingTable(err) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("%w: %v", ErrReadControl, err)
		}
		if count > 0 {
			return false, fmt.Errorf("%w: records without enable provenance", ErrReadControl)
		}
	}
	return false, nil
}

func readSelection(ctx context.Context, q rowQuery, lock string) (s ReadSelection, err error) {
	err = q.QueryRowContext(ctx, `SELECT version_id,contract_sha256,handoff_id,read_revision,current_operation_id,updated_at FROM sync_projection_read_selection WHERE selector_id=1`+lock).Scan(&s.VersionID, &s.ContractSHA256, &s.HandoffID, &s.ReadRevision, &s.CurrentOperationID, &s.UpdatedAt)
	if err != nil {
		return s, fmt.Errorf("%w: read selection: %v", ErrReadControl, err)
	}
	if err = s.Validate(); err != nil {
		return s, err
	}
	v, err := readVersion(ctx, q, s.VersionID)
	if err != nil || v.Fingerprint() != s.ContractSHA256 {
		return s, fmt.Errorf("%w: selection contract differs: %v", ErrReadControl, err)
	}
	initialID, initialHash, err := readSelectionChain(ctx, q, s)
	if err != nil {
		return s, err
	}
	var enabled int
	if err = q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_projection_control_log WHERE operation='enable_read_control' AND reference_id=? AND JSON_UNQUOTE(JSON_EXTRACT(details,'$.contract_sha256'))=? AND JSON_EXTRACT(details,'$.read_revision')=1`, initialID, initialHash).Scan(&enabled); err != nil || enabled != 1 {
		return s, fmt.Errorf("%w: selection lacks exact enable provenance: %v", ErrReadControl, err)
	}
	return s, nil
}
func (s *ReadControlStore) Selection(ctx context.Context) (ReadSelection, error) {
	return readSelection(ctx, s.DB, "")
}
func (s *ReadControlStore) ReadSelectionIn(ctx context.Context, tx *sql.Tx) (ReadSelection, error) {
	return readSelection(ctx, tx, "")
}
func (s *ReadControlStore) LockSelection(ctx context.Context, tx *sql.Tx) (ReadSelection, error) {
	return readSelection(ctx, tx, " FOR UPDATE")
}
func (s *ReadControlStore) ShareSelection(ctx context.Context, tx *sql.Tx) (ReadSelection, error) {
	return readSelection(ctx, tx, " FOR SHARE")
}

func readSwitch(ctx context.Context, q rowQuery, id, lock string) (o ReadSwitch, err error) {
	var proof []byte
	err = q.QueryRowContext(ctx, `SELECT operation_id,direction,from_version_id,to_version_id,from_contract_sha256,to_contract_sha256,ck_database_uuid,expected_read_revision,freeze_id,freeze_revision,stage,operation_revision,marker_id,marker_issued_at,proof,proof_sha256,validated_at,last_error,created_at,updated_at,cancelled_at FROM sync_projection_read_switches WHERE operation_id=?`+lock, id).Scan(&o.ID, &o.Direction, &o.FromVersionID, &o.ToVersionID, &o.FromContractSHA256, &o.ToContractSHA256, &o.CKDatabaseUUID, &o.ExpectedReadRevision, &o.FreezeID, &o.FreezeRevision, &o.Stage, &o.Revision, &o.MarkerID, &o.MarkerIssuedAt, &proof, &o.ProofSHA256, &o.ValidatedAt, &o.LastError, &o.CreatedAt, &o.UpdatedAt, &o.CancelledAt)
	o.Proof = proof
	if err != nil {
		return o, err
	}
	installed, e := ReadRollbackInstalled(ctx, q)
	if e != nil {
		return o, e
	}
	if installed {
		if err = q.QueryRowContext(ctx, "SELECT rollback_of,superseded_by FROM sync_projection_read_switches WHERE operation_id=?", id).Scan(&o.RollbackOf, &o.SupersededBy); err != nil {
			return o, err
		}
	}
	return o, o.Validate()
}
func (s *ReadControlStore) Operation(ctx context.Context, id string) (ReadSwitch, error) {
	return readSwitch(ctx, s.DB, id, "")
}
func (s *ReadControlStore) ReadOperationIn(ctx context.Context, tx *sql.Tx, id string) (ReadSwitch, error) {
	return readSwitch(ctx, tx, id, "")
}
func (s *ReadControlStore) LockOperation(ctx context.Context, tx *sql.Tx, id string) (ReadSwitch, error) {
	return readSwitch(ctx, tx, id, " FOR UPDATE")
}

func (s *ReadControlStore) Initialize(ctx context.Context, tx *sql.Tx, v Version) (ReadSelection, error) {
	if v.Phase != Active {
		return ReadSelection{}, fmt.Errorf("read initialization requires original registered ACTIVE")
	}
	if err := v.Validate(); err != nil {
		return ReadSelection{}, err
	}
	var installed int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_control_log WHERE operation='install_read_control'").Scan(&installed); err != nil || installed == 0 {
		return ReadSelection{}, fmt.Errorf("read control initialization requires completed explicit 013 installation: %v", err)
	}
	var operations int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_read_switches").Scan(&operations); err != nil {
		return ReadSelection{}, fmt.Errorf("%w: 013 operation table unavailable: %v", ErrReadControl, err)
	}
	enabled, err := ReadControlEnabled(ctx, tx)
	if err != nil {
		return ReadSelection{}, err
	}
	if enabled {
		selected, err := readSelection(ctx, tx, " FOR UPDATE")
		if err != nil {
			return selected, err
		}
		if selected.VersionID != v.ID || selected.ContractSHA256 != v.Fingerprint() || selected.HandoffID != nil {
			return selected, fmt.Errorf("read control already initialized differently")
		}
		return selected, nil
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO sync_projection_read_selection(selector_id,version_id,contract_sha256,read_revision,updated_at) VALUES(1,?,?,1,UTC_TIMESTAMP(6))`, v.ID, v.Fingerprint()); err != nil {
		return ReadSelection{}, err
	}
	if err = logControl(ctx, tx, "enable_read_control", v.ID, map[string]any{"contract_sha256": v.Fingerprint(), "read_revision": 1}); err != nil {
		return ReadSelection{}, err
	}
	return readSelection(ctx, tx, "")
}

// Shared by Begin, Cancel and proof saving. The gate lock is acquired first.
func MatchPlanned(g Gate, s ReadSelection, o ReadSwitch) error {
	if o.Stage != ReadPlanned || g.State != "FROZEN" || g.FreezeID == nil || *g.FreezeID != o.FreezeID || g.Revision != o.FreezeRevision || s.VersionID != o.FromVersionID || s.ContractSHA256 != o.FromContractSHA256 || s.ReadRevision != o.ExpectedReadRevision || s.CurrentOperationID == nil || *s.CurrentOperationID != o.ID {
		return fmt.Errorf("READ_SWITCH_STATE_CHANGED: selection/freeze/operation differs")
	}
	return nil
}

// Called while Resume holds the exclusive gate. Validate even terminal rows so
// corrupted metadata cannot convert an incomplete operation into permission.
func CheckReadResume(ctx context.Context, tx *sql.Tx, g Gate) error {
	enabled, err := ReadControlEnabled(ctx, tx)
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	s, err := readSelection(ctx, tx, " FOR UPDATE")
	if err != nil {
		return err
	}
	v, err := readVersion(ctx, tx, s.VersionID)
	if err != nil || v.Fingerprint() != s.ContractSHA256 {
		return fmt.Errorf("%w: selected contract unavailable/different: %v", ErrReadControl, err)
	}
	if v.Phase == Active && s.HandoffID != nil || v.Phase == Prepared && s.HandoffID == nil {
		return fmt.Errorf("%w: selected state scope differs", ErrReadControl)
	}
	rows, err := tx.QueryContext(ctx, "SELECT operation_id FROM sync_projection_read_switches ORDER BY operation_id")
	if err != nil {
		return fmt.Errorf("%w: operations: %v", ErrReadControl, err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	foundCurrent := false
	cancelNeedsLiveCheck := false
	for _, id := range ids {
		o, err := readSwitch(ctx, tx, id, " FOR UPDATE")
		if err != nil {
			return fmt.Errorf("%w: %v", ErrReadControl, err)
		}
		if o.Stage != ReadPlanned && o.Stage != ReadCancelled && o.Stage != ReadResumed {
			if o.SupersededBy != nil {
				next, e := readSwitch(ctx, tx, *o.SupersededBy, "")
				if e != nil || next.RollbackOf == nil || *next.RollbackOf != o.ID || !next.Terminal() || next.Stage == ReadCancelled {
					return fmt.Errorf("READ_SWITCH_INCOMPLETE: unfinished explicit reverse recovery")
				}
				if _, e = checkExecutionStage(ctx, tx, o); e != nil {
					return e
				}
				continue
			}
			return fmt.Errorf("READ_SWITCH_INCOMPLETE: read-switch stage %s requires explicit completion", o.Stage)
		}
		if o.Stage == ReadResumed {
			if _, e := checkExecutionStage(ctx, tx, o); e != nil {
				return e
			}
		}
		from, e := readVersion(ctx, tx, o.FromVersionID)
		to, e2 := readVersion(ctx, tx, o.ToVersionID)
		if e != nil || e2 != nil || from.Fingerprint() != o.FromContractSHA256 || to.Fingerprint() != o.ToContractSHA256 {
			return fmt.Errorf("%w: operation contract differs", ErrReadControl)
		}
		if s.CurrentOperationID != nil && *s.CurrentOperationID == id {
			foundCurrent = true
		}
		if o.Stage == ReadCancelled {
			var record []byte
			err = tx.QueryRowContext(ctx, `SELECT details FROM sync_projection_control_log WHERE operation='read_switch_cancelled' AND reference_id=?`, o.ID).Scan(&record)
			actual, e := HashJSON(record)
			data, _ := json.Marshal(o)
			expected, e2 := HashJSON(data)
			if err != nil || e != nil || e2 != nil || actual != expected {
				return fmt.Errorf("%w: cancellation lacks exact committed control record", ErrReadControl)
			}
			if o.RollbackOf != nil && g.FreezeID != nil && *g.FreezeID == o.FreezeID && g.Revision == o.FreezeRevision {
				cancelNeedsLiveCheck = true
			}
		}
		if !o.Terminal() {
			if g.FreezeID == nil || *g.FreezeID != o.FreezeID || g.Revision != o.FreezeRevision {
				return fmt.Errorf("READ_SWITCH_FREEZE_CHANGED: pending operation differs from freeze")
			}
			return fmt.Errorf("READ_SWITCH_INCOMPLETE: explicit cancellation or future read-switch completion required")
		}
	}
	if s.CurrentOperationID != nil {
		if !foundCurrent {
			return fmt.Errorf("%w: referenced operation missing", ErrReadControl)
		}
		o, e := readSwitch(ctx, tx, *s.CurrentOperationID, "")
		if e != nil {
			return e
		}
		if o.Stage != ReadResumed || s.VersionID != o.ToVersionID || s.ReadRevision != o.ExpectedReadRevision+1 {
			return fmt.Errorf("%w: invalid terminal selection reference", ErrReadControl)
		}
	}
	if cancelNeedsLiveCheck {
		return fmt.Errorf("%w: live selected API must be revalidated by explicit control", ErrReadCancelResume)
	}
	return nil
}

func LogReadControl(ctx context.Context, tx *sql.Tx, operation, id string, details any) error {
	return logControl(ctx, tx, operation, id, details)
}
