package projection

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"
)

var ErrTargetMismatch = errors.New("PROJECTION_TARGET_MISMATCH")

// HashJSON canonicalizes MySQL's JSON representation before hashing it.
func HashJSON(data []byte) (string, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return "", fmt.Errorf("expected one JSON value")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(canonical)
	return hex.EncodeToString(hash[:]), nil
}

func (v Version) Fingerprint() string {
	data, _ := json.Marshal(v.Contract)
	hash, _ := HashJSON(data)
	return hash
}
func (v Version) CursorTarget() string { return v.ID + ":" + v.Fingerprint() }

type VersionStore struct{ DB *sql.DB }

func readVersion(ctx context.Context, q rowQuery, id string) (v Version, err error) {
	var contract []byte
	err = q.QueryRowContext(ctx, "SELECT version_id,phase,contract,validation_report,report_sha256,freeze_revision,checked_at,created_at,updated_at FROM sync_projection_versions WHERE version_id=?", id).Scan(&v.ID, &v.Phase, &contract, &v.ValidationReport, &v.ReportSHA256, &v.FreezeRevision, &v.CheckedAt, &v.CreatedAt, &v.UpdatedAt)
	if err != nil {
		return v, err
	}
	if err = json.Unmarshal(contract, &v.Contract); err != nil {
		return v, err
	}
	if err = v.Validate(); err != nil {
		return v, err
	}
	return v, nil
}
func (s *VersionStore) Get(ctx context.Context, id string) (Version, error) {
	return readVersion(ctx, s.DB, id)
}

// Insert only enrolls the first existing ACTIVE contract or adds a PREPARED
// record. There is deliberately no update/activate/promote API in M4-03A.
// Caller holds the durable frozen gate throughout live validation and insertion.
func (s *VersionStore) Insert(ctx context.Context, tx *sql.Tx, v Version) error {
	if err := v.Validate(); err != nil {
		return err
	}
	g, err := LockFrozen(ctx, tx)
	if err != nil {
		return err
	}
	if v.FreezeRevision == nil || *v.FreezeRevision != g.Revision {
		return fmt.Errorf("validation belongs to another freeze revision")
	}
	existing, err := readVersion(ctx, tx, v.ID)
	if err == nil {
		if existing.Phase == v.Phase && reflect.DeepEqual(existing.Contract, v.Contract) && existing.FreezeRevision != nil && *existing.FreezeRevision == *v.FreezeRevision {
			return nil
		}
		return fmt.Errorf("projection version ID is immutable")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	contract, err := json.Marshal(v.Contract)
	if err != nil {
		return err
	}
	var report any
	if len(v.ValidationReport) > 0 {
		report = []byte(v.ValidationReport)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO sync_projection_versions(version_id,phase,contract,validation_report,report_sha256,freeze_revision,checked_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))", v.ID, v.Phase, contract, report, v.ReportSHA256, v.FreezeRevision, v.CheckedAt)
	if err != nil {
		return err
	}
	if v.Phase == Active {
		for component, binding := range map[string]Binding{"es": v.Contract.ESWriter, "ck": v.Contract.CKWriter} {
			data, _ := json.Marshal(binding)
			if _, err = tx.ExecContext(ctx, "INSERT INTO sync_projection_progress_bindings(component,version_id,binding,created_at) VALUES(?,?,?,UTC_TIMESTAMP(6))", component, v.ID, data); err != nil {
				return err
			}
		}
	}
	return logControl(ctx, tx, "register_"+v.Phase, v.ID, map[string]any{"contract_sha256": v.Fingerprint(), "handoff_verified": false})
}

func (s *VersionStore) VerifyBinding(ctx context.Context, v Version, component string) error {
	var id string
	var data []byte
	err := s.DB.QueryRowContext(ctx, "SELECT version_id,binding FROM sync_projection_progress_bindings WHERE component=?", component).Scan(&id, &data)
	if err != nil {
		return fmt.Errorf("progress binding unavailable: %w", err)
	}
	var binding Binding
	if err = json.Unmarshal(data, &binding); err != nil {
		return err
	}
	expected := v.Contract.ESWriter
	if component == "ck" {
		expected = v.Contract.CKWriter
	}
	if id != v.ID || binding != expected {
		return fmt.Errorf("progress/status binding differs from projection version")
	}
	return nil
}

func NewCheckedVersion(id, phase string, c Contract, report json.RawMessage, freezeRevision *int64) (Version, error) {
	v := Version{ID: id, Phase: phase, Contract: c, ValidationReport: report, FreezeRevision: freezeRevision, CheckedAt: time.Now().UTC()}
	if len(report) > 0 {
		hash, err := HashJSON(report)
		if err != nil {
			return v, err
		}
		v.ReportSHA256 = &hash
	}
	return v, v.Validate()
}
