package projection

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	assets "ticketpulse/db"
	"time"
)

type ReadExecution struct {
	OperationID    string          `json:"operation_id"`
	DrainScope     json.RawMessage `json:"drain_scope"`
	DrainScopeHash *string         `json:"drain_scope_sha256"`
	Drained        json.RawMessage `json:"drained"`
	DrainedHash    *string         `json:"drained_sha256"`
	Committed      json.RawMessage `json:"committed"`
	CommittedHash  *string         `json:"committed_sha256"`
	Readiness      json.RawMessage `json:"readiness"`
	ReadinessHash  *string         `json:"readiness_sha256"`
	Resumed        json.RawMessage `json:"resumed"`
	ResumedHash    *string         `json:"resumed_sha256"`
	Revision       int64           `json:"revision"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

func EnsureReadRuntimeSchema(ctx context.Context, db *sql.DB) error {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_control_log WHERE operation='install_read_control'").Scan(&count); err != nil || count == 0 {
		return fmt.Errorf("014 requires explicit 013 installation: %v", err)
	}
	for _, statement := range strings.Split(assets.ProjectionReadRuntimeMigration, ";") {
		if strings.TrimSpace(statement) != "" {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	}
	_, err := db.ExecContext(ctx, `INSERT INTO sync_projection_control_log(operation,reference_id,details,occurred_at) SELECT 'install_read_runtime','014_projection_read_runtime',JSON_OBJECT('schema',14),UTC_TIMESTAMP(6) WHERE NOT EXISTS(SELECT 1 FROM sync_projection_control_log existing WHERE existing.operation='install_read_runtime')`)
	return err
}
func readExecution(ctx context.Context, q rowQuery, id, lock string) (e ReadExecution, err error) {
	e.OperationID = id
	var scope, drain, commit, ready, resume []byte
	err = q.QueryRowContext(ctx, `SELECT drain_scope,drain_scope_sha256,drained,drained_sha256,committed,committed_sha256,readiness,readiness_sha256,resumed,resumed_sha256,revision,updated_at FROM sync_projection_read_execution WHERE operation_id=?`+lock, id).Scan(&scope, &e.DrainScopeHash, &drain, &e.DrainedHash, &commit, &e.CommittedHash, &ready, &e.ReadinessHash, &resume, &e.ResumedHash, &e.Revision, &e.UpdatedAt)
	e.DrainScope, e.Drained, e.Committed, e.Readiness, e.Resumed = scope, drain, commit, ready, resume
	if err != nil {
		return e, fmt.Errorf("%w: execution: %v", ErrReadControl, err)
	}
	if e.Revision < 1 {
		return e, ErrReadControl
	}
	for _, item := range []struct {
		data json.RawMessage
		hash *string
	}{{e.DrainScope, e.DrainScopeHash}, {e.Drained, e.DrainedHash}, {e.Committed, e.CommittedHash}, {e.Readiness, e.ReadinessHash}, {e.Resumed, e.ResumedHash}} {
		if (len(item.data) == 0) != (item.hash == nil) {
			return e, fmt.Errorf("%w: partial execution evidence", ErrReadControl)
		}
		if item.hash != nil {
			h, err := HashJSON(item.data)
			if err != nil || h != *item.hash {
				return e, fmt.Errorf("%w: execution hash differs", ErrReadControl)
			}
		}
	}
	return e, nil
}
func (s *ReadControlStore) Execution(ctx context.Context, id string) (ReadExecution, error) {
	return readExecution(ctx, s.DB, id, "")
}
func (s *ReadControlStore) LockExecution(ctx context.Context, tx *sql.Tx, id string) (ReadExecution, error) {
	return readExecution(ctx, tx, id, " FOR UPDATE")
}
func (s *ReadControlStore) ExecutionIn(ctx context.Context, tx *sql.Tx, id string) (ReadExecution, error) {
	return readExecution(ctx, tx, id, "")
}

// Exact transition logs prove a submitted stage. Editing a stage or selector
// without the transaction's complete evidence cannot authorize reading/resume.
func checkExecutionStage(ctx context.Context, q rowQuery, o ReadSwitch) (ReadExecution, error) {
	e, err := readExecution(ctx, q, o.ID, "")
	if err != nil {
		return e, err
	}
	if len(e.Committed) == 0 || o.MarkerID == nil || o.ProofSHA256 == nil {
		return e, fmt.Errorf("%w: missing commit evidence", ErrReadControl)
	}
	var commit struct {
		OperationID    string  `json:"operation_id"`
		VersionID      string  `json:"version_id"`
		Hash           string  `json:"contract_sha256"`
		ReadRevision   int64   `json:"read_revision"`
		FreezeID       string  `json:"freeze_id"`
		FreezeRevision int64   `json:"freeze_revision"`
		ProofHash      string  `json:"proof_sha256"`
		RollbackOf     *string `json:"rollback_of,omitempty"`
	}
	if json.Unmarshal(e.Committed, &commit) != nil || commit.OperationID != o.ID || commit.VersionID != o.ToVersionID || commit.Hash != o.ToContractSHA256 || commit.ReadRevision != o.ExpectedReadRevision+1 || commit.FreezeID != o.FreezeID || commit.FreezeRevision != o.FreezeRevision || commit.ProofHash != *o.ProofSHA256 {
		return e, fmt.Errorf("%w: commit linkage differs", ErrReadControl)
	}
	if !sameOptionalID(commit.RollbackOf, o.RollbackOf) {
		return e, fmt.Errorf("%w: reverse audit linkage differs", ErrReadControl)
	}
	if o.Stage == ReadAPIReady || o.Stage == ReadResumed {
		var ready struct {
			OperationID    string `json:"operation_id"`
			VersionID      string `json:"version_id"`
			ReadRevision   int64  `json:"read_revision"`
			FreezeID       string `json:"freeze_id"`
			FreezeRevision int64  `json:"freeze_revision"`
			API            struct {
				Identity APIIdentity `json:"identity"`
			} `json:"api"`
		}
		if json.Unmarshal(e.Readiness, &ready) != nil || ready.OperationID != o.ID || ready.VersionID != o.ToVersionID || ready.ReadRevision != o.ExpectedReadRevision+1 || ready.FreezeID != o.FreezeID || ready.FreezeRevision != o.FreezeRevision || ready.API.Identity.VersionID != ready.VersionID || ready.API.Identity.ReadRevision != ready.ReadRevision || !readTokenPattern.MatchString(ready.API.Identity.BootID) {
			return e, fmt.Errorf("%w: readiness belongs to another boundary", ErrReadControl)
		}
	}
	if o.Stage == ReadResumed {
		var resumed struct {
			OperationID    string `json:"operation_id"`
			VersionID      string `json:"version_id"`
			ReadRevision   int64  `json:"read_revision"`
			FreezeID       string `json:"freeze_id"`
			FreezeRevision int64  `json:"freeze_revision"`
			BootID         string `json:"boot_id"`
			Gate           Gate   `json:"gate"`
		}
		if json.Unmarshal(e.Resumed, &resumed) != nil || resumed.OperationID != o.ID || resumed.VersionID != o.ToVersionID || resumed.ReadRevision != o.ExpectedReadRevision+1 || resumed.FreezeID != o.FreezeID || resumed.FreezeRevision != o.FreezeRevision || resumed.Gate.State != "OPEN" || resumed.Gate.Revision != o.FreezeRevision+1 || !readTokenPattern.MatchString(resumed.BootID) {
			return e, fmt.Errorf("%w: Resume evidence differs", ErrReadControl)
		}
	}
	for _, item := range []struct {
		operation string
		data      json.RawMessage
		required  bool
	}{{"read_switch_committed", e.Committed, true}, {"read_switch_api_ready", e.Readiness, o.Stage == ReadAPIReady || o.Stage == ReadResumed}, {"read_switch_resumed", e.Resumed, o.Stage == ReadResumed}} {
		if !item.required {
			continue
		}
		if len(item.data) == 0 {
			return e, fmt.Errorf("%w: missing %s", ErrReadControl, item.operation)
		}
		h, _ := HashJSON(item.data)
		var records int
		err = q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_projection_control_log WHERE operation=? AND reference_id=? AND JSON_UNQUOTE(JSON_EXTRACT(details,'$.evidence_sha256'))=?`, item.operation, o.ID, h).Scan(&records)
		if err != nil || records != 1 {
			return e, fmt.Errorf("%w: missing exact %s audit", ErrReadControl, item.operation)
		}
		var logged []byte
		if err = q.QueryRowContext(ctx, `SELECT JSON_EXTRACT(details,'$.evidence') FROM sync_projection_control_log WHERE operation=? AND reference_id=? AND JSON_UNQUOTE(JSON_EXTRACT(details,'$.evidence_sha256'))=?`, item.operation, o.ID, h).Scan(&logged); err != nil {
			return e, err
		}
		actual, err := HashJSON(logged)
		if err != nil || actual != h {
			return e, fmt.Errorf("%w: transition audit bytes differ", ErrReadControl)
		}
	}
	return e, nil
}
