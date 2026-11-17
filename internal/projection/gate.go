package projection

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	assets "ticketpulse/db"
	"ticketpulse/internal/ticket"
)

type Gate struct {
	State     string     `json:"state"`
	Revision  int64      `json:"revision"`
	FreezeID  *string    `json:"freeze_id"`
	Reason    string     `json:"reason"`
	FrozenAt  *time.Time `json:"frozen_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

func MissingTable(err error) bool {
	var e *mysql.MySQLError
	return errors.As(err, &e) && e.Number == 1146
}

// AcquireBusinessWrite must be the first locking read in every business
// transaction. FOR SHARE is a current read and is held through COMMIT/ROLLBACK.
// Legacy schemas without migration 011 remain usable. Once a Store has seen
// the gate, deletion or query failure can never downgrade it to legacy mode.
func AcquireBusinessWrite(ctx context.Context, tx *sql.Tx, required bool) (bool, error) {
	var state string
	err := tx.QueryRowContext(ctx, "SELECT state FROM sync_business_write_gate WHERE gate_id=1 FOR SHARE").Scan(&state)
	if MissingTable(err) && !required {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("business write gate unavailable: %w", err)
	}
	if state == "FROZEN" {
		return true, ticket.ErrWritesFrozen
	}
	if state != "OPEN" {
		return true, fmt.Errorf("invalid business write gate state")
	}
	return true, nil
}

// Migration is explicit. Install it before admitting business writers, and
// restart legacy binaries when opting in; it cannot fence arbitrary SQL clients.
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	for _, statement := range strings.Split(assets.ProjectionControlMigration, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	_, err := ReadGate(ctx, db)
	return err
}

type rowQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readGate(ctx context.Context, q rowQuery, lock string) (g Gate, err error) {
	err = q.QueryRowContext(ctx, "SELECT state,revision,freeze_id,reason,frozen_at,updated_at FROM sync_business_write_gate WHERE gate_id=1"+lock).Scan(&g.State, &g.Revision, &g.FreezeID, &g.Reason, &g.FrozenAt, &g.UpdatedAt)
	return
}
func ReadGate(ctx context.Context, q rowQuery) (Gate, error) { return readGate(ctx, q, "") }

func LockFrozen(ctx context.Context, tx *sql.Tx) (Gate, error) {
	g, err := readGate(ctx, tx, " FOR SHARE")
	if err != nil {
		return g, err
	}
	if g.State != "FROZEN" || g.FreezeID == nil {
		return g, fmt.Errorf("preparation requires a durable business freeze")
	}
	return g, nil
}

func (g Gate) Valid() bool {
	if g.Revision < 0 {
		return false
	}
	if g.State == "OPEN" {
		return g.FreezeID == nil && g.FrozenAt == nil
	}
	return g.State == "FROZEN" && g.FreezeID != nil && *g.FreezeID != "" && g.FrozenAt != nil
}

func logControl(ctx context.Context, tx *sql.Tx, operation, id string, details any) error {
	data, err := json.Marshal(details)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO sync_projection_control_log(operation,reference_id,details,occurred_at) VALUES(?,?,?,UTC_TIMESTAMP(6))", operation, id, data)
	return err
}

func Freeze(ctx context.Context, db *sql.DB, reason string) (Gate, error) {
	if strings.TrimSpace(reason) == "" || len([]rune(reason)) > 500 {
		return Gate{}, fmt.Errorf("freeze requires a reason of 1–500 characters")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Gate{}, err
	}
	defer tx.Rollback()
	// The exclusive gate lock cannot be obtained until every admitted business
	// transaction has ended. New writers wait and then observe FROZEN.
	g, err := readGate(ctx, tx, " FOR UPDATE")
	if err != nil {
		return g, err
	}
	if g.State != "OPEN" && g.State != "FROZEN" {
		return g, fmt.Errorf("invalid business write gate state")
	}
	if g.State != "FROZEN" {
		var raw [16]byte
		if _, err = rand.Read(raw[:]); err != nil {
			return Gate{}, err
		}
		id := hex.EncodeToString(raw[:])
		if _, err = tx.ExecContext(ctx, "UPDATE sync_business_write_gate SET state='FROZEN',revision=revision+1,freeze_id=?,reason=?,frozen_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) WHERE gate_id=1", id, reason); err != nil {
			return Gate{}, err
		}
		g, err = readGate(ctx, tx, "")
		if err != nil {
			return g, err
		}
		if err = logControl(ctx, tx, "freeze", id, g); err != nil {
			return g, err
		}
	}
	if err = tx.Commit(); err != nil {
		return Gate{}, fmt.Errorf("freeze result unknown; read durable gate before retry: %w", err)
	}
	return verifyGate(ctx, db, g)
}

func Resume(ctx context.Context, db *sql.DB, freezeID string) (Gate, error) {
	if freezeID == "" {
		return Gate{}, fmt.Errorf("resume requires the exact freeze_id")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Gate{}, err
	}
	defer tx.Rollback()
	g, err := readGate(ctx, tx, " FOR UPDATE")
	if err != nil {
		return g, err
	}
	if g.State != "FROZEN" || g.FreezeID == nil || *g.FreezeID != freezeID {
		return g, fmt.Errorf("freeze identity changed; resume rejected")
	}
	// Holding the exclusive gate prevents a new handoff intent from racing this
	// resume. A crash cannot clear an incomplete handoff's maintenance boundary.
	var pending int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_handoffs WHERE phase<>'VERIFIED'").Scan(&pending)
	if err != nil && !MissingTable(err) {
		return g, err
	}
	if MissingTable(err) {
		var installed bool
		if installed, err = HandoffInstalled(ctx, tx); err != nil {
			return g, err
		}
		if installed {
			return g, fmt.Errorf("HANDOFF_METADATA_UNAVAILABLE: installed handoff table unavailable; resume refused")
		}
	}
	if pending != 0 {
		return g, fmt.Errorf("HANDOFF_INCOMPLETE: business resume requires VERIFIED handoff")
	}
	if err = CheckReadResume(ctx, tx, g); err != nil {
		return g, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sync_business_write_gate SET state='OPEN',revision=revision+1,freeze_id=NULL,reason='',frozen_at=NULL,updated_at=UTC_TIMESTAMP(6) WHERE gate_id=1"); err != nil {
		return Gate{}, err
	}
	g, err = readGate(ctx, tx, "")
	if err != nil {
		return g, err
	}
	if err = logControl(ctx, tx, "resume", freezeID, g); err != nil {
		return g, err
	}
	if err = tx.Commit(); err != nil {
		return Gate{}, fmt.Errorf("resume result unknown; inspect durable gate: %w", err)
	}
	return verifyGate(ctx, db, g)
}
func verifyGate(ctx context.Context, db *sql.DB, expected Gate) (Gate, error) {
	actual, err := ReadGate(ctx, db)
	if err != nil {
		return Gate{}, err
	}
	if actual.State != expected.State || actual.Revision != expected.Revision {
		return actual, fmt.Errorf("gate changed before confirmation")
	}
	return actual, nil
}
