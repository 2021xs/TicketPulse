package syncpipe

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"

	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

const projectionColumns = "id,organization_id,ticket_no,title,description,status,priority,category,creator_id,assignee_id,initial_team_id,current_team_id,created_at,updated_at,version"

func CurrentPosition(ctx context.Context, db *sql.DB) (Position, error) {
	var p Position
	if err := db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&p.ServerUUID); err != nil {
		return p, err
	}
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		return p, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return p, err
	}
	if len(cols) < 2 || !rows.Next() {
		return p, fmt.Errorf("MySQL binary log is unavailable")
	}
	extra := make([]sql.RawBytes, len(cols)-2)
	args := []any{&p.File, &p.Pos}
	for i := range extra {
		args = append(args, &extra[i])
	}
	if err := rows.Scan(args...); err != nil {
		return p, err
	}
	return p, rows.Err()
}

// The durable starting position is installed before snapshot reads. CDC later resumes there.
// Commits in the overlap may be published twice; external versions make that safe.
func Bootstrap(ctx context.Context, store *StateStore, q *Queue) (Checkpoint, error) {
	if err := store.VerifyQueue(ctx, q); err != nil {
		return Checkpoint{}, err
	}
	cp, err := store.Checkpoint(ctx)
	if err == sql.ErrNoRows {
		var legacyExists bool
		if err := store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_cdc_checkpoint WHERE source_name='tickets-v1')").Scan(&legacyExists); err != nil {
			return cp, err
		}
		if legacyExists {
			return cp, fmt.Errorf("KAFKA_MIGRATION_REQUIRED: retained legacy CDC checkpoint exists; run migrate-kafka before starting capture")
		}
		p, err := CurrentPosition(ctx, store.DB)
		if err != nil {
			return cp, err
		}
		cp = Checkpoint{Position: p}
		if err := store.SaveCheckpoint(ctx, p, false); err != nil {
			return cp, err
		}
	} else if err != nil {
		return cp, err
	}
	if !cp.BootstrapComplete {
		if _, err := Snapshot(ctx, store, q); err != nil {
			return cp, err
		}
		if err := store.SaveCheckpoint(ctx, cp.Position, true); err != nil {
			return cp, err
		}
		cp.BootstrapComplete = true
	}
	return cp, nil
}

// Re-runnable import. Existing documents retain their ID; it never moves the CDC checkpoint.
func Snapshot(ctx context.Context, store *StateStore, q *Queue) (int, error) {
	release, err := store.Lock(ctx, "snapshot")
	if err != nil {
		return 0, err
	}
	defer release()
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT "+projectionColumns+" FROM tickets ORDER BY id")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	run := make([]byte, 8)
	if _, err := rand.Read(run); err != nil {
		return 0, err
	}
	for rows.Next() {
		var d search.Document
		err := rows.Scan(&d.ID, &d.OrganizationID, &d.TicketNo, &d.Title, &d.Description, &d.Status, &d.Priority, &d.Category, &d.CreatorID, &d.AssigneeID, &d.InitialTeamID, &d.CurrentTeamID, &d.CreatedAt, &d.UpdatedAt, &d.Version)
		if err != nil {
			return count, err
		}
		d.PriorityRank = map[string]int{ticket.PriorityLow: 1, ticket.PriorityMedium: 2, ticket.PriorityHigh: 3, ticket.PriorityUrgent: 4}[d.Priority]
		e := Event{Schema: 1, ID: fmt.Sprintf("snapshot:%s:%x:%d:%d", store.Schema, run, d.ID, d.Version), Source: "snapshot", TicketID: d.ID, Version: d.Version, EventAt: d.UpdatedAt, Document: d}
		if _, err := q.Publish(ctx, e); err != nil {
			return count, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, err
	}
	if err := rows.Close(); err != nil {
		return count, err
	}
	return count, tx.Commit()
}
