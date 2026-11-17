package mysqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"ticketpulse/internal/ticket"
)

// The caller's transaction owns both the ticket write and its immutable history snapshot.
func insertHistory(ctx context.Context, tx *sql.Tx, operatorID int64, action string, before *ticket.State, after ticket.Ticket) error {
	return insertActionHistory(ctx, tx, operatorID, action, before, after, nil, nil)
}

func insertActionHistory(ctx context.Context, tx *sql.Tx, operatorID int64, action string, before *ticket.State, after ticket.Ticket, reason *string, commentID *int64) error {
	var beforeJSON []byte
	var err error
	if before != nil {
		beforeJSON, err = json.Marshal(before)
		if err != nil {
			return fmt.Errorf("encode before state: %w", err)
		}
	}
	afterJSON, err := json.Marshal(after.State())
	if err != nil {
		return fmt.Errorf("encode after state: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO ticket_histories (organization_id, ticket_id, action, operator_id, version, before_state, after_state, responsible_team_id, occurred_at, reason, comment_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		after.OrganizationID, after.ID, action, operatorID, after.Version, beforeJSON, afterJSON, after.CurrentTeamID, after.UpdatedAt, reason, commentID)
	return err
}
