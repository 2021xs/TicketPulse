package mysqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"ticketpulse/internal/ticket"
)

func (s *Store) Create(ctx context.Context, userID int64, request ticket.CreateRequest, hash [32]byte, candidates []int64) (ticket.CreateResult, error) {
	// READ COMMITTED ensures a duplicate-key loser sees the winner after waiting for its commit.
	// All statements, including team validity and the result reservation, share this transaction.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ticket.CreateResult{}, fmt.Errorf("begin create: %w", err)
	}
	defer tx.Rollback()
	if err := s.acquireBusinessWrite(ctx, tx); err != nil {
		return ticket.CreateResult{}, err
	}
	user, err := loadUser(ctx, tx, userID, true)
	if err != nil {
		return ticket.CreateResult{}, err
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	reservation, err := tx.ExecContext(ctx, `INSERT INTO ticket_create_requests (organization_id, creator_id, request_id, payload_hash, created_at) VALUES (?, ?, ?, ?, ?)`, user.OrganizationID, user.ID, request.RequestID, hash[:], now)
	if err != nil {
		var duplicate *mysql.MySQLError
		if !errors.As(err, &duplicate) || duplicate.Number != 1062 {
			return ticket.CreateResult{}, fmt.Errorf("reserve request: %w", err)
		}
		result, err := replay(ctx, tx, user, request.RequestID, hash)
		if err != nil {
			return ticket.CreateResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return ticket.CreateResult{}, fmt.Errorf("commit replay: %w", err)
		}
		return result, nil
	}
	id, err := reservation.LastInsertId()
	if err != nil || id <= 0 {
		return ticket.CreateResult{}, fmt.Errorf("allocate ticket id: %v", err)
	}
	team, err := chooseTeam(ctx, tx, user.OrganizationID, candidates)
	if err != nil {
		return ticket.CreateResult{}, err
	}
	t := ticket.Ticket{ID: id, OrganizationID: user.OrganizationID, TicketNo: fmt.Sprintf("TP-%012d", id),
		Title: request.Title, Description: request.Description, Category: request.Category, Priority: request.Priority,
		Status: ticket.StatusPending, CreatorID: user.ID, InitialTeamID: team.ID, CurrentTeamID: team.ID, Version: 1, CreatedAt: now, UpdatedAt: now}
	_, err = tx.ExecContext(ctx, `INSERT INTO tickets (`+ticketColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.OrganizationID, t.TicketNo, t.Title, t.Description, t.ResolutionNote, t.Category, t.Priority, t.Status, t.CreatorID, t.AssigneeID, t.InitialTeamID, t.CurrentTeamID, t.Version, t.CreatedAt, t.UpdatedAt, t.FirstResolvedAt, t.ClosedAt, t.ReopenCount)
	if err != nil {
		return ticket.CreateResult{}, fmt.Errorf("insert ticket: %w", err)
	}
	if err := insertHistory(ctx, tx, user.ID, ticket.ActionCreated, nil, t); err != nil {
		return ticket.CreateResult{}, fmt.Errorf("insert creation history: %w", err)
	}
	updated, err := tx.ExecContext(ctx, "UPDATE ticket_create_requests SET ticket_id = ? WHERE id = ? AND ticket_id IS NULL", t.ID, id)
	if err != nil {
		return ticket.CreateResult{}, fmt.Errorf("finalize request: %w", err)
	}
	if count, err := updated.RowsAffected(); err != nil || count != 1 {
		return ticket.CreateResult{}, fmt.Errorf("request finalization did not update one row")
	}
	if err := tx.Commit(); err != nil {
		return ticket.CreateResult{}, fmt.Errorf("%w: %w", ticket.ErrCreateResultUnknown, err)
	}
	return ticket.CreateResult{Ticket: t}, nil
}

func replay(ctx context.Context, tx *sql.Tx, user ticket.User, requestID string, hash [32]byte) (ticket.CreateResult, error) {
	var originalHash []byte
	var ticketID sql.NullInt64
	err := tx.QueryRowContext(ctx, "SELECT payload_hash, ticket_id FROM ticket_create_requests WHERE organization_id = ? AND creator_id = ? AND request_id = ?", user.OrganizationID, user.ID, requestID).Scan(&originalHash, &ticketID)
	if err != nil {
		return ticket.CreateResult{}, fmt.Errorf("read existing request: %w", err)
	}
	if !bytes.Equal(hash[:], originalHash) {
		return ticket.CreateResult{}, ticket.ErrIdempotencyConflict
	}
	if !ticketID.Valid {
		return ticket.CreateResult{}, fmt.Errorf("existing request has no committed ticket")
	}
	t, err := loadTicket(ctx, tx, user.OrganizationID, ticketID.Int64, false)
	if err != nil {
		return ticket.CreateResult{}, fmt.Errorf("read replay ticket: %w", err)
	}
	return ticket.CreateResult{Ticket: t, Replayed: true}, nil
}

func chooseTeam(ctx context.Context, tx *sql.Tx, organizationID int64, candidates []int64) (ticket.Team, error) {
	for _, id := range candidates {
		var t ticket.Team
		err := tx.QueryRowContext(ctx, "SELECT id, organization_id, name, active FROM teams WHERE organization_id = ? AND id = ? FOR SHARE", organizationID, id).Scan(&t.ID, &t.OrganizationID, &t.Name, &t.Active)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return t, fmt.Errorf("load routing team: %w", err)
		}
		if t.Active {
			return t, nil
		}
	}
	return ticket.Team{}, ticket.ErrNoActiveTeam
}
