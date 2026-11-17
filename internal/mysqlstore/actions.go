package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"ticketpulse/internal/ticket"
)

// Each explicit action shares the locked transaction; only comments return an associated row.
type actionChange func(*sql.Tx, ticket.Ticket, ticket.User, int64, time.Time) (ticket.Ticket, *ticket.Comment, error)

func (s *Store) Claim(ctx context.Context, userID int64, request ticket.ActionRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request, ticket.ActionClaimed, "", func(_ *sql.Tx, t ticket.Ticket, u ticket.User, version int64, _ time.Time) (ticket.Ticket, *ticket.Comment, error) {
		after, err := t.Claim(u, version)
		return after, nil, err
	})
}

func (s *Store) Start(ctx context.Context, userID int64, request ticket.ActionRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request, ticket.ActionProcessingStarted, "", func(_ *sql.Tx, t ticket.Ticket, u ticket.User, version int64, _ time.Time) (ticket.Ticket, *ticket.Comment, error) {
		after, err := t.Start(u, version)
		return after, nil, err
	})
}

func (s *Store) Resolve(ctx context.Context, userID int64, request ticket.ResolveRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request.ActionRequest, ticket.ActionResolved, "", func(_ *sql.Tx, t ticket.Ticket, u ticket.User, version int64, at time.Time) (ticket.Ticket, *ticket.Comment, error) {
		after, err := t.Resolve(u, version, request.ResolutionNote, at)
		return after, nil, err
	})
}

func (s *Store) Reopen(ctx context.Context, userID int64, request ticket.ActionRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request, ticket.ActionReopened, "", func(tx *sql.Tx, t ticket.Ticket, u ticket.User, version int64, _ time.Time) (ticket.Ticket, *ticket.Comment, error) {
		if _, err := t.Reopen(u, version); err != nil {
			return ticket.Ticket{}, nil, err
		}
		active, err := activeTeam(ctx, tx, t.OrganizationID, t.CurrentTeamID)
		if err != nil {
			return ticket.Ticket{}, nil, err
		}
		if !active {
			return ticket.Ticket{}, nil, ticket.ErrTeamUnavailable
		}
		if err := requireActiveAssignee(ctx, tx, t); errors.Is(err, ticket.ErrAssigneeUnavailable) {
			t.AssigneeID = nil
		} else if err != nil {
			return ticket.Ticket{}, nil, err
		}
		after, err := t.Reopen(u, version)
		return after, nil, err
	})
}

func (s *Store) Close(ctx context.Context, userID int64, request ticket.ActionRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request, ticket.ActionClosed, "", func(_ *sql.Tx, t ticket.Ticket, u ticket.User, version int64, at time.Time) (ticket.Ticket, *ticket.Comment, error) {
		after, err := t.Close(u, version, at)
		return after, nil, err
	})
}

// Shared by explicit business actions; neither the action nor the transition comes from the client.
func (s *Store) updateAction(ctx context.Context, userID int64, request ticket.ActionRequest, action, reason string, change actionChange) (ticket.ActionResult, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ticket.ActionResult{}, fmt.Errorf("begin action: %w", err)
	}
	defer tx.Rollback()
	if err := s.acquireBusinessWrite(ctx, tx); err != nil {
		return ticket.ActionResult{}, err
	}
	user, err := loadUser(ctx, tx, userID, true)
	if err != nil {
		return ticket.ActionResult{}, err
	}
	before, err := loadTicket(ctx, tx, user.OrganizationID, request.TicketID, true)
	if err != nil {
		return ticket.ActionResult{}, err
	}
	switch action {
	case ticket.ActionClaimed, ticket.ActionProcessingStarted, ticket.ActionResolved, ticket.ActionAssigneeTransferred:
		if action == ticket.ActionAssigneeTransferred && user.IsManager() {
			break
		}
		if user.Role != ticket.RoleAgent {
			return ticket.ActionResult{}, ticket.ErrActionForbidden
		}
		active, err := activeTeamMember(ctx, tx, user.OrganizationID, before.CurrentTeamID, user.ID, true)
		if err != nil {
			return ticket.ActionResult{}, fmt.Errorf("check action membership: %w", err)
		}
		if !active {
			return ticket.ActionResult{}, ticket.ErrActionForbidden
		}
	case ticket.ActionReopened, ticket.ActionClosed:
		if before.CreatorID != user.ID {
			return ticket.ActionResult{}, ticket.ErrActionForbidden
		}
	case ticket.ActionTeamTransferred, ticket.ActionManagementReopened, ticket.ActionManuallyClosed:
		if !user.IsManager() {
			return ticket.ActionResult{}, ticket.ErrActionForbidden
		}
	case ticket.ActionCommented:
		allowed, err := canView(ctx, tx, user, before, true)
		if err != nil {
			return ticket.ActionResult{}, err
		}
		if !allowed {
			return ticket.ActionResult{}, ticket.ErrActionForbidden
		}
	default:
		return ticket.ActionResult{}, fmt.Errorf("unsupported internal action")
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	if !at.After(before.UpdatedAt) {
		at = before.UpdatedAt.Add(time.Microsecond)
	}
	after, comment, err := change(tx, before, user, request.ExpectedVersion, at)
	if err != nil {
		return ticket.ActionResult{}, err
	}
	if action == ticket.ActionReopened && after.Status == ticket.StatusPending {
		action = ticket.ActionReopenedRequeued
		reason = "原负责人失效，重开后重新进入当前团队待处理队列"
	}
	after.UpdatedAt = at
	updated, err := tx.ExecContext(ctx, `UPDATE tickets SET assignee_id = ?, current_team_id = ?, status = ?, version = ?, updated_at = ?, first_resolved_at = ?, closed_at = ?, reopen_count = ?, resolution_note = ? WHERE organization_id = ? AND id = ? AND version = ?`,
		after.AssigneeID, after.CurrentTeamID, after.Status, after.Version, after.UpdatedAt, after.FirstResolvedAt, after.ClosedAt, after.ReopenCount, after.ResolutionNote, after.OrganizationID, after.ID, request.ExpectedVersion)
	if err != nil {
		return ticket.ActionResult{}, fmt.Errorf("update action ticket: %w", err)
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return ticket.ActionResult{}, fmt.Errorf("check action update: %w", err)
	}
	if count != 1 {
		return ticket.ActionResult{}, ticket.ErrVersionConflict
	}
	state := before.State()
	var historyReason *string
	if reason != "" {
		historyReason = &reason
	}
	var commentID *int64
	if comment != nil {
		commentID = &comment.ID
	}
	if err := insertActionHistory(ctx, tx, user.ID, action, &state, after, historyReason, commentID); err != nil {
		return ticket.ActionResult{}, fmt.Errorf("insert action history: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ticket.ActionResult{}, fmt.Errorf("%w: %w", ticket.ErrActionResultUnknown, err)
	}
	return ticket.ActionResult{Ticket: after, Comment: comment}, nil
}

func requireActiveAssignee(ctx context.Context, tx *sql.Tx, t ticket.Ticket) error {
	if t.AssigneeID == nil {
		return ticket.ErrAssigneeUnavailable
	}
	return requireAgentInTeam(ctx, tx, t.OrganizationID, t.CurrentTeamID, *t.AssigneeID)
}

func requireAgentInTeam(ctx context.Context, tx *sql.Tx, organizationID, teamID, userID int64) error {
	owner, err := loadUser(ctx, tx, userID, true)
	if errors.Is(err, ticket.ErrUnauthenticated) {
		return ticket.ErrAssigneeUnavailable
	}
	if err != nil {
		return err
	}
	if owner.Role != ticket.RoleAgent || owner.OrganizationID != organizationID {
		return ticket.ErrAssigneeUnavailable
	}
	active, err := activeTeamMember(ctx, tx, organizationID, teamID, owner.ID, true)
	if err != nil {
		return fmt.Errorf("check retained assignee membership: %w", err)
	}
	if !active {
		return ticket.ErrAssigneeUnavailable
	}
	return nil
}
