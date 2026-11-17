package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"ticketpulse/internal/ticket"
)

func activeTeam(ctx context.Context, tx *sql.Tx, organizationID, teamID int64) (bool, error) {
	var active bool
	err := tx.QueryRowContext(ctx, "SELECT active FROM teams WHERE organization_id = ? AND id = ? FOR SHARE", organizationID, teamID).Scan(&active)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check team: %w", err)
	}
	return active, nil
}

// Detail and comments have the same current visibility rule, with locks only for writes.
func canView(ctx context.Context, tx *sql.Tx, u ticket.User, t ticket.Ticket, lock bool) (bool, error) {
	if t.CreatorID == u.ID || u.IsManager() {
		return true, nil
	}
	if u.Role != ticket.RoleAgent {
		return false, nil
	}
	allowed, err := activeTeamMember(ctx, tx, u.OrganizationID, t.CurrentTeamID, u.ID, lock)
	if err != nil {
		return false, fmt.Errorf("check visible membership: %w", err)
	}
	return allowed, nil
}

func (s *Store) Transfer(ctx context.Context, userID int64, request ticket.TransferRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request.ActionRequest, ticket.ActionAssigneeTransferred, request.Reason, func(tx *sql.Tx, t ticket.Ticket, u ticket.User, _ int64, _ time.Time) (ticket.Ticket, *ticket.Comment, error) {
		after, err := t.Transfer(u, request)
		if err != nil {
			return ticket.Ticket{}, nil, err
		}
		active, err := activeTeam(ctx, tx, t.OrganizationID, t.CurrentTeamID)
		if err != nil {
			return ticket.Ticket{}, nil, err
		}
		if !active {
			return ticket.Ticket{}, nil, ticket.ErrTeamUnavailable
		}
		if err := requireAgentInTeam(ctx, tx, t.OrganizationID, t.CurrentTeamID, request.AssigneeID); errors.Is(err, ticket.ErrAssigneeUnavailable) {
			return ticket.Ticket{}, nil, ticket.ErrInvalidAssignee
		} else if err != nil {
			return ticket.Ticket{}, nil, err
		}
		return after, nil, nil
	})
}

func (s *Store) TransferTeam(ctx context.Context, userID int64, request ticket.TeamTransferRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request.ActionRequest, ticket.ActionTeamTransferred, request.Reason, func(tx *sql.Tx, t ticket.Ticket, u ticket.User, _ int64, _ time.Time) (ticket.Ticket, *ticket.Comment, error) {
		after, err := t.TransferTeam(u, request)
		if err != nil {
			return ticket.Ticket{}, nil, err
		}
		active, err := activeTeam(ctx, tx, t.OrganizationID, request.TeamID)
		if err != nil {
			return ticket.Ticket{}, nil, err
		}
		if !active {
			return ticket.Ticket{}, nil, ticket.ErrInvalidTeam
		}
		return after, nil, nil
	})
}

func (s *Store) ManagementReopen(ctx context.Context, userID int64, request ticket.ManagementReopenRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request.ActionRequest, ticket.ActionManagementReopened, request.Reason, func(tx *sql.Tx, t ticket.Ticket, u ticket.User, version int64, _ time.Time) (ticket.Ticket, *ticket.Comment, error) {
		if t.Version != version {
			return ticket.Ticket{}, nil, ticket.ErrVersionConflict
		}
		if t.Status != ticket.StatusAwaitingConfirmation {
			return ticket.Ticket{}, nil, ticket.ErrInvalidState
		}
		teamID := request.TeamID
		if teamID == 0 {
			teamID = t.CurrentTeamID
		}
		active, err := activeTeam(ctx, tx, t.OrganizationID, teamID)
		if err != nil {
			return ticket.Ticket{}, nil, err
		}
		if !active {
			if teamID == t.CurrentTeamID {
				return ticket.Ticket{}, nil, ticket.ErrTeamUnavailable
			}
			return ticket.Ticket{}, nil, ticket.ErrInvalidTeam
		}
		owner := t.AssigneeID
		if teamID != t.CurrentTeamID {
			owner = nil
		} else if err := requireActiveAssignee(ctx, tx, t); errors.Is(err, ticket.ErrAssigneeUnavailable) {
			owner = nil
		} else if err != nil {
			return ticket.Ticket{}, nil, err
		}
		after, err := t.ManagementReopen(u, version, teamID, owner)
		return after, nil, err
	})
}

func (s *Store) ManualClose(ctx context.Context, userID int64, request ticket.ReasonRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request.ActionRequest, ticket.ActionManuallyClosed, request.Reason, func(_ *sql.Tx, t ticket.Ticket, u ticket.User, version int64, at time.Time) (ticket.Ticket, *ticket.Comment, error) {
		after, err := t.ManualClose(u, version, at)
		return after, nil, err
	})
}

func (s *Store) Comment(ctx context.Context, userID int64, request ticket.CommentRequest) (ticket.ActionResult, error) {
	return s.updateAction(ctx, userID, request.ActionRequest, ticket.ActionCommented, "", func(tx *sql.Tx, t ticket.Ticket, u ticket.User, version int64, at time.Time) (ticket.Ticket, *ticket.Comment, error) {
		if t.Version != version {
			return ticket.Ticket{}, nil, ticket.ErrVersionConflict
		}
		inserted, err := tx.ExecContext(ctx, `INSERT INTO ticket_comments (organization_id,ticket_id,author_id,content,version,created_at) VALUES (?,?,?,?,?,?)`, t.OrganizationID, t.ID, u.ID, request.Content, t.Version+1, at)
		if err != nil {
			return ticket.Ticket{}, nil, fmt.Errorf("insert comment: %w", err)
		}
		id, err := inserted.LastInsertId()
		if err != nil {
			return ticket.Ticket{}, nil, fmt.Errorf("comment id: %w", err)
		}
		t.Version++
		return t, &ticket.Comment{ID: id, TicketID: t.ID, AuthorID: u.ID, Content: request.Content, Version: t.Version, CreatedAt: at}, nil
	})
}
