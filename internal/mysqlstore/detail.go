package mysqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ticketpulse/internal/ticket"
)

func (s *Store) Detail(ctx context.Context, userID, ticketID int64) (ticket.Detail, error) {
	// One snapshot covers authorization, ticket state, team/assignee, and every history row.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return ticket.Detail{}, fmt.Errorf("begin detail: %w", err)
	}
	defer tx.Rollback()
	user, err := loadUser(ctx, tx, userID, false)
	if err != nil {
		return ticket.Detail{}, err
	}
	t, err := loadTicket(ctx, tx, user.OrganizationID, ticketID, false)
	if err != nil {
		return ticket.Detail{}, err
	}
	allowed, err := canView(ctx, tx, user, t, false)
	if err != nil {
		return ticket.Detail{}, err
	}
	if !allowed {
		return ticket.Detail{}, ticket.ErrForbidden
	}
	result := ticket.Detail{Ticket: t, History: make([]ticket.History, 0), Comments: make([]ticket.Comment, 0)}
	err = tx.QueryRowContext(ctx, "SELECT id, organization_id, name, active FROM teams WHERE organization_id = ? AND id = ?", t.OrganizationID, t.CurrentTeamID).Scan(&result.CurrentTeam.ID, &result.CurrentTeam.OrganizationID, &result.CurrentTeam.Name, &result.CurrentTeam.Active)
	if err != nil {
		return ticket.Detail{}, fmt.Errorf("read current team: %w", err)
	}
	if t.AssigneeID != nil {
		var assignee ticket.User
		err = tx.QueryRowContext(ctx, "SELECT id, organization_id, name, role, active FROM users WHERE organization_id = ? AND id = ?", t.OrganizationID, *t.AssigneeID).Scan(&assignee.ID, &assignee.OrganizationID, &assignee.Name, &assignee.Role, &assignee.Active)
		if err != nil {
			return ticket.Detail{}, fmt.Errorf("read assignee: %w", err)
		}
		result.Assignee = &assignee
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, ticket_id, action, operator_id, version, before_state, after_state, responsible_team_id, occurred_at, reason, comment_id FROM ticket_histories WHERE organization_id = ? AND ticket_id = ? ORDER BY version ASC, id ASC`, t.OrganizationID, t.ID)
	if err != nil {
		return ticket.Detail{}, fmt.Errorf("read history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var h ticket.History
		var before, after []byte
		if err := rows.Scan(&h.ID, &h.TicketID, &h.Action, &h.OperatorID, &h.Version, &before, &after, &h.ResponsibleTeamID, &h.OccurredAt, &h.Reason, &h.CommentID); err != nil {
			return ticket.Detail{}, fmt.Errorf("scan history: %w", err)
		}
		if before != nil {
			h.Before = &ticket.State{}
			if err := json.Unmarshal(before, h.Before); err != nil {
				return ticket.Detail{}, fmt.Errorf("decode before state: %w", err)
			}
		}
		if err := json.Unmarshal(after, &h.After); err != nil {
			return ticket.Detail{}, fmt.Errorf("decode after state: %w", err)
		}
		result.History = append(result.History, h)
	}
	if err := rows.Err(); err != nil {
		return ticket.Detail{}, fmt.Errorf("iterate history: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ticket.Detail{}, fmt.Errorf("close history: %w", err)
	}
	comments, err := tx.QueryContext(ctx, `SELECT id,ticket_id,author_id,content,version,created_at FROM ticket_comments WHERE organization_id=? AND ticket_id=? ORDER BY created_at ASC,id ASC`, t.OrganizationID, t.ID)
	if err != nil {
		return ticket.Detail{}, fmt.Errorf("read comments: %w", err)
	}
	defer comments.Close()
	for comments.Next() {
		var c ticket.Comment
		if err := comments.Scan(&c.ID, &c.TicketID, &c.AuthorID, &c.Content, &c.Version, &c.CreatedAt); err != nil {
			return ticket.Detail{}, fmt.Errorf("scan comment: %w", err)
		}
		result.Comments = append(result.Comments, c)
	}
	if err := comments.Err(); err != nil {
		return ticket.Detail{}, fmt.Errorf("iterate comments: %w", err)
	}
	if err := comments.Close(); err != nil {
		return ticket.Detail{}, fmt.Errorf("close comments: %w", err)
	}
	userIDs, teamIDs := map[int64]bool{t.CreatorID: true}, map[int64]bool{t.CurrentTeamID: true, t.InitialTeamID: true}
	addState := func(state *ticket.State) {
		if state == nil {
			return
		}
		userIDs[state.CreatorID] = true
		if state.AssigneeID != nil {
			userIDs[*state.AssigneeID] = true
		}
		teamIDs[state.InitialTeamID] = true
		teamIDs[state.CurrentTeamID] = true
	}
	for _, h := range result.History {
		userIDs[h.OperatorID] = true
		addState(h.Before)
		addState(&h.After)
	}
	for _, c := range result.Comments {
		userIDs[c.AuthorID] = true
	}
	result.Participants, err = detailParticipants(ctx, tx, t.OrganizationID, userIDs)
	if err != nil {
		return ticket.Detail{}, err
	}
	result.Teams, err = detailTeams(ctx, tx, t.OrganizationID, teamIDs)
	if err != nil {
		return ticket.Detail{}, err
	}
	if err := tx.Commit(); err != nil {
		return ticket.Detail{}, fmt.Errorf("commit detail: %w", err)
	}
	return result, nil
}

func idArguments(orgID int64, ids map[int64]bool) ([]any, string) {
	ordered := make([]int64, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	slices.Sort(ordered)
	args := []any{orgID}
	slots := make([]string, 0, len(ordered))
	for _, id := range ordered {
		args = append(args, id)
		slots = append(slots, "?")
	}
	return args, strings.Join(slots, ",")
}

func detailParticipants(ctx context.Context, tx *sql.Tx, orgID int64, ids map[int64]bool) ([]ticket.User, error) {
	args, slots := idArguments(orgID, ids)
	rows, err := tx.QueryContext(ctx, "SELECT id,organization_id,name,role,active FROM users WHERE organization_id=? AND id IN ("+slots+") ORDER BY id", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ticket.User, 0, len(ids))
	for rows.Next() {
		var u ticket.User
		if err := rows.Scan(&u.ID, &u.OrganizationID, &u.Name, &u.Role, &u.Active); err != nil {
			return nil, err
		}
		result = append(result, u)
	}
	return result, rows.Err()
}

func detailTeams(ctx context.Context, tx *sql.Tx, orgID int64, ids map[int64]bool) ([]ticket.Team, error) {
	args, slots := idArguments(orgID, ids)
	rows, err := tx.QueryContext(ctx, "SELECT id,organization_id,name,active FROM teams WHERE organization_id=? AND id IN ("+slots+") ORDER BY id", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ticket.Team, 0, len(ids))
	for rows.Next() {
		var t ticket.Team
		if err := rows.Scan(&t.ID, &t.OrganizationID, &t.Name, &t.Active); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}
