package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"ticketpulse/internal/ticket"
)

const visibleTeamMember = `EXISTS (SELECT 1 FROM team_members m JOIN teams tm ON tm.organization_id=m.organization_id AND tm.id=m.team_id WHERE m.organization_id=t.organization_id AND m.team_id=t.current_team_id AND m.user_id=? AND m.active=TRUE AND tm.active=TRUE)`

func (s *Store) List(ctx context.Context, userID int64, request ticket.ListRequest) (ticket.ListResult, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return ticket.ListResult{}, fmt.Errorf("begin list: %w", err)
	}
	defer tx.Rollback()
	u, err := loadUser(ctx, tx, userID, false)
	if err != nil {
		return ticket.ListResult{}, err
	}
	where := "t.organization_id=?"
	args := []any{u.OrganizationID}
	switch request.View {
	case ticket.ViewCreatedByMe:
		where += " AND t.creator_id=?"
		args = append(args, u.ID)
	case ticket.ViewAssignedToMe:
		where += " AND t.assignee_id=?"
		args = append(args, u.ID)
		if !u.IsManager() {
			where += " AND (t.creator_id=?"
			args = append(args, u.ID)
			if u.Role == ticket.RoleAgent {
				where += " OR " + visibleTeamMember
				args = append(args, u.ID)
			}
			where += ")"
		}
	case ticket.ViewTeamPending:
		where += " AND t.status='pending'"
		if u.IsManager() {
			where += " AND EXISTS (SELECT 1 FROM teams tm WHERE tm.organization_id=t.organization_id AND tm.id=t.current_team_id AND tm.active=TRUE)"
		} else if u.Role == ticket.RoleAgent {
			where += " AND " + visibleTeamMember
			args = append(args, u.ID)
		} else {
			where += " AND FALSE"
		}
	default:
		return ticket.ListResult{}, ticket.Invalid("未知列表视图")
	}
	result := ticket.ListResult{Items: make([]ticket.TicketSummary, 0), View: request.View, Page: request.Page, PageSize: request.PageSize}
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM tickets t WHERE "+where, args...).Scan(&result.Total); err != nil {
		return ticket.ListResult{}, fmt.Errorf("count authorized list: %w", err)
	}
	offset := (request.Page - 1) * request.PageSize
	queryArgs := append(append([]any{}, args...), request.PageSize, offset)
	rows, err := tx.QueryContext(ctx, `SELECT t.id,t.ticket_no,t.title,t.priority,t.status,t.assignee_id,u.name,t.current_team_id,tm.name,t.version,t.updated_at FROM tickets t LEFT JOIN users u ON u.organization_id=t.organization_id AND u.id=t.assignee_id JOIN teams tm ON tm.organization_id=t.organization_id AND tm.id=t.current_team_id WHERE `+where+" ORDER BY t.updated_at DESC,t.id DESC LIMIT ? OFFSET ?", queryArgs...)
	if err != nil {
		return ticket.ListResult{}, fmt.Errorf("fetch authorized list: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item ticket.TicketSummary
		if err := rows.Scan(&item.ID, &item.TicketNo, &item.Title, &item.Priority, &item.Status, &item.AssigneeID, &item.AssigneeName, &item.CurrentTeamID, &item.CurrentTeamName, &item.Version, &item.UpdatedAt); err != nil {
			return ticket.ListResult{}, fmt.Errorf("scan list: %w", err)
		}
		result.Items = append(result.Items, item)
	}
	if err := rows.Err(); err != nil {
		return ticket.ListResult{}, fmt.Errorf("iterate list: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ticket.ListResult{}, fmt.Errorf("close list: %w", err)
	}
	result.HasMore = int64(offset+len(result.Items)) < result.Total
	if err := tx.Commit(); err != nil {
		return ticket.ListResult{}, fmt.Errorf("commit list: %w", err)
	}
	return result, nil
}
