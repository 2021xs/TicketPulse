package mysqlstore

import (
	"context"
	"database/sql"

	"ticketpulse/internal/ticket"
)

func (s *Store) Options(ctx context.Context, userID, ticketID int64) (ticket.Options, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return ticket.Options{}, err
	}
	defer tx.Rollback()
	u, err := loadUser(ctx, tx, userID, false)
	if err != nil {
		return ticket.Options{}, err
	}
	t, err := loadTicket(ctx, tx, u.OrganizationID, ticketID, false)
	if err != nil {
		return ticket.Options{}, err
	}
	allowed, err := canView(ctx, tx, u, t, false)
	if err != nil {
		return ticket.Options{}, err
	}
	if !allowed {
		return ticket.Options{}, ticket.ErrForbidden
	}
	member, err := activeTeamMember(ctx, tx, u.OrganizationID, t.CurrentTeamID, u.ID, false)
	if err != nil {
		return ticket.Options{}, err
	}
	result := ticket.Options{Actions: ticket.AvailableActions(u, t, member), Assignees: make([]ticket.User, 0), Teams: make([]ticket.Team, 0), Version: t.Version}
	if err := tx.QueryRowContext(ctx, "SELECT active FROM teams WHERE organization_id=? AND id=?", t.OrganizationID, t.CurrentTeamID).Scan(&result.CurrentTeamActive); err != nil {
		return result, err
	}
	if t.AssigneeID != nil {
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users u JOIN team_members m ON m.organization_id=u.organization_id AND m.user_id=u.id JOIN teams tm ON tm.organization_id=m.organization_id AND tm.id=m.team_id WHERE u.id=? AND u.organization_id=? AND u.active=TRUE AND u.role='agent' AND m.team_id=? AND m.active=TRUE AND tm.active=TRUE)", *t.AssigneeID, t.OrganizationID, t.CurrentTeamID).Scan(&result.AssigneeValid); err != nil {
			return result, err
		}
	}
	transfer := false
	for _, action := range result.Actions {
		if action == "transfer" {
			transfer = true
		}
	}
	if transfer && result.CurrentTeamActive {
		ownerID := int64(0)
		if t.AssigneeID != nil {
			ownerID = *t.AssigneeID
		}
		rows, err := tx.QueryContext(ctx, "SELECT u.id,u.organization_id,u.name,u.role,u.active FROM users u JOIN team_members m ON m.organization_id=u.organization_id AND m.user_id=u.id WHERE u.organization_id=? AND m.team_id=? AND u.active=TRUE AND u.role='agent' AND m.active=TRUE AND u.id<>? ORDER BY u.name,u.id", t.OrganizationID, t.CurrentTeamID, ownerID)
		if err != nil {
			return result, err
		}
		for rows.Next() {
			var candidate ticket.User
			if err := rows.Scan(&candidate.ID, &candidate.OrganizationID, &candidate.Name, &candidate.Role, &candidate.Active); err != nil {
				rows.Close()
				return result, err
			}
			result.Assignees = append(result.Assignees, candidate)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return result, err
		}
		if err := rows.Close(); err != nil {
			return result, err
		}
	}
	if u.IsManager() && (t.Status == ticket.StatusPending || t.Status == ticket.StatusInProgress || t.Status == ticket.StatusAwaitingConfirmation) {
		rows, err := tx.QueryContext(ctx, "SELECT id,organization_id,name,active FROM teams WHERE organization_id=? AND active=TRUE ORDER BY name,id", t.OrganizationID)
		if err != nil {
			return result, err
		}
		for rows.Next() {
			var team ticket.Team
			if err := rows.Scan(&team.ID, &team.OrganizationID, &team.Name, &team.Active); err != nil {
				rows.Close()
				return result, err
			}
			result.Teams = append(result.Teams, team)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return result, err
		}
		if err := rows.Close(); err != nil {
			return result, err
		}
	}
	return result, tx.Commit()
}
