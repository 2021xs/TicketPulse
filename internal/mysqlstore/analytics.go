package mysqlstore

import (
	"context"
	"database/sql"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/ticket"
)

func (s *Store) AnalyticsAccess(ctx context.Context, userID int64) (ticket.User, []ticket.Team, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return ticket.User{}, nil, err
	}
	defer tx.Rollback()
	user, err := loadUser(ctx, tx, userID, false)
	if err != nil {
		return user, nil, err
	}
	if !user.IsManager() {
		return user, nil, analytics.ErrForbidden
	}
	rows, err := tx.QueryContext(ctx, "SELECT id,organization_id,name,active FROM teams WHERE organization_id=? ORDER BY id", user.OrganizationID)
	if err != nil {
		return user, nil, err
	}
	defer rows.Close()
	teams := []ticket.Team{}
	for rows.Next() {
		var team ticket.Team
		if err := rows.Scan(&team.ID, &team.OrganizationID, &team.Name, &team.Active); err != nil {
			return user, nil, err
		}
		teams = append(teams, team)
	}
	if err := rows.Err(); err != nil {
		return user, nil, err
	}
	if err := rows.Close(); err != nil {
		return user, nil, err
	}
	return user, teams, tx.Commit()
}
