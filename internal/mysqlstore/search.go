package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

type searchAccess struct {
	tx          *sql.Tx
	user        ticket.User
	teams       []int64
	diagnostics *search.Diagnostics
	heldAt      time.Time
}

func (a *searchAccess) Identity() ticket.User { return a.user }
func (a *searchAccess) TeamIDs() []int64      { return a.teams }
func (a *searchAccess) Close() error {
	err := a.tx.Rollback()
	if a.diagnostics != nil {
		a.diagnostics.ConnectionHeldMS += search.Milliseconds(a.heldAt)
	}
	return err
}

func (s *Store) BeginSearch(ctx context.Context, userID int64) (search.Access, error) {
	d := search.Diagnostic(ctx)
	start := time.Now()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if d != nil {
		d.AcquireAndBeginMS += search.Milliseconds(start)
	}
	if err != nil {
		return nil, fmt.Errorf("begin search authorization: %w", err)
	}
	heldAt := time.Now()
	failed := true
	defer func() {
		if failed && d != nil {
			d.ConnectionHeldMS += search.Milliseconds(heldAt)
		}
	}()
	if d != nil {
		d.MySQLQueries++
	}
	u, err := loadUser(ctx, tx, userID, false)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	a := &searchAccess{tx: tx, user: u, teams: []int64{}, diagnostics: d, heldAt: heldAt}
	if u.Role == ticket.RoleAgent {
		if d != nil {
			d.MySQLQueries++
		}
		rows, err := tx.QueryContext(ctx, `SELECT m.team_id FROM team_members m JOIN teams tm ON tm.organization_id=m.organization_id AND tm.id=m.team_id WHERE m.organization_id=? AND m.user_id=? AND m.active=TRUE AND tm.active=TRUE ORDER BY m.team_id`, u.OrganizationID, u.ID)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				tx.Rollback()
				return nil, err
			}
			a.teams = append(a.teams, id)
		}
		if err := rows.Err(); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	failed = false
	return a, nil
}

func (a *searchAccess) Authorized(ctx context.Context, ids []int64) (map[int64]ticket.TicketSummary, error) {
	result := make(map[int64]ticket.TicketSummary)
	if len(ids) == 0 {
		return result, nil
	}
	slots := make([]string, len(ids))
	args := []any{a.user.OrganizationID}
	for i, id := range ids {
		slots[i] = "?"
		args = append(args, id)
	}
	where := "t.organization_id=? AND t.id IN (" + strings.Join(slots, ",") + ")"
	if !a.user.IsManager() {
		where += " AND (t.creator_id=?"
		args = append(args, a.user.ID)
		if a.user.Role == ticket.RoleAgent {
			where += " OR " + visibleTeamMember
			args = append(args, a.user.ID)
		}
		where += ")"
	}
	if a.diagnostics != nil {
		a.diagnostics.MySQLQueries++
	}
	rows, err := a.tx.QueryContext(ctx, `SELECT t.id,t.ticket_no,t.title,t.priority,t.status,t.assignee_id,u.name,t.current_team_id,tm.name,t.version,t.updated_at FROM tickets t LEFT JOIN users u ON u.organization_id=t.organization_id AND u.id=t.assignee_id JOIN teams tm ON tm.organization_id=t.organization_id AND tm.id=t.current_team_id WHERE `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("batch authorize search: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item ticket.TicketSummary
		if err := rows.Scan(&item.ID, &item.TicketNo, &item.Title, &item.Priority, &item.Status, &item.AssigneeID, &item.AssigneeName, &item.CurrentTeamID, &item.CurrentTeamName, &item.Version, &item.UpdatedAt); err != nil {
			return nil, err
		}
		result[item.ID] = item
	}
	return result, rows.Err()
}

// No broker counts/watermarks from other users are exposed through Search API.
func (s *Store) SearchFreshness(ctx context.Context) (search.Freshness, error) {
	d := search.Diagnostic(ctx)
	result := search.Freshness{State: "unknown"}
	var heartbeat time.Time
	if d != nil {
		d.MySQLQueries++
	}
	err := s.db.QueryRowContext(ctx, `SELECT state,heartbeat_at,last_event_at FROM sync_worker_state WHERE component='es'`).Scan(&result.State, &heartbeat, &result.LastEventAt)
	if err == sql.ErrNoRows {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if time.Since(heartbeat) > 10*time.Second {
		result.State = "stalled"
	}
	var isolated bool
	if d != nil {
		d.MySQLQueries++
	}
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_kafka_es_deliveries WHERE status='isolated')").Scan(&isolated); err != nil {
		return result, err
	}
	if isolated && result.State == "running" {
		result.State = "degraded"
	}
	if d != nil {
		d.MySQLQueries++
	}
	err = s.db.QueryRowContext(ctx, `SELECT MAX(last_seen_at) FROM sync_kafka_es_deliveries WHERE status IN ('applied','duplicate_or_old')`).Scan(&result.LastSuccessAt)
	return result, err
}

// Filter candidates use current identities/memberships, never hardcoded Web IDs.
func (s *Store) SearchOptions(ctx context.Context, userID int64) (search.Options, error) {
	access, err := s.BeginSearch(ctx, userID)
	if err != nil {
		return search.Options{}, err
	}
	defer access.Close()
	a := access.(*searchAccess)
	result := search.Options{Teams: []ticket.Team{}, Assignees: []ticket.User{}, Scope: "current_authorization"}
	args := []any{a.user.OrganizationID}
	where := "tm.organization_id=? AND tm.active=TRUE"
	if !a.user.IsManager() {
		where += " AND EXISTS(SELECT 1 FROM team_members m WHERE m.team_id=tm.id AND m.organization_id=tm.organization_id AND m.user_id=? AND m.active=TRUE)"
		args = append(args, a.user.ID)
	}
	rows, err := a.tx.QueryContext(ctx, "SELECT tm.id,tm.organization_id,tm.name,tm.active FROM teams tm WHERE "+where+" ORDER BY tm.id", args...)
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
	rows.Close()
	if len(result.Teams) == 0 {
		return result, nil
	}
	slots := make([]string, len(result.Teams))
	args = []any{a.user.OrganizationID}
	for i, team := range result.Teams {
		slots[i] = "?"
		args = append(args, team.ID)
	}
	rows, err = a.tx.QueryContext(ctx, "SELECT DISTINCT u.id,u.organization_id,u.name,u.role,u.active FROM users u JOIN team_members m ON m.organization_id=u.organization_id AND m.user_id=u.id WHERE u.organization_id=? AND u.active=TRUE AND u.role='agent' AND m.active=TRUE AND m.team_id IN ("+strings.Join(slots, ",")+") ORDER BY u.id", args...)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var u ticket.User
		if err := rows.Scan(&u.ID, &u.OrganizationID, &u.Name, &u.Role, &u.Active); err != nil {
			return result, err
		}
		result.Assignees = append(result.Assignees, u)
	}
	return result, rows.Err()
}
