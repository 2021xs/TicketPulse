package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"
	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

// A missing login still performs bcrypt work; no credential or hash is returned to the client.
const dummyPasswordHash = "$2a$12$IF8KNCJBAzl43xgBieaAmulBVmSREoNFiTzlPTWPBL9VM4A2ydk5."

func (s *Store) Login(ctx context.Context, request ticket.LoginRequest, hash [32]byte, ttl time.Duration) (ticket.LoginResult, error) {
	var userID int64
	passwordHash := dummyPasswordHash
	err := s.db.QueryRowContext(ctx, "SELECT user_id,password_hash FROM login_accounts WHERE username=?", request.Username).Scan(&userID, &passwordHash)
	if err != nil && err != sql.ErrNoRows {
		return ticket.LoginResult{}, fmt.Errorf("read login: %w", err)
	}
	valid := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(request.Password)) == nil
	if err == sql.ErrNoRows || !valid {
		return ticket.LoginResult{}, ticket.ErrInvalidCredentials
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ticket.LoginResult{}, err
	}
	defer tx.Rollback()
	u, err := loadUser(ctx, tx, userID, true)
	if err == ticket.ErrUnauthenticated {
		return ticket.LoginResult{}, ticket.ErrInvalidCredentials
	}
	if err != nil {
		return ticket.LoginResult{}, err
	}
	var currentHash string
	if err := tx.QueryRowContext(ctx, "SELECT password_hash FROM login_accounts WHERE user_id=? AND username=? FOR SHARE", userID, request.Username).Scan(&currentHash); err != nil {
		if err == sql.ErrNoRows {
			return ticket.LoginResult{}, ticket.ErrInvalidCredentials
		}
		return ticket.LoginResult{}, err
	}
	if currentHash != passwordHash {
		return ticket.LoginResult{}, ticket.ErrInvalidCredentials
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	expires := at.Add(ttl)
	if _, err := tx.ExecContext(ctx, "INSERT INTO user_sessions (token_hash,user_id,created_at,expires_at) VALUES (?,?,?,?)", hash[:], userID, at, expires); err != nil {
		return ticket.LoginResult{}, fmt.Errorf("create session: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ticket.LoginResult{}, fmt.Errorf("commit session: %w", err)
	}
	return ticket.LoginResult{User: u, ExpiresAt: expires}, nil
}

func (s *Store) Session(ctx context.Context, hash [32]byte, at time.Time) (ticket.SessionIdentity, error) {
	var result ticket.SessionIdentity
	var active bool
	var revoked sql.NullTime
	if d := search.Diagnostic(ctx); d != nil {
		d.MySQLQueries++
	}
	err := s.db.QueryRowContext(ctx, "SELECT s.user_id,s.expires_at,s.revoked_at,u.active FROM user_sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=?", hash[:]).Scan(&result.UserID, &result.ExpiresAt, &revoked, &active)
	if err == sql.ErrNoRows || (err == nil && (!active || revoked.Valid)) {
		return result, ticket.ErrUnauthenticated
	}
	if err != nil {
		return result, fmt.Errorf("read session: %w", err)
	}
	if !at.Before(result.ExpiresAt) {
		return result, ticket.ErrSessionExpired
	}
	return result, nil
}

func (s *Store) Logout(ctx context.Context, hash [32]byte, at time.Time) error {
	_, err := s.db.ExecContext(ctx, "UPDATE user_sessions SET revoked_at=? WHERE token_hash=? AND revoked_at IS NULL", at, hash[:])
	if err != nil {
		return fmt.Errorf("revoke session: %w", err)
	}
	return nil
}

func (s *Store) CurrentUser(ctx context.Context, userID int64) (ticket.Profile, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return ticket.Profile{}, err
	}
	defer tx.Rollback()
	u, err := loadUser(ctx, tx, userID, false)
	if err != nil {
		return ticket.Profile{}, err
	}
	result := ticket.Profile{User: u, Teams: make([]ticket.Team, 0)}
	rows, err := tx.QueryContext(ctx, "SELECT tm.id,tm.organization_id,tm.name,tm.active FROM team_members m JOIN teams tm ON tm.organization_id=m.organization_id AND tm.id=m.team_id WHERE m.organization_id=? AND m.user_id=? AND m.active=TRUE AND tm.active=TRUE ORDER BY tm.id", u.OrganizationID, u.ID)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var team ticket.Team
		if err := rows.Scan(&team.ID, &team.OrganizationID, &team.Name, &team.Active); err != nil {
			return result, err
		}
		result.Teams = append(result.Teams, team)
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	if err := rows.Close(); err != nil {
		return result, err
	}
	return result, tx.Commit()
}
