package mysqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

type Store struct {
	db           *sql.DB
	gateRequired atomic.Bool
}

func New(db *sql.DB) *Store { return &Store{db: db} }

// Managed runtimes require migration 011 before they admit any business writes.
func (s *Store) RequireBusinessGate() { s.gateRequired.Store(true) }

func (s *Store) acquireBusinessWrite(ctx context.Context, tx *sql.Tx) error {
	seen, err := projection.AcquireBusinessWrite(ctx, tx, s.gateRequired.Load())
	if seen {
		s.gateRequired.Store(true)
	}
	if !seen && err == nil && s.gateRequired.Load() {
		return fmt.Errorf("business write gate disappeared during admission")
	}
	return err
}

func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid MySQL DSN")
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("MySQL database name is required")
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.MultiStatements = false
	cfg.ClientFoundRows = false
	cfg.Params = map[string]string{"time_zone": "'+00:00'", "sql_mode": "'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'"}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open MySQL: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(3 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect MySQL: %w", err)
	}
	return db, nil
}

// Authenticate resolves only a server-stored credential. User/organization/role headers are never used.
func (s *Store) Authenticate(ctx context.Context, token string) (int64, error) {
	if len(token) < 8 || len(token) > 512 {
		return 0, ticket.ErrUnauthenticated
	}
	hash := sha256.Sum256([]byte(token))
	var id int64
	if d := search.Diagnostic(ctx); d != nil {
		d.MySQLQueries++
	}
	err := s.db.QueryRowContext(ctx, "SELECT id FROM users WHERE auth_token_hash = ? AND active = TRUE", hash[:]).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, ticket.ErrUnauthenticated
	}
	if err != nil {
		return 0, fmt.Errorf("authenticate user: %w", err)
	}
	return id, nil
}

func loadUser(ctx context.Context, tx *sql.Tx, id int64, lock bool) (ticket.User, error) {
	query := "SELECT id, organization_id, name, role, active FROM users WHERE id = ?"
	if lock {
		query += " FOR SHARE"
	}
	var u ticket.User
	err := tx.QueryRowContext(ctx, query, id).Scan(&u.ID, &u.OrganizationID, &u.Name, &u.Role, &u.Active)
	if err == sql.ErrNoRows {
		return u, ticket.ErrUnauthenticated
	}
	if err != nil {
		return u, fmt.Errorf("load user: %w", err)
	}
	if !u.Active {
		return u, ticket.ErrUnauthenticated
	}
	return u, nil
}

const ticketColumns = "id, organization_id, ticket_no, title, description, resolution_note, category, priority, status, creator_id, assignee_id, initial_team_id, current_team_id, version, created_at, updated_at, first_resolved_at, closed_at, reopen_count"

type scanner interface{ Scan(...any) error }

func scanTicket(row scanner) (ticket.Ticket, error) {
	var t ticket.Ticket
	err := row.Scan(&t.ID, &t.OrganizationID, &t.TicketNo, &t.Title, &t.Description, &t.ResolutionNote, &t.Category, &t.Priority, &t.Status, &t.CreatorID, &t.AssigneeID, &t.InitialTeamID, &t.CurrentTeamID, &t.Version, &t.CreatedAt, &t.UpdatedAt, &t.FirstResolvedAt, &t.ClosedAt, &t.ReopenCount)
	return t, err
}

func loadTicket(ctx context.Context, tx *sql.Tx, organizationID, id int64, lock bool) (ticket.Ticket, error) {
	query := "SELECT " + ticketColumns + " FROM tickets WHERE organization_id = ? AND id = ?"
	if lock {
		query += " FOR UPDATE"
	}
	t, err := scanTicket(tx.QueryRowContext(ctx, query, organizationID, id))
	if err == sql.ErrNoRows {
		return t, ticket.ErrNotFound
	}
	if err != nil {
		return t, fmt.Errorf("load ticket: %w", err)
	}
	return t, nil
}

// FOR SHARE on this join protects both the team and membership used to authorize a write.
// Non-locking reads preserve Detail's existing consistent snapshot semantics.
func activeTeamMember(ctx context.Context, tx *sql.Tx, organizationID, teamID, userID int64, lock bool) (bool, error) {
	query := `SELECT m.active AND tm.active FROM team_members m JOIN teams tm ON tm.organization_id = m.organization_id AND tm.id = m.team_id WHERE m.organization_id = ? AND m.team_id = ? AND m.user_id = ?`
	if lock {
		query += " FOR SHARE"
	}
	var active bool
	err := tx.QueryRowContext(ctx, query, organizationID, teamID, userID).Scan(&active)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return active, err
}
