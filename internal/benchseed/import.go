package benchseed

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"ticketpulse/internal/projection"
	"ticketpulse/internal/ticket"
)

// Generate imports whole ticket chains in bounded transactions, refusing any
// pre-existing ticket/history/comment/request data. Interrupted partial imports
// remain preserved; rerunning requires a new isolated schema, never truncation.
func Generate(ctx context.Context, db *sql.DB, o Options) (m Manifest, err error) {
	if err = o.Validate(); err != nil {
		return m, err
	}
	if o.DryEstimate {
		return Estimate(o)
	}
	m = newManifest(o)
	m.StartedAt = time.Now().UTC()
	m.Status = "running"
	m.HashScope = "SHA256 of committed ticket/history/comment Bundle JSON in ticket ID order, newline delimited; deterministic across batch sizes"
	expectedRules(o, &m)
	defer func() {
		m.CompletedAt = time.Now().UTC()
		m.ElapsedSeconds = m.CompletedAt.Sub(m.StartedAt).Seconds()
		if m.ElapsedSeconds > 0 {
			m.SQLImportTicketsPerSecond = finite(float64(m.Counts.Tickets) / m.ElapsedSeconds)
		}
		if err != nil {
			m.Status = "failed"
			m.Failure = err.Error()
		}
	}()
	if db == nil || !strings.HasPrefix(o.Schema, "ticketpulse_test_m5_") || len(o.Schema) > 64 || o.ExpectedUUID == "" {
		return m, fmt.Errorf("import requires verified ticketpulse_test_m5_ schema and explicit MySQL UUID")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return m, err
	}
	defer conn.Close()
	var locked int
	lockName := "tp:benchseed:" + o.Schema
	if len(lockName) > 64 {
		lockName = fmt.Sprintf("tp:benchseed:%x", sha256.Sum256([]byte(o.Schema)))[:64]
	}
	if err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?,0)", lockName).Scan(&locked); err != nil || locked != 1 {
		return m, fmt.Errorf("isolated seed writer already active or lock unavailable")
	}
	defer func() {
		release, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = conn.ExecContext(release, "SELECT RELEASE_LOCK(?)", lockName)
	}()
	var uuid, schema string
	if err = conn.QueryRowContext(ctx, "SELECT @@server_uuid,DATABASE()").Scan(&uuid, &schema); err != nil {
		return m, err
	}
	if uuid != o.ExpectedUUID || schema != o.Schema {
		return m, fmt.Errorf("MySQL source UUID/schema differs; refusing import")
	}
	m.SourceUUID = uuid
	// Fixture import uses bulk SQL and an allocator DDL, not the business
	// transaction path. It may only run before installing maintenance metadata.
	// Never use it as a writer around a managed/frozen application source.
	if _, gateErr := projection.ReadGate(ctx, conn); !projection.MissingTable(gateErr) {
		return m, fmt.Errorf("bulk fixture import requires an unmanaged schema without migration 011; refusing managed/maintenance source")
	}
	for _, table := range []string{"tickets", "ticket_histories", "ticket_comments", "ticket_create_requests"} {
		var count int64
		if err = conn.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			return m, err
		}
		if count != 0 {
			return m, fmt.Errorf("refusing to seed nonempty %s; preserve partial import and create a new isolated schema", table)
		}
	}
	if err = validateActors(ctx, conn); err != nil {
		return m, err
	}
	// Reserve all fixture IDs before import. HTTP writers obtain their IDs from
	// this allocator and therefore cannot collide with seeded ticket IDs.
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE ticket_create_requests AUTO_INCREMENT=%d", o.Tickets+1000)); err != nil {
		return m, err
	}
	digest := sha256.New()
	eventDigest := sha256.New()
	var commentBase int64
	for first := int64(1); first <= o.Tickets; first += int64(o.BatchSize) {
		if err = ctx.Err(); err != nil {
			return m, err
		}
		end := first + int64(o.BatchSize)
		if end > o.Tickets+1 {
			end = o.Tickets + 1
		}
		batch := make([]Bundle, 0, end-first)
		for id := first; id < end; id++ {
			b, problem := GenerateTicket(id, o.Seed, commentBase)
			if problem != nil {
				return m, problem
			}
			if problem = ValidateBundle(b); problem != nil {
				return m, problem
			}
			commentBase += int64(len(b.Comments))
			batch = append(batch, b)
		}
		tx, problem := conn.BeginTx(ctx, nil)
		if problem != nil {
			return m, problem
		}
		problem = insertBatch(ctx, tx, batch)
		if problem != nil {
			_ = tx.Rollback()
			return m, problem
		}
		if problem = tx.Commit(); problem != nil {
			m.CommitOutcomeUnknown = true
			return m, fmt.Errorf("SQL batch commit outcome unknown for ticket range %d–%d: %w", first, end-1, problem)
		}
		for _, b := range batch {
			payload, _ := json.Marshal(b)
			digest.Write(payload)
			digest.Write([]byte{'\n'})
			m.EstimatedPayloadBytes += int64(len(payload) + 1)
			countBundle(&m, b)
		}
		m.CommittedBatches++
		m.SHA256 = fmt.Sprintf("%x", digest.Sum(nil))
		for _, b := range batch {
			if o.EventsWriter != nil {
				lines, problem := eventLines(b, o.Seed, o.Schema)
				if problem != nil {
					return m, problem
				}
				for _, line := range lines {
					if _, problem = o.EventsWriter.Write(line); problem != nil {
						return m, fmt.Errorf("committed SQL rows preserved; event export failed: %w", problem)
					}
					eventDigest.Write(line)
					m.ExportedEvents++
				}
				m.EventsSHA256 = fmt.Sprintf("%x", eventDigest.Sum(nil))
			}
		}
	}
	if m.Counts != m.Expected {
		return m, fmt.Errorf("generated counts differ from independent profile expectations")
	}
	m.Status = "completed"
	m.HashScope = "SHA256 of committed ticket/history/comment Bundle JSON in ticket ID order, newline delimited; deterministic across batch sizes"
	return m, nil
}
func validateActors(ctx context.Context, conn *sql.Conn) error {
	for _, actor := range []struct {
		id   int64
		role string
	}{{1, ticket.RoleRequester}, {2, ticket.RoleRequester}, {3, ticket.RoleAgent}, {4, ticket.RoleAgent}, {5, ticket.RoleSupervisor}, {10, ticket.RoleAgent}, {11, ticket.RoleAgent}} {
		var org int64
		var active bool
		var role string
		if err := conn.QueryRowContext(ctx, "SELECT organization_id,active,role FROM users WHERE id=?", actor.id).Scan(&org, &active, &role); err != nil {
			return fmt.Errorf("fixture seed actor %d missing", actor.id)
		}
		if org != 1 || !active || role != actor.role {
			return fmt.Errorf("fixture actor %d organization/role/activity differs", actor.id)
		}
	}
	for _, team := range []int64{10, 20, 90} {
		var org int64
		var active bool
		if err := conn.QueryRowContext(ctx, "SELECT organization_id,active FROM teams WHERE id=?", team).Scan(&org, &active); err != nil || org != 1 || !active {
			return fmt.Errorf("fixture team %d missing/inactive", team)
		}
	}
	for _, p := range [][2]int64{{10, 3}, {10, 11}, {20, 4}, {90, 10}} {
		var active bool
		if err := conn.QueryRowContext(ctx, "SELECT active FROM team_members WHERE organization_id=1 AND team_id=? AND user_id=?", p[0], p[1]).Scan(&active); err != nil || !active {
			return fmt.Errorf("fixture active membership %d/%d missing", p[0], p[1])
		}
	}
	return nil
}

func insertRows(ctx context.Context, tx *sql.Tx, table, columns string, width int, rows [][]any) error {
	// MySQL has a 65535 prepared-parameter ceiling; an independent row chunk
	// also limits each history statement's payload without splitting ticket txs.
	for start := 0; start < len(rows); start += 300 {
		end := start + 300
		if end > len(rows) {
			end = len(rows)
		}
		args := make([]any, 0, (end-start)*width)
		groups := make([]string, 0, end-start)
		group := "(" + strings.TrimRight(strings.Repeat("?,", width), ",") + ")"
		for _, row := range rows[start:end] {
			if len(row) != width {
				return fmt.Errorf("internal seed SQL width mismatch")
			}
			args = append(args, row...)
			groups = append(groups, group)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+" ("+columns+") VALUES "+strings.Join(groups, ","), args...); err != nil {
			return err
		}
	}
	return nil
}
func insertBatch(ctx context.Context, tx *sql.Tx, batch []Bundle) error {
	tickets, comments, histories := make([][]any, 0, len(batch)), make([][]any, 0), make([][]any, 0)
	for _, b := range batch {
		t := b.Ticket
		tickets = append(tickets, []any{t.ID, t.OrganizationID, t.TicketNo, t.Title, t.Description, t.ResolutionNote, t.Category, t.Priority, t.Status, t.CreatorID, t.AssigneeID, t.InitialTeamID, t.CurrentTeamID, t.Version, t.CreatedAt, t.UpdatedAt, t.FirstResolvedAt, t.ClosedAt, t.ReopenCount})
		for _, c := range b.Comments {
			comments = append(comments, []any{c.ID, t.OrganizationID, c.TicketID, c.AuthorID, c.Content, c.Version, c.CreatedAt})
		}
		for _, h := range b.Histories {
			var before any
			if h.Before != nil {
				data, _ := json.Marshal(h.Before)
				before = data
			}
			after, _ := json.Marshal(h.After)
			histories = append(histories, []any{t.OrganizationID, h.TicketID, h.Action, h.OperatorID, h.Version, before, after, h.ResponsibleTeamID, h.OccurredAt, h.Reason, h.CommentID})
		}
	}
	if err := insertRows(ctx, tx, "tickets", "id,organization_id,ticket_no,title,description,resolution_note,category,priority,status,creator_id,assignee_id,initial_team_id,current_team_id,version,created_at,updated_at,first_resolved_at,closed_at,reopen_count", 19, tickets); err != nil {
		return err
	}
	if err := insertRows(ctx, tx, "ticket_comments", "id,organization_id,ticket_id,author_id,content,version,created_at", 7, comments); err != nil {
		return err
	}
	return insertRows(ctx, tx, "ticket_histories", "organization_id,ticket_id,action,operator_id,version,before_state,after_state,responsible_team_id,occurred_at,reason,comment_id", 11, histories)
}
