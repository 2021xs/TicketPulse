package syncpipe

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

// Historical versions come from immutable history snapshots, not a current-team reconstruction.
// CDC already runs while this repeatable read is scanned; event/state keys deduplicate overlap.
func AnalyticsSnapshot(ctx context.Context, s *StateStore, q *Queue) (int, map[int32]int64, error) {
	release, err := s.Lock(ctx, "analytics-snapshot")
	if err != nil {
		return 0, nil, err
	}
	defer release()
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return 0, nil, err
	}
	batch := fmt.Sprintf("%x", random)
	count := 0
	// Join only to retrieve immutable ticket identity/created_at, never historical ownership.
	rows, err := tx.QueryContext(ctx, `SELECT h.id,h.ticket_id,h.action,h.operator_id,h.version,h.before_state,h.after_state,h.responsible_team_id,h.occurred_at,h.reason,h.comment_id,t.organization_id,t.ticket_no,t.created_at FROM ticket_histories h JOIN tickets t ON t.id=h.ticket_id AND t.organization_id=h.organization_id ORDER BY h.ticket_id,h.version`)
	if err != nil {
		return 0, nil, err
	}
	for rows.Next() {
		var h ticket.History
		var before, after []byte
		var org int64
		var number string
		var created time.Time
		if err := rows.Scan(&h.ID, &h.TicketID, &h.Action, &h.OperatorID, &h.Version, &before, &after, &h.ResponsibleTeamID, &h.OccurredAt, &h.Reason, &h.CommentID, &org, &number, &created); err != nil {
			rows.Close()
			return count, nil, err
		}
		if before != nil {
			h.Before = &ticket.State{}
			if err := json.Unmarshal(before, h.Before); err != nil {
				rows.Close()
				return count, nil, err
			}
		}
		if err := json.Unmarshal(after, &h.After); err != nil {
			rows.Close()
			return count, nil, err
		}
		state := h.After
		d := search.FromTicket(ticket.Ticket{ID: h.TicketID, OrganizationID: org, TicketNo: number, Title: state.Title, Description: state.Description, Category: state.Category, Priority: state.Priority, Status: state.Status, CreatorID: state.CreatorID, AssigneeID: state.AssigneeID, InitialTeamID: state.InitialTeamID, CurrentTeamID: state.CurrentTeamID, Version: h.Version, CreatedAt: created, UpdatedAt: h.OccurredAt})
		e := Event{Schema: 2, Kind: "ticket", ID: fmt.Sprintf("history:%s:%s:%d:%d", s.Schema, batch, h.TicketID, h.Version), Source: "snapshot", TicketID: h.TicketID, Version: h.Version, EventAt: h.OccurredAt, Document: d, History: &h, FirstResolvedAt: state.FirstResolvedAt}
		if _, err := q.Publish(ctx, e); err != nil {
			rows.Close()
			return count, nil, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return count, nil, err
	}
	rows.Close()
	// Also publish the authoritative current row, including tickets whose latest history is missing.
	rows, err = tx.QueryContext(ctx, "SELECT "+projectionColumns+",first_resolved_at FROM tickets ORDER BY id")
	if err != nil {
		return count, nil, err
	}
	for rows.Next() {
		var d search.Document
		var first *time.Time
		if err := rows.Scan(&d.ID, &d.OrganizationID, &d.TicketNo, &d.Title, &d.Description, &d.Status, &d.Priority, &d.Category, &d.CreatorID, &d.AssigneeID, &d.InitialTeamID, &d.CurrentTeamID, &d.CreatedAt, &d.UpdatedAt, &d.Version, &first); err != nil {
			rows.Close()
			return count, nil, err
		}
		d.PriorityRank = map[string]int{"low": 1, "medium": 2, "high": 3, "urgent": 4}[d.Priority]
		e := Event{Schema: 2, Kind: "ticket", ID: fmt.Sprintf("current:%s:%s:%d:%d", s.Schema, batch, d.ID, d.Version), Source: "snapshot", TicketID: d.ID, Version: d.Version, EventAt: d.UpdatedAt, Document: d, FirstResolvedAt: first}
		if _, err := q.Publish(ctx, e); err != nil {
			rows.Close()
			return count, nil, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return count, nil, err
	}
	rows.Close()
	if err := tx.Commit(); err != nil {
		return count, nil, err
	}
	end := Event{Schema: 2, Kind: "watermark", ID: "snapshot-end:" + batch, Source: "snapshot", EventAt: time.Now().UTC()}
	offsets, err := q.PublishBarrier(ctx, end)
	if err != nil {
		return count, nil, err
	}
	// All partition end offsets must be committed to prove that history import was handled.
	data, err := json.Marshal(offsets)
	if err != nil {
		return count, nil, err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO sync_kafka_analytics_bootstrap(pipeline,topic,end_offsets,published_at) VALUES('ck-v1',?,?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE topic=VALUES(topic),end_offsets=VALUES(end_offsets),published_at=VALUES(published_at)`, q.Topic, data)
	return count, offsets, err
}
