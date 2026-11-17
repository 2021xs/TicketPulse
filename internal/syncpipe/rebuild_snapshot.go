package syncpipe

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

func (r *Rebuilder) freeze(ctx context.Context, j RebuildJob) (RebuildJob, error) {
	if err := r.Queue.Ensure(ctx); err != nil {
		return j, err
	}
	if err := r.Store.VerifyQueue(ctx, r.Queue); err != nil {
		return j, err
	}
	topics, err := r.Queue.Admin.ListTopics(ctx, r.Queue.Topic)
	if err != nil {
		return j, err
	}
	j.TopicID = topics[r.Queue.Topic].ID.String()
	ends, err := r.Queue.Admin.ListEndOffsets(ctx, r.Queue.Topic)
	if err != nil {
		return j, err
	}
	j.StartOffsets = map[int32]int64{}
	for p := int32(0); p < r.Queue.Partitions; p++ {
		end, ok := ends.Lookup(r.Queue.Topic, p)
		if !ok || end.Err != nil {
			return j, fmt.Errorf("freeze end offset unavailable")
		}
		j.StartOffsets[p] = end.Offset
	}
	// Starts precede the source snapshot: in-flight publications are included in
	// the overlap, even when CDC had already read a transaction before freezing.
	j.SourcePosition, err = CurrentPosition(ctx, r.Store.DB)
	if err != nil {
		return j, err
	}
	j.SourceUUID = j.SourcePosition.ServerUUID
	tx, err := r.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return j, err
	}
	defer tx.Rollback()
	var tickets, histories int64
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM tickets),(SELECT count(*) FROM ticket_histories)`).Scan(&tickets, &histories); err != nil {
		return j, err
	}
	hasher := sha256.New()
	sequence := int64(0)
	appendEvent := func(e Event) error {
		if err := e.Validate(); err != nil {
			return err
		}
		if j.Target != "es" {
			if _, _, err := AnalysisProjection(e); err != nil {
				return err
			}
		}
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if len(data) > 512<<10 {
			return fmt.Errorf("source event exceeds bounded payload budget")
		}
		sequence++
		hasher.Write(data)
		_, err = tx.ExecContext(ctx, `INSERT INTO sync_rebuild_items(job_id,sequence_id,payload) VALUES(?,?,?)`, j.ID, sequence, data)
		return err
	}
	if j.Target != "es" {
		var ticketID, version int64
		var previous *ticket.State
		for {
			rows, err := tx.QueryContext(ctx, `SELECT h.id,h.ticket_id,h.action,h.operator_id,h.version,h.before_state,h.after_state,h.responsible_team_id,h.occurred_at,h.reason,h.comment_id,t.organization_id,t.ticket_no,t.created_at FROM ticket_histories h JOIN tickets t ON t.id=h.ticket_id AND t.organization_id=h.organization_id WHERE h.ticket_id>? OR (h.ticket_id=? AND h.version>?) ORDER BY h.ticket_id,h.version LIMIT ?`, ticketID, ticketID, version, j.Options.BatchSize)
			if err != nil {
				return j, err
			}
			events := []Event{}
			for rows.Next() {
				var h ticket.History
				var before, after []byte
				var org int64
				var number string
				var created time.Time
				if err = rows.Scan(&h.ID, &h.TicketID, &h.Action, &h.OperatorID, &h.Version, &before, &after, &h.ResponsibleTeamID, &h.OccurredAt, &h.Reason, &h.CommentID, &org, &number, &created); err != nil {
					rows.Close()
					return j, err
				}
				if before != nil {
					h.Before = &ticket.State{}
					if err = json.Unmarshal(before, h.Before); err != nil {
						rows.Close()
						return j, err
					}
				}
				if err = json.Unmarshal(after, &h.After); err != nil {
					rows.Close()
					return j, err
				}
				if h.TicketID != ticketID {
					previous = nil
					version = 0
				}
				if h.Version != version+1 || (previous == nil && (h.Action != ticket.ActionCreated || h.Before != nil)) {
					rows.Close()
					return j, fmt.Errorf("SOURCE_HISTORY_INCOMPLETE: ticket %d version %d", h.TicketID, h.Version)
				}
				if previous != nil {
					a, _ := json.Marshal(previous)
					b, _ := json.Marshal(h.Before)
					if string(a) != string(b) {
						rows.Close()
						return j, fmt.Errorf("SOURCE_HISTORY_CHAIN_CONFLICT: ticket %d version %d", h.TicketID, h.Version)
					}
				}
				state := h.After
				d := search.FromTicket(ticket.Ticket{ID: h.TicketID, OrganizationID: org, TicketNo: number, Title: state.Title, Description: state.Description, Category: state.Category, Priority: state.Priority, Status: state.Status, CreatorID: state.CreatorID, AssigneeID: state.AssigneeID, InitialTeamID: state.InitialTeamID, CurrentTeamID: state.CurrentTeamID, Version: h.Version, CreatedAt: created, UpdatedAt: h.OccurredAt})
				e := Event{Schema: 2, Kind: "ticket", ID: fmt.Sprintf("rebuild:%s:history:%d:%d", j.ID, h.TicketID, h.Version), Source: "snapshot", TicketID: h.TicketID, Version: h.Version, EventAt: h.OccurredAt, Document: d, History: &h, FirstResolvedAt: state.FirstResolvedAt}
				events = append(events, e)
				previous = &state
				ticketID = h.TicketID
				version = h.Version
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return j, err
			}
			for _, e := range events {
				if err = appendEvent(e); err != nil {
					return j, err
				}
			}
			if len(events) < j.Options.BatchSize {
				break
			}
		}
		if sequence != histories {
			return j, fmt.Errorf("SOURCE_HISTORY_INCOMPLETE: orphaned/missing history rows")
		}
	}
	lastID := int64(0)
	currentCount := int64(0)
	for {
		rows, err := tx.QueryContext(ctx, `SELECT t.id,t.organization_id,t.ticket_no,t.title,t.description,t.resolution_note,t.category,t.priority,t.status,t.creator_id,t.assignee_id,t.initial_team_id,t.current_team_id,t.version,t.created_at,t.updated_at,t.first_resolved_at,t.closed_at,t.reopen_count,h.after_state,(SELECT count(*) FROM ticket_histories allh WHERE allh.ticket_id=t.id AND allh.organization_id=t.organization_id) FROM tickets t LEFT JOIN ticket_histories h ON h.ticket_id=t.id AND h.organization_id=t.organization_id AND h.version=t.version WHERE t.id>? ORDER BY t.id LIMIT ?`, lastID, j.Options.BatchSize)
		if err != nil {
			return j, err
		}
		events := []Event{}
		for rows.Next() {
			var t ticket.Ticket
			var after []byte
			var count int64
			if err = rows.Scan(&t.ID, &t.OrganizationID, &t.TicketNo, &t.Title, &t.Description, &t.ResolutionNote, &t.Category, &t.Priority, &t.Status, &t.CreatorID, &t.AssigneeID, &t.InitialTeamID, &t.CurrentTeamID, &t.Version, &t.CreatedAt, &t.UpdatedAt, &t.FirstResolvedAt, &t.ClosedAt, &t.ReopenCount, &after, &count); err != nil {
				rows.Close()
				return j, err
			}
			if j.Target != "es" {
				var latest ticket.State
				if len(after) == 0 || count != t.Version {
					rows.Close()
					return j, fmt.Errorf("SOURCE_HISTORY_INCOMPLETE: ticket %d has %d histories/version %d", t.ID, count, t.Version)
				}
				if err = json.Unmarshal(after, &latest); err != nil {
					rows.Close()
					return j, err
				}
				a, _ := json.Marshal(latest)
				b, _ := json.Marshal(t.State())
				if string(a) != string(b) {
					rows.Close()
					return j, fmt.Errorf("SOURCE_HISTORY_CURRENT_CONFLICT: ticket %d", t.ID)
				}
			}
			events = append(events, Event{Schema: 2, Kind: "ticket", ID: fmt.Sprintf("rebuild:%s:current:%d:%d", j.ID, t.ID, t.Version), Source: "snapshot", TicketID: t.ID, Version: t.Version, EventAt: t.UpdatedAt, Document: search.FromTicket(t), FirstResolvedAt: t.FirstResolvedAt})
			lastID = t.ID
			currentCount++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return j, err
		}
		for _, e := range events {
			if err = appendEvent(e); err != nil {
				return j, err
			}
		}
		if len(events) < j.Options.BatchSize {
			break
		}
	}
	if currentCount != tickets {
		return j, fmt.Errorf("source current scan incomplete")
	}
	j.Total = sequence
	j.SourceSHA256 = fmt.Sprintf("%x", hasher.Sum(nil))
	j.Phase = "snapshot"
	j.Status = "frozen"
	starts, _ := json.Marshal(j.StartOffsets)
	pos, _ := json.Marshal(j.SourcePosition)
	_, err = tx.ExecContext(ctx, `UPDATE sync_rebuild_jobs SET status='frozen',phase='snapshot',source_uuid=?,source_position=?,topic_id=?,start_offsets=?,source_sha256=?,total=?,updated_at=UTC_TIMESTAMP(6) WHERE job_id=?`, j.SourceUUID, pos, j.TopicID, starts, j.SourceSHA256, j.Total, j.ID)
	if err != nil {
		return j, err
	}
	for p, n := range j.StartOffsets {
		if _, err = tx.ExecContext(ctx, `INSERT INTO sync_rebuild_offsets(job_id,partition_id,next_offset) VALUES(?,?,?)`, j.ID, p, n); err != nil {
			return j, err
		}
	}
	details, _ := json.Marshal(map[string]any{"tickets": tickets, "histories": histories, "sha256": j.SourceSHA256, "start_offsets": j.StartOffsets, "source_position": j.SourcePosition})
	if _, err = tx.ExecContext(ctx, `INSERT INTO sync_rebuild_log(job_id,phase,outcome,details,occurred_at) VALUES(?,'freeze','frozen',?,UTC_TIMESTAMP(6))`, j.ID, details); err != nil {
		return j, err
	}
	if err = tx.Commit(); err != nil {
		return j, err
	}
	return r.Status(ctx, j.ID)
}

// Validate the frozen input independently of its cursor before resuming. A
// corrupted/missing control row must not silently become a different rebuild.
func (r *Rebuilder) verifySnapshot(ctx context.Context, j RebuildJob) error {
	hasher := sha256.New()
	var sequence int64
	for {
		rows, err := r.Store.DB.QueryContext(ctx, `SELECT sequence_id,payload FROM sync_rebuild_items WHERE job_id=? AND sequence_id>? ORDER BY sequence_id LIMIT ?`, j.ID, sequence, j.Options.BatchSize)
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			var next int64
			var payload []byte
			var event Event
			if err = rows.Scan(&next, &payload); err != nil {
				rows.Close()
				return err
			}
			if next != sequence+1 {
				rows.Close()
				return fmt.Errorf("REBUILD_SNAPSHOT_CORRUPT: frozen sequence has a gap")
			}
			if err = json.Unmarshal(payload, &event); err != nil {
				rows.Close()
				return err
			}
			canonical, err := json.Marshal(event)
			if err != nil {
				rows.Close()
				return err
			}
			hasher.Write(canonical)
			sequence = next
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if count < j.Options.BatchSize {
			break
		}
	}
	if sequence != j.Total || fmt.Sprintf("%x", hasher.Sum(nil)) != j.SourceSHA256 {
		return fmt.Errorf("REBUILD_SNAPSHOT_CORRUPT: count or digest differs; preserve evidence and create a new job")
	}
	return nil
}
