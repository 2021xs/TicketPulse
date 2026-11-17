package syncpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	sourcedriver "github.com/go-sql-driver/mysql"

	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

type CaptureConfig struct {
	Address, User, Password string
	ServerID                uint32
}
type Capture struct {
	Store  *StateStore
	Queue  *Queue
	Config CaptureConfig
	Log    *slog.Logger
	// Optional boundary observer. Tests can interrupt immediately after an actual
	// durable publication to verify partial-transaction replay; the CLI leaves it nil.
	AfterPublished func(Event, RecordRef) error
	Check          func(context.Context) error
}

func (c *Capture) VerifySource(ctx context.Context, cp Checkpoint) error {
	var uuid, format, image, metadata string
	var enabled int
	if err := c.Store.DB.QueryRowContext(ctx, "SELECT @@server_uuid,@@log_bin,@@binlog_format,@@binlog_row_image,@@binlog_row_metadata").Scan(&uuid, &enabled, &format, &image, &metadata); err != nil {
		return err
	}
	if uuid != cp.ServerUUID || enabled != 1 || format != "ROW" || image != "FULL" || metadata != "FULL" {
		return fmt.Errorf("CDC source UUID/config differs from checkpoint; operator inspection required")
	}
	if c.Check != nil {
		// The Binlog TCP address is configured separately from the status DB
		// DSN. Verify the actual replication source before opening its stream.
		if err := c.verifyCaptureEndpoint(ctx, cp.ServerUUID); err != nil {
			return fmt.Errorf("%w: CDC source endpoint: %v", projection.ErrTargetMismatch, err)
		}
	}
	rows, err := c.Store.DB.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return err
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var name string
		var size uint64
		var encrypted string
		if err := rows.Scan(&name, &size, &encrypted); err != nil {
			return err
		}
		if name == cp.File {
			found = true
			if uint64(cp.Pos) > size {
				return fmt.Errorf("checkpoint exceeds binary log size")
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("CDC_BINLOG_EXPIRED: checkpoint log is no longer retained; explicit re-bootstrap required")
	}
	return nil
}

func (c *Capture) verifyCaptureEndpoint(ctx context.Context, expectedUUID string) error {
	if _, _, err := net.SplitHostPort(c.Config.Address); err != nil {
		return fmt.Errorf("invalid CDC source address")
	}
	cfg := sourcedriver.NewConfig()
	cfg.Net, cfg.Addr, cfg.User, cfg.Passwd = "tcp", c.Config.Address, c.Config.User, c.Config.Password
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 3*time.Second, 3*time.Second, 3*time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var uuid string
	if err = db.QueryRowContext(checkCtx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		return err
	}
	if uuid != expectedUUID {
		return fmt.Errorf("actual Binlog server UUID differs from business source")
	}
	return nil
}

func (c *Capture) Run(ctx context.Context) (err error) {
	if c.Check != nil {
		if err := c.Check(ctx); err != nil {
			return err
		}
	}
	release, err := c.Store.Lock(ctx, "cdc")
	if err != nil {
		return err
	}
	defer release()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		state := "stopped"
		if err != nil && ctx.Err() == nil {
			state = "failed"
		}
		_ = c.Store.Heartbeat(shutdown, "cdc", state, nil, err)
	}()
	var cp Checkpoint
	if c.Check != nil {
		cp, err = c.Store.Checkpoint(ctx)
		if err == nil && !cp.BootstrapComplete {
			err = fmt.Errorf("managed capture requires a completed existing bootstrap")
		}
	} else {
		cp, err = Bootstrap(ctx, c.Store, c.Queue)
	}
	if err != nil {
		return err
	}
	if err := c.VerifySource(ctx, cp); err != nil {
		return err
	}
	var analyticsEnabled bool
	if err := c.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name='sync_source_watermark')", c.Store.Schema).Scan(&analyticsEnabled); err != nil {
		return err
	}
	host, portText, err := net.SplitHostPort(c.Config.Address)
	if err != nil {
		return err
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return err
	}
	log := c.Log
	if log == nil {
		log = slog.Default()
	}
	// The library's reconnect-from-read-buffer semantics are not our durable checkpoint.
	// On a disconnect we exit and the outer runner restarts from acknowledged publications.
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: c.Config.ServerID, Flavor: "mysql", Host: host, Port: uint16(port), User: c.Config.User, Password: c.Config.Password, Charset: "utf8mb4", ParseTime: false, TimestampStringLocation: time.UTC, VerifyChecksum: true, DisableRetrySync: true, HeartbeatPeriod: time.Second, ReadTimeout: 10 * time.Second, Logger: slog.New(slog.NewTextHandler(discardLog{}, nil))})
	defer syncer.Close()
	streamer, err := syncer.StartSync(gomysql.Position{Name: cp.File, Pos: cp.Pos})
	if err != nil {
		return fmt.Errorf("start binlog stream: %w", err)
	}
	log.Info("CDC resumed", "file", cp.File, "position", cp.Pos, "schema", c.Store.Schema)
	pending := map[string]Event{}
	histories := map[string]*ticket.History{}
	order := []string{}
	position := cp.Position
	safePosition := cp.Position
	lastCheckpoint := time.Now()
	lastBeat := time.Time{}
	lastWatermark := time.Time{}
	var watermark *Event
	committed := func() error {
		if c.Check != nil {
			if err := c.Check(ctx); err != nil {
				return err
			}
		}
		if len(order) == 0 && watermark == nil {
			histories = map[string]*ticket.History{}
			safePosition = position
			return nil
		}
		for _, key := range order {
			event := pending[key]
			commit := position
			event.Commit = &commit
			event.ID = fmt.Sprintf("binlog:%s:%s:%d:%d:%d", position.ServerUUID, position.File, position.Pos, event.TicketID, event.Version)
			event.History = histories[key]
			ref, err := c.Queue.Publish(ctx, event)
			if err != nil {
				return fmt.Errorf("publish committed change: %w", err)
			}
			if c.AfterPublished != nil {
				if err := c.AfterPublished(event, ref); err != nil {
					return err
				}
			}
		}
		if watermark != nil {
			commit := position
			watermark.Commit = &commit
			if _, err := c.Queue.PublishBarrier(ctx, *watermark); err != nil {
				return err
			}
		}
		// No advancement until every event in this committed transaction has a durable MQ ack.
		if err := c.Store.SaveCheckpoint(ctx, position, true); err != nil {
			return fmt.Errorf("save CDC checkpoint: %w", err)
		}
		safePosition = position
		lastCheckpoint = time.Now()
		var eventAt *time.Time
		if len(order) > 0 {
			at := pending[order[len(order)-1]].EventAt
			eventAt = &at
		}
		if err := c.Store.Heartbeat(ctx, "cdc", "running", eventAt, nil); err != nil {
			return err
		}
		pending = map[string]Event{}
		histories = map[string]*ticket.History{}
		order = []string{}
		watermark = nil
		return nil
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(lastBeat) > 2*time.Second {
			if err := c.Store.Heartbeat(ctx, "cdc", "running", nil, nil); err != nil {
				return err
			}
			lastBeat = time.Now()
		}
		if analyticsEnabled && time.Since(lastWatermark) > 2*time.Second {
			if err := c.Store.IssueWatermark(ctx); err != nil {
				return err
			}
			lastWatermark = time.Now()
		}
		// Advance across irrelevant committed/control traffic periodically, not once per
		// checkpoint row (which itself is binlogged). Keeps an idle live capture resumable.
		if time.Since(lastCheckpoint) > 30*time.Second && safePosition != cp.Position {
			if err := c.Store.SaveCheckpoint(ctx, safePosition, true); err != nil {
				return err
			}
			cp.Position = safePosition
			lastCheckpoint = time.Now()
		}
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		event, readErr := streamer.GetEvent(readCtx)
		cancel()
		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if readErr == context.DeadlineExceeded {
				continue
			}
			return fmt.Errorf("binlog stream stopped: %w", readErr)
		}
		if event.Header.LogPos > 0 {
			position.Pos = event.Header.LogPos
		}
		switch value := event.Event.(type) {
		case *replication.RotateEvent:
			if len(order) > 0 {
				return fmt.Errorf("unexpected rotation inside ticket transaction")
			}
			position.File = string(value.NextLogName)
			position.Pos = uint32(value.Position)
			safePosition = position
		case *replication.RowsEvent:
			if string(value.Table.Schema) != c.Store.Schema {
				continue
			}
			table := string(value.Table.Table)
			if table != "tickets" && table != "ticket_histories" && (table != "sync_source_watermark" || !analyticsEnabled) {
				continue
			}
			if event.Header.EventType == replication.DELETE_ROWS_EVENTv1 || event.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
				return fmt.Errorf("physical deletion of ticket/history is outside the M1 contract; capture halted")
			}
			rows := value.Rows
			update := event.Header.EventType == replication.UPDATE_ROWS_EVENTv1 || event.Header.EventType == replication.UPDATE_ROWS_EVENTv2 || event.Header.EventType == replication.PARTIAL_UPDATE_ROWS_EVENT
			if update && len(rows)%2 != 0 {
				return fmt.Errorf("incomplete update row pair")
			}
			for i, row := range rows {
				if update && i%2 == 0 {
					continue
				}
				fields, err := rowFields(value.Table, row)
				if err != nil {
					return err
				}
				if table == "tickets" {
					d, err := decodeDocument(fields)
					if err != nil {
						return fmt.Errorf("decode committed ticket row: %w", err)
					}
					key := fmt.Sprintf("%d:%d", d.ID, d.Version)
					if _, exists := pending[key]; !exists {
						order = append(order, key)
					}
					item := Event{Schema: 1, Source: "binlog", TicketID: d.ID, Version: d.Version, EventAt: d.UpdatedAt, Document: d}
					if analyticsEnabled {
						item.Schema = 2
						item.Kind = "ticket"
						item.FirstResolvedAt, err = firstResolved(fields["first_resolved_at"])
						if err != nil {
							return err
						}
					}
					pending[key] = item
				} else if table == "ticket_histories" {
					h, err := decodeHistory(fields)
					if err != nil {
						return fmt.Errorf("decode history row: %w", err)
					}
					histories[fmt.Sprintf("%d:%d", h.TicketID, h.Version)] = &h
				} else {
					at, err := rowTime(fields["issued_at"])
					if err != nil {
						return err
					}
					watermark = &Event{Schema: 2, Kind: "watermark", ID: rowText(fields["barrier_id"]), Source: "binlog", EventAt: at}
				}
				if len(pending) > 10000 {
					return fmt.Errorf("transaction exceeds CDC ticket budget; capture halted before checkpoint")
				}
			}
		case *replication.XIDEvent:
			if err := committed(); err != nil {
				return err
			}
		case *replication.QueryEvent:
			query := strings.ToUpper(strings.TrimSpace(string(value.Query)))
			if query == "COMMIT" {
				if err := committed(); err != nil {
					return err
				}
			}
			if query == "ROLLBACK" {
				pending = map[string]Event{}
				histories = map[string]*ticket.History{}
				order = []string{}
				watermark = nil
			}
		}
	}
}

// Avoid library config logging: credentials are passed only to the connector, never slog.
type discardLog struct{}

func (discardLog) Write(b []byte) (int, error) { return len(b), nil }

func rowFields(table *replication.TableMapEvent, row []any) (map[string]any, error) {
	if len(table.ColumnName) != len(row) {
		return nil, fmt.Errorf("ROW/FULL column-name metadata is required")
	}
	result := make(map[string]any, len(row))
	for i, name := range table.ColumnName {
		result[string(name)] = row[i]
	}
	return result, nil
}
func rowInt(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case uint64:
		if n > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("integer overflow")
		}
		return int64(n), nil
	case int32:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case int:
		return int64(n), nil
	case uint8:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case []byte:
		return strconv.ParseInt(string(n), 10, 64)
	case string:
		return strconv.ParseInt(n, 10, 64)
	}
	return 0, fmt.Errorf("unexpected integer row type %T", v)
}
func rowText(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return ""
	}
}
func rowTime(v any) (time.Time, error) {
	if t, ok := v.(time.Time); ok {
		return t.UTC(), nil
	}
	return time.ParseInLocation("2006-01-02 15:04:05.999999", rowText(v), time.UTC)
}
func decodeDocument(fields map[string]any) (search.Document, error) {
	var d search.Document
	for _, field := range []struct {
		name   string
		target *int64
	}{{"id", &d.ID}, {"organization_id", &d.OrganizationID}, {"creator_id", &d.CreatorID}, {"initial_team_id", &d.InitialTeamID}, {"current_team_id", &d.CurrentTeamID}, {"version", &d.Version}} {
		n, err := rowInt(fields[field.name])
		if err != nil {
			return d, err
		}
		*field.target = n
	}
	if fields["assignee_id"] != nil {
		n, err := rowInt(fields["assignee_id"])
		if err != nil {
			return d, err
		}
		d.AssigneeID = &n
	}
	d.TicketNo = rowText(fields["ticket_no"])
	d.Title = rowText(fields["title"])
	d.Description = rowText(fields["description"])
	d.Status = rowText(fields["status"])
	d.Priority = rowText(fields["priority"])
	d.Category = rowText(fields["category"])
	var err error
	d.CreatedAt, err = rowTime(fields["created_at"])
	if err != nil {
		return d, err
	}
	d.UpdatedAt, err = rowTime(fields["updated_at"])
	if err != nil {
		return d, err
	}
	d.PriorityRank = map[string]int{"low": 1, "medium": 2, "high": 3, "urgent": 4}[d.Priority]
	return d, d.Validate()
}
func decodeHistory(f map[string]any) (ticket.History, error) {
	var h ticket.History
	for _, field := range []struct {
		name   string
		target *int64
	}{{"id", &h.ID}, {"ticket_id", &h.TicketID}, {"operator_id", &h.OperatorID}, {"version", &h.Version}, {"responsible_team_id", &h.ResponsibleTeamID}} {
		n, err := rowInt(f[field.name])
		if err != nil {
			return h, err
		}
		*field.target = n
	}
	h.Action = rowText(f["action"])
	var err error
	h.OccurredAt, err = rowTime(f["occurred_at"])
	if err != nil {
		return h, err
	}
	if f["before_state"] != nil {
		h.Before = &ticket.State{}
		if err := json.Unmarshal([]byte(rowText(f["before_state"])), h.Before); err != nil {
			return h, err
		}
	}
	if err := json.Unmarshal([]byte(rowText(f["after_state"])), &h.After); err != nil {
		return h, err
	}
	if f["reason"] != nil {
		v := rowText(f["reason"])
		h.Reason = &v
	}
	if f["comment_id"] != nil {
		n, err := rowInt(f["comment_id"])
		if err != nil {
			return h, err
		}
		h.CommentID = &n
	}
	return h, nil
}
