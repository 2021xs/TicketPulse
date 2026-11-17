package syncpipe

import (
	"fmt"
	"time"

	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

type Position struct {
	ServerUUID string `json:"server_uuid"`
	File       string `json:"binlog_file"`
	Pos        uint32 `json:"binlog_pos"`
}

// One message per committed ticket/version, regardless of associated comment/history rows.
// History is retained for a future independent analytics consumer, not interpreted by ES.
type Event struct {
	EventType       string          `json:"event_type,omitempty"`
	Schema          int             `json:"schema"`
	Kind            string          `json:"kind,omitempty"` // schema 2: ticket or watermark; schema 1 remains readable.
	ID              string          `json:"event_id"`
	Source          string          `json:"source"`
	TicketID        int64           `json:"ticket_id"`
	Version         int64           `json:"version"`
	EventAt         time.Time       `json:"event_at"`
	Commit          *Position       `json:"commit,omitempty"`
	Document        search.Document `json:"document"`
	History         *ticket.History `json:"history,omitempty"`
	FirstResolvedAt *time.Time      `json:"first_resolved_at,omitempty"`
}

func (e Event) Validate() error {
	if (e.Schema != 1 && e.Schema != 2) || e.ID == "" || len(e.ID) > 255 || (e.Source != "binlog" && e.Source != "snapshot") || e.EventAt.IsZero() {
		return fmt.Errorf("invalid event identity/schema")
	}
	if e.IsWatermark() {
		if e.TicketID != 0 || e.Version != 0 || e.History != nil {
			return fmt.Errorf("watermark contains business data")
		}
		if e.Source == "binlog" && (e.Commit == nil || e.Commit.ServerUUID == "" || e.Commit.File == "" || e.Commit.Pos < 4) {
			return fmt.Errorf("watermark has no commit")
		}
		return nil
	}
	if (e.Schema == 2 && e.Kind != "ticket") || e.TicketID != e.Document.ID || e.Version != e.Document.Version {
		return fmt.Errorf("invalid event identity/schema")
	}
	if e.Source == "binlog" && (e.Commit == nil || e.Commit.File == "" || e.Commit.Pos < 4 || e.Commit.ServerUUID == "") {
		return fmt.Errorf("missing committed binlog position")
	}
	if e.History != nil && (e.History.TicketID != e.TicketID || e.History.Version != e.Version) {
		return fmt.Errorf("history version disagrees with ticket")
	}
	return e.Document.Validate()
}

func (e Event) IsWatermark() bool { return e.Schema == 2 && e.Kind == "watermark" }
