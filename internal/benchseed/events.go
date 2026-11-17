package benchseed

import (
	"encoding/json"
	"fmt"

	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

// Events emits each immutable history and one authoritative current row. It is
// intended for direct isolated projection loading, not as CDC throughput data.
func Events(b Bundle, seed int64, schema string) ([]syncpipe.Event, error) {
	events := make([]syncpipe.Event, 0, len(b.Histories)+1)
	t := b.Ticket
	for _, history := range b.Histories {
		h := history
		s := h.After
		d := search.FromTicket(ticket.Ticket{ID: t.ID, OrganizationID: t.OrganizationID, TicketNo: t.TicketNo, Title: s.Title, Description: s.Description, Category: s.Category, Priority: s.Priority, Status: s.Status, CreatorID: s.CreatorID, AssigneeID: s.AssigneeID, InitialTeamID: s.InitialTeamID, CurrentTeamID: s.CurrentTeamID, Version: h.Version, CreatedAt: t.CreatedAt, UpdatedAt: h.OccurredAt})
		e := syncpipe.Event{Schema: 2, Kind: "ticket", ID: fmt.Sprintf("bench:%d:%s:history:%d:%d", seed, schema, t.ID, h.Version), Source: "snapshot", TicketID: t.ID, Version: h.Version, EventAt: h.OccurredAt, Document: d, History: &h, FirstResolvedAt: s.FirstResolvedAt}
		if _, _, err := syncpipe.AnalysisProjection(e); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	e := syncpipe.Event{Schema: 2, Kind: "ticket", ID: fmt.Sprintf("bench:%d:%s:current:%d:%d", seed, schema, t.ID, t.Version), Source: "snapshot", TicketID: t.ID, Version: t.Version, EventAt: t.UpdatedAt, Document: search.FromTicket(t), FirstResolvedAt: t.FirstResolvedAt}
	if _, _, err := syncpipe.AnalysisProjection(e); err != nil {
		return nil, err
	}
	events = append(events, e)
	return events, nil
}
func eventLines(b Bundle, seed int64, schema string) ([][]byte, error) {
	events, err := Events(b, seed, schema)
	if err != nil {
		return nil, err
	}
	lines := make([][]byte, 0, len(events))
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		lines = append(lines, append(data, 10))
	}
	return lines, nil
}
