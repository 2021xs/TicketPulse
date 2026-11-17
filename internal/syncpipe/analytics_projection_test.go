package syncpipe

import (
	"testing"
	"time"

	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

func TestAnalysisProjectionUsesHistoricalVersionTeam(t *testing.T) {
	created := time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC)
	resolved := created.Add(49 * time.Hour)
	tkt := ticket.Ticket{ID: 1, OrganizationID: 1, TicketNo: "T01", Title: "Historical ownership", Description: "source", Category: "network", Priority: "high", Status: "awaiting_confirmation", CreatorID: 1, InitialTeamID: 10, CurrentTeamID: 20, Version: 7, CreatedAt: created, UpdatedAt: resolved, FirstResolvedAt: &resolved}
	before := tkt.State()
	before.Version = 6
	before.Status = "in_progress"
	before.FirstResolvedAt = nil
	history := ticket.History{TicketID: 1, Action: "resolved", Version: 7, Before: &before, After: tkt.State(), ResponsibleTeamID: 20, OccurredAt: resolved}
	e := Event{Schema: 1, Source: "snapshot", ID: "old-schema-history", TicketID: 1, Version: 7, EventAt: resolved, Document: search.FromTicket(tkt), History: &history}
	f, current, err := AnalysisProjection(e)
	if err != nil || f.EventTeamID != 20 || f.InitialTeamID != 10 || current.Complete != 1 || current.FirstResolvedAt == nil {
		t.Fatal(f, current, err)
	}
	history.ResponsibleTeamID = 10
	if _, _, err := AnalysisProjection(e); err == nil {
		t.Fatal("inconsistent event-team snapshot accepted")
	}
	history.ResponsibleTeamID = 20
	history.Before.Version = 4
	if _, _, err := AnalysisProjection(e); err == nil {
		t.Fatal("noncontiguous history version accepted")
	}
	e.History = nil
	_, current, err = AnalysisProjection(e)
	if err != nil || current.Complete != 0 || current.FirstResolvedAt != nil {
		t.Fatal(current, err)
	}
	e.Schema = 2
	e.Kind = "ticket"
	e.FirstResolvedAt = &resolved
	_, current, err = AnalysisProjection(e)
	if err != nil || current.Complete != 1 || current.FirstResolvedAt == nil {
		t.Fatal(current, err)
	}
}
