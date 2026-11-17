// Package benchseed creates deterministic, isolated SQL import fixtures. It
// exercises valid model transitions; SQL import throughput is not HTTP TPS.
package benchseed

import (
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"ticketpulse/internal/ticket"
)

const GeneratorVersion = "ticketpulse-benchseed/v1"
const MaxTickets int64 = 1000000

type Options struct {
	Tickets      int64     `json:"requested_tickets"`
	Seed         int64     `json:"seed"`
	BatchSize    int       `json:"batch_size"`
	DryEstimate  bool      `json:"dry_estimate"`
	Schema       string    `json:"schema"`
	ExpectedUUID string    `json:"-"`
	EventsWriter io.Writer `json:"-"`
	EventsOutput string    `json:"events_output,omitempty"`
}

func (o Options) Validate() error {
	if o.Tickets < 1 || o.Tickets > MaxTickets {
		return fmt.Errorf("tickets must be 1–1000000")
	}
	if o.BatchSize < 1 || o.BatchSize > 500 {
		return fmt.Errorf("batch must be 1–500")
	}
	return nil
}

type Bundle struct {
	Ticket    ticket.Ticket    `json:"ticket"`
	Histories []ticket.History `json:"histories"`
	Comments  []ticket.Comment `json:"comments"`
	Hot       bool             `json:"hot"`
	Needle    bool             `json:"needle"`
}
type Counts struct {
	Tickets       int64 `json:"tickets"`
	Histories     int64 `json:"histories"`
	Comments      int64 `json:"comments"`
	FirstResolved int64 `json:"first_resolved"`
	Reopened      int64 `json:"reopened"`
	Backlog       int64 `json:"current_backlog"`
}
type Distributions struct {
	Version     map[string]int64 `json:"version"`
	Status      map[string]int64 `json:"status"`
	Action      map[string]int64 `json:"action"`
	InitialTeam map[string]int64 `json:"initial_team"`
	CurrentTeam map[string]int64 `json:"current_team"`
	EventTeam   map[string]int64 `json:"event_team"`
	Assignee    map[string]int64 `json:"assignee"`
	Creator     map[string]int64 `json:"creator"`
}
type Metric struct {
	Created                       int64    `json:"created"`
	FirstResolved                 int64    `json:"first_resolved"`
	Backlog                       int64    `json:"current_backlog"`
	TotalFirstResolutionSeconds   float64  `json:"total_first_resolution_seconds"`
	AverageFirstResolutionSeconds *float64 `json:"average_first_resolution_seconds"`
}

func (m *Metric) finish() {
	if m.FirstResolved > 0 {
		v := m.TotalFirstResolutionSeconds / float64(m.FirstResolved)
		m.AverageFirstResolutionSeconds = &v
	}
}

type ExpectedMetrics struct {
	Start      string            `json:"start"`
	End        string            `json:"end"`
	Timezone   string            `json:"timezone"`
	Overview   Metric            `json:"overview"`
	Daily      map[string]Metric `json:"daily"`
	Teams      map[string]Metric `json:"teams"`
	Derivation string            `json:"derivation"`
}
type Manifest struct {
	Generator string `json:"generator"`
	Options
	Status string `json:"status"`
	Counts
	Expected                  Counts          `json:"expected_counts"`
	Distributions             Distributions   `json:"distributions"`
	ExpectedMetrics           ExpectedMetrics `json:"expected_metrics"`
	SampleIDs                 []int64         `json:"sample_ids"`
	HotTicketID               int64           `json:"hot_ticket_id"`
	HotTicketIDs              []int64         `json:"hot_ticket_ids"`
	NeedleIDs                 []int64         `json:"needle_ids"`
	HotTicketCount            int64           `json:"hot_ticket_count"`
	NeedleCount               int64           `json:"needle_count"`
	SHA256                    string          `json:"sha256"`
	HashScope                 string          `json:"hash_scope"`
	EstimatedPayloadBytes     int64           `json:"estimated_payload_bytes"`
	MeanVersions              float64         `json:"mean_versions"`
	StartedAt                 time.Time       `json:"started_at"`
	CompletedAt               time.Time       `json:"completed_at"`
	ElapsedSeconds            float64         `json:"elapsed_seconds"`
	SQLImportTicketsPerSecond float64         `json:"sql_import_tickets_per_second"`
	ThroughputMeaning         string          `json:"throughput_meaning"`
	CommittedBatches          int64           `json:"committed_batches"`
	SourceUUID                string          `json:"source_uuid,omitempty"`
	CommitOutcomeUnknown      bool            `json:"commit_outcome_unknown"`
	ExportedEvents            int64           `json:"exported_events"`
	EventsSHA256              string          `json:"events_sha256,omitempty"`
	EstimatedEventBytes       int64           `json:"estimated_event_bytes"`
	Failure                   string          `json:"failure,omitempty"`
}

var beijing = time.FixedZone("Asia/Shanghai", 8*3600)

func mix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
func slotFor(id, seed int64) int64 { return (id - 1 + int64(mix(uint64(seed))%1000)) % 1000 }
func createdFor(id, seed int64) time.Time {
	h := mix(uint64(id) ^ uint64(seed))
	return time.Date(2026, 11, 1+int((id-1)%8), 1, 0, 0, 0, beijing).Add(time.Duration(h%18000)*time.Second + time.Duration(h%1000000)*time.Microsecond).UTC()
}
func teamFor(slot int64) int64 {
	if slot%100 < 70 {
		return 10
	}
	if slot%100 < 90 {
		return 20
	}
	return 90
}
func actorFor(team, id, seed int64) ticket.User {
	actor := int64(10)
	if team == 20 {
		actor = 4
	} else if team == 10 {
		actor = 3
		if mix(uint64(id)+uint64(seed))%2 == 1 {
			actor = 11
		}
	}
	return ticket.User{ID: actor, OrganizationID: 1, Role: ticket.RoleAgent, Active: true}
}

// One ticket is bounded to at most 40 versions. A 1% hotspot adds exactly 25
// comments. Model methods implement state transitions; snapshots retain all
// state fields and never alias a subsequently mutated state.
func GenerateTicket(id, seed, commentBase int64) (Bundle, error) {
	slot := slotFor(id, seed)
	profile := slot % 10
	team := teamFor(slot)
	creator := int64(1 + mix(uint64(id)+uint64(seed)*7)%2)
	t := ticket.Ticket{ID: id, OrganizationID: 1, TicketNo: fmt.Sprintf("TP-%012d", id), Title: fmt.Sprintf("benchmark recover request %09d", id), Description: "recover benchmark support fixture 数据恢复 λ " + strings.Repeat("p", 400), Category: ticket.CategoryNetwork, Priority: []string{ticket.PriorityLow, ticket.PriorityMedium, ticket.PriorityHigh, ticket.PriorityUrgent}[slot%4], Status: ticket.StatusPending, CreatorID: creator, InitialTeamID: team, CurrentTeamID: team, Version: 1, CreatedAt: createdFor(id, seed)}
	if team == 20 {
		t.Category = ticket.CategoryDevice
	} else if team == 90 {
		t.Category = ticket.CategoryOther
	}
	t.UpdatedAt = t.CreatedAt
	b := Bundle{Hot: slot%100 == 0, Needle: slot == 7, Histories: make([]ticket.History, 0, 40), Comments: make([]ticket.Comment, 0, 28)}
	if b.Needle {
		t.Title = "needle " + t.Title
	}
	b.Histories = append(b.Histories, ticket.History{TicketID: id, Action: ticket.ActionCreated, OperatorID: creator, Version: 1, After: t.State(), ResponsibleTeamID: team, OccurredAt: t.CreatedAt})
	step := func(action string, operator int64, change func(ticket.Ticket, time.Time) (ticket.Ticket, error), reason *string, comment bool) error {
		before := t.State()
		at := t.UpdatedAt.Add(15 * time.Minute)
		next, err := change(t, at)
		if err != nil {
			return err
		}
		next.UpdatedAt = at
		t = next
		h := ticket.History{TicketID: id, Action: action, OperatorID: operator, Version: t.Version, Before: &before, After: t.State(), ResponsibleTeamID: t.CurrentTeamID, OccurredAt: at, Reason: reason}
		if comment {
			cid := commentBase + int64(len(b.Comments)) + 1
			h.CommentID = &cid
			b.Comments = append(b.Comments, ticket.Comment{ID: cid, TicketID: id, AuthorID: operator, Content: fmt.Sprintf("benchmark recover comment ticket=%d version=%d", id, t.Version), Version: t.Version, CreatedAt: at})
		}
		b.Histories = append(b.Histories, h)
		return nil
	}
	comment := func(n int) error {
		for i := 0; i < n; i++ {
			if err := step(ticket.ActionCommented, creator, func(t ticket.Ticket, _ time.Time) (ticket.Ticket, error) { t.Version++; return t, nil }, nil, true); err != nil {
				return err
			}
		}
		return nil
	}
	claim := func() error {
		u := actorFor(t.CurrentTeamID, id, seed)
		return step(ticket.ActionClaimed, u.ID, func(t ticket.Ticket, _ time.Time) (ticket.Ticket, error) { return t.Claim(u, t.Version) }, nil, false)
	}
	start := func() error {
		u := actorFor(t.CurrentTeamID, id, seed)
		return step(ticket.ActionProcessingStarted, u.ID, func(t ticket.Ticket, _ time.Time) (ticket.Ticket, error) { return t.Start(u, t.Version) }, nil, false)
	}
	transfer := func(target int64) error {
		reason := "benchmark cross-team routing preserves immutable ownership"
		return step(ticket.ActionTeamTransferred, 5, func(t ticket.Ticket, _ time.Time) (ticket.Ticket, error) {
			return t.TransferTeam(ticket.User{ID: 5, OrganizationID: 1, Role: ticket.RoleSupervisor, Active: true}, ticket.TeamTransferRequest{ActionRequest: ticket.ActionRequest{TicketID: id, ExpectedVersion: t.Version}, TeamID: target, Reason: reason})
		}, &reason, false)
	}
	if profile >= 1 {
		if err := claim(); err != nil {
			return b, err
		}
	}
	if profile >= 2 {
		if err := start(); err != nil {
			return b, err
		}
	}
	if profile >= 4 {
		other := int64(20)
		if team == 20 {
			other = 10
		}
		if err := transfer(other); err != nil {
			return b, err
		}
	}
	if profile >= 4 {
		if err := claim(); err != nil {
			return b, err
		}
		if err := start(); err != nil {
			return b, err
		}
		if err := comment(1); err != nil {
			return b, err
		}
		if err := transfer(team); err != nil {
			return b, err
		}
	}
	if profile >= 5 {
		if err := claim(); err != nil {
			return b, err
		}
		if err := start(); err != nil {
			return b, err
		}
	}
	// Extra comments vary version density without changing business status.
	count := 2
	if profile == 2 {
		count = 3
	}
	if profile >= 6 {
		count = 1
	}
	if err := comment(count); err != nil {
		return b, err
	}
	if b.Hot {
		if err := comment(25); err != nil {
			return b, err
		}
	}
	resolve := func() error {
		u := actorFor(t.CurrentTeamID, id, seed)
		return step(ticket.ActionResolved, u.ID, func(t ticket.Ticket, at time.Time) (ticket.Ticket, error) {
			return t.Resolve(u, t.Version, fmt.Sprintf("benchmark resolution ticket=%d version=%d", id, t.Version+1), at)
		}, nil, false)
	}
	if profile >= 6 {
		if err := resolve(); err != nil {
			return b, err
		}
	}
	if profile >= 7 {
		u := ticket.User{ID: creator, OrganizationID: 1, Role: ticket.RoleRequester, Active: true}
		if err := step(ticket.ActionReopened, creator, func(t ticket.Ticket, _ time.Time) (ticket.Ticket, error) { return t.Reopen(u, t.Version) }, nil, false); err != nil {
			return b, err
		}
	}
	if profile == 7 {
		if err := comment(1); err != nil {
			return b, err
		}
	}
	if profile >= 8 {
		if err := resolve(); err != nil {
			return b, err
		}
	}
	if profile == 9 {
		u := ticket.User{ID: creator, OrganizationID: 1, Role: ticket.RoleRequester, Active: true}
		if err := step(ticket.ActionClosed, creator, func(t ticket.Ticket, at time.Time) (ticket.Ticket, error) { return t.Close(u, t.Version, at) }, nil, false); err != nil {
			return b, err
		}
	}
	b.Ticket = t
	return b, nil
}
func finite(value float64) float64 {
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return 0
	}
	return value
}
