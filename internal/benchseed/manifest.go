package benchseed

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"

	"ticketpulse/internal/ticket"
)

func newManifest(o Options) Manifest {
	return Manifest{Generator: GeneratorVersion, Options: o, Status: "planned", Distributions: Distributions{Version: map[string]int64{}, Status: map[string]int64{}, Action: map[string]int64{}, InitialTeam: map[string]int64{}, CurrentTeam: map[string]int64{}, EventTeam: map[string]int64{}, Assignee: map[string]int64{}, Creator: map[string]int64{}}, SampleIDs: []int64{}, HotTicketIDs: []int64{}, NeedleIDs: []int64{}, ThroughputMeaning: "direct SQL fixture import tickets/sec including deterministic generation and transactions; not HTTP TPS or CDC throughput", ExpectedMetrics: ExpectedMetrics{Start: "2026-11-01", End: "2026-11-09", Timezone: "Asia/Shanghai", Daily: map[string]Metric{}, Teams: map[string]Metric{}, Derivation: "independent fixed profile rules: creation initial team, profiles 6–9 first resolution at +9900 seconds in original team, profiles 7–9 reopened once, profile9 closed; no production analytics SQL"}}
}
func addMap(m map[string]int64, key int64, n int64) { m[strconv.FormatInt(key, 10)] += n }
func profileCounts(profile int64, hot bool) (version, comments int64) {
	versions := []int64{3, 4, 6, 5, 10, 12, 12, 14, 14, 15}
	counts := []int64{2, 2, 3, 2, 3, 3, 2, 3, 2, 2}
	version, comments = versions[profile], counts[profile]
	if hot {
		version += 25
		comments += 25
	}
	return
}
func statusFor(profile int64) string {
	if profile == 0 || profile == 1 || profile == 4 {
		return ticket.StatusPending
	}
	if profile == 6 || profile == 8 {
		return ticket.StatusAwaitingConfirmation
	}
	if profile == 9 {
		return ticket.StatusClosed
	}
	return ticket.StatusInProgress
}
func expectedRules(o Options, m *Manifest) {
	for id := int64(1); id <= o.Tickets; id++ {
		slot := slotFor(id, o.Seed)
		profile := slot % 10
		hot := slot%100 == 0
		team := teamFor(slot)
		version, comments := profileCounts(profile, hot)
		status := statusFor(profile)
		m.Expected.Tickets++
		m.Expected.Histories += version
		m.Expected.Comments += comments
		if profile >= 6 {
			m.Expected.FirstResolved++
		}
		if profile >= 7 {
			m.Expected.Reopened++
		}
		if status == ticket.StatusPending || status == ticket.StatusInProgress {
			m.Expected.Backlog++
		}
		day := fmt.Sprintf("2026-11-%02d", 1+(id-1)%8)
		dm := m.ExpectedMetrics.Daily[day]
		tm := m.ExpectedMetrics.Teams[strconv.FormatInt(team, 10)]
		dm.Created++
		tm.Created++
		m.ExpectedMetrics.Overview.Created++
		if profile >= 6 {
			dm.FirstResolved++
			dm.TotalFirstResolutionSeconds += 9900
			tm.FirstResolved++
			tm.TotalFirstResolutionSeconds += 9900
			m.ExpectedMetrics.Overview.FirstResolved++
			m.ExpectedMetrics.Overview.TotalFirstResolutionSeconds += 9900
		}
		if status == ticket.StatusPending || status == ticket.StatusInProgress {
			tm.Backlog++
			m.ExpectedMetrics.Overview.Backlog++
		}
		m.ExpectedMetrics.Daily[day] = dm
		m.ExpectedMetrics.Teams[strconv.FormatInt(team, 10)] = tm
		if hot {
			m.HotTicketCount++
			if len(m.HotTicketIDs) < 20 {
				m.HotTicketIDs = append(m.HotTicketIDs, id)
			}
			if m.HotTicketID == 0 {
				m.HotTicketID = id
			}
		}
		if slot == 7 {
			m.NeedleCount++
			if len(m.NeedleIDs) < 20 {
				m.NeedleIDs = append(m.NeedleIDs, id)
			}
		}
		if id <= 5 || id == o.Tickets/2 || id == o.Tickets {
			m.SampleIDs = append(m.SampleIDs, id)
		}
	}
	m.ExpectedMetrics.Overview.finish()
	for k, v := range m.ExpectedMetrics.Daily {
		v.finish()
		m.ExpectedMetrics.Daily[k] = v
	}
	for k, v := range m.ExpectedMetrics.Teams {
		v.finish()
		m.ExpectedMetrics.Teams[k] = v
	}
	m.MeanVersions = float64(m.Expected.Histories) / float64(o.Tickets)
}
func countBundle(m *Manifest, b Bundle) {
	t := b.Ticket
	m.Counts.Tickets++
	m.Histories += int64(len(b.Histories))
	m.Comments += int64(len(b.Comments))
	addMap(m.Distributions.Version, t.Version, 1)
	m.Distributions.Status[t.Status]++
	addMap(m.Distributions.InitialTeam, t.InitialTeamID, 1)
	addMap(m.Distributions.CurrentTeam, t.CurrentTeamID, 1)
	addMap(m.Distributions.Creator, t.CreatorID, 1)
	if t.AssigneeID == nil {
		m.Distributions.Assignee["unassigned"]++
	} else {
		addMap(m.Distributions.Assignee, *t.AssigneeID, 1)
	}
	if t.FirstResolvedAt != nil {
		m.FirstResolved++
	}
	m.Reopened += t.ReopenCount
	if t.Status == ticket.StatusPending || t.Status == ticket.StatusInProgress {
		m.Backlog++
	}
	for _, h := range b.Histories {
		m.Distributions.Action[h.Action]++
		addMap(m.Distributions.EventTeam, h.ResponsibleTeamID, 1)
	}
}

// Estimate avoids a million-bundle pass. Counts and expected metrics follow the
// exact profile rules; payload bytes are an extrapolation from at most 1000 rows.
// Its digest identifies the plan, not an imported data set.
func Estimate(o Options) (Manifest, error) {
	if err := o.Validate(); err != nil {
		return Manifest{}, err
	}
	m := newManifest(o)
	m.DryEstimate = true
	m.Status = "dry_estimate"
	expectedRules(o, &m)
	sample := o.Tickets
	if sample > 1000 {
		sample = 1000
	}
	var bytes int64
	var eventBytes int64
	for id := int64(1); id <= sample; id++ {
		b, err := GenerateTicket(id, o.Seed, 0)
		if err != nil {
			return m, err
		}
		payload, _ := json.Marshal(b)
		bytes += int64(len(payload))
		lines, err := eventLines(b, o.Seed, "ticketpulse_test_m5_estimate")
		if err != nil {
			return m, err
		}
		for _, line := range lines {
			eventBytes += int64(len(line))
		}
	}
	m.EstimatedPayloadBytes = bytes * o.Tickets / sample
	m.EstimatedEventBytes = eventBytes * o.Tickets / sample
	// Populate exact planned distributions independently of generated snapshots.
	for id := int64(1); id <= o.Tickets; id++ {
		slot := slotFor(id, o.Seed)
		profile := slot % 10
		team := teamFor(slot)
		v, c := profileCounts(profile, slot%100 == 0)
		addMap(m.Distributions.Version, v, 1)
		m.Distributions.Status[statusFor(profile)]++
		addMap(m.Distributions.InitialTeam, team, 1)
		addMap(m.Distributions.CurrentTeam, team, 1)
		creator := int64(1 + mix(uint64(id)+uint64(o.Seed)*7)%2)
		addMap(m.Distributions.Creator, creator, 1)
		if profile == 0 || profile == 4 {
			m.Distributions.Assignee["unassigned"]++
		} else {
			addMap(m.Distributions.Assignee, actorFor(team, id, o.Seed).ID, 1)
		}
		m.Distributions.Action[ticket.ActionCreated]++
		m.Distributions.Action[ticket.ActionCommented] += c
		if profile >= 1 {
			m.Distributions.Action[ticket.ActionClaimed]++
		}
		if profile >= 2 {
			m.Distributions.Action[ticket.ActionProcessingStarted]++
		}
		if profile >= 4 {
			m.Distributions.Action[ticket.ActionTeamTransferred] += 2
			m.Distributions.Action[ticket.ActionClaimed]++
			m.Distributions.Action[ticket.ActionProcessingStarted]++
			other := int64(20)
			if team == 20 {
				other = 10
			}
			addMap(m.Distributions.EventTeam, other, 4)
			addMap(m.Distributions.EventTeam, team, v-4)
		} else {
			addMap(m.Distributions.EventTeam, team, v)
		}
		if profile >= 5 {
			m.Distributions.Action[ticket.ActionClaimed]++
			m.Distributions.Action[ticket.ActionProcessingStarted]++
		}
		if profile >= 6 {
			m.Distributions.Action[ticket.ActionResolved]++
		}
		if profile >= 7 {
			m.Distributions.Action[ticket.ActionReopened]++
		}
		if profile >= 8 {
			m.Distributions.Action[ticket.ActionResolved]++
		}
		if profile == 9 {
			m.Distributions.Action[ticket.ActionClosed]++
		}
	}
	plan, _ := json.Marshal(struct {
		Generator     string
		Tickets, Seed int64
	}{GeneratorVersion, o.Tickets, o.Seed})
	m.SHA256 = fmt.Sprintf("%x", sha256.Sum256(plan))
	m.HashScope = "generator/tickets/seed plan digest; no rows imported, payload estimate sampled <=1000 tickets"
	return m, nil
}
