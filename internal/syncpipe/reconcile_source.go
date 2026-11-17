package syncpipe

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/ticket"
)

func reconcileStatesEqual(a, b ticket.State) bool {
	normalize := func(s ticket.State) ticket.State {
		if s.FirstResolvedAt != nil {
			at := s.FirstResolvedAt.UTC()
			s.FirstResolvedAt = &at
		}
		if s.ClosedAt != nil {
			at := s.ClosedAt.UTC()
			s.ClosedAt = &at
		}
		return s
	}
	ad, _ := json.Marshal(normalize(a))
	bd, _ := json.Marshal(normalize(b))
	return jsonEqual(ad, bd)
}
func reconcileStatesEqualProjected(a, b ticket.State) bool {
	a.ResolutionNote = nil
	b.ResolutionNote = nil
	a.ClosedAt = nil
	b.ClosedAt = nil
	return reconcileStatesEqual(a, b)
}

type reconcileMetricKey struct {
	Metric string
	Day    string
	Team   int64
}
type reconcileMetricValue struct {
	Amount int64
	Micros int64
}
type reconcileTicketMetrics struct {
	Created      *time.Time
	InitialTeam  int64
	Resolved     *time.Time
	ResolvedTeam int64
}

// Metrics are computed independently from MySQL facts, in business version
// order. Only one ticket's first events and a bounded aggregate map are retained.
func reconcileSource(ctx context.Context, db reconcileQuery, o ReconcileOptions, metrics bool) (ReconcileSource, []analytics.MetricRow, error) {
	var source ReconcileSource
	currentHash, historyHash := sha256.New(), sha256.New()
	key := analytics.ReconcileKey{}
	metricValues := map[reconcileMetricKey]reconcileMetricValue{}
	var start, end time.Time
	if metrics {
		start, end, _ = o.MetricsRequest.Window()
	}
	metricsOrg := o.MetricsOrganizationID
	if metricsOrg == 0 {
		metricsOrg = o.OrganizationID
	}
	var observed int64
	selected := func(team int64) bool {
		if len(o.MetricsRequest.TeamIDs) == 0 {
			return true
		}
		for _, id := range o.MetricsRequest.TeamIDs {
			if id == team {
				return true
			}
		}
		return false
	}
	add := func(metric, day string, team, amount, micros int64) error {
		k := reconcileMetricKey{metric, day, team}
		v := metricValues[k]
		v.Amount += amount
		v.Micros += micros
		metricValues[k] = v
		if len(metricValues) > 10000 {
			return fmt.Errorf("MySQL metrics exceed complete-result budget")
		}
		return nil
	}
	for {
		page, err := reconcileCurrentPage(ctx, db, o.OrganizationID, key, o.BatchSize)
		if err != nil {
			return source, nil, err
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			d := row.Document
			source.Tickets++
			reconcileHash(currentHash, row)
			key = reconcileKey(d.OrganizationID, d.ID, 0)
			if metrics && d.OrganizationID == metricsOrg {
				observed++
				if err = add("observed", "", 0, 1, 0); err != nil {
					return source, nil, err
				}
				if (d.Status == ticket.StatusPending || d.Status == ticket.StatusInProgress) && selected(d.CurrentTeamID) {
					if err = add("backlog", "", d.CurrentTeamID, 1, 0); err != nil {
						return source, nil, err
					}
				}
			}
		}
	}
	key = analytics.ReconcileKey{}
	var ticketKey analytics.ReconcileKey
	var sample reconcileTicketMetrics
	resolvedSeen := false
	currentIterator := &reconcileIterator[reconcileCurrentRow]{}
	currentCursor := analytics.ReconcileKey{}
	currentIterator.load = func() ([]reconcileCurrentRow, error) {
		page, err := reconcileCurrentPage(ctx, db, o.OrganizationID, currentCursor, o.BatchSize)
		if len(page) > 0 {
			d := page[len(page)-1].Document
			currentCursor = reconcileKey(d.OrganizationID, d.ID, 0)
		}
		return page, err
	}
	var current reconcileCurrentRow
	haveCurrent := false
	var previous *ticket.State
	var version, reopens int64
	var latestAt time.Time
	finishIntegrity := func() error {
		if !haveCurrent {
			return nil
		}
		d := current.Document
		if version != d.Version {
			return fmt.Errorf("source history incomplete for %d/%d: latest history version=%d current version=%d", d.OrganizationID, d.ID, version, d.Version)
		}
		state := ticket.State{Title: d.Title, Description: d.Description, Category: d.Category, Priority: d.Priority, Status: d.Status, CreatorID: d.CreatorID, AssigneeID: d.AssigneeID, InitialTeamID: d.InitialTeamID, CurrentTeamID: d.CurrentTeamID, Version: d.Version, FirstResolvedAt: current.First, ReopenCount: current.ReopenCount}
		if previous == nil || !reconcileStatesEqualProjected(*previous, state) || !latestAt.Equal(d.UpdatedAt) || reopens != current.ReopenCount {
			return fmt.Errorf("source latest history disagrees with authoritative current row %d/%d", d.OrganizationID, d.ID)
		}
		return nil
	}
	finish := func() error {
		if !metrics || ticketKey.OrganizationID != metricsOrg {
			return nil
		}
		if sample.Created != nil && selected(sample.InitialTeam) && !sample.Created.Before(start) && sample.Created.Before(end) {
			if err := add("created", sample.Created.In(analytics.Shanghai).Format("2006-01-02"), sample.InitialTeam, 1, 0); err != nil {
				return err
			}
		}
		if sample.Created != nil && sample.Resolved != nil && selected(sample.ResolvedTeam) && !sample.Resolved.Before(start) && sample.Resolved.Before(end) {
			return add("resolved", sample.Resolved.In(analytics.Shanghai).Format("2006-01-02"), sample.ResolvedTeam, 1, sample.Resolved.Sub(*sample.Created).Microseconds())
		}
		return nil
	}
	for {
		page, err := reconcileHistoryPage(ctx, db, o.OrganizationID, key, o.BatchSize)
		if err != nil {
			return source, nil, err
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			h := row.History
			newKey := reconcileKey(row.OrganizationID, h.TicketID, 0)
			if newKey != ticketKey {
				if err = finishIntegrity(); err != nil {
					return source, nil, err
				}
				if err = finish(); err != nil {
					return source, nil, err
				}
				ticketKey = newKey
				sample = reconcileTicketMetrics{}
				resolvedSeen = false
				current, haveCurrent, err = currentIterator.next()
				if err != nil {
					return source, nil, err
				}
				if !haveCurrent || reconcileKey(current.Document.OrganizationID, current.Document.ID, 0) != newKey {
					return source, nil, fmt.Errorf("source ticket has missing or orphaned history at %d/%d", newKey.OrganizationID, newKey.TicketID)
				}
				version = 0
				reopens = 0
				previous = nil
			}
			source.Histories++
			reconcileHash(historyHash, row)
			key = reconcileKey(row.OrganizationID, h.TicketID, h.Version)
			// Malformed immutable history is a source integrity error, not a projection lag.
			if _, err = reconcileHistoryFact(row); err != nil {
				return source, nil, fmt.Errorf("invalid MySQL history %d/%d/%d: %w", row.OrganizationID, h.TicketID, h.Version, err)
			}
			if h.Version != version+1 || version == 0 && (h.Action != ticket.ActionCreated || h.Before != nil) || version > 0 && (h.Before == nil || !reconcileStatesEqual(*previous, *h.Before)) {
				return source, nil, fmt.Errorf("source history chain broken at %d/%d/%d", row.OrganizationID, h.TicketID, h.Version)
			}
			version = h.Version
			latestAt = h.OccurredAt
			afterState := h.After
			previous = &afterState
			if h.Action == ticket.ActionCreated && sample.Created == nil {
				at := h.OccurredAt
				sample.Created = &at
				sample.InitialTeam = h.After.InitialTeamID
			}
			if h.Action == ticket.ActionResolved && h.After.Status == ticket.StatusAwaitingConfirmation && !resolvedSeen {
				source.FirstResolved++
				resolvedSeen = true
				at := h.OccurredAt
				sample.Resolved = &at
				sample.ResolvedTeam = h.ResponsibleTeamID
			}
			if reconcileReopen(h.Action) {
				source.Reopened++
				reopens++
			}
			if h.After.ReopenCount != reopens {
				return source, nil, fmt.Errorf("source reopen_count disagrees with immutable events at %d/%d/%d", row.OrganizationID, h.TicketID, h.Version)
			}
			if sample.Resolved == nil && h.After.FirstResolvedAt != nil || sample.Resolved != nil && (h.After.FirstResolvedAt == nil || !sample.Resolved.Equal(*h.After.FirstResolvedAt)) {
				return source, nil, fmt.Errorf("source first_resolved_at disagrees with first resolution at %d/%d/%d", row.OrganizationID, h.TicketID, h.Version)
			}
		}
	}
	if err := finishIntegrity(); err != nil {
		return source, nil, err
	}
	if _, more, err := currentIterator.next(); err != nil {
		return source, nil, err
	} else if more {
		return source, nil, fmt.Errorf("source current ticket has no complete history")
	}
	if err := finish(); err != nil {
		return source, nil, err
	}
	if metrics && observed == 0 {
		if err := add("observed", "", 0, 0, 0); err != nil {
			return source, nil, err
		}
	}
	source.CurrentSHA256 = reconcileSHA(currentHash)
	source.HistorySHA256 = reconcileSHA(historyHash)
	combined := sha256.New()
	reconcileHash(combined, source)
	source.Fingerprint = reconcileSHA(combined)
	result := make([]analytics.MetricRow, 0, len(metricValues))
	for k, v := range metricValues {
		result = append(result, analytics.MetricRow{Metric: k.Metric, Day: k.Day, TeamID: k.Team, Amount: v.Amount, Duration: float64(v.Micros) / 1000000})
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Metric != b.Metric {
			return a.Metric < b.Metric
		}
		if a.Day != b.Day {
			return a.Day < b.Day
		}
		return a.TeamID < b.TeamID
	})
	return source, result, nil
}
func reconcileReopen(action string) bool {
	return action == ticket.ActionReopened || action == ticket.ActionReopenedRequeued || action == ticket.ActionManagementReopened
}
func reconcileCompareMetrics(r *ReconcileReport, expected, actual []analytics.MetricRow) {
	values := map[reconcileMetricKey]analytics.MetricRow{}
	for _, v := range actual {
		k := reconcileMetricKey{v.Metric, v.Day, v.TeamID}
		if old, exists := values[k]; exists {
			r.difference("ck_metrics", analytics.ReconcileKey{}, "duplicate_metric_row", old, v)
		}
		values[k] = v
	}
	for _, v := range expected {
		k := reconcileMetricKey{v.Metric, v.Day, v.TeamID}
		a, exists := values[k]
		field := fmt.Sprintf("%s/%s/%d", k.Metric, k.Day, k.Team)
		if !exists {
			r.difference("ck_metrics", analytics.ReconcileKey{}, field, v, nil)
		} else {
			r.compare("ck_metrics", analytics.ReconcileKey{}, v, a)
			delete(values, k)
		}
	}
	keys := make([]reconcileMetricKey, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.Metric != b.Metric {
			return a.Metric < b.Metric
		}
		if a.Day != b.Day {
			return a.Day < b.Day
		}
		return a.Team < b.Team
	})
	for _, k := range keys {
		r.difference("ck_metrics", analytics.ReconcileKey{}, fmt.Sprintf("extra/%s/%s/%d", k.Metric, k.Day, k.Team), nil, values[k])
	}
}
