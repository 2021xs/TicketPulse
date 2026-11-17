package analytics

import (
	"context"
	"slices"
	"time"

	"ticketpulse/internal/ticket"
)

type Service struct {
	Auth   Authorizer
	CK     *ClickHouse
	Status StatusReader
	Check  func(context.Context) error
}

func (s *Service) Query(ctx context.Context, userID int64, r Request) (Result, error) {
	user, teams, err := s.Auth.AnalyticsAccess(ctx, userID)
	if err != nil {
		return Result{}, err
	}
	start, end, err := r.Window()
	if err != nil {
		return Result{}, err
	}
	for _, id := range r.TeamIDs {
		if !slices.ContainsFunc(teams, func(t ticket.Team) bool { return t.ID == id }) {
			return Result{}, ticket.Invalid("team_id 不属于当前组织")
		}
	}
	if s.Check != nil {
		if err := s.Check(ctx); err != nil {
			return Result{}, ErrUnavailable
		}
	}
	result := Result{Start: r.Start, End: r.End, Timezone: "Asia/Shanghai", Daily: []Day{}, Teams: []TeamMetrics{}, AvailableTeams: teams, RefreshedAt: time.Now().UTC(), BacklogScope: "current_latest_state_independent_of_date_range"}
	if s.Status != nil {
		result.Sync, err = s.Status.AnalyticsStatus(ctx)
		if err != nil {
			return result, err
		}
	} else {
		result.Sync = SyncStatus{State: "unknown", ObservedAt: time.Now().UTC()}
	}
	rows, err := s.CK.Metrics(ctx, user.OrganizationID, r)
	if err != nil {
		return Result{}, err
	}
	days := map[string]*Day{}
	teamMetrics := map[int64]*TeamMetrics{}
	for at := start.In(Shanghai); at.Before(end.In(Shanghai)); at = at.AddDate(0, 0, 1) {
		result.Daily = append(result.Daily, Day{Date: at.Format("2006-01-02")})
	}
	for i := range result.Daily {
		days[result.Daily[i].Date] = &result.Daily[i]
	}
	for _, team := range teams {
		if len(r.TeamIDs) > 0 && !slices.Contains(r.TeamIDs, team.ID) {
			continue
		}
		result.Teams = append(result.Teams, TeamMetrics{ID: team.ID, Name: team.Name, Active: team.Active, DrilldownURL: Drilldown(team.ID)})
	}
	for i := range result.Teams {
		teamMetrics[result.Teams[i].ID] = &result.Teams[i]
	}
	observed := int64(0)
	totalDuration := float64(0)
	dayDuration := map[string]float64{}
	teamDuration := map[int64]float64{}
	for _, row := range rows {
		if row.Metric == "observed" {
			observed = row.Amount
			continue
		}
		tm := teamMetrics[row.TeamID]
		day := days[row.Day]
		switch row.Metric {
		case "created":
			result.Overview.Created += row.Amount
			if day != nil {
				day.Created += row.Amount
			}
			if tm != nil {
				tm.Created += row.Amount
			}
		case "resolved":
			result.Overview.FirstResolved += row.Amount
			totalDuration += row.Duration
			if day != nil {
				day.FirstResolved += row.Amount
				dayDuration[row.Day] += row.Duration
			}
			if tm != nil {
				tm.FirstResolved += row.Amount
				teamDuration[row.TeamID] += row.Duration
			}
		case "backlog":
			result.Overview.Backlog += row.Amount
			if tm != nil {
				tm.Backlog += row.Amount
			}
		}
	}
	average := func(seconds float64, count int64) *float64 {
		if count == 0 {
			return nil
		}
		value := seconds / float64(count)
		return &value
	}
	result.Overview.AverageFirstResolutionSeconds = average(totalDuration, result.Overview.FirstResolved)
	for i := range result.Daily {
		d := &result.Daily[i]
		d.AverageFirstResolutionSeconds = average(dayDuration[d.Date], d.FirstResolved)
	}
	for i := range result.Teams {
		t := &result.Teams[i]
		t.AverageFirstResolutionSeconds = average(teamDuration[t.ID], t.FirstResolved)
	}
	result.DataState = "ready"
	if observed == 0 {
		result.DataState = "empty"
	}
	if !result.Sync.BootstrapComplete || !result.Sync.CaughtUp {
		result.DataState = "partial"
	}
	return result, nil
}
