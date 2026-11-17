package ticket

import "context"

type Options struct {
	Actions           []string `json:"actions"`
	Assignees         []User   `json:"assignees"`
	Teams             []Team   `json:"teams"`
	CurrentTeamActive bool     `json:"current_team_active"`
	AssigneeValid     bool     `json:"assignee_valid"`
	Version           int64    `json:"version"`
}

func (s *Service) Options(ctx context.Context, userID, ticketID int64) (Options, error) {
	if userID <= 0 {
		return Options{}, ErrUnauthenticated
	}
	if ticketID <= 0 {
		return Options{}, Invalid("工单 id 必须为正整数")
	}
	return s.repo.Options(ctx, userID, ticketID)
}

// Display capability only. Every write still repeats its existing locked permission checks.
func AvailableActions(u User, t Ticket, member bool) []string {
	actions := []string{"comments"} // Caller already verified detail visibility.
	owner := u.Role == RoleAgent && member && t.AssigneeID != nil && *t.AssigneeID == u.ID
	if u.Role == RoleAgent && member && t.Status == StatusPending && t.AssigneeID == nil {
		actions = append(actions, "claim")
	}
	if owner && t.Status == StatusPending {
		actions = append(actions, "start")
	}
	if owner && t.Status == StatusInProgress {
		actions = append(actions, "resolve")
	}
	if t.Status == StatusPending || t.Status == StatusInProgress {
		if owner || u.IsManager() {
			actions = append(actions, "transfer")
		}
		if u.IsManager() {
			actions = append(actions, "transfer-team")
		}
	}
	if t.Status == StatusAwaitingConfirmation {
		if u.ID == t.CreatorID {
			actions = append(actions, "reopen", "close")
		}
		if u.IsManager() {
			actions = append(actions, "management-reopen", "manual-close")
		}
	}
	return actions
}
