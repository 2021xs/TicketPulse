package ticket

import (
	"context"
	"time"
)

const (
	ViewCreatedByMe  = "created_by_me"
	ViewAssignedToMe = "assigned_to_me"
	ViewTeamPending  = "team_pending"
)

type ListRequest struct {
	View     string
	Page     int
	PageSize int
}

func (r ListRequest) Validate() error {
	if r.View != ViewCreatedByMe && r.View != ViewAssignedToMe && r.View != ViewTeamPending {
		return Invalid("view 必须为 created_by_me、assigned_to_me 或 team_pending")
	}
	if r.Page < 1 || r.Page > 10000 || r.PageSize < 1 || r.PageSize > 100 {
		return Invalid("page 必须为 1–10000，page_size 必须为 1–100")
	}
	return nil
}

type TicketSummary struct {
	ID              int64     `json:"id"`
	TicketNo        string    `json:"ticket_no"`
	Title           string    `json:"title"`
	Priority        string    `json:"priority"`
	Status          string    `json:"status"`
	AssigneeID      *int64    `json:"assignee_id"`
	AssigneeName    *string   `json:"assignee_name"`
	CurrentTeamID   int64     `json:"current_team_id"`
	CurrentTeamName string    `json:"current_team_name"`
	Version         int64     `json:"version"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type ListResult struct {
	Items    []TicketSummary `json:"items"`
	View     string          `json:"view"`
	Page     int             `json:"page"`
	PageSize int             `json:"page_size"`
	Total    int64           `json:"total"`
	HasMore  bool            `json:"has_more"`
}

func (s *Service) List(ctx context.Context, userID int64, r ListRequest) (ListResult, error) {
	if userID <= 0 {
		return ListResult{}, ErrUnauthenticated
	}
	if err := r.Validate(); err != nil {
		return ListResult{}, err
	}
	return s.repo.List(ctx, userID, r)
}
