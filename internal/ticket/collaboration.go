package ticket

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"
)

type TransferRequest struct {
	ActionRequest
	AssigneeID int64  `json:"assignee_id"`
	Reason     string `json:"reason"`
}

type TeamTransferRequest struct {
	ActionRequest
	TeamID int64  `json:"team_id"`
	Reason string `json:"reason"`
}

type ManagementReopenRequest struct {
	ActionRequest
	TeamID int64  `json:"team_id"` // Zero keeps the current team; a different team requeues without an owner.
	Reason string `json:"reason"`
}

type ReasonRequest struct {
	ActionRequest
	Reason string `json:"reason"`
}

type CommentRequest struct {
	ActionRequest
	Content string `json:"content"`
}

func validateText(value, field string, limit int, required bool) error {
	if value == "" && !required {
		return nil
	}
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || utf8.RuneCountInString(value) > limit {
		return Invalid(field + " 必须为非空文本且不能超过规定字符上限")
	}
	return nil
}

func (r TransferRequest) Validate() error {
	if err := r.ActionRequest.Validate(); err != nil {
		return err
	}
	if r.AssigneeID <= 0 {
		return Invalid("assignee_id 必须为正整数")
	}
	return validateText(r.Reason, "reason", 1000, false)
}

func (r TeamTransferRequest) Validate() error {
	if err := r.ActionRequest.Validate(); err != nil {
		return err
	}
	if r.TeamID <= 0 {
		return Invalid("team_id 必须为正整数")
	}
	return validateText(r.Reason, "reason", 1000, true)
}

func (r ManagementReopenRequest) Validate() error {
	if err := r.ActionRequest.Validate(); err != nil {
		return err
	}
	if r.TeamID < 0 {
		return Invalid("team_id 不允许为负数")
	}
	return validateText(r.Reason, "reason", 1000, true)
}

func (r ReasonRequest) Validate() error {
	if err := r.ActionRequest.Validate(); err != nil {
		return err
	}
	return validateText(r.Reason, "reason", 1000, true)
}

func (r CommentRequest) Validate() error {
	if err := r.ActionRequest.Validate(); err != nil {
		return err
	}
	return validateText(r.Content, "content", 5000, true)
}

func (s *Service) Transfer(ctx context.Context, userID int64, r TransferRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := r.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.Transfer(ctx, userID, r)
}

func (s *Service) TransferTeam(ctx context.Context, userID int64, r TeamTransferRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := r.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.TransferTeam(ctx, userID, r)
}

func (s *Service) ManagementReopen(ctx context.Context, userID int64, r ManagementReopenRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := r.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.ManagementReopen(ctx, userID, r)
}

func (s *Service) ManualClose(ctx context.Context, userID int64, r ReasonRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := r.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.ManualClose(ctx, userID, r)
}

func (s *Service) Comment(ctx context.Context, userID int64, r CommentRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := r.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.Comment(ctx, userID, r)
}

func (u User) IsManager() bool { return u.Role == RoleSupervisor || u.Role == RoleAdmin }

func (t Ticket) checkManager(u User) error {
	if !u.Active || u.ID <= 0 {
		return ErrUnauthenticated
	}
	if u.OrganizationID != t.OrganizationID || !u.IsManager() {
		return ErrActionForbidden
	}
	return nil
}

func (t Ticket) Transfer(u User, r TransferRequest) (Ticket, error) {
	if u.IsManager() {
		if err := t.checkManager(u); err != nil {
			return Ticket{}, err
		}
	} else {
		if err := t.checkAgent(u); err != nil {
			return Ticket{}, err
		}
		if t.AssigneeID == nil || *t.AssigneeID != u.ID {
			return Ticket{}, ErrActionForbidden
		}
	}
	if err := t.checkTransferState(r.ExpectedVersion); err != nil {
		return Ticket{}, err
	}
	if t.AssigneeID != nil && *t.AssigneeID == r.AssigneeID {
		return Ticket{}, ErrAlreadyAssigned
	}
	t.AssigneeID = &r.AssigneeID
	t.Version++
	return t, nil
}

func (t Ticket) TransferTeam(u User, r TeamTransferRequest) (Ticket, error) {
	if err := t.checkManager(u); err != nil {
		return Ticket{}, err
	}
	if err := t.checkTransferState(r.ExpectedVersion); err != nil {
		return Ticket{}, err
	}
	if r.TeamID == t.CurrentTeamID {
		return Ticket{}, Invalid("跨团队转派必须选择不同团队")
	}
	t.CurrentTeamID = r.TeamID
	t.AssigneeID = nil
	t.Status = StatusPending
	t.Version++
	return t, nil
}

func (t Ticket) checkTransferState(expectedVersion int64) error {
	if t.Version != expectedVersion {
		return ErrVersionConflict
	}
	if t.Status != StatusPending && t.Status != StatusInProgress {
		return ErrInvalidState
	}
	return nil
}

// Store validates the effective target team and retained owner before invoking this exception.
func (t Ticket) ManagementReopen(u User, expectedVersion, teamID int64, owner *int64) (Ticket, error) {
	if err := t.checkManager(u); err != nil {
		return Ticket{}, err
	}
	if t.Version != expectedVersion {
		return Ticket{}, ErrVersionConflict
	}
	if t.Status != StatusAwaitingConfirmation {
		return Ticket{}, ErrInvalidState
	}
	t.CurrentTeamID = teamID
	t.AssigneeID = owner
	t.Status = StatusPending
	if owner != nil {
		t.Status = StatusInProgress
	}
	t.ReopenCount++
	t.Version++
	return t, nil
}

func (t Ticket) ManualClose(u User, expectedVersion int64, at time.Time) (Ticket, error) {
	if err := t.checkManager(u); err != nil {
		return Ticket{}, err
	}
	if t.Version != expectedVersion {
		return Ticket{}, ErrVersionConflict
	}
	if t.Status != StatusAwaitingConfirmation {
		return Ticket{}, ErrInvalidState
	}
	t.Status = StatusClosed
	t.ClosedAt = &at
	t.Version++
	return t, nil
}
