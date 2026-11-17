package ticket

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"
)

type ResolveRequest struct {
	ActionRequest
	ResolutionNote string `json:"resolution_note"`
}

func (r ResolveRequest) Validate() error {
	if err := r.ActionRequest.Validate(); err != nil {
		return err
	}
	return validateResolutionNote(r.ResolutionNote)
}

func validateResolutionNote(note string) error {
	if !utf8.ValidString(note) || strings.TrimSpace(note) == "" || utf8.RuneCountInString(note) > 5000 {
		return Invalid("resolution_note 必须为 1–5000 个字符且不能全部为空白")
	}
	return nil
}

func (s *Service) Resolve(ctx context.Context, userID int64, request ResolveRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := request.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.Resolve(ctx, userID, request)
}

func (s *Service) Reopen(ctx context.Context, userID int64, request ActionRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := request.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.Reopen(ctx, userID, request)
}

func (s *Service) Close(ctx context.Context, userID int64, request ActionRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := request.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.Close(ctx, userID, request)
}

// Store supplies one operation time and protects the current owner and team in its transaction.
func (t Ticket) Resolve(user User, expectedVersion int64, note string, occurredAt time.Time) (Ticket, error) {
	if err := t.checkAgent(user); err != nil {
		return Ticket{}, err
	}
	if t.AssigneeID == nil || *t.AssigneeID != user.ID {
		return Ticket{}, ErrActionForbidden
	}
	if t.Version != expectedVersion {
		return Ticket{}, ErrVersionConflict
	}
	if t.Status != StatusInProgress {
		return Ticket{}, ErrInvalidState
	}
	if err := validateResolutionNote(note); err != nil {
		return Ticket{}, err
	}
	t.Status = StatusAwaitingConfirmation
	t.Version++
	t.ResolutionNote = &note
	if t.FirstResolvedAt == nil {
		t.FirstResolvedAt = &occurredAt
	}
	return t, nil
}

// Store clears an unavailable owner before this rule; it always verifies the current team is active.
func (t Ticket) Reopen(user User, expectedVersion int64) (Ticket, error) {
	if err := t.checkCreator(user); err != nil {
		return Ticket{}, err
	}
	if t.Version != expectedVersion {
		return Ticket{}, ErrVersionConflict
	}
	if t.Status != StatusAwaitingConfirmation {
		return Ticket{}, ErrInvalidState
	}
	t.Status = StatusInProgress
	if t.AssigneeID == nil {
		t.Status = StatusPending
	}
	t.Version++
	t.ReopenCount++
	return t, nil
}

func (t Ticket) Close(user User, expectedVersion int64, occurredAt time.Time) (Ticket, error) {
	if err := t.checkCreator(user); err != nil {
		return Ticket{}, err
	}
	if t.Version != expectedVersion {
		return Ticket{}, ErrVersionConflict
	}
	if t.Status != StatusAwaitingConfirmation {
		return Ticket{}, ErrInvalidState
	}
	t.Status = StatusClosed
	t.Version++
	t.ClosedAt = &occurredAt
	return t, nil
}

func (t Ticket) checkCreator(user User) error {
	if !user.Active || user.ID <= 0 {
		return ErrUnauthenticated
	}
	if user.ID != t.CreatorID || user.OrganizationID != t.OrganizationID {
		return ErrActionForbidden
	}
	return nil
}
