package ticket

import "context"

type ActionRequest struct {
	TicketID        int64 `json:"ticket_id"`
	ExpectedVersion int64 `json:"expected_version"`
}

func (r ActionRequest) Validate() error {
	if r.TicketID <= 0 {
		return Invalid("ticket_id 必须为正整数")
	}
	if r.ExpectedVersion <= 0 {
		return Invalid("expected_version 必须为正整数")
	}
	return nil
}

type ActionResult struct {
	Ticket  Ticket   `json:"ticket"`
	Comment *Comment `json:"comment,omitempty"`
}

func (s *Service) Claim(ctx context.Context, userID int64, request ActionRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := request.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.Claim(ctx, userID, request)
}

func (s *Service) Start(ctx context.Context, userID int64, request ActionRequest) (ActionResult, error) {
	if userID <= 0 {
		return ActionResult{}, ErrUnauthenticated
	}
	if err := request.Validate(); err != nil {
		return ActionResult{}, err
	}
	return s.repo.Start(ctx, userID, request)
}

// Claim/Start run only after Store has locked the ticket and checked current active membership.
// These pure rules validate actor, version and business state without persistence side effects.
func (t Ticket) Claim(user User, expectedVersion int64) (Ticket, error) {
	if err := t.checkAgent(user); err != nil {
		return Ticket{}, err
	}
	if t.Version != expectedVersion {
		return Ticket{}, ErrVersionConflict
	}
	if t.Status != StatusPending {
		return Ticket{}, ErrInvalidState
	}
	if t.AssigneeID != nil {
		return Ticket{}, ErrAlreadyClaimed
	}
	t.AssigneeID = &user.ID
	t.Version++
	return t, nil
}

func (t Ticket) Start(user User, expectedVersion int64) (Ticket, error) {
	if err := t.checkAgent(user); err != nil {
		return Ticket{}, err
	}
	// Owners alone can start; management roles and team colleagues have no implicit override.
	if t.AssigneeID == nil || *t.AssigneeID != user.ID {
		return Ticket{}, ErrActionForbidden
	}
	if t.Version != expectedVersion {
		return Ticket{}, ErrVersionConflict
	}
	if t.Status != StatusPending {
		return Ticket{}, ErrInvalidState
	}
	t.Status = StatusInProgress
	t.Version++
	return t, nil
}

func (t Ticket) checkAgent(user User) error {
	if !user.Active || user.ID <= 0 {
		return ErrUnauthenticated
	}
	if user.Role != RoleAgent || user.OrganizationID != t.OrganizationID {
		return ErrActionForbidden
	}
	return nil
}
