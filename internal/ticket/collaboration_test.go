package ticket

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCollaborationInput(t *testing.T) {
	base := ActionRequest{TicketID: 10, ExpectedVersion: 4}
	invalid := []interface{ Validate() error }{
		TransferRequest{ActionRequest: base},
		TransferRequest{ActionRequest: base, AssigneeID: 3, Reason: " "},
		TransferRequest{ActionRequest: base, AssigneeID: 3, Reason: strings.Repeat("文", 1001)},
		TeamTransferRequest{ActionRequest: base, TeamID: 20},
		TeamTransferRequest{ActionRequest: base, TeamID: 0, Reason: "修复"},
		ManagementReopenRequest{ActionRequest: base, TeamID: -1, Reason: "修复"},
		ManagementReopenRequest{ActionRequest: base, Reason: "\n\t"},
		ReasonRequest{ActionRequest: base},
		CommentRequest{ActionRequest: base, Content: " "},
		CommentRequest{ActionRequest: base, Content: strings.Repeat("😀", 5001)},
		CommentRequest{ActionRequest: base, Content: string([]byte{0xff})},
		CommentRequest{ActionRequest: ActionRequest{0, 4}, Content: "内容"},
		CommentRequest{ActionRequest: ActionRequest{10, 0}, Content: "内容"},
	}
	for _, r := range invalid {
		var business *Error
		if err := r.Validate(); !errors.As(err, &business) || business.Code != "INVALID_ARGUMENT" {
			t.Fatalf("invalid request accepted: %#v, %v", r, err)
		}
	}
	for _, r := range []interface{ Validate() error }{
		TransferRequest{ActionRequest: base, AssigneeID: 11},
		TeamTransferRequest{ActionRequest: base, TeamID: 20, Reason: strings.Repeat("😀", 1000)},
		ManagementReopenRequest{ActionRequest: base, Reason: "  人工重开\n原文保留  "},
		ReasonRequest{ActionRequest: base, Reason: "人工关闭"},
		CommentRequest{ActionRequest: base, Content: strings.Repeat("😀", 5000)},
	} {
		if err := r.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTransferAndManagementRules(t *testing.T) {
	owner := User{ID: 3, OrganizationID: 1, Role: RoleAgent, Active: true}
	manager := User{ID: 5, OrganizationID: 1, Role: RoleSupervisor, Active: true}
	current := Ticket{ID: 1, OrganizationID: 1, CreatorID: 1, CurrentTeamID: 10, InitialTeamID: 10, AssigneeID: &owner.ID, Status: StatusInProgress, Version: 3}
	r := TransferRequest{ActionRequest: ActionRequest{1, 3}, AssigneeID: 11}
	changed, err := current.Transfer(owner, r)
	if err != nil || changed.Version != 4 || *changed.AssigneeID != 11 || changed.Status != StatusInProgress || *current.AssigneeID != 3 {
		t.Fatal("same-team transfer lost state", err)
	}
	if _, err := changed.Resolve(owner, 4, "旧负责人解决", time.Now()); err != ErrActionForbidden {
		t.Fatal("old owner retained resolve permission", err)
	}
	colleague := owner
	colleague.ID = 11
	if _, err := current.Transfer(colleague, r); err != ErrActionForbidden {
		t.Fatal("non-owner transferred", err)
	}
	noOwner := current
	noOwner.AssigneeID = nil
	noOwner.Status = StatusPending
	if _, err := noOwner.Transfer(owner, r); err != ErrActionForbidden {
		t.Fatal("unassigned transfer by Agent", err)
	}
	if assigned, err := noOwner.Transfer(manager, r); err != nil || assigned.Status != StatusPending || *assigned.AssigneeID != 11 {
		t.Fatal("manager cannot assign", err)
	}
	for _, status := range []string{StatusPending, StatusInProgress} {
		source := current
		source.Status = status
		result, err := source.TransferTeam(manager, TeamTransferRequest{ActionRequest: ActionRequest{1, 3}, TeamID: 20, Reason: "设备问题"})
		if err != nil || result.Status != StatusPending || result.AssigneeID != nil || result.CurrentTeamID != 20 || result.InitialTeamID != 10 || result.Version != 4 {
			t.Fatal("cross-team requeue wrong", err)
		}
	}
	awaiting := current
	awaiting.Status = StatusAwaitingConfirmation
	first := time.Now().UTC()
	awaiting.FirstResolvedAt = &first
	if _, err := awaiting.Transfer(manager, r); err != ErrInvalidState {
		t.Fatal("transfer awaiting allowed", err)
	}
	if _, err := current.TransferTeam(owner, TeamTransferRequest{ActionRequest: ActionRequest{1, 3}, TeamID: 20}); err != ErrActionForbidden {
		t.Fatal("Agent cross-team allowed", err)
	}
	for _, retained := range []*int64{nil, &owner.ID} {
		reopened, err := awaiting.ManagementReopen(manager, 3, 20, retained)
		if err != nil || reopened.ReopenCount != 1 || reopened.Version != 4 || reopened.FirstResolvedAt != &first {
			t.Fatal("management reopen lifecycle fields", err)
		}
		if retained == nil && reopened.Status != StatusPending || retained != nil && reopened.Status != StatusInProgress {
			t.Fatal("management reopen queue rule")
		}
	}
	closed, err := awaiting.ManualClose(manager, 3, first.Add(time.Hour))
	if err != nil || closed.Status != StatusClosed || closed.Version != 4 || closed.ClosedAt == nil || closed.FirstResolvedAt != &first {
		t.Fatal("manual close lifecycle fields", err)
	}
	if _, err := closed.ManagementReopen(manager, 4, 20, nil); err != ErrInvalidState {
		t.Fatal("manager reopened closed", err)
	}
	if _, err := awaiting.ManualClose(owner, 3, first); err != ErrActionForbidden {
		t.Fatal("Agent manually closed", err)
	}
	if _, err := awaiting.ManualClose(manager, 2, first); err != ErrVersionConflict {
		t.Fatal("manual close accepted stale version", err)
	}
}

func TestListInput(t *testing.T) {
	for _, r := range []ListRequest{{"all", 1, 20}, {ViewCreatedByMe, 0, 20}, {ViewCreatedByMe, 10001, 20}, {ViewTeamPending, 1, 0}, {ViewAssignedToMe, 1, 101}} {
		if err := r.Validate(); err == nil {
			t.Fatalf("invalid list accepted: %+v", r)
		}
	}
	if err := (ListRequest{ViewTeamPending, 10000, 100}).Validate(); err != nil {
		t.Fatal(err)
	}
}
