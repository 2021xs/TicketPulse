package ticket

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestResolutionInput(t *testing.T) {
	for _, request := range []ResolveRequest{
		{ActionRequest: ActionRequest{0, 3}, ResolutionNote: "已修复"},
		{ActionRequest: ActionRequest{1, 0}, ResolutionNote: "已修复"},
		{ActionRequest: ActionRequest{1, 3}},
		{ActionRequest: ActionRequest{1, 3}, ResolutionNote: " \n\t"},
		{ActionRequest: ActionRequest{1, 3}, ResolutionNote: strings.Repeat("文", 5001)},
		{ActionRequest: ActionRequest{1, 3}, ResolutionNote: string([]byte{0xff})},
	} {
		var business *Error
		if err := request.Validate(); !errors.As(err, &business) || business.Code != "INVALID_ARGUMENT" {
			t.Fatalf("invalid request accepted: %v", err)
		}
	}
	if err := (ResolveRequest{ActionRequest: ActionRequest{1, 3}, ResolutionNote: strings.Repeat("😀", 5000)}).Validate(); err != nil {
		t.Fatal("valid maximum resolution rejected", err)
	}
}

func TestLifecycleBusinessRules(t *testing.T) {
	owner := User{ID: 3, OrganizationID: 1, Role: RoleAgent, Active: true}
	creator := User{ID: 1, OrganizationID: 1, Role: RoleRequester, Active: true}
	processing := Ticket{ID: 7, OrganizationID: 1, CreatorID: 1, AssigneeID: &owner.ID, Status: StatusInProgress, Version: 3, CurrentTeamID: 10, InitialTeamID: 10}
	firstAt := time.Date(2026, 11, 8, 8, 0, 0, 1e3, time.UTC)
	secondAt := firstAt.Add(time.Hour)
	note1, note2 := "已修复主线路，等待员工确认。", "已修复备用线路，说明包含换行\n验证通过。"
	first, err := processing.Resolve(owner, 3, note1, firstAt)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != StatusAwaitingConfirmation || first.Version != 4 || first.FirstResolvedAt == nil || !first.FirstResolvedAt.Equal(firstAt) || first.ResolutionNote == nil || *first.ResolutionNote != note1 {
		t.Fatalf("incorrect first resolution: %+v", first)
	}
	if processing.FirstResolvedAt != nil || processing.ResolutionNote != nil || processing.Version != 3 {
		t.Fatal("resolve mutated before state")
	}
	reopened, err := first.Reopen(creator, 4)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Status != StatusInProgress || reopened.Version != 5 || reopened.ReopenCount != 1 || *reopened.AssigneeID != owner.ID || !reopened.FirstResolvedAt.Equal(firstAt) || *reopened.ResolutionNote != note1 {
		t.Fatalf("incorrect reopen: %+v", reopened)
	}
	second, err := reopened.Resolve(owner, 5, note2, secondAt)
	if err != nil {
		t.Fatal(err)
	}
	if second.Version != 6 || !second.FirstResolvedAt.Equal(firstAt) || *second.ResolutionNote != note2 || *first.ResolutionNote != note1 {
		t.Fatal("second resolve lost first time or old resolution snapshot")
	}
	closed, err := second.Close(creator, 6, secondAt.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if closed.Version != 7 || closed.Status != StatusClosed || closed.ClosedAt == nil || !closed.ClosedAt.Equal(secondAt.Add(time.Minute)) || closed.ReopenCount != 1 || !closed.FirstResolvedAt.Equal(firstAt) || *closed.ResolutionNote != note2 {
		t.Fatalf("incorrect close: %+v", closed)
	}

	for _, test := range []struct {
		name    string
		state   Ticket
		actor   User
		version int64
		action  string
		want    error
	}{
		{"resolve_non_owner", processing, User{ID: 11, OrganizationID: 1, Role: RoleAgent, Active: true}, 3, "resolve", ErrActionForbidden},
		{"resolve_requester", processing, creator, 3, "resolve", ErrActionForbidden},
		{"resolve_disabled_owner", processing, User{ID: 3, OrganizationID: 1, Role: RoleAgent}, 3, "resolve", ErrUnauthenticated},
		{"resolve_admin_owner", processing, User{ID: 3, OrganizationID: 1, Role: RoleAdmin, Active: true}, 3, "resolve", ErrActionForbidden},
		{"resolve_old_version", processing, owner, 2, "resolve", ErrVersionConflict},
		{"reopen_non_creator", first, owner, 4, "reopen", ErrActionForbidden},
		{"close_non_creator", first, owner, 4, "close", ErrActionForbidden},
		{"reopen_old_version", first, creator, 3, "reopen", ErrVersionConflict},
		{"close_old_version", first, creator, 3, "close", ErrVersionConflict},
		{"reopen_closed", closed, creator, 7, "reopen", ErrInvalidState},
		{"close_closed", closed, creator, 7, "close", ErrInvalidState},
		{"resolve_closed", closed, owner, 7, "resolve", ErrInvalidState},
		{"reopen_processing", processing, creator, 3, "reopen", ErrInvalidState},
		{"close_processing", processing, creator, 3, "close", ErrInvalidState},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := test.state.State()
			var err error
			switch test.action {
			case "resolve":
				_, err = test.state.Resolve(test.actor, test.version, note1, firstAt)
			case "reopen":
				_, err = test.state.Reopen(test.actor, test.version)
			case "close":
				_, err = test.state.Close(test.actor, test.version, secondAt)
			}
			if err != test.want {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
			if !reflect.DeepEqual(before, test.state.State()) {
				t.Fatal("failure changed before state")
			}
		})
	}
	noOwner := first
	noOwner.AssigneeID = nil
	if requeued, err := noOwner.Reopen(creator, 4); err != nil || requeued.Status != StatusPending || requeued.AssigneeID != nil || requeued.ReopenCount != 1 || requeued.Version != 5 {
		t.Fatal("missing owner must requeue after Store validates current team", err)
	}
	for _, role := range []string{RoleRequester, RoleAgent, RoleSupervisor, RoleAdmin} {
		actor := creator
		actor.Role = role
		if _, err := first.Reopen(actor, 4); err != nil {
			t.Fatal("creator's current valid role incorrectly removed creator relation", err)
		}
		if _, err := first.Close(actor, 4, secondAt); err != nil {
			t.Fatal(err)
		}
	}
}
