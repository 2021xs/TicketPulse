package ticket

import (
	"errors"
	"reflect"
	"testing"
)

func TestActionInputContract(t *testing.T) {
	for _, r := range []ActionRequest{{0, 1}, {-1, 1}, {1, 0}, {1, -1}} {
		var business *Error
		if err := r.Validate(); !errors.As(err, &business) || business.Code != "INVALID_ARGUMENT" {
			t.Fatalf("request %+v: %v", r, err)
		}
	}
	if err := (ActionRequest{1, 1}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestClaimAndStartBusinessRules(t *testing.T) {
	agent := User{ID: 3, OrganizationID: 1, Role: RoleAgent, Active: true}
	before := Ticket{ID: 1, OrganizationID: 1, Status: StatusPending, Version: 1, CurrentTeamID: 10, InitialTeamID: 10, CreatorID: 1}
	claimed, err := before.Claim(agent, 1)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.AssigneeID == nil || *claimed.AssigneeID != 3 || claimed.Version != 2 || claimed.Status != StatusPending {
		t.Fatalf("claim result: %+v", claimed)
	}
	if before.AssigneeID != nil || before.Version != 1 {
		t.Fatal("claim changed its before snapshot")
	}
	started, err := claimed.Start(agent, 2)
	if err != nil {
		t.Fatal(err)
	}
	if started.Status != StatusInProgress || started.Version != 3 || *started.AssigneeID != 3 || claimed.Status != StatusPending {
		t.Fatalf("start result or before snapshot: %+v %+v", started, claimed)
	}
	for _, phase := range []struct {
		name    string
		state   Ticket
		version int64
		apply   func(Ticket, User, int64) (Ticket, error)
	}{
		{"claim", before, 1, Ticket.Claim},
		{"start", claimed, 2, Ticket.Start},
	} {
		for _, role := range []string{RoleRequester, RoleSupervisor, RoleAdmin} {
			t.Run(phase.name+"_role_"+role, func(t *testing.T) {
				actor := agent
				actor.Role = role
				_, err := phase.apply(phase.state, actor, phase.version)
				if err != ErrActionForbidden {
					t.Fatalf("role override: %v", err)
				}
			})
		}
	}
	for _, test := range []struct {
		name    string
		state   Ticket
		actor   User
		version int64
		apply   func(Ticket, User, int64) (Ticket, error)
		want    error
	}{
		{"repeat_claim_old_version", claimed, agent, 1, Ticket.Claim, ErrVersionConflict},
		{"repeat_claim_current_version", claimed, agent, 2, Ticket.Claim, ErrAlreadyClaimed},
		{"repeat_start_old_version", started, agent, 2, Ticket.Start, ErrVersionConflict},
		{"repeat_start_current_version", started, agent, 3, Ticket.Start, ErrInvalidState},
		{"start_non_owner", claimed, User{ID: 11, OrganizationID: 1, Role: RoleAgent, Active: true}, 2, Ticket.Start, ErrActionForbidden},
		{"start_unassigned", before, agent, 1, Ticket.Start, ErrActionForbidden},
		{"claim_disabled", before, User{ID: 3, OrganizationID: 1, Role: RoleAgent, Active: false}, 1, Ticket.Claim, ErrUnauthenticated},
		{"start_disabled", claimed, User{ID: 3, OrganizationID: 1, Role: RoleAgent, Active: false}, 2, Ticket.Start, ErrUnauthenticated},
		{"claim_other_org", before, User{ID: 3, OrganizationID: 2, Role: RoleAgent, Active: true}, 1, Ticket.Claim, ErrActionForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := test.state.State()
			_, err := test.apply(test.state, test.actor, test.version)
			if err != test.want {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
			if !reflect.DeepEqual(original, test.state.State()) {
				t.Fatal("failed action changed before state")
			}
		})
	}
	for _, status := range []string{StatusInProgress, StatusAwaitingConfirmation, StatusClosed} {
		t.Run("invalid_state_"+status, func(t *testing.T) {
			state := before
			state.Status = status
			if _, err := state.Claim(agent, 1); err != ErrInvalidState {
				t.Fatal(err)
			}
			state.AssigneeID = &agent.ID
			if _, err := state.Start(agent, 1); err != ErrInvalidState {
				t.Fatal(err)
			}
		})
	}
}
