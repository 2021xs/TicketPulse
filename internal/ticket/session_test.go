package ticket

import (
	"encoding/base64"
	"slices"
	"testing"
)

func TestSessionTokenValidationAndCSRF(t *testing.T) {
	token := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	if !validSessionToken(token) || validSessionToken(token+"=") || validSessionToken("short") {
		t.Fatal("session token byte/encoding contract changed")
	}
	other := base64.RawURLEncoding.EncodeToString(append([]byte{1}, make([]byte, 31)...))
	if SessionCSRF(token) == SessionCSRF(other) || SessionCSRF(token) == token || len(SessionCSRF(token)) != 43 {
		t.Fatal("CSRF value must be bound to the secret session")
	}
}

func TestAvailableActionsRespectOwnerAndManager(t *testing.T) {
	owner := int64(3)
	ticket := Ticket{CreatorID: 1, AssigneeID: &owner, Status: StatusInProgress}
	u := User{ID: 11, Role: RoleAgent}
	if slices.Contains(AvailableActions(u, ticket, true), "resolve") || slices.Contains(AvailableActions(u, ticket, true), "transfer") {
		t.Fatal("non-owner display capabilities leaked")
	}
	u.ID = 3
	if !slices.Contains(AvailableActions(u, ticket, true), "resolve") || slices.Contains(AvailableActions(u, ticket, false), "resolve") {
		t.Fatal("effective owner capability changed")
	}
	u.Role = RoleSupervisor
	if slices.Contains(AvailableActions(u, ticket, false), "resolve") || !slices.Contains(AvailableActions(u, ticket, false), "transfer-team") {
		t.Fatal("manager processing override")
	}
	ticket.Status = StatusClosed
	if got := AvailableActions(u, ticket, false); !slices.Equal(got, []string{"comments"}) {
		t.Fatal("closed state displays state actions", got)
	}
}
