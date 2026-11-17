//go:build integration

package mysqlstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/ticket"
)

func actionRequest(t ticket.Ticket) ticket.ActionRequest {
	return ticket.ActionRequest{TicketID: t.ID, ExpectedVersion: t.Version}
}

func (f *fixture) claim(t *testing.T, userID int64, ticket ticket.Ticket) ticket.Ticket {
	t.Helper()
	result, err := f.service.Claim(context.Background(), userID, actionRequest(ticket))
	if err != nil {
		t.Fatal(err)
	}
	return result.Ticket
}

func (f *fixture) detail(t *testing.T, id int64) ticket.Detail {
	t.Helper()
	result, err := f.service.Detail(context.Background(), 1, id)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (f *fixture) actionCounts(t *testing.T, histories int) {
	t.Helper()
	for table, want := range map[string]int{"tickets": 1, "ticket_create_requests": 1, "ticket_histories": histories} {
		var got int
		if err := f.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s count=%d want=%d", table, got, want)
		}
	}
}

func TestClaimStartTimeline(t *testing.T) {
	f := newFixture(t)
	created := f.create(t, 1, request("REQ-claim-start")).Ticket
	claimed := f.claim(t, 3, created)
	if claimed.Version != 2 || claimed.Status != ticket.StatusPending || claimed.AssigneeID == nil || *claimed.AssigneeID != 3 || !claimed.UpdatedAt.After(created.UpdatedAt) {
		t.Fatalf("incorrect claim: %+v", claimed)
	}
	started, err := f.service.Start(context.Background(), 3, actionRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	if started.Ticket.Version != 3 || started.Ticket.Status != ticket.StatusInProgress || *started.Ticket.AssigneeID != 3 || !started.Ticket.UpdatedAt.After(claimed.UpdatedAt) {
		t.Fatalf("incorrect start: %+v", started)
	}
	detail := f.detail(t, created.ID)
	if !reflect.DeepEqual(detail.Ticket, started.Ticket) || detail.Assignee == nil || detail.Assignee.ID != 3 || len(detail.History) != 3 {
		t.Fatalf("incorrect final detail: %+v", detail)
	}
	states := []ticket.Ticket{created, claimed, started.Ticket}
	actions := []string{ticket.ActionCreated, ticket.ActionClaimed, ticket.ActionProcessingStarted}
	operators := []int64{1, 3, 3}
	for i, h := range detail.History {
		if h.Version != int64(i+1) || h.Action != actions[i] || h.OperatorID != operators[i] || h.ResponsibleTeamID != 10 || !reflect.DeepEqual(h.After, states[i].State()) || !h.OccurredAt.Equal(states[i].UpdatedAt) {
			t.Fatalf("history %d is not its committed business state: %+v", i+1, h)
		}
		if i == 0 {
			if h.Before != nil {
				t.Fatal("create before state changed")
			}
		} else if h.Before == nil || !reflect.DeepEqual(*h.Before, states[i-1].State()) {
			t.Fatalf("history %d does not continue previous version", i+1)
		}
	}
	if !started.Ticket.CreatedAt.Equal(created.CreatedAt) || started.Ticket.FirstResolvedAt != nil || started.Ticket.ClosedAt != nil || started.Ticket.ReopenCount != 0 || started.Ticket.InitialTeamID != 10 {
		t.Fatal("claim/start changed unrelated fields")
	}
	// Creation replay must return the same now-current ticket, without another creation history.
	replayed := f.create(t, 1, request("REQ-claim-start"))
	if !replayed.Replayed || !reflect.DeepEqual(replayed.Ticket, started.Ticket) {
		t.Fatal("creation replay regressed after actions")
	}
	f.actionCounts(t, 3)
}

func TestConcurrentEngineersClaim(t *testing.T) {
	f := newFixture(t)
	created := f.create(t, 1, request("REQ-engineer-race")).Ticket
	const n = 20
	start := make(chan struct{})
	type outcome struct {
		user   int64
		result ticket.ActionResult
		err    error
	}
	results := make(chan outcome, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		user := int64(3)
		if i%2 == 1 {
			user = 11
		}
		wg.Add(1)
		go func(user int64) {
			defer wg.Done()
			<-start
			result, err := f.service.Claim(context.Background(), user, actionRequest(created))
			results <- outcome{user, result, err}
		}(user)
	}
	close(start)
	wg.Wait()
	close(results)
	var winner int64
	successes, conflicts := 0, 0
	for outcome := range results {
		if outcome.err != nil {
			checkCode(t, outcome.err, "VERSION_CONFLICT")
			conflicts++
			continue
		}
		successes++
		winner = outcome.user
		if outcome.result.Ticket.AssigneeID == nil || *outcome.result.Ticket.AssigneeID != winner || outcome.result.Ticket.Version != 2 {
			t.Fatal("race winner result incorrect")
		}
	}
	if successes != 1 || conflicts != n-1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	detail := f.detail(t, created.ID)
	if detail.Ticket.Status != ticket.StatusPending || *detail.Ticket.AssigneeID != winner || len(detail.History) != 2 || detail.History[1].OperatorID != winner {
		t.Fatal("race has extra/missing effects")
	}
	if _, err := f.service.Start(context.Background(), winner, actionRequest(detail.Ticket)); err != nil {
		t.Fatal(err)
	}
	f.actionCounts(t, 3)
}

func TestActionRetriesNeverDuplicateHistory(t *testing.T) {
	f := newFixture(t)
	created := f.create(t, 1, request("REQ-repeat-actions")).Ticket
	claimed := f.claim(t, 3, created)
	for _, user := range []int64{3, 11} {
		_, err := f.service.Claim(context.Background(), user, actionRequest(created))
		checkCode(t, err, "VERSION_CONFLICT")
		_, err = f.service.Claim(context.Background(), user, actionRequest(claimed))
		checkCode(t, err, "ALREADY_CLAIMED")
	}
	f.actionCounts(t, 2)
	_, err := f.service.Start(context.Background(), 3, actionRequest(created))
	checkCode(t, err, "VERSION_CONFLICT")
	started, err := f.service.Start(context.Background(), 3, actionRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		input ticket.Ticket
		code  string
	}{{claimed, "VERSION_CONFLICT"}, {started.Ticket, "INVALID_STATE"}} {
		_, err = f.service.Start(context.Background(), 3, actionRequest(test.input))
		checkCode(t, err, test.code)
	}
	if !reflect.DeepEqual(f.detail(t, created.ID).Ticket, started.Ticket) {
		t.Fatal("stale action overwrote new ticket")
	}
	f.actionCounts(t, 3)
}

func TestConcurrentOwnerStart(t *testing.T) {
	f := newFixture(t)
	created := f.create(t, 1, request("REQ-concurrent-start")).Ticket
	claimed := f.claim(t, 3, created)
	const n = 10
	start := make(chan struct{})
	results := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := f.service.Start(context.Background(), 3, actionRequest(claimed))
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			checkCode(t, err, "VERSION_CONFLICT")
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent start successes=%d", successes)
	}
	detail := f.detail(t, created.ID)
	if detail.Ticket.Version != 3 || detail.Ticket.Status != ticket.StatusInProgress || detail.History[2].Action != ticket.ActionProcessingStarted {
		t.Fatal("concurrent start wrote wrong state/history")
	}
	f.actionCounts(t, 3)
}

func TestActionPermissions(t *testing.T) {
	tests := []struct {
		name           string
		user           int64
		mutation, code string
	}{
		{"requester", 1, "", "ACTION_FORBIDDEN"},
		{"other_team", 4, "", "ACTION_FORBIDDEN"},
		{"supervisor", 5, "", "ACTION_FORBIDDEN"},
		{"admin", 6, "", "ACTION_FORBIDDEN"},
		{"inactive_membership", 7, "", "ACTION_FORBIDDEN"},
		{"disabled_user", 8, "", "UNAUTHENTICATED"},
		{"other_organization", 9, "", "TICKET_NOT_FOUND"},
		{"missing_user", 9999, "", "UNAUTHENTICATED"},
		{"no_login", 0, "", "UNAUTHENTICATED"},
		{"team_disabled", 3, "UPDATE teams SET active=FALSE WHERE id=10", "ACTION_FORBIDDEN"},
		{"removed_member", 3, "DELETE FROM team_members WHERE team_id=10 AND user_id=3", "ACTION_FORBIDDEN"},
		{"membership_disabled", 3, "UPDATE team_members SET active=FALSE WHERE team_id=10 AND user_id=3", "ACTION_FORBIDDEN"},
		{"user_disabled_after_claim", 3, "UPDATE users SET active=FALSE WHERE id=3", "UNAUTHENTICATED"},
		{"role_changed", 3, "UPDATE users SET role='supervisor' WHERE id=3", "ACTION_FORBIDDEN"},
	}
	for _, operation := range []string{"claim", "start"} {
		for _, test := range tests {
			t.Run(operation+"/"+test.name, func(t *testing.T) {
				f := newFixture(t)
				current := f.create(t, 1, request("REQ-auth-action")).Ticket
				histories := 1
				if operation == "start" {
					current = f.claim(t, 3, current)
					histories++
				}
				if test.mutation != "" {
					f.exec(t, test.mutation)
				}
				var err error
				if operation == "claim" {
					_, err = f.service.Claim(context.Background(), test.user, actionRequest(current))
				} else {
					_, err = f.service.Start(context.Background(), test.user, actionRequest(current))
				}
				checkCode(t, err, test.code)
				if !reflect.DeepEqual(f.detail(t, current.ID).Ticket, current) {
					t.Fatal("unauthorized action changed ticket")
				}
				f.actionCounts(t, histories)
			})
		}
	}
}

func TestStartRequiresCurrentOwner(t *testing.T) {
	f := newFixture(t)
	created := f.create(t, 1, request("REQ-owner-only")).Ticket
	_, err := f.service.Start(context.Background(), 3, actionRequest(created))
	checkCode(t, err, "ACTION_FORBIDDEN")
	claimed := f.claim(t, 3, created)
	_, err = f.service.Start(context.Background(), 11, actionRequest(claimed))
	checkCode(t, err, "ACTION_FORBIDDEN")
	f.actionCounts(t, 2)
}

func TestInvalidActionStates(t *testing.T) {
	for _, operation := range []string{"claim", "start"} {
		for _, state := range []string{ticket.StatusInProgress, ticket.StatusAwaitingConfirmation, ticket.StatusClosed} {
			t.Run(operation+"/"+state, func(t *testing.T) {
				f := newFixture(t)
				current := f.create(t, 1, request("REQ-state-action")).Ticket
				histories := 1
				if operation == "start" {
					current = f.claim(t, 3, current)
					histories++
				}
				// Fixture-only state, without committing a partial ticket/history update.
				after := current.State()
				after.Status = state
				after.Version++
				beforeJSON, _ := json.Marshal(current.State())
				afterJSON, _ := json.Marshal(after)
				tx, err := f.db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.Exec("UPDATE tickets SET status=?, version=version+1 WHERE id=?", state, current.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(`INSERT INTO ticket_histories (organization_id,ticket_id,action,operator_id,version,before_state,after_state,responsible_team_id,occurred_at) VALUES (1,?,'fixture',5,?,?,?,10,?)`, current.ID, after.Version, beforeJSON, afterJSON, current.UpdatedAt); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				histories++
				current = f.detail(t, current.ID).Ticket
				if operation == "claim" {
					_, err = f.service.Claim(context.Background(), 3, actionRequest(current))
				} else {
					_, err = f.service.Start(context.Background(), 3, actionRequest(current))
				}
				checkCode(t, err, "INVALID_STATE")
				if !reflect.DeepEqual(f.detail(t, current.ID).Ticket, current) {
					t.Fatal("invalid state action updated ticket")
				}
				f.actionCounts(t, histories)
			})
		}
	}
}

func TestActionHistoryFailureRollsBackUpdate(t *testing.T) {
	for _, operation := range []string{"claim", "start"} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			current := f.create(t, 1, request("REQ-action-rollback")).Ticket
			histories := 1
			if operation == "start" {
				current = f.claim(t, 3, current)
				histories++
			}
			before := f.detail(t, current.ID)
			f.exec(t, `CREATE TRIGGER fail_action_history BEFORE INSERT ON ticket_histories FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected action history failure'`)
			var err error
			if operation == "claim" {
				_, err = f.service.Claim(context.Background(), 3, actionRequest(current))
			} else {
				_, err = f.service.Start(context.Background(), 3, actionRequest(current))
			}
			if err == nil {
				t.Fatal("history failure returned success")
			}
			if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
				t.Fatal("history failure did not roll back entire update")
			}
			f.actionCounts(t, histories)
			f.exec(t, "DROP TRIGGER fail_action_history")
			if operation == "claim" {
				_, err = f.service.Claim(context.Background(), 3, actionRequest(current))
			} else {
				_, err = f.service.Start(context.Background(), 3, actionRequest(current))
			}
			if err != nil {
				t.Fatal("same version could not retry after rollback", err)
			}
			f.actionCounts(t, histories+1)
		})
	}
}

func TestActionHTTPCompetitionAndErrors(t *testing.T) {
	f := newFixture(t)
	server := httptest.NewServer(httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	type response struct {
		status int
		body   []byte
		err    error
	}
	call := func(path, token, payload string) response {
		r, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(payload))
		if err != nil {
			return response{err: err}
		}
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-User-ID", "6")
		r.Header.Set("X-Role", "admin")
		resp, err := server.Client().Do(r)
		if err != nil {
			return response{err: err}
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		return response{resp.StatusCode, data, err}
	}
	payload, _ := json.Marshal(request("REQ-http-actions"))
	createdHTTP := call("/api/v1/tickets", "local-alice", string(payload))
	if createdHTTP.err != nil || createdHTTP.status != 201 {
		t.Fatalf("create: %+v", createdHTTP)
	}
	var created ticket.CreateResult
	if err := json.Unmarshal(createdHTTP.body, &created); err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(actionRequest(created.Ticket))
	start := make(chan struct{})
	results := make(chan struct {
		token    string
		response response
	}, 2)
	for _, token := range []string{"local-it-agent", "local-it-agent-2"} {
		go func(token string) {
			<-start
			results <- struct {
				token    string
				response response
			}{token, call("/api/v1/tickets/claim", token, string(input))}
		}(token)
	}
	close(start)
	winner, loser := "", ""
	var claimed ticket.ActionResult
	for i := 0; i < 2; i++ {
		result := <-results
		if result.response.err != nil {
			t.Fatal(result.response.err)
		}
		switch result.response.status {
		case 200:
			if winner != "" {
				t.Fatal("two HTTP claims succeeded")
			}
			winner = result.token
			if err := json.Unmarshal(result.response.body, &claimed); err != nil {
				t.Fatal(err)
			}
		case 409:
			loser = result.token
			if !bytes.Contains(result.response.body, []byte("VERSION_CONFLICT")) {
				t.Fatalf("wrong race conflict: %s", result.response.body)
			}
		default:
			t.Fatalf("race status=%d body=%s", result.response.status, result.response.body)
		}
	}
	if winner == "" || loser == "" {
		t.Fatal("race needs exactly one success and one conflict")
	}
	startInput, _ := json.Marshal(actionRequest(claimed.Ticket))
	alreadyClaimed := call("/api/v1/tickets/claim", loser, string(startInput))
	if alreadyClaimed.status != 409 || !bytes.Contains(alreadyClaimed.body, []byte("ALREADY_CLAIMED")) {
		t.Fatalf("current-version duplicate claim: %d %s", alreadyClaimed.status, alreadyClaimed.body)
	}
	for _, token := range []string{"local-supervisor", "local-admin"} {
		denied := call("/api/v1/tickets/start", token, string(startInput))
		if denied.status != 403 || !bytes.Contains(denied.body, []byte("ACTION_FORBIDDEN")) {
			t.Fatalf("management role bypass: %d %s", denied.status, denied.body)
		}
	}
	unauthorized := call("/api/v1/tickets/start", loser, string(startInput))
	if unauthorized.status != 403 {
		t.Fatalf("non-owner HTTP start: %d %s", unauthorized.status, unauthorized.body)
	}
	startedHTTP := call("/api/v1/tickets/start", winner, string(startInput))
	if startedHTTP.status != 200 {
		t.Fatalf("start: %d %s", startedHTTP.status, startedHTTP.body)
	}
	var started ticket.ActionResult
	if err := json.Unmarshal(startedHTTP.body, &started); err != nil {
		t.Fatal(err)
	}
	if started.Ticket.Status != ticket.StatusInProgress || started.Ticket.Version != 3 {
		t.Fatal("HTTP final state incorrect")
	}
	currentInput, _ := json.Marshal(actionRequest(started.Ticket))
	invalidState := call("/api/v1/tickets/start", winner, string(currentInput))
	if invalidState.status != 409 || !bytes.Contains(invalidState.body, []byte("INVALID_STATE")) {
		t.Fatalf("current-version invalid state: %d %s", invalidState.status, invalidState.body)
	}
	for _, path := range []string{"/api/v1/tickets/claim", "/api/v1/tickets/start"} {
		for _, test := range []struct {
			token, payload, code string
			status               int
		}{
			{"", string(startInput), "UNAUTHENTICATED", 401},
			{"local-disabled", string(startInput), "UNAUTHENTICATED", 401},
			{winner, `{"ticket_id":1}`, "INVALID_ARGUMENT", 400},
			{winner, `{"ticket_id":0,"expected_version":1}`, "INVALID_ARGUMENT", 400},
			{winner, `{"ticket_id":1,"expected_version":1.5}`, "INVALID_ARGUMENT", 400},
			{winner, `{"ticket_id":1,"expected_version":1,"user_id":6}`, "INVALID_ARGUMENT", 400},
			{winner, `{"ticket_id":1,"expected_version":1,"role":"admin"}`, "INVALID_ARGUMENT", 400},
			{winner, `{"ticket_id":1,"expected_version":1,"team_id":10}`, "INVALID_ARGUMENT", 400},
			{winner, "{} {}", "INVALID_ARGUMENT", 400},
			{winner, `{"ticket_id":99999999,"expected_version":1}`, "TICKET_NOT_FOUND", 404},
		} {
			resp := call(path, test.token, test.payload)
			if resp.err != nil {
				t.Fatal(resp.err)
			}
			if resp.status != test.status || !bytes.Contains(resp.body, []byte(test.code)) {
				t.Fatalf("%s: status=%d body=%s", path, resp.status, resp.body)
			}
			if bytes.Contains(resp.body, []byte(created.Ticket.Title)) {
				t.Fatal("error disclosed title")
			}
		}
	}
	repeat := call("/api/v1/tickets/start", winner, string(startInput))
	if repeat.status != 409 || !bytes.Contains(repeat.body, []byte("VERSION_CONFLICT")) {
		t.Fatal("HTTP repeated start was not a conflict")
	}
	detail := f.detail(t, created.Ticket.ID)
	if detail.Ticket.Version != 3 || len(detail.History) != 3 {
		t.Fatal("HTTP action history incorrect")
	}
	f.actionCounts(t, 3)
	t.Logf("real HTTP race winner=%s; loser=409; final ticket=%d version=3 history=3", winner, created.Ticket.ID)
}

func TestLostActionResponseRetriesWithConflict(t *testing.T) {
	for _, operation := range []string{"claim", "start"} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			current := f.create(t, 1, request("REQ-lost-action-response")).Ticket
			histories := 1
			if operation == "start" {
				current = f.claim(t, 3, current)
				histories++
			}
			api := httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil)))
			var once sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				drop := false
				once.Do(func() { drop = true })
				if !drop {
					api.ServeHTTP(w, r)
					return
				}
				recorder := httptest.NewRecorder()
				api.ServeHTTP(recorder, r)
				if recorder.Code != 200 {
					t.Errorf("before dropping response: %d %s", recorder.Code, recorder.Body)
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				conn.Close()
			}))
			defer server.Close()
			input, _ := json.Marshal(actionRequest(current))
			post := func() (*http.Response, error) {
				r, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/tickets/"+operation, bytes.NewReader(input))
				if err != nil {
					return nil, err
				}
				r.Header.Set("Authorization", "Bearer local-it-agent")
				r.Header.Set("Content-Type", "application/json")
				return server.Client().Do(r)
			}
			response, err := post()
			if err == nil {
				response.Body.Close()
				t.Fatal("client received dropped response")
			}
			response, err = post()
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != 409 || !bytes.Contains(body, []byte("VERSION_CONFLICT")) {
				t.Fatalf("retry: %d %s", response.StatusCode, body)
			}
			if f.detail(t, current.ID).Ticket.Version != current.Version+1 {
				t.Fatal("retry added another version")
			}
			f.actionCounts(t, histories+1)
		})
	}
}

func TestActionDatabaseFailureReturnsHTTPError(t *testing.T) {
	f := newFixture(t)
	created := f.create(t, 1, request("REQ-http-db-failure")).Ticket
	f.exec(t, `CREATE TRIGGER fail_action_ticket BEFORE UPDATE ON tickets FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected update failure'`)
	server := httptest.NewServer(httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	body := fmt.Sprintf(`{"ticket_id":%d,"expected_version":1}`, created.ID)
	r, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/tickets/claim", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer local-it-agent")
	r.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if response.StatusCode != 500 || !bytes.Contains(data, []byte("INTERNAL_ERROR")) || bytes.Contains(data, []byte("injected")) {
		t.Fatalf("database failure HTTP=%d body=%s", response.StatusCode, data)
	}
	if !reflect.DeepEqual(f.detail(t, created.ID).Ticket, created) {
		t.Fatal("DB failure changed ticket")
	}
	f.actionCounts(t, 1)
}
