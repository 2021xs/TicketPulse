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
	"time"

	assets "ticketpulse/db"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/ticket"
)

func (f *fixture) processing(t *testing.T, requestID string) ticket.Ticket {
	t.Helper()
	created := f.create(t, 1, request(requestID)).Ticket
	claimed := f.claim(t, 3, created)
	started, err := f.service.Start(context.Background(), 3, actionRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	return started.Ticket
}

func resolution(t ticket.Ticket, note string) ticket.ResolveRequest {
	return ticket.ResolveRequest{ActionRequest: actionRequest(t), ResolutionNote: note}
}

func (f *fixture) resolve(t *testing.T, current ticket.Ticket, note string) ticket.Ticket {
	t.Helper()
	result, err := f.service.Resolve(context.Background(), 3, resolution(current, note))
	if err != nil {
		t.Fatal(err)
	}
	return result.Ticket
}

func (f *fixture) lifecycle(t *testing.T, userID int64, action string, current ticket.Ticket) (ticket.ActionResult, error) {
	t.Helper()
	switch action {
	case "resolve":
		return f.service.Resolve(context.Background(), userID, resolution(current, "已恢复网络，验证连通性。"))
	case "reopen":
		return f.service.Reopen(context.Background(), userID, actionRequest(current))
	case "close":
		return f.service.Close(context.Background(), userID, actionRequest(current))
	default:
		t.Fatalf("unknown test action %s", action)
		return ticket.ActionResult{}, nil
	}
}

func TestLifecycleRoundTrip(t *testing.T) {
	for _, withReopen := range []bool{false, true} {
		t.Run(fmt.Sprintf("reopen_%t", withReopen), func(t *testing.T) {
			f := newFixture(t)
			processing := f.processing(t, "REQ-lifecycle")
			beforeResolve := f.detail(t, processing.ID)
			note1 := "  已修复主线路。\n保留中文、空格及 😀。  "
			note2 := "再次解决：更换备用交换机\n已检查所有线路。"
			first := f.resolve(t, processing, note1)
			if first.Status != ticket.StatusAwaitingConfirmation || first.Version != 4 || first.FirstResolvedAt == nil || !first.FirstResolvedAt.Equal(first.UpdatedAt) || first.ClosedAt != nil || first.ResolutionNote == nil || *first.ResolutionNote != note1 {
				t.Fatalf("wrong first resolution: %+v", first)
			}
			if !reflect.DeepEqual(f.detail(t, first.ID).Ticket, first) {
				t.Fatal("resolution not persisted")
			}
			current := first
			if withReopen {
				reopened, err := f.service.Reopen(context.Background(), 1, actionRequest(first))
				if err != nil {
					t.Fatal(err)
				}
				if reopened.Ticket.Version != 5 || reopened.Ticket.Status != ticket.StatusInProgress || reopened.Ticket.ReopenCount != 1 || *reopened.Ticket.AssigneeID != 3 || !reopened.Ticket.FirstResolvedAt.Equal(*first.FirstResolvedAt) || *reopened.Ticket.ResolutionNote != note1 {
					t.Fatalf("wrong reopen: %+v", reopened)
				}
				current = f.resolve(t, reopened.Ticket, note2)
				if current.Version != 6 || !current.FirstResolvedAt.Equal(*first.FirstResolvedAt) || *current.ResolutionNote != note2 || !current.UpdatedAt.After(first.UpdatedAt) {
					t.Fatal("second resolution overwrote first time or note")
				}
			}
			closed, err := f.service.Close(context.Background(), 1, actionRequest(current))
			if err != nil {
				t.Fatal(err)
			}
			wantVersion := int64(5)
			if withReopen {
				wantVersion = 7
			}
			if closed.Ticket.Status != ticket.StatusClosed || closed.Ticket.Version != wantVersion || closed.Ticket.ClosedAt == nil || !closed.Ticket.ClosedAt.Equal(closed.Ticket.UpdatedAt) || !closed.Ticket.FirstResolvedAt.Equal(*first.FirstResolvedAt) || closed.Ticket.InitialTeamID != processing.InitialTeamID {
				t.Fatalf("wrong close: %+v", closed)
			}
			detail := f.detail(t, processing.ID)
			if !reflect.DeepEqual(detail.Ticket, closed.Ticket) || len(detail.History) != int(wantVersion) {
				t.Fatal("current ticket and history differ")
			}
			actions := []string{ticket.ActionCreated, ticket.ActionClaimed, ticket.ActionProcessingStarted, ticket.ActionResolved, ticket.ActionClosed}
			if withReopen {
				actions = []string{ticket.ActionCreated, ticket.ActionClaimed, ticket.ActionProcessingStarted, ticket.ActionResolved, ticket.ActionReopened, ticket.ActionResolved, ticket.ActionClosed}
			}
			for i, h := range detail.History {
				if h.Version != int64(i+1) || h.Action != actions[i] || h.ResponsibleTeamID != 10 {
					t.Fatalf("wrong history %d: %+v", i+1, h)
				}
				if i > 0 && (h.Before == nil || !reflect.DeepEqual(*h.Before, detail.History[i-1].After)) {
					t.Fatal("history before/after chain broken")
				}
				if i < 3 && !reflect.DeepEqual(h, beforeResolve.History[i]) {
					t.Fatal("existing history overwritten")
				}
				if i >= 3 && !h.After.FirstResolvedAt.Equal(*first.FirstResolvedAt) {
					t.Fatal("historical first time changed")
				}
				if h.Action == ticket.ActionResolved && h.OperatorID != 3 || (h.Action == ticket.ActionReopened || h.Action == ticket.ActionClosed) && h.OperatorID != 1 {
					t.Fatal("history operator incorrect")
				}
			}
			if *detail.History[3].After.ResolutionNote != note1 || withReopen && *detail.History[5].After.ResolutionNote != note2 {
				t.Fatal("resolution history lost text")
			}
			if detail.Ticket.ReopenCount != int64(boolInt(withReopen)) {
				t.Fatal("wrong reopen_count")
			}
			f.actionCounts(t, int(wantVersion))
		})
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestLifecyclePermissionAndAssigneeValidity(t *testing.T) {
	for _, action := range []string{"resolve", "reopen", "close"} {
		for _, actor := range []int64{0, 2, 4, 5, 6, 7, 8, 9, 11} {
			t.Run(fmt.Sprintf("%s/user_%d", action, actor), func(t *testing.T) {
				f := newFixture(t)
				current := f.processing(t, "REQ-lifecycle-permission")
				if action != "resolve" {
					current = f.resolve(t, current, "首次解决")
				}
				before := f.detail(t, current.ID)
				_, err := f.lifecycle(t, actor, action, current)
				code := "ACTION_FORBIDDEN"
				if actor == 0 || actor == 8 {
					code = "UNAUTHENTICATED"
				}
				if actor == 9 {
					code = "TICKET_NOT_FOUND"
				}
				checkCode(t, err, code)
				if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
					t.Fatal("unauthorized lifecycle changed ticket/history")
				}
			})
		}
	}
	mutations := []struct{ name, sql string }{
		{"disabled_owner", "UPDATE users SET active=FALSE WHERE id=3"},
		{"owner_role_changed", "UPDATE users SET role='requester' WHERE id=3"},
		{"removed_owner", "DELETE FROM team_members WHERE team_id=10 AND user_id=3"},
		{"inactive_membership", "UPDATE team_members SET active=FALSE WHERE team_id=10 AND user_id=3"},
		{"inactive_team", "UPDATE teams SET active=FALSE WHERE id=10"},
	}
	for _, test := range mutations {
		t.Run("reopen/"+test.name, func(t *testing.T) {
			f := newFixture(t)
			current := f.resolve(t, f.processing(t, "REQ-reopen-owner-validity"), "修复后待确认")
			f.exec(t, test.sql)
			before := f.detail(t, current.ID)
			reopened, err := f.service.Reopen(context.Background(), 1, actionRequest(current))
			if test.name == "inactive_team" {
				checkCode(t, err, "TEAM_UNAVAILABLE")
				if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
					t.Fatal("inactive team reopen changed ticket")
				}
				// Ordinary close remains available, but TEAM_UNAVAILABLE points to management recovery.
				closed, err := f.service.Close(context.Background(), 1, actionRequest(current))
				if err != nil || closed.Ticket.Status != ticket.StatusClosed {
					t.Fatal("creator cannot close with unavailable team", err)
				}
			} else {
				if err != nil || reopened.Ticket.Status != ticket.StatusPending || reopened.Ticket.AssigneeID != nil || reopened.Ticket.ReopenCount != 1 || reopened.Ticket.Version != 5 || !reopened.Ticket.FirstResolvedAt.Equal(*current.FirstResolvedAt) {
					t.Fatal("unavailable owner did not requeue", err)
				}
				h := f.detail(t, current.ID).History[4]
				if h.Action != ticket.ActionReopenedRequeued || h.Reason == nil || h.Before.AssigneeID == nil || h.After.AssigneeID != nil {
					t.Fatal("requeue exception missing audit")
				}
			}
			f.actionCounts(t, 5)
		})
		t.Run("resolve/"+test.name, func(t *testing.T) {
			f := newFixture(t)
			current := f.processing(t, "REQ-resolve-owner-validity")
			f.exec(t, test.sql)
			_, err := f.lifecycle(t, 3, "resolve", current)
			code := "ACTION_FORBIDDEN"
			if test.name == "disabled_owner" {
				code = "UNAUTHENTICATED"
			}
			checkCode(t, err, code)
			f.actionCounts(t, 3)
		})
	}
	for _, action := range []string{"reopen", "close"} {
		t.Run(action+"/disabled_creator", func(t *testing.T) {
			f := newFixture(t)
			current := f.resolve(t, f.processing(t, "REQ-disabled-creator"), "修复后待确认")
			f.exec(t, "UPDATE users SET active=FALSE WHERE id=1")
			_, err := f.lifecycle(t, 1, action, current)
			checkCode(t, err, "UNAUTHENTICATED")
			f.actionCounts(t, 4)
		})
	}
}

func TestLifecycleStatesVersionsAndRetries(t *testing.T) {
	for _, action := range []string{"resolve", "reopen", "close"} {
		t.Run(action, func(t *testing.T) {
			f := newFixture(t)
			created := f.create(t, 1, request("REQ-state-version")).Ticket
			actor := int64(1)
			if action == "resolve" {
				actor = 3
			}
			current := f.claim(t, 3, created)
			_, err := f.lifecycle(t, actor, action, current)
			checkCode(t, err, "INVALID_STATE")
			processing, err := f.service.Start(context.Background(), 3, actionRequest(current))
			if err != nil {
				t.Fatal(err)
			}
			current = processing.Ticket
			if action != "resolve" {
				_, err = f.lifecycle(t, actor, action, current)
				checkCode(t, err, "INVALID_STATE")
				current = f.resolve(t, current, "有效解决说明")
			}
			stale := current
			stale.Version--
			before := f.detail(t, current.ID)
			_, err = f.lifecycle(t, actor, action, stale)
			checkCode(t, err, "VERSION_CONFLICT")
			if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
				t.Fatal("stale version changed ticket")
			}
			success, err := f.lifecycle(t, actor, action, current)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.lifecycle(t, actor, action, current)
			checkCode(t, err, "VERSION_CONFLICT")
			_, err = f.lifecycle(t, actor, action, success.Ticket)
			checkCode(t, err, "INVALID_STATE")
			if !reflect.DeepEqual(f.detail(t, current.ID).Ticket, success.Ticket) {
				t.Fatal("retry altered successful state")
			}
			f.actionCounts(t, int(success.Ticket.Version))
		})
	}
	f := newFixture(t)
	resolved := f.resolve(t, f.processing(t, "REQ-closed-immutable"), "修复完成")
	closed, err := f.service.Close(context.Background(), 1, actionRequest(resolved))
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"claim", "start", "resolve", "reopen", "close"} {
		var err error
		switch action {
		case "claim":
			_, err = f.service.Claim(context.Background(), 3, actionRequest(closed.Ticket))
		case "start":
			_, err = f.service.Start(context.Background(), 3, actionRequest(closed.Ticket))
		case "resolve":
			_, err = f.lifecycle(t, 3, action, closed.Ticket)
		default:
			_, err = f.lifecycle(t, 1, action, closed.Ticket)
		}
		checkCode(t, err, "INVALID_STATE")
	}
	f.actionCounts(t, 5)
}

func TestLifecycleHistoryFailureRollback(t *testing.T) {
	for _, action := range []string{"resolve", "reopen", "close"} {
		t.Run(action, func(t *testing.T) {
			f := newFixture(t)
			current := f.processing(t, "REQ-lifecycle-rollback")
			actor := int64(3)
			if action != "resolve" {
				current = f.resolve(t, current, "修复完成")
				actor = 1
			}
			before := f.detail(t, current.ID)
			f.exec(t, `CREATE TRIGGER fail_lifecycle_history BEFORE INSERT ON ticket_histories FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected lifecycle history failure'`)
			_, err := f.lifecycle(t, actor, action, current)
			if err == nil {
				t.Fatal("history failure returned success")
			}
			if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
				t.Fatal("state/version/time/count/note not rolled back with history")
			}
			f.actionCounts(t, int(current.Version))
			f.exec(t, "DROP TRIGGER fail_lifecycle_history")
			if _, err := f.lifecycle(t, actor, action, current); err != nil {
				t.Fatal("same version cannot retry after rollback", err)
			}
			f.actionCounts(t, int(current.Version)+1)
		})
	}
}

func TestReopenCloseCompetition(t *testing.T) {
	f := newFixture(t)
	current := f.resolve(t, f.processing(t, "REQ-reopen-close-race"), "已修复")
	start := make(chan struct{})
	type result struct {
		action string
		value  ticket.ActionResult
		err    error
	}
	results := make(chan result, 2)
	for _, action := range []string{"reopen", "close"} {
		go func(action string) {
			<-start
			value, err := f.lifecycle(t, 1, action, current)
			results <- result{action, value, err}
		}(action)
	}
	close(start)
	successes, conflicts := 0, 0
	var winner result
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err == nil {
			successes++
			winner = got
		} else {
			conflicts++
			checkCode(t, got.err, "VERSION_CONFLICT")
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("race success=%d conflicts=%d", successes, conflicts)
	}
	detail := f.detail(t, current.ID)
	expectedAction := ticket.ActionReopened
	if winner.action == "close" {
		expectedAction = ticket.ActionClosed
	}
	if !reflect.DeepEqual(detail.Ticket, winner.value.Ticket) || detail.Ticket.Version != 5 || len(detail.History) != 5 || detail.History[4].Action != expectedAction {
		t.Fatal("race lost or duplicated effect")
	}
	if winner.action == "reopen" && (detail.Ticket.ReopenCount != 1 || detail.Ticket.ClosedAt != nil) {
		t.Fatal("reopen winner has close effect")
	}
	if winner.action == "close" && (detail.Ticket.ReopenCount != 0 || detail.Ticket.ClosedAt == nil) {
		t.Fatal("close winner has reopen effect")
	}
	f.actionCounts(t, 5)
}

func TestConcurrentResolution(t *testing.T) {
	f := newFixture(t)
	current := f.processing(t, "REQ-resolve-race")
	const n = 12
	start := make(chan struct{})
	results := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := f.service.Resolve(context.Background(), 3, resolution(current, fmt.Sprintf("解决说明 %d", i)))
			results <- err
		}(i)
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
		t.Fatal("concurrent resolves succeeded more than once")
	}
	detail := f.detail(t, current.ID)
	if detail.Ticket.Version != 4 || detail.Ticket.FirstResolvedAt == nil || detail.Ticket.ResolutionNote == nil {
		t.Fatal("resolution race state incorrect")
	}
	f.actionCounts(t, 4)
}

type lifecycleHTTPResult struct {
	status int
	body   []byte
	err    error
}

func httpLifecycle(server *httptest.Server, method, path, token string, input any) lifecycleHTTPResult {
	var payload []byte
	if input != nil {
		var err error
		payload, err = json.Marshal(input)
		if err != nil {
			return lifecycleHTTPResult{err: err}
		}
	}
	r, err := http.NewRequest(method, server.URL+path, bytes.NewReader(payload))
	if err != nil {
		return lifecycleHTTPResult{err: err}
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-User-ID", "1")
	r.Header.Set("X-Role", "admin")
	r.Header.Set("X-Team-ID", "10")
	response, err := server.Client().Do(r)
	if err != nil {
		return lifecycleHTTPResult{err: err}
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return lifecycleHTTPResult{response.StatusCode, data, err}
}

func TestLifecycleHTTPFlows(t *testing.T) {
	for _, withReopen := range []bool{false, true} {
		t.Run(fmt.Sprintf("reopen_%t", withReopen), func(t *testing.T) {
			f := newFixture(t)
			server := httptest.NewServer(httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil))))
			defer server.Close()
			post := func(path, token string, input any) ticket.Ticket {
				t.Helper()
				response := httpLifecycle(server, http.MethodPost, "/api/v1/tickets/"+path, token, input)
				if response.err != nil || response.status != 200 {
					t.Fatalf("HTTP %s status=%d body=%s error=%v", path, response.status, response.body, response.err)
				}
				var result ticket.ActionResult
				if err := json.Unmarshal(response.body, &result); err != nil {
					t.Fatal(err)
				}
				return result.Ticket
			}
			createdHTTP := httpLifecycle(server, http.MethodPost, "/api/v1/tickets", "local-alice", request("REQ-http-lifecycle"))
			if createdHTTP.err != nil || createdHTTP.status != 201 {
				t.Fatalf("create: %+v", createdHTTP)
			}
			var created ticket.CreateResult
			if err := json.Unmarshal(createdHTTP.body, &created); err != nil {
				t.Fatal(err)
			}
			claimed := post("claim", "local-it-agent", actionRequest(created.Ticket))
			processing := post("start", "local-it-agent", actionRequest(claimed))
			note1, note2 := "  解决说明原文\n主网恢复，中文和 😀。  ", "第二次解决：备用网络已恢复\n测试完成。"
			first := post("resolve", "local-it-agent", resolution(processing, note1))
			if first.ResolutionNote == nil || *first.ResolutionNote != note1 || first.FirstResolvedAt == nil {
				t.Fatal("HTTP resolution lost content/time")
			}
			current := first
			if withReopen {
				current = post("reopen", "local-alice", actionRequest(current))
				if current.Status != ticket.StatusInProgress || current.ReopenCount != 1 || *current.AssigneeID != 3 {
					t.Fatal("HTTP reopen wrong")
				}
				current = post("resolve", "local-it-agent", resolution(current, note2))
				if !current.FirstResolvedAt.Equal(*first.FirstResolvedAt) || *current.ResolutionNote != note2 {
					t.Fatal("HTTP second resolve overwrote first time")
				}
			}
			closed := post("close", "local-alice", actionRequest(current))
			wantVersion := int64(5)
			if withReopen {
				wantVersion = 7
			}
			finalHTTP := httpLifecycle(server, http.MethodGet, fmt.Sprintf("/api/v1/tickets/%d", closed.ID), "local-alice", nil)
			if finalHTTP.err != nil || finalHTTP.status != 200 {
				t.Fatalf("detail: %+v", finalHTTP)
			}
			var detail ticket.Detail
			if err := json.Unmarshal(finalHTTP.body, &detail); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(detail.Ticket, closed) || detail.Ticket.Version != wantVersion || detail.Ticket.Status != ticket.StatusClosed || len(detail.History) != int(wantVersion) || detail.Assignee == nil || detail.Assignee.ID != 3 || !closed.FirstResolvedAt.Equal(*first.FirstResolvedAt) || closed.ClosedAt == nil || !closed.ClosedAt.Equal(closed.UpdatedAt) || !closed.ClosedAt.After(*first.FirstResolvedAt) {
				t.Fatal("final HTTP lifecycle detail incorrect")
			}
			for i, h := range detail.History {
				if h.Version != int64(i+1) || i > 0 && !reflect.DeepEqual(h.Before, &detail.History[i-1].After) {
					t.Fatal("HTTP history is not continuous")
				}
			}
			if *detail.History[3].After.ResolutionNote != note1 || withReopen && *detail.History[5].After.ResolutionNote != note2 {
				t.Fatal("HTTP historical notes missing")
			}
			// An independent SQL read must agree with the HTTP source, not a projected response.
			var status, note string
			var version, reopens int64
			var firstSQL, closedSQL time.Time
			if err := f.db.QueryRow("SELECT status,version,reopen_count,first_resolved_at,closed_at,resolution_note FROM tickets WHERE id=?", closed.ID).Scan(&status, &version, &reopens, &firstSQL, &closedSQL, &note); err != nil {
				t.Fatal(err)
			}
			if status != ticket.StatusClosed || version != wantVersion || reopens != int64(boolInt(withReopen)) || !firstSQL.Equal(*first.FirstResolvedAt) || !closedSQL.Equal(*closed.ClosedAt) || note != *closed.ResolutionNote {
				t.Fatal("MySQL disagrees with HTTP")
			}
			f.actionCounts(t, int(wantVersion))
		})
	}
}

func TestLifecycleHTTPErrorsAndRace(t *testing.T) {
	f := newFixture(t)
	processing := f.processing(t, "REQ-http-race-errors")
	server := httptest.NewServer(httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	assertError := func(action, token string, input any, status int, code string) {
		t.Helper()
		response := httpLifecycle(server, http.MethodPost, "/api/v1/tickets/"+action, token, input)
		if response.err != nil || response.status != status || !bytes.Contains(response.body, []byte(code)) {
			t.Fatalf("%s: status=%d body=%s error=%v", action, response.status, response.body, response.err)
		}
		if bytes.Contains(response.body, []byte(processing.Title)) {
			t.Fatal("error disclosed title")
		}
	}
	for _, token := range []string{"local-alice", "local-it-agent-2", "local-supervisor", "local-admin", "local-device-agent"} {
		assertError("resolve", token, resolution(processing, "已修复"), 403, "ACTION_FORBIDDEN")
	}
	for _, input := range []any{
		map[string]any{"ticket_id": processing.ID, "expected_version": 3},
		map[string]any{"ticket_id": processing.ID, "expected_version": 3, "resolution_note": " \n\t"},
		map[string]any{"ticket_id": processing.ID, "expected_version": 3, "resolution_note": strings.Repeat("文", 5001)},
		map[string]any{"ticket_id": processing.ID, "expected_version": 3, "resolution_note": "修复", "assignee_id": 3},
	} {
		assertError("resolve", "local-it-agent", input, 400, "INVALID_ARGUMENT")
	}
	for _, action := range []string{"resolve", "reopen", "close"} {
		input := any(actionRequest(processing))
		if action == "resolve" {
			input = resolution(processing, "已修复")
		}
		assertError(action, "", input, 401, "UNAUTHENTICATED")
		assertError(action, "local-disabled", input, 401, "UNAUTHENTICATED")
		assertError(action, "local-alice", map[string]any{"ticket_id": processing.ID, "expected_version": 3, "user_id": 1}, 400, "INVALID_ARGUMENT")
		missing := map[string]any{"ticket_id": 99999999, "expected_version": 1}
		if action == "resolve" {
			missing["resolution_note"] = "修复"
		}
		assertError(action, "local-alice", missing, 404, "TICKET_NOT_FOUND")
	}
	resolvedHTTP := httpLifecycle(server, http.MethodPost, "/api/v1/tickets/resolve", "local-it-agent", resolution(processing, "修复主网"))
	if resolvedHTTP.err != nil || resolvedHTTP.status != 200 {
		t.Fatal("HTTP resolve failed", resolvedHTTP)
	}
	var resolved ticket.ActionResult
	if err := json.Unmarshal(resolvedHTTP.body, &resolved); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"reopen", "close"} {
		for _, token := range []string{"local-bob", "local-it-agent", "local-it-agent-2", "local-supervisor", "local-admin"} {
			assertError(action, token, actionRequest(resolved.Ticket), 403, "ACTION_FORBIDDEN")
		}
	}
	assertError("resolve", "local-it-agent", resolution(processing, "原版本重试"), 409, "VERSION_CONFLICT")
	barrier := make(chan struct{})
	results := make(chan lifecycleHTTPResult, 2)
	for _, action := range []string{"reopen", "close"} {
		go func(action string) {
			<-barrier
			results <- httpLifecycle(server, http.MethodPost, "/api/v1/tickets/"+action, "local-alice", actionRequest(resolved.Ticket))
		}(action)
	}
	close(barrier)
	success, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.status == 200 {
			success++
		} else if got.status == 409 && bytes.Contains(got.body, []byte("VERSION_CONFLICT")) {
			conflicts++
		} else {
			t.Fatalf("race=%d %s", got.status, got.body)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatal("reopen/close HTTP race did not have a single winner")
	}
	f.actionCounts(t, 5)
}

func TestLifecycleLostHTTPResponse(t *testing.T) {
	for _, action := range []string{"resolve", "reopen", "close"} {
		t.Run(action, func(t *testing.T) {
			f := newFixture(t)
			current := f.processing(t, "REQ-lost-lifecycle-response")
			token := "local-it-agent"
			input := any(resolution(current, "已修复"))
			if action != "resolve" {
				current = f.resolve(t, current, "已修复")
				token = "local-alice"
				input = actionRequest(current)
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
					t.Errorf("before lost response: %d %s", recorder.Code, recorder.Body)
				}
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				connection.Close()
			}))
			defer server.Close()
			first := httpLifecycle(server, http.MethodPost, "/api/v1/tickets/"+action, token, input)
			if first.err == nil {
				t.Fatal("response loss not observed")
			}
			retry := httpLifecycle(server, http.MethodPost, "/api/v1/tickets/"+action, token, input)
			if retry.err != nil || retry.status != 409 || !bytes.Contains(retry.body, []byte("VERSION_CONFLICT")) {
				t.Fatalf("retry duplicated action: %+v", retry)
			}
			f.actionCounts(t, int(current.Version)+1)
		})
	}
}

func TestResolutionMigrationPreservesExistingData(t *testing.T) {
	f := newFixture(t)
	current := f.processing(t, "REQ-migration-existing")
	before := f.detail(t, current.ID)
	// Reproduce the previous schema in this isolated test database before applying the real migration.
	f.exec(t, "ALTER TABLE tickets DROP CHECK ck_tickets_resolution_note, DROP COLUMN resolution_note")
	var status string
	var version int64
	if err := f.db.QueryRow("SELECT status,version FROM tickets WHERE id=?", current.ID).Scan(&status, &version); err != nil {
		t.Fatal(err)
	}
	if status != ticket.StatusInProgress || version != 3 {
		t.Fatal("test baseline changed")
	}
	if _, err := f.db.Exec(assets.ResolutionNoteMigration); err != nil {
		t.Fatal("migration failed", err)
	}
	if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
		t.Fatal("migration rewrote existing ticket/history")
	}
	resolved := f.resolve(t, current, "迁移后解决说明")
	if resolved.Version != 4 || *resolved.ResolutionNote != "迁移后解决说明" {
		t.Fatal("migrated schema cannot resolve")
	}
	f.actionCounts(t, 4)
}

func TestMaximumResolutionPersistence(t *testing.T) {
	f := newFixture(t)
	current := f.processing(t, "REQ-resolution-maximum")
	note := strings.Repeat("😀", 5000)
	resolved := f.resolve(t, current, note)
	detail := f.detail(t, current.ID)
	if *detail.Ticket.ResolutionNote != note || *detail.History[3].After.ResolutionNote != note || !detail.Ticket.FirstResolvedAt.Equal(*resolved.FirstResolvedAt) {
		t.Fatal("maximum resolution text lost")
	}
	bad := resolution(current, "")
	if _, err := f.service.Resolve(context.Background(), 3, bad); err == nil {
		t.Fatal("empty resolution accepted")
	}
	f.actionCounts(t, 4)
}
