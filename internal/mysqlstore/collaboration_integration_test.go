//go:build integration

package mysqlstore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	assets "ticketpulse/db"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/ticket"
)

func (f *fixture) collaboration(t *testing.T, actor int64, action string, current ticket.Ticket) (ticket.ActionResult, error) {
	t.Helper()
	base := actionRequest(current)
	switch action {
	case "transfer":
		return f.service.Transfer(context.Background(), actor, ticket.TransferRequest{ActionRequest: base, AssigneeID: 11, Reason: "同团队交接\n保留原文"})
	case "transfer-team":
		return f.service.TransferTeam(context.Background(), actor, ticket.TeamTransferRequest{ActionRequest: base, TeamID: 20, Reason: "设备团队负责后续排查"})
	case "management-reopen":
		return f.service.ManagementReopen(context.Background(), actor, ticket.ManagementReopenRequest{ActionRequest: base, TeamID: 20, Reason: "原支持团队停用，重开并恢复处理"})
	case "manual-close":
		return f.service.ManualClose(context.Background(), actor, ticket.ReasonRequest{ActionRequest: base, Reason: "主管核验处理结果后人工关闭"})
	case "comments":
		return f.service.Comment(context.Background(), actor, ticket.CommentRequest{ActionRequest: base, Content: "  处理进展\n中文与 😀。  "})
	default:
		t.Fatalf("unknown collaboration action %s", action)
		return ticket.ActionResult{}, nil
	}
}

func assertTimeline(t *testing.T, detail ticket.Detail) {
	t.Helper()
	if len(detail.History) != int(detail.Ticket.Version) {
		t.Fatalf("history=%d version=%d", len(detail.History), detail.Ticket.Version)
	}
	for i, h := range detail.History {
		if h.Version != int64(i+1) || h.After.Version != h.Version || h.ResponsibleTeamID != h.After.CurrentTeamID {
			t.Fatalf("incorrect history version/team: %+v", h)
		}
		if i > 0 && (h.Before == nil || !reflect.DeepEqual(*h.Before, detail.History[i-1].After)) {
			t.Fatalf("discontinuous history: %+v", h)
		}
	}
	if !reflect.DeepEqual(detail.History[len(detail.History)-1].After, detail.Ticket.State()) {
		t.Fatal("history does not match current ticket")
	}
}

func TestSameTeamTransferAndAssignment(t *testing.T) {
	for _, status := range []string{ticket.StatusPending, ticket.StatusInProgress} {
		t.Run(status, func(t *testing.T) {
			f := newFixture(t)
			current := f.claim(t, 3, f.create(t, 1, request("REQ-same-transfer")).Ticket)
			if status == ticket.StatusInProgress {
				started, err := f.service.Start(context.Background(), 3, actionRequest(current))
				if err != nil {
					t.Fatal(err)
				}
				current = started.Ticket
			}
			changed, err := f.collaboration(t, 3, "transfer", current)
			if err != nil || changed.Ticket.Version != current.Version+1 || *changed.Ticket.AssigneeID != 11 || changed.Ticket.Status != status || changed.Ticket.CurrentTeamID != 10 || !changed.Ticket.UpdatedAt.After(current.UpdatedAt) {
				t.Fatal("transfer state incorrect", err)
			}
			detail := f.detail(t, current.ID)
			h := detail.History[len(detail.History)-1]
			if h.Action != ticket.ActionAssigneeTransferred || h.OperatorID != 3 || *h.Before.AssigneeID != 3 || *h.After.AssigneeID != 11 || h.Reason == nil || *h.Reason != "同团队交接\n保留原文" {
				t.Fatal("transfer history incorrect")
			}
			_, err = f.service.Resolve(context.Background(), 3, resolution(changed.Ticket, "旧负责人无权解决"))
			checkCode(t, err, "ACTION_FORBIDDEN")
			_, err = f.service.Start(context.Background(), 3, actionRequest(changed.Ticket))
			checkCode(t, err, "ACTION_FORBIDDEN")
			if status == ticket.StatusPending {
				started, err := f.service.Start(context.Background(), 11, actionRequest(changed.Ticket))
				if err != nil {
					t.Fatal(err)
				}
				changed = started
			}
			if _, err := f.service.Resolve(context.Background(), 11, resolution(changed.Ticket, "新负责人解决")); err != nil {
				t.Fatal(err)
			}
			assertTimeline(t, f.detail(t, current.ID))
		})
	}
	for _, manager := range []int64{5, 6} {
		t.Run(fmt.Sprintf("manager_%d_assign_and_restore", manager), func(t *testing.T) {
			f := newFixture(t)
			current := f.create(t, 1, request("REQ-manager-assign")).Ticket
			_, err := f.collaboration(t, 3, "transfer", current)
			checkCode(t, err, "ACTION_FORBIDDEN")
			assigned, err := f.collaboration(t, manager, "transfer", current)
			if err != nil || assigned.Ticket.Status != ticket.StatusPending || *assigned.Ticket.AssigneeID != 11 {
				t.Fatal("manager cannot assign pending unowned", err)
			}
			started, err := f.service.Start(context.Background(), 11, actionRequest(assigned.Ticket))
			if err != nil {
				t.Fatal(err)
			}
			f.exec(t, "UPDATE users SET active=FALSE WHERE id=11")
			restored, err := f.service.Transfer(context.Background(), manager, ticket.TransferRequest{ActionRequest: actionRequest(started.Ticket), AssigneeID: 3})
			if err != nil || restored.Ticket.Status != ticket.StatusInProgress || *restored.Ticket.AssigneeID != 3 {
				t.Fatal("manager cannot replace invalid owner", err)
			}
			assertTimeline(t, f.detail(t, current.ID))
		})
	}
}

func TestCrossTeamTransferCurrentAuthorization(t *testing.T) {
	for _, source := range []string{ticket.StatusPending, ticket.StatusInProgress} {
		t.Run(source, func(t *testing.T) {
			f := newFixture(t)
			current := f.create(t, 1, request("REQ-cross-transfer")).Ticket
			if source == ticket.StatusInProgress {
				current = f.processing(t, "REQ-cross-processing")
			}
			changed, err := f.collaboration(t, 5, "transfer-team", current)
			if err != nil || changed.Ticket.Status != ticket.StatusPending || changed.Ticket.AssigneeID != nil || changed.Ticket.CurrentTeamID != 20 || changed.Ticket.InitialTeamID != 10 || changed.Ticket.Version != current.Version+1 {
				t.Fatal("cross-team state incorrect", err)
			}
			for _, old := range []int64{3, 11} {
				_, err := f.service.Detail(context.Background(), old, current.ID)
				checkCode(t, err, "FORBIDDEN")
				_, err = f.service.Claim(context.Background(), old, actionRequest(changed.Ticket))
				checkCode(t, err, "ACTION_FORBIDDEN")
				_, err = f.service.Comment(context.Background(), old, ticket.CommentRequest{ActionRequest: actionRequest(changed.Ticket), Content: "已失权评论"})
				checkCode(t, err, "ACTION_FORBIDDEN")
			}
			for _, allowed := range []int64{1, 4, 5, 6} {
				if _, err := f.service.Detail(context.Background(), allowed, current.ID); err != nil {
					t.Fatal("current team/creator/manager cannot view", err)
				}
			}
			detail := f.detail(t, current.ID)
			h := detail.History[len(detail.History)-1]
			if h.Action != ticket.ActionTeamTransferred || h.Before.CurrentTeamID != 10 || h.After.CurrentTeamID != 20 || h.ResponsibleTeamID != 20 || h.Reason == nil || h.OperatorID != 5 {
				t.Fatal("cross-team history lacks original/new team")
			}
			claimed := f.claim(t, 4, changed.Ticket)
			if _, err := f.service.Start(context.Background(), 4, actionRequest(claimed)); err != nil {
				t.Fatal("new team cannot process", err)
			}
			assertTimeline(t, f.detail(t, current.ID))
		})
	}
}

func TestCollaborationPermissionAndTargets(t *testing.T) {
	f := newFixture(t)
	processing := f.processing(t, "REQ-permission-target")
	for _, actor := range []int64{0, 1, 2, 4, 7, 8, 9, 11} {
		for _, action := range []string{"transfer", "transfer-team"} {
			_, err := f.collaboration(t, actor, action, processing)
			code := "ACTION_FORBIDDEN"
			if actor == 0 || actor == 8 {
				code = "UNAUTHENTICATED"
			}
			if actor == 9 {
				code = "TICKET_NOT_FOUND"
			}
			checkCode(t, err, code)
		}
	}
	before := f.detail(t, processing.ID)
	for _, action := range []string{"management-reopen", "manual-close"} {
		_, err := f.collaboration(t, 5, action, processing)
		checkCode(t, err, "INVALID_STATE")
	}
	for _, target := range []int64{1, 4, 5, 6, 7, 8, 9, 12345} {
		_, err := f.service.Transfer(context.Background(), 5, ticket.TransferRequest{ActionRequest: actionRequest(processing), AssigneeID: target})
		checkCode(t, err, "INVALID_ASSIGNEE")
	}
	_, err := f.service.Transfer(context.Background(), 3, ticket.TransferRequest{ActionRequest: actionRequest(processing), AssigneeID: 3})
	checkCode(t, err, "ALREADY_ASSIGNED")
	for _, target := range []int64{99, 200, 12345} {
		_, err := f.service.TransferTeam(context.Background(), 6, ticket.TeamTransferRequest{ActionRequest: actionRequest(processing), TeamID: target, Reason: "无效目标"})
		checkCode(t, err, "INVALID_TEAM")
	}
	_, err = f.service.TransferTeam(context.Background(), 5, ticket.TeamTransferRequest{ActionRequest: actionRequest(processing), TeamID: 10, Reason: "同一团队"})
	checkCode(t, err, "INVALID_ARGUMENT")
	if !reflect.DeepEqual(before, f.detail(t, processing.ID)) {
		t.Fatal("rejected transfer changed ticket")
	}
	stale := processing
	stale.Version--
	for _, action := range []string{"transfer", "transfer-team", "comments"} {
		_, err := f.collaboration(t, 5, action, stale)
		checkCode(t, err, "VERSION_CONFLICT")
	}
	awaiting := f.resolve(t, processing, "完成排查")
	for _, action := range []string{"transfer", "transfer-team"} {
		_, err := f.collaboration(t, 5, action, awaiting)
		checkCode(t, err, "INVALID_STATE")
	}
	for _, actor := range []int64{1, 3, 4, 7, 11} {
		for _, action := range []string{"management-reopen", "manual-close"} {
			_, err := f.collaboration(t, actor, action, awaiting)
			checkCode(t, err, "ACTION_FORBIDDEN")
		}
	}
	stale = awaiting
	stale.Version--
	for _, action := range []string{"management-reopen", "manual-close"} {
		_, err := f.collaboration(t, 5, action, stale)
		checkCode(t, err, "VERSION_CONFLICT")
		_, err = f.collaboration(t, 5, action, processing)
		checkCode(t, err, "VERSION_CONFLICT")
	}
	closed, err := f.collaboration(t, 6, "manual-close", awaiting)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"transfer", "transfer-team", "management-reopen", "manual-close"} {
		_, err := f.collaboration(t, 5, action, closed.Ticket)
		checkCode(t, err, "INVALID_STATE")
	}
	assertTimeline(t, f.detail(t, processing.ID))
}

func TestRequeueAndManagementRecovery(t *testing.T) {
	f := newFixture(t)
	server := collaborationServer(f)
	defer server.Close()
	first := f.resolve(t, f.processing(t, "REQ-owner-requeue"), "第一轮解决说明")
	f.exec(t, "UPDATE users SET active=FALSE WHERE id=3")
	reopened := ticket.ActionResult{Ticket: httpTicket(t, server, "reopen", "local-alice", actionRequest(first))}
	if reopened.Ticket.Status != ticket.StatusPending || reopened.Ticket.AssigneeID != nil || reopened.Ticket.ReopenCount != 1 || reopened.Ticket.Version != 5 {
		t.Fatal("disabled owner did not requeue")
	}
	claimed := f.claim(t, 11, reopened.Ticket)
	started, err := f.service.Start(context.Background(), 11, actionRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.service.Resolve(context.Background(), 11, resolution(started.Ticket, "第二轮解决说明"))
	if err != nil || !second.Ticket.FirstResolvedAt.Equal(*first.FirstResolvedAt) {
		t.Fatal("requeue lost first resolution time", err)
	}
	f.exec(t, "UPDATE teams SET active=FALSE WHERE id=10")
	before := f.detail(t, first.ID)
	_, err = f.service.Reopen(context.Background(), 1, actionRequest(second.Ticket))
	checkCode(t, err, "TEAM_UNAVAILABLE")
	if !reflect.DeepEqual(before, f.detail(t, first.ID)) {
		t.Fatal("disabled team allowed ordinary reopen")
	}
	_, err = f.service.ManagementReopen(context.Background(), 5, ticket.ManagementReopenRequest{ActionRequest: actionRequest(second.Ticket), Reason: "未选择有效团队"})
	checkCode(t, err, "TEAM_UNAVAILABLE")
	_, err = f.service.ManagementReopen(context.Background(), 5, ticket.ManagementReopenRequest{ActionRequest: actionRequest(second.Ticket), TeamID: 99, Reason: "无效目标团队"})
	checkCode(t, err, "INVALID_TEAM")
	recovered := ticket.ActionResult{Ticket: httpTicket(t, server, "management-reopen", "local-supervisor", ticket.ManagementReopenRequest{ActionRequest: actionRequest(second.Ticket), TeamID: 20, Reason: "原支持团队停用，重开并恢复处理"})}
	if recovered.Ticket.Status != ticket.StatusPending || recovered.Ticket.AssigneeID != nil || recovered.Ticket.CurrentTeamID != 20 || recovered.Ticket.InitialTeamID != 10 || recovered.Ticket.ReopenCount != 2 || recovered.Ticket.Version != 9 || !recovered.Ticket.FirstResolvedAt.Equal(*first.FirstResolvedAt) {
		t.Fatal("management recovery wrong")
	}
	claimed = f.claim(t, 4, recovered.Ticket)
	started, err = f.service.Start(context.Background(), 4, actionRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	final, err := f.service.Resolve(context.Background(), 4, resolution(started.Ticket, "新团队完成修复"))
	if err != nil {
		t.Fatal(err)
	}
	closed := ticket.ActionResult{Ticket: httpTicket(t, server, "manual-close", "local-admin", ticket.ReasonRequest{ActionRequest: actionRequest(final.Ticket), Reason: "主管核验处理结果后人工关闭"})}
	if closed.Ticket.Status != ticket.StatusClosed || closed.Ticket.ClosedAt == nil || !closed.Ticket.ClosedAt.Equal(closed.Ticket.UpdatedAt) || !closed.Ticket.FirstResolvedAt.Equal(*first.FirstResolvedAt) {
		t.Fatal("manual close recovery wrong")
	}
	detail := f.detail(t, first.ID)
	assertTimeline(t, detail)
	if detail.History[4].Action != ticket.ActionReopenedRequeued || detail.History[8].Action != ticket.ActionManagementReopened || detail.History[8].Before.CurrentTeamID != 10 || detail.History[8].After.CurrentTeamID != 20 || detail.History[12].Action != ticket.ActionManuallyClosed || detail.History[12].Reason == nil {
		t.Fatal("exception audit missing")
	}
}

func TestManagementReopenValidOwner(t *testing.T) {
	for _, manager := range []int64{5, 6} {
		t.Run(fmt.Sprintf("manager_%d", manager), func(t *testing.T) {
			f := newFixture(t)
			awaiting := f.resolve(t, f.processing(t, "REQ-management-normal-reopen"), "首次说明")
			result, err := f.service.ManagementReopen(context.Background(), manager, ticket.ManagementReopenRequest{ActionRequest: actionRequest(awaiting), Reason: "主管确认问题仍存在"})
			if err != nil || result.Ticket.Status != ticket.StatusInProgress || *result.Ticket.AssigneeID != 3 || result.Ticket.CurrentTeamID != 10 || result.Ticket.ReopenCount != 1 {
				t.Fatal("manager normal exception wrong", err)
			}
			assertTimeline(t, f.detail(t, awaiting.ID))
		})
	}
}

func TestCommentsCurrentPermissionsAndSnapshots(t *testing.T) {
	f := newFixture(t)
	current := f.processing(t, "REQ-comment-permission")
	for _, actor := range []int64{1, 3, 11, 5, 6} {
		before := current
		result, err := f.collaboration(t, actor, "comments", current)
		if err != nil || result.Comment == nil || result.Comment.AuthorID != actor || result.Comment.Version != current.Version+1 || result.Comment.Content != "  处理进展\n中文与 😀。  " || !result.Comment.CreatedAt.Equal(result.Ticket.UpdatedAt) || result.Ticket.Status != before.Status || result.Ticket.AssigneeID == nil || result.Ticket.Version != before.Version+1 {
			t.Fatal("comment commit wrong", err)
		}
		current = result.Ticket
		_, err = f.collaboration(t, actor, "comments", before)
		checkCode(t, err, "VERSION_CONFLICT")
	}
	before := f.detail(t, current.ID)
	for _, actor := range []int64{2, 4, 7, 8, 9} {
		_, err := f.collaboration(t, actor, "comments", current)
		code := "ACTION_FORBIDDEN"
		if actor == 8 {
			code = "UNAUTHENTICATED"
		}
		if actor == 9 {
			code = "TICKET_NOT_FOUND"
		}
		checkCode(t, err, code)
	}
	if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
		t.Fatal("unauthorized comment altered detail")
	}
	for _, c := range before.Comments {
		h := before.History[c.Version-1]
		if h.Action != ticket.ActionCommented || h.CommentID == nil || *h.CommentID != c.ID || h.OperatorID != c.AuthorID || !h.OccurredAt.Equal(c.CreatedAt) {
			t.Fatal("comment/history relationship wrong")
		}
	}
	// Ties use the immutable comment id, independently of action timestamps.
	f.exec(t, "UPDATE ticket_comments SET created_at='2026-11-08 00:00:00.000000' WHERE ticket_id=?", current.ID)
	got := f.detail(t, current.ID)
	for i := 1; i < len(got.Comments); i++ {
		if got.Comments[i-1].ID >= got.Comments[i].ID {
			t.Fatal("same-time comment order unstable")
		}
	}
	changed, err := f.collaboration(t, 5, "transfer-team", current)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.service.Detail(context.Background(), 3, current.ID)
	checkCode(t, err, "FORBIDDEN")
	visible, err := f.service.Detail(context.Background(), 4, current.ID)
	if err != nil || len(visible.Comments) != 5 {
		t.Fatal("new team cannot read authorized prior comments", err)
	}
	_, err = f.collaboration(t, 3, "comments", changed.Ticket)
	checkCode(t, err, "ACTION_FORBIDDEN")
}

func TestMaximumCommentPersistenceAndClosedComments(t *testing.T) {
	f := newFixture(t)
	awaiting := f.resolve(t, f.processing(t, "REQ-comment-maximum"), "确认修复")
	closed, err := f.service.Close(context.Background(), 1, actionRequest(awaiting))
	if err != nil {
		t.Fatal(err)
	}
	content := strings.Repeat("😀", 5000)
	commented, err := f.service.Comment(context.Background(), 1, ticket.CommentRequest{ActionRequest: actionRequest(closed.Ticket), Content: content})
	if err != nil || commented.Ticket.Status != ticket.StatusClosed || !commented.Ticket.ClosedAt.Equal(*closed.Ticket.ClosedAt) || !commented.Ticket.FirstResolvedAt.Equal(*awaiting.FirstResolvedAt) {
		t.Fatal("closed comment changed lifecycle", err)
	}
	detail := f.detail(t, awaiting.ID)
	if len(detail.Comments) != 1 || detail.Comments[0].Content != content {
		t.Fatal("maximum comment content lost")
	}
	assertTimeline(t, detail)
}

func TestCollaborationHistoryAndWriteRollback(t *testing.T) {
	for _, action := range []string{"transfer", "transfer-team", "management-reopen", "manual-close", "comments", "reopen-requeue"} {
		t.Run(action, func(t *testing.T) {
			f := newFixture(t)
			current := f.processing(t, "REQ-collaboration-rollback")
			actor := int64(5)
			if action == "management-reopen" || action == "manual-close" || action == "reopen-requeue" {
				current = f.resolve(t, current, "需要确认")
			}
			if action == "reopen-requeue" {
				f.exec(t, "UPDATE users SET active=FALSE WHERE id=3")
				actor = 1
			}
			before := f.detail(t, current.ID)
			invoke := func() (ticket.ActionResult, error) {
				if action == "reopen-requeue" {
					return f.service.Reopen(context.Background(), actor, actionRequest(current))
				}
				return f.collaboration(t, actor, action, current)
			}
			f.exec(t, `CREATE TRIGGER fail_collaboration_history BEFORE INSERT ON ticket_histories FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected history failure'`)
			if _, err := invoke(); err == nil {
				t.Fatal("history failure returned success")
			}
			if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
				t.Fatal("history failure partially committed")
			}
			var comments int
			if err := f.db.QueryRow("SELECT COUNT(*) FROM ticket_comments").Scan(&comments); err != nil || comments != 0 {
				t.Fatal("failed comment left a row", err)
			}
			f.exec(t, "DROP TRIGGER fail_collaboration_history")
			f.exec(t, `CREATE TRIGGER fail_ticket_write BEFORE UPDATE ON tickets FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected update failure'`)
			if _, err := invoke(); err == nil {
				t.Fatal("ticket update failure returned success")
			}
			if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
				t.Fatal("write failure partially committed")
			}
			f.exec(t, "DROP TRIGGER fail_ticket_write")
			committed, err := invoke()
			if err != nil || committed.Ticket.Version != current.Version+1 {
				t.Fatal("retry after rollback failed", err)
			}
			assertTimeline(t, f.detail(t, current.ID))
		})
	}
	f := newFixture(t)
	current := f.processing(t, "REQ-comment-row-failure")
	f.exec(t, `CREATE TRIGGER fail_comment_write BEFORE INSERT ON ticket_comments FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected comment failure'`)
	before := f.detail(t, current.ID)
	server := collaborationServer(f)
	defer server.Close()
	failed := httpLifecycle(server, http.MethodPost, "/api/v1/tickets/comments", "local-alice", ticket.CommentRequest{ActionRequest: actionRequest(current), Content: "失败时不应保存的评论"})
	if failed.err != nil || failed.status != 500 || !strings.Contains(string(failed.body), "INTERNAL_ERROR") || strings.Contains(string(failed.body), "injected comment failure") {
		t.Fatal("comment database failure HTTP contract", failed)
	}
	if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
		t.Fatal("comment insertion failure changed ticket/history")
	}
}

func TestCollaborationVersionCompetition(t *testing.T) {
	for _, actions := range [][]string{{"transfer", "transfer-team"}, {"transfer-team", "comments"}, {"management-reopen", "manual-close"}} {
		t.Run(strings.Join(actions, "_vs_"), func(t *testing.T) {
			f := newFixture(t)
			current := f.processing(t, "REQ-collaboration-race")
			if actions[0] == "management-reopen" {
				current = f.resolve(t, current, "竞争中的待确认工单")
			}
			var group sync.WaitGroup
			barrier := make(chan struct{})
			results := make(chan error, len(actions))
			for _, action := range actions {
				group.Add(1)
				go func(action string) {
					defer group.Done()
					<-barrier
					_, err := f.collaboration(t, 5, action, current)
					results <- err
				}(action)
			}
			close(barrier)
			group.Wait()
			close(results)
			success := 0
			for err := range results {
				if err == nil {
					success++
				} else {
					checkCode(t, err, "VERSION_CONFLICT")
				}
			}
			if success != 1 {
				t.Fatal("competition did not have one winner")
			}
			detail := f.detail(t, current.ID)
			if detail.Ticket.Version != current.Version+1 {
				t.Fatal("version lost or double increment")
			}
			assertTimeline(t, detail)
		})
	}
}

func (f *fixture) list(t *testing.T, actor int64, view string, page, size int) ticket.ListResult {
	t.Helper()
	result, err := f.service.List(context.Background(), actor, ticket.ListRequest{View: view, Page: page, PageSize: size})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func listIDs(result ticket.ListResult) []int64 {
	ids := make([]int64, 0, len(result.Items))
	for _, item := range result.Items {
		ids = append(ids, item.ID)
	}
	return ids
}

func TestListsCurrentStateAndMultipleMembership(t *testing.T) {
	f := newFixture(t)
	it := f.create(t, 1, request("REQ-list-it")).Ticket
	deviceReq := request("REQ-list-device")
	deviceReq.Category = ticket.CategoryDevice
	device := f.create(t, 2, deviceReq).Ticket
	other := f.create(t, 1, ticket.CreateRequest{RequestID: "REQ-list-default", Title: "其他类别需要支持", Description: "默认团队工单", Category: ticket.CategoryOther, Priority: ticket.PriorityLow}).Ticket
	if got := f.list(t, 3, ticket.ViewTeamPending, 1, 20); got.Total != 1 || !reflect.DeepEqual(listIDs(got), []int64{it.ID}) {
		t.Fatal("team pending leaked other teams", got)
	}
	f.exec(t, "INSERT INTO team_members (organization_id,team_id,user_id,active,created_at) VALUES (1,20,3,TRUE,UTC_TIMESTAMP(6))")
	if got := f.list(t, 3, ticket.ViewTeamPending, 1, 20); got.Total != 2 || len(got.Items) != 2 {
		t.Fatal("multiple active teams omitted", got)
	}
	if got := f.list(t, 1, ticket.ViewCreatedByMe, 1, 20); got.Total != 2 || got.Items[0].ID != other.ID {
		t.Fatal("creator list scope/order wrong", got)
	}
	if got := f.list(t, 2, ticket.ViewCreatedByMe, 1, 20); got.Total != 1 || got.Items[0].ID != device.ID {
		t.Fatal("creator list leaked", got)
	}
	claimed := f.claim(t, 3, it)
	assigned := f.list(t, 3, ticket.ViewAssignedToMe, 1, 20)
	if assigned.Total != 1 || assigned.Items[0].ID != it.ID || assigned.Items[0].AssigneeName == nil || assigned.Items[0].Version != 2 {
		t.Fatal("assigned list missing current owner", assigned)
	}
	if got := f.list(t, 3, ticket.ViewTeamPending, 1, 20); got.Total != 2 {
		t.Fatal("claim must keep pending queue", got)
	}
	started, err := f.service.Start(context.Background(), 3, actionRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	if got := f.list(t, 3, ticket.ViewTeamPending, 1, 20); got.Total != 1 || got.Items[0].ID != device.ID {
		t.Fatal("start not reflected in queue", got)
	}
	changed, err := f.collaboration(t, 3, "transfer", started.Ticket)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.list(t, 3, ticket.ViewAssignedToMe, 1, 20); got.Total != 0 || len(got.Items) != 0 {
		t.Fatal("old owner still assigned", got)
	}
	if got := f.list(t, 11, ticket.ViewAssignedToMe, 1, 20); got.Total != 1 {
		t.Fatal("new owner not assigned", got)
	}
	f.exec(t, "UPDATE team_members SET active=FALSE WHERE user_id=11 AND team_id=10")
	if got := f.list(t, 11, ticket.ViewAssignedToMe, 1, 20); got.Total != 0 || len(got.Items) != 0 {
		t.Fatal("invalid membership disclosed assigned count", got)
	}
	f.exec(t, "DELETE FROM team_members WHERE user_id=3 AND team_id=20")
	cross, err := f.collaboration(t, 5, "transfer-team", changed.Ticket)
	if err != nil || cross.Ticket.Status != ticket.StatusPending {
		t.Fatal(err)
	}
	if got := f.list(t, 3, ticket.ViewTeamPending, 1, 20); got.Total != 0 {
		t.Fatal("old team queue disclosed transferred ticket", got)
	}
	if got := f.list(t, 4, ticket.ViewTeamPending, 1, 20); got.Total != 2 || got.Items[0].ID != it.ID {
		t.Fatal("new queue missing transferred ticket", got)
	}
	if got := f.list(t, 5, ticket.ViewTeamPending, 1, 20); got.Total != 3 {
		t.Fatal("organization manager queue wrong", got)
	}
	f.exec(t, "UPDATE teams SET active=FALSE WHERE id=20")
	if got := f.list(t, 4, ticket.ViewTeamPending, 1, 20); got.Total != 0 {
		t.Fatal("inactive team queue disclosed", got)
	}
	if got := f.list(t, 5, ticket.ViewTeamPending, 1, 20); got.Total != 1 || got.Items[0].ID != other.ID {
		t.Fatal("manager queue includes inactive team", got)
	}
	for _, actor := range []int64{1, 7, 9} {
		if got := f.list(t, actor, ticket.ViewTeamPending, 1, 20); got.Total != 0 || len(got.Items) != 0 {
			t.Fatal("unauthorized queue or cross-org count", actor, got)
		}
	}
	_, err = f.service.List(context.Background(), 8, ticket.ListRequest{View: ticket.ViewCreatedByMe, Page: 1, PageSize: 20})
	checkCode(t, err, "UNAUTHENTICATED")
}

func TestListsStablePaginationAndAuthorizedTotal(t *testing.T) {
	f := newFixture(t)
	var ids []int64
	for i := 0; i < 5; i++ {
		ids = append(ids, f.create(t, 1, request(fmt.Sprintf("REQ-list-page-%d", i))).Ticket.ID)
	}
	f.create(t, 2, request("REQ-list-other-creator"))
	f.exec(t, "UPDATE tickets SET updated_at='2026-11-08 00:00:00.000000'")
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	var combined []int64
	for page := 1; page <= 3; page++ {
		got := f.list(t, 1, ticket.ViewCreatedByMe, page, 2)
		if got.Total != 5 || got.Page != page || got.PageSize != 2 || got.HasMore != (page < 3) {
			t.Fatal("pagination metadata incorrect", got)
		}
		combined = append(combined, listIDs(got)...)
		if again := f.list(t, 1, ticket.ViewCreatedByMe, page, 2); !reflect.DeepEqual(got, again) {
			t.Fatal("same-time pagination unstable")
		}
	}
	if !reflect.DeepEqual(combined, ids) {
		t.Fatal("pagination missing/duplicating rows", combined, ids)
	}
	if got := f.list(t, 1, ticket.ViewCreatedByMe, 4, 2); got.Total != 5 || got.HasMore || len(got.Items) != 0 || got.Items == nil {
		t.Fatal("empty page contract incorrect", got)
	}
	if got := f.list(t, 9, ticket.ViewCreatedByMe, 1, 20); got.Total != 0 || len(got.Items) != 0 {
		t.Fatal("count leaked across organizations", got)
	}
}

func collaborationServer(f *fixture) *httptest.Server {
	return httptest.NewServer(httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil))))
}

func httpTicket(t *testing.T, server *httptest.Server, action, token string, input any) ticket.Ticket {
	t.Helper()
	response := httpLifecycle(server, http.MethodPost, "/api/v1/tickets/"+action, token, input)
	if response.err != nil || response.status != 200 {
		t.Fatalf("HTTP %s=%d %s %v", action, response.status, response.body, response.err)
	}
	var result ticket.ActionResult
	if err := json.Unmarshal(response.body, &result); err != nil {
		t.Fatal(err)
	}
	return result.Ticket
}

func TestCollaborationHTTPFlow(t *testing.T) {
	f := newFixture(t)
	server := collaborationServer(f)
	defer server.Close()
	created := httpLifecycle(server, http.MethodPost, "/api/v1/tickets", "local-alice", request("REQ-http-collaboration"))
	if created.err != nil || created.status != 201 {
		t.Fatal("HTTP create", created)
	}
	var result ticket.CreateResult
	if err := json.Unmarshal(created.body, &result); err != nil {
		t.Fatal(err)
	}
	current := httpTicket(t, server, "claim", "local-it-agent", actionRequest(result.Ticket))
	current = httpTicket(t, server, "start", "local-it-agent", actionRequest(current))
	current = httpTicket(t, server, "comments", "local-alice", ticket.CommentRequest{ActionRequest: actionRequest(current), Content: "网络异常影响会议，请协助定位。"})
	current = httpTicket(t, server, "transfer", "local-it-agent", ticket.TransferRequest{ActionRequest: actionRequest(current), AssigneeID: 11, Reason: "同团队交接排查"})
	current = httpTicket(t, server, "transfer-team", "local-supervisor", ticket.TeamTransferRequest{ActionRequest: actionRequest(current), TeamID: 20, Reason: "设备团队更换交换机"})
	denied := httpLifecycle(server, http.MethodGet, fmt.Sprintf("/api/v1/tickets/%d", current.ID), "local-it-agent", nil)
	if denied.status != 403 || strings.Contains(string(denied.body), "办公网络") {
		t.Fatal("old team HTTP data leaked", denied)
	}
	current = httpTicket(t, server, "claim", "local-device-agent", actionRequest(current))
	current = httpTicket(t, server, "start", "local-device-agent", actionRequest(current))
	current = httpTicket(t, server, "comments", "local-device-agent", ticket.CommentRequest{ActionRequest: actionRequest(current), Content: "已更换交换机并验证连通性。"})
	current = httpTicket(t, server, "resolve", "local-device-agent", resolution(current, "首次更换设备，等待验证。"))
	first := *current.FirstResolvedAt
	current = httpTicket(t, server, "reopen", "local-alice", actionRequest(current))
	current = httpTicket(t, server, "resolve", "local-device-agent", resolution(current, "再次检查设备固件，故障已解决。"))
	current = httpTicket(t, server, "manual-close", "local-supervisor", ticket.ReasonRequest{ActionRequest: actionRequest(current), Reason: "主管与员工核验后人工确认关闭"})
	response := httpLifecycle(server, http.MethodGet, fmt.Sprintf("/api/v1/tickets/%d", current.ID), "local-alice", nil)
	var detail ticket.Detail
	if response.err != nil || response.status != 200 || json.Unmarshal(response.body, &detail) != nil {
		t.Fatal("HTTP final detail", response)
	}
	if !reflect.DeepEqual(detail, f.detail(t, current.ID)) || current.Version != 13 || current.Status != ticket.StatusClosed || current.ReopenCount != 1 || !current.FirstResolvedAt.Equal(first) || len(detail.Comments) != 2 {
		t.Fatal("HTTP collaboration final fields incorrect")
	}
	assertTimeline(t, detail)
	var status string
	var version int64
	if err := f.db.QueryRow("SELECT status,version FROM tickets WHERE id=?", current.ID).Scan(&status, &version); err != nil || status != ticket.StatusClosed || version != 13 {
		t.Fatal("SQL HTTP mismatch", err)
	}
}

func TestCollaborationHTTPErrorsAndRace(t *testing.T) {
	f := newFixture(t)
	current := f.processing(t, "REQ-http-errors")
	server := collaborationServer(f)
	defer server.Close()
	assertError := func(method, path, token string, input any, status int, code string) {
		t.Helper()
		got := httpLifecycle(server, method, path, token, input)
		var body struct {
			Error ticket.Error `json:"error"`
		}
		if got.err != nil || got.status != status || json.Unmarshal(got.body, &body) != nil || body.Error.Code != code || strings.Contains(string(got.body), "办公网络") {
			t.Fatalf("HTTP error got=%d %s want=%d/%s", got.status, got.body, status, code)
		}
	}
	base := map[string]any{"ticket_id": current.ID, "expected_version": current.Version}
	for _, action := range []string{"transfer", "transfer-team", "management-reopen", "manual-close", "comments"} {
		input := map[string]any{}
		for k, v := range base {
			input[k] = v
		}
		if action == "transfer" {
			input["assignee_id"] = 11
		}
		if action == "transfer-team" || action == "management-reopen" {
			input["team_id"] = 20
		}
		if action == "comments" {
			input["content"] = "评论"
		} else {
			input["reason"] = "原因"
		}
		path := "/api/v1/tickets/" + action
		assertError(http.MethodPost, path, "", input, 401, "UNAUTHENTICATED")
		input["role"] = "admin"
		assertError(http.MethodPost, path, "local-supervisor", input, 400, "INVALID_ARGUMENT")
	}
	assertError(http.MethodPost, "/api/v1/tickets/transfer", "local-it-agent-2", ticket.TransferRequest{ActionRequest: actionRequest(current), AssigneeID: 3}, 403, "ACTION_FORBIDDEN")
	assertError(http.MethodPost, "/api/v1/tickets/transfer-team", "local-it-agent", ticket.TeamTransferRequest{ActionRequest: actionRequest(current), TeamID: 20, Reason: "伪造管理头部"}, 403, "ACTION_FORBIDDEN")
	assertError(http.MethodPost, "/api/v1/tickets/transfer", "local-supervisor", ticket.TransferRequest{ActionRequest: actionRequest(current), AssigneeID: 8}, 422, "INVALID_ASSIGNEE")
	assertError(http.MethodPost, "/api/v1/tickets/transfer-team", "local-supervisor", ticket.TeamTransferRequest{ActionRequest: actionRequest(current), TeamID: 99, Reason: "停用团队"}, 422, "INVALID_TEAM")
	for _, query := range []string{"?team_id=20", "?view=all", "?view=team_pending&view=created_by_me", "?page=0", "?page=10001", "?page_size=101", "?page=abc", "?page=", "?foo=1", "?view=%ZZ"} {
		assertError(http.MethodGet, "/api/v1/tickets"+query, "local-it-agent", nil, 400, "INVALID_ARGUMENT")
	}
	assertError(http.MethodGet, "/api/v1/tickets", "", nil, 401, "UNAUTHENTICATED")
	awaiting := f.resolve(t, current, "已处理，等待确认")
	for _, action := range []string{"management-reopen", "manual-close"} {
		assertError(http.MethodPost, "/api/v1/tickets/"+action, "local-it-agent", ticket.ReasonRequest{ActionRequest: actionRequest(awaiting), Reason: "越权"}, 403, "ACTION_FORBIDDEN")
		assertError(http.MethodPost, "/api/v1/tickets/"+action, "local-supervisor", ticket.ReasonRequest{ActionRequest: actionRequest(awaiting)}, 400, "INVALID_ARGUMENT")
	}
	f.exec(t, "UPDATE teams SET active=FALSE WHERE id=10")
	assertError(http.MethodPost, "/api/v1/tickets/reopen", "local-alice", actionRequest(awaiting), 409, "TEAM_UNAVAILABLE")
	barrier := make(chan struct{})
	results := make(chan lifecycleHTTPResult, 2)
	for _, action := range []string{"management-reopen", "manual-close"} {
		input := map[string]any{"ticket_id": awaiting.ID, "expected_version": awaiting.Version, "reason": "同版人工操作"}
		if action == "management-reopen" {
			input["team_id"] = 20
		}
		go func(action string, input any) {
			<-barrier
			results <- httpLifecycle(server, http.MethodPost, "/api/v1/tickets/"+action, "local-supervisor", input)
		}(action, input)
	}
	close(barrier)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatal("HTTP competition transport failed")
	}
	statuses := []int{first.status, second.status}
	sort.Ints(statuses)
	if !reflect.DeepEqual(statuses, []int{200, 409}) {
		t.Fatal("HTTP competition did not have one winner", statuses)
	}
	for _, result := range []lifecycleHTTPResult{first, second} {
		if result.status == 409 && !strings.Contains(string(result.body), "VERSION_CONFLICT") {
			t.Fatal("HTTP competition loser not version conflict", string(result.body))
		}
	}
	assertTimeline(t, f.detail(t, current.ID))
}

func TestCollaborationMigrationPreservesBusinessData(t *testing.T) {
	f := newFixture(t)
	current := f.resolve(t, f.processing(t, "REQ-collaboration-migration"), "迁移前说明")
	before := f.detail(t, current.ID)
	var oldHistory, oldRequests string
	if err := f.db.QueryRow("SELECT CAST(JSON_ARRAYAGG(JSON_OBJECT('id',id,'ticket_id',ticket_id,'action',action,'before',before_state,'after',after_state)) AS CHAR) FROM ticket_histories").Scan(&oldHistory); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRow("SELECT CONCAT(HEX(request_id),HEX(payload_hash),ticket_id) FROM ticket_create_requests").Scan(&oldRequests); err != nil {
		t.Fatal(err)
	}
	// Restore M1-02B structure in this disposable database, then execute the actual migration statements.
	f.exec(t, "ALTER TABLE ticket_histories DROP FOREIGN KEY fk_history_comment, DROP CHECK ck_history_reason, DROP CHECK ck_history_required_reason, DROP CHECK ck_history_comment, DROP COLUMN reason, DROP COLUMN comment_id")
	f.exec(t, "DROP TABLE ticket_comments")
	f.exec(t, "CREATE INDEX fk_tickets_assignee ON tickets (organization_id,assignee_id)")
	f.exec(t, "ALTER TABLE tickets DROP INDEX idx_tickets_creator, DROP INDEX idx_tickets_team_status, DROP INDEX idx_tickets_assignee_updated, DROP INDEX idx_tickets_status_updated, ADD INDEX idx_tickets_creator (organization_id,creator_id,created_at,id), ADD INDEX idx_tickets_team_status (organization_id,current_team_id,status,created_at,id)")
	for _, statement := range strings.Split(assets.CollaborationMigration, ";") {
		if strings.TrimSpace(statement) != "" {
			f.exec(t, statement)
		}
	}
	if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
		t.Fatal("migration changed existing business fields/history")
	}
	var afterHistory, afterRequests string
	if err := f.db.QueryRow("SELECT CAST(JSON_ARRAYAGG(JSON_OBJECT('id',id,'ticket_id',ticket_id,'action',action,'before',before_state,'after',after_state)) AS CHAR) FROM ticket_histories").Scan(&afterHistory); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRow("SELECT CONCAT(HEX(request_id),HEX(payload_hash),ticket_id) FROM ticket_create_requests").Scan(&afterRequests); err != nil {
		t.Fatal(err)
	}
	if oldHistory != afterHistory || oldRequests != afterRequests {
		t.Fatal("migration rewrote audit/request rows")
	}
	commented, err := f.collaboration(t, 1, "comments", current)
	if err != nil {
		t.Fatal("migrated database cannot comment", err)
	}
	if _, err := f.service.Close(context.Background(), 1, actionRequest(commented.Ticket)); err != nil {
		t.Fatal(err)
	}
	assertTimeline(t, f.detail(t, current.ID))
}
