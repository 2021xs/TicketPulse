//go:build integration

package mysqlstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	assets "ticketpulse/db"
	"ticketpulse/internal/config"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/ticket"
)

type fixture struct {
	db      *sql.DB
	store   *mysqlstore.Store
	service *ticket.Service
	routing ticket.Routing
	name    string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dsn := os.Getenv("TICKETPULSE_TEST_DSN")
	if dsn == "" {
		t.Fatal("real MySQL integration requires TICKETPULSE_TEST_DSN; no database test has run")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DBName != "" {
		t.Fatal("test admin DSN must have no database; tests only create new randomly named databases")
	}
	switch cfg.Net {
	case "unix":
		if !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m5-02-mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m5-04/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-02/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03a/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03b/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c1/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c2/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c3/mysql/mysql.sock") {
			t.Fatal("integration socket must belong to this isolated TicketPulse instance")
		}
	case "tcp":
		host, port, err := net.SplitHostPort(cfg.Addr)
		if err != nil || host != "127.0.0.1" || (port != "33306" && port != "33307") {
			t.Fatal("integration only permits the isolated 127.0.0.1:33306 endpoint")
		}
	default:
		t.Fatal("integration requires explicitly isolated unix or tcp endpoint")
	}
	expected := os.Getenv("TICKETPULSE_TEST_SERVER_UUID")
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c3" && (cfg.Net != "unix" || !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c3/mysql/mysql.sock")) {
		t.Fatal("M4-03C3 prohibits every other database endpoint")
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c2" && (cfg.Net != "unix" || !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c2/mysql/mysql.sock")) {
		t.Fatal("M4-03C2 prohibits every other database endpoint")
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c1" && (cfg.Net != "unix" || !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c1/mysql/mysql.sock")) {
		t.Fatal("M4-03C1 prohibits every other database endpoint")
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03b" && (cfg.Net != "unix" || !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03b/mysql/mysql.sock")) {
		t.Fatal("M4-03B prohibits every other database endpoint")
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03a" && (cfg.Net != "unix" || !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03a/mysql/mysql.sock")) {
		t.Fatal("M4-03A suite prohibits every other database endpoint")
	}
	if expected == "" {
		t.Fatal("TICKETPULSE_TEST_SERVER_UUID is required to verify owned MySQL")
	}
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var uuid, version string
	if err := admin.QueryRowContext(ctx, "SELECT @@server_uuid, VERSION()").Scan(&uuid, &version); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	if uuid != expected {
		admin.Close()
		t.Fatal("MySQL identity mismatch; refusing to create any test database")
	}
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	name := "ticketpulse_test_" + hex.EncodeToString(random)
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	// This cleanup removes only the unique database created above, never an existing database.
	t.Cleanup(func() {
		if os.Getenv("TICKETPULSE_RETAIN_C3_RESOURCES") == "true" {
			admin.Close()
			return
		}
		if _, err := admin.Exec("DROP DATABASE " + name); err != nil {
			t.Errorf("cleanup %s: %v", name, err)
		}
		admin.Close()
	})
	cfg.DBName = name
	cfg.MultiStatements = true
	initializer, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	for _, script := range []string{assets.Schema, assets.Seed, assets.LoginSeed} {
		if _, err := initializer.ExecContext(ctx, script); err != nil {
			initializer.Close()
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	if err := initializer.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.MultiStatements = false
	db, err := mysqlstore.Open(context.Background(), cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	routing, err := config.LoadRouting("../../configs/local.json")
	if err != nil {
		t.Fatal(err)
	}
	store := mysqlstore.New(db)
	service, err := ticket.NewService(store, routing)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("real MySQL=%s new database=%s", version, name)
	return &fixture{db: db, store: store, service: service, routing: routing, name: name}
}

func request(id string) ticket.CreateRequest {
	return ticket.CreateRequest{RequestID: id, Title: "办公网络无法连接", Description: "三楼无线网络持续断开，请协助排查。", Category: ticket.CategoryNetwork, Priority: ticket.PriorityUrgent}
}

func (f *fixture) create(t *testing.T, userID int64, r ticket.CreateRequest) ticket.CreateResult {
	t.Helper()
	result, err := f.service.Create(context.Background(), userID, r)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (f *fixture) counts(t *testing.T, want int) {
	t.Helper()
	for _, table := range []string{"tickets", "ticket_histories", "ticket_create_requests"} {
		var count int
		if err := f.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Errorf("%s count=%d want=%d", table, count, want)
		}
	}
	var unfinished int
	if err := f.db.QueryRow("SELECT COUNT(*) FROM ticket_create_requests WHERE ticket_id IS NULL").Scan(&unfinished); err != nil {
		t.Fatal(err)
	}
	if unfinished != 0 {
		t.Errorf("committed incomplete reservations=%d", unfinished)
	}
}

func (f *fixture) exec(t *testing.T, statement string, args ...any) {
	t.Helper()
	if _, err := f.db.Exec(statement, args...); err != nil {
		t.Fatal(err)
	}
}

func checkCode(t *testing.T, err error, code string) {
	t.Helper()
	var business *ticket.Error
	if !errors.As(err, &business) || business.Code != code {
		t.Fatalf("error=%v want=%s", err, code)
	}
}

func TestCreateAndDetail(t *testing.T) {
	f := newFixture(t)
	result := f.create(t, 1, request("REQ-normal"))
	got := result.Ticket
	if got.ID <= 0 || got.TicketNo == "" || got.Status != ticket.StatusPending || got.AssigneeID != nil || got.Version != 1 || got.CreatorID != 1 || got.InitialTeamID != 10 || got.CurrentTeamID != 10 || got.FirstResolvedAt != nil || got.ClosedAt != nil || got.ReopenCount != 0 {
		t.Fatalf("incorrect created ticket: %+v", got)
	}
	if !got.CreatedAt.Equal(got.UpdatedAt) || got.CreatedAt.Location() != time.UTC {
		t.Fatal("timestamps not equal UTC")
	}
	detail, err := f.service.Detail(context.Background(), 1, got.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(detail.Ticket, got) || detail.CurrentTeam.ID != 10 || detail.Assignee != nil || len(detail.History) != 1 {
		t.Fatalf("unexpected detail: %+v", detail)
	}
	h := detail.History[0]
	if h.Action != ticket.ActionCreated || h.OperatorID != 1 || h.Version != 1 || h.Before != nil || h.ResponsibleTeamID != 10 || !h.OccurredAt.Equal(got.CreatedAt) || !reflect.DeepEqual(h.After, got.State()) {
		t.Fatalf("incorrect creation history: %+v", h)
	}
	f.counts(t, 1)
}

func TestLostResponseAfterCommitCanRetry(t *testing.T) {
	f := newFixture(t)
	api := httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var drop atomic.Bool
	drop.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !drop.CompareAndSwap(true, false) {
			api.ServeHTTP(w, r)
			return
		}
		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, r) // The real create transaction commits before this returns.
		if recorder.Code != http.StatusCreated {
			t.Errorf("create before dropping response: status=%d body=%s", recorder.Code, recorder.Body)
		}
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		connection.Close() // No success response reaches the client.
	}))
	defer server.Close()
	payload, _ := json.Marshal(request("REQ-lost-response"))
	post := func() (*http.Response, error) {
		r, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/tickets", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer local-alice")
		r.Header.Set("Content-Type", "application/json")
		return server.Client().Do(r)
	}
	response, err := post()
	if err == nil {
		response.Body.Close()
		t.Fatal("injected lost response was not observed by client")
	}
	f.counts(t, 1)
	var committedID int64
	if err := f.db.QueryRow("SELECT ticket_id FROM ticket_create_requests WHERE creator_id = 1 AND request_id = 'REQ-lost-response'").Scan(&committedID); err != nil {
		t.Fatal(err)
	}
	response, err = post()
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var replayed ticket.CreateResult
	if err := json.NewDecoder(response.Body).Decode(&replayed); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !replayed.Replayed || replayed.Ticket.ID != committedID {
		t.Fatalf("retry after lost response: status=%d result=%+v", response.StatusCode, replayed)
	}
	f.counts(t, 1)
}

func TestMaximumUnicodeJSONContent(t *testing.T) {
	f := newFixture(t)
	server := httptest.NewServer(httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	input := request("REQ-unicode-maximum")
	input.Title = strings.Repeat("😀", 120)
	input.Description = strings.Repeat("😀", 5000)
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	// A valid JSON encoder may emit surrogate pairs instead of the UTF-8 characters.
	escaped := strings.ReplaceAll(string(encoded), "😀", `\ud83d\ude00`)
	r, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/tickets", strings.NewReader(escaped))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer local-alice")
	r.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var created ticket.CreateResult
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated || created.Ticket.Title != input.Title || created.Ticket.Description != input.Description {
		t.Fatalf("valid maximum Unicode input rejected or changed: status=%d", response.StatusCode)
	}
	detail, err := f.service.Detail(context.Background(), 1, created.Ticket.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Ticket.Description != input.Description || detail.History[0].After.Description != input.Description {
		t.Fatal("MySQL ticket/history changed maximum Unicode content")
	}
	f.counts(t, 1)
}

func TestCurrentTeamDeterminesVisibility(t *testing.T) {
	f := newFixture(t)
	created := f.create(t, 1, request("REQ-current-team"))
	// Fixture-only state change: no transfer API is implemented in M1-01.
	after := created.Ticket.State()
	after.CurrentTeamID = 20
	after.Version = 2
	beforeJSON, _ := json.Marshal(created.Ticket.State())
	afterJSON, _ := json.Marshal(after)
	tx, err := f.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("UPDATE tickets SET current_team_id = 20, version = 2 WHERE id = ?", created.Ticket.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO ticket_histories (organization_id, ticket_id, action, operator_id, version, before_state, after_state, responsible_team_id, occurred_at) VALUES (1, ?, 'fixture', 5, 2, ?, ?, 20, ?)`, created.Ticket.ID, beforeJSON, afterJSON, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, err = f.service.Detail(context.Background(), 3, created.Ticket.ID)
	checkCode(t, err, "FORBIDDEN")
	detail, err := f.service.Detail(context.Background(), 4, created.Ticket.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.CurrentTeam.ID != 20 || detail.Ticket.InitialTeamID != 10 || detail.History[0].ResponsibleTeamID != 10 || detail.History[0].After.CurrentTeamID != 10 || detail.History[1].ResponsibleTeamID != 20 {
		t.Fatalf("initial history mutated or current team is stale: %+v", detail)
	}
}

func TestSchemaSupportsPRDStatuses(t *testing.T) {
	f := newFixture(t)
	created := f.create(t, 1, request("REQ-status-schema"))
	// Only schema capacity is under test; these direct updates do not implement legal transitions.
	for _, status := range []string{ticket.StatusPending, ticket.StatusInProgress, ticket.StatusAwaitingConfirmation, ticket.StatusClosed} {
		tx, err := f.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("UPDATE tickets SET status = ? WHERE id = ?", status, created.Ticket.ID); err != nil {
			tx.Rollback()
			t.Fatalf("schema cannot store PRD status %s: %v", status, err)
		}
		var stored string
		if err := tx.QueryRow("SELECT status FROM tickets WHERE id = ?", created.Ticket.ID).Scan(&stored); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if stored != status {
			tx.Rollback()
			t.Fatalf("status truncated: %q != %q", stored, status)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRouting(t *testing.T) {
	tests := []struct {
		name, category, mutation string
		wantTeam                 int64
		wantError                bool
	}{
		{"network_route", ticket.CategoryNetwork, "", 10, false},
		{"device_route", ticket.CategoryDevice, "", 20, false},
		{"unmapped_default", ticket.CategoryOther, "", 90, false},
		{"inactive_mapped_team", ticket.CategoryNetwork, "UPDATE teams SET active = FALSE WHERE id = 10", 90, false},
		{"missing_mapped_team", ticket.CategoryNetwork, "", 90, false},
		{"cross_org_mapped_team", ticket.CategoryNetwork, "", 90, false},
		{"inactive_default", ticket.CategoryOther, "UPDATE teams SET active = FALSE WHERE id = 90", 0, true},
		{"all_teams_inactive", ticket.CategoryNetwork, "UPDATE teams SET active = FALSE WHERE organization_id = 1", 0, true},
		{"missing_default", ticket.CategoryOther, "", 0, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			if test.mutation != "" {
				f.exec(t, test.mutation)
			}
			if test.name == "missing_mapped_team" {
				f.routing.CategoryTeams[ticket.CategoryNetwork] = 9999
			}
			if test.name == "cross_org_mapped_team" {
				f.routing.CategoryTeams[ticket.CategoryNetwork] = 200
			}
			if test.name == "missing_default" {
				f.routing.DefaultTeamID = 9999
			}
			service, err := ticket.NewService(f.store, f.routing)
			if err != nil {
				t.Fatal(err)
			}
			r := request("REQ-routing")
			r.Category = test.category
			result, err := service.Create(context.Background(), 1, r)
			if test.wantError {
				checkCode(t, err, "NO_ACTIVE_TEAM")
				f.counts(t, 0)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Ticket.CurrentTeamID != test.wantTeam || result.Ticket.InitialTeamID != test.wantTeam {
				t.Fatalf("team=%d initial=%d want=%d", result.Ticket.CurrentTeamID, result.Ticket.InitialTeamID, test.wantTeam)
			}
			f.counts(t, 1)
		})
	}
}

func TestIdempotency(t *testing.T) {
	f := newFixture(t)
	r := request("REQ-retry")
	original := f.create(t, 1, r)
	f.exec(t, "UPDATE teams SET active = FALSE WHERE organization_id = 1")
	retry := f.create(t, 1, r)
	if retry.Ticket.ID != original.Ticket.ID || !retry.Replayed {
		t.Fatal("retry did not return original committed ticket")
	}
	r.Title = "同标识但不同的工单标题"
	_, err := f.service.Create(context.Background(), 1, r)
	checkCode(t, err, "IDEMPOTENCY_CONFLICT")
	f.counts(t, 1)
	f.exec(t, "UPDATE teams SET active = TRUE WHERE organization_id = 1")
	r = request("REQ-retry")
	other := f.create(t, 2, r)
	if other.Ticket.ID == original.Ticket.ID || other.Replayed {
		t.Fatal("different user's request collides")
	}
	r.RequestID = "req-retry"
	lower := f.create(t, 1, r)
	if lower.Ticket.ID == original.Ticket.ID {
		t.Fatal("request_id is not byte/case sensitive")
	}
	f.counts(t, 3)
}

func TestConcurrentDuplicateCreate(t *testing.T) {
	f := newFixture(t)
	const n = 24
	start := make(chan struct{})
	results := make(chan ticket.CreateResult, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, err := f.service.Create(context.Background(), 1, request("REQ-concurrent"))
			if err != nil {
				errs <- err
				return
			}
			results <- r
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var id int64
	created, total := 0, 0
	for r := range results {
		total++
		if id == 0 {
			id = r.Ticket.ID
		}
		if r.Ticket.ID != id {
			t.Error("concurrent request generated different ticket")
		}
		if !r.Replayed {
			created++
		}
	}
	if total != n || created != 1 {
		t.Errorf("responses=%d creates=%d want=%d/1", total, created, n)
	}
	f.counts(t, 1)
}

func TestConcurrentDifferentContent(t *testing.T) {
	f := newFixture(t)
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			<-start
			r := request("REQ-race-content")
			r.Description = fmt.Sprintf("不同的创建意图 %d", i)
			_, err := f.service.Create(context.Background(), 1, r)
			errs <- err
		}(i)
	}
	close(start)
	success, conflict := 0, 0
	for i := 0; i < 2; i++ {
		err := <-errs
		if err == nil {
			success++
		} else {
			checkCode(t, err, "IDEMPOTENCY_CONFLICT")
			conflict++
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
	f.counts(t, 1)
}

func TestHistoryFailureRollsBackEverything(t *testing.T) {
	f := newFixture(t)
	f.exec(t, `CREATE TRIGGER fail_history BEFORE INSERT ON ticket_histories FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected history failure'`)
	_, err := f.service.Create(context.Background(), 1, request("REQ-failure"))
	if err == nil {
		t.Fatal("history failure returned success")
	}
	f.counts(t, 0)
	f.exec(t, "DROP TRIGGER fail_history")
	f.create(t, 1, request("REQ-failure"))
	f.counts(t, 1)
}

func TestReservationFailureRollsBackTicketAndHistory(t *testing.T) {
	f := newFixture(t)
	f.exec(t, `CREATE TRIGGER fail_finalize BEFORE UPDATE ON ticket_create_requests FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected finalization failure'`)
	_, err := f.service.Create(context.Background(), 1, request("REQ-finalize-failure"))
	if err == nil {
		t.Fatal("finalization failure returned success")
	}
	f.counts(t, 0)
}

func TestDetailAuthorization(t *testing.T) {
	f := newFixture(t)
	id := f.create(t, 1, request("REQ-auth")).Ticket.ID
	for _, userID := range []int64{1, 3, 5, 6} {
		if _, err := f.service.Detail(context.Background(), userID, id); err != nil {
			t.Errorf("authorized user %d: %v", userID, err)
		}
	}
	for _, userID := range []int64{2, 4, 7, 10} {
		_, err := f.service.Detail(context.Background(), userID, id)
		checkCode(t, err, "FORBIDDEN")
	}
	_, err := f.service.Detail(context.Background(), 9, id)
	checkCode(t, err, "TICKET_NOT_FOUND")
	for _, userID := range []int64{0, 8, 9999} {
		_, err := f.service.Detail(context.Background(), userID, id)
		checkCode(t, err, "UNAUTHENTICATED")
	}
	f.exec(t, "UPDATE team_members SET active = FALSE WHERE team_id = 10 AND user_id = 3")
	_, err = f.service.Detail(context.Background(), 3, id)
	checkCode(t, err, "FORBIDDEN")
	f.exec(t, "UPDATE team_members SET active = TRUE WHERE team_id = 10 AND user_id = 3")
	f.exec(t, "UPDATE users SET role = 'requester' WHERE id = 3")
	_, err = f.service.Detail(context.Background(), 3, id)
	checkCode(t, err, "FORBIDDEN")
	f.exec(t, "UPDATE users SET role = 'agent' WHERE id = 3")
	f.exec(t, "UPDATE teams SET active = FALSE WHERE id = 10")
	_, err = f.service.Detail(context.Background(), 3, id)
	checkCode(t, err, "FORBIDDEN")
	if _, err := f.service.Detail(context.Background(), 1, id); err != nil {
		t.Fatal("creator lost view of own ticket when team disabled", err)
	}
}

func TestStableHistoryOrder(t *testing.T) {
	f := newFixture(t)
	created := f.create(t, 1, request("REQ-history"))
	state := created.Ticket.State()
	state.Version = 3
	three, _ := json.Marshal(state)
	state.Version = 2
	two, _ := json.Marshal(state)
	// Insert out of version order with equal timestamps; detail must still order by version.
	f.exec(t, `INSERT INTO ticket_histories (organization_id,ticket_id,action,operator_id,version,before_state,after_state,responsible_team_id,occurred_at) VALUES (1,?,'fixture',1,3,NULL,?,10,?),(1,?,'fixture',1,2,NULL,?,10,?)`, created.Ticket.ID, three, created.Ticket.CreatedAt, created.Ticket.ID, two, created.Ticket.CreatedAt)
	f.exec(t, "UPDATE tickets SET version = 3 WHERE id = ?", created.Ticket.ID)
	detail, err := f.service.Detail(context.Background(), 1, created.Ticket.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Ticket.Version != 3 || len(detail.History) != 3 {
		t.Fatalf("wrong detail: %+v", detail)
	}
	for i, h := range detail.History {
		if h.Version != int64(i+1) {
			t.Fatalf("history order=%+v", detail.History)
		}
	}
}

func TestHTTPCreationAndAccess(t *testing.T) {
	f := newFixture(t)
	server := httptest.NewServer(httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	call := func(method, path, token, body string) (int, []byte) {
		t.Helper()
		r, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-User-ID", "6")
		r.Header.Set("X-Role", "admin")
		resp, err := server.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("missing no-store")
		}
		return resp.StatusCode, data
	}
	payload, _ := json.Marshal(request("REQ-http"))
	status, body := call("POST", "/api/v1/tickets", "local-alice", string(payload))
	if status != 201 {
		t.Fatalf("create status=%d body=%s", status, body)
	}
	var created ticket.CreateResult
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if created.Ticket.CreatorID != 1 {
		t.Fatal("spoofed headers changed creator")
	}
	path := fmt.Sprintf("/api/v1/tickets/%d", created.Ticket.ID)
	status, body = call("GET", path, "local-alice", "")
	if status != 200 || !bytes.Contains(body, []byte(`"history"`)) {
		t.Fatalf("detail status=%d body=%s", status, body)
	}
	status, body = call("POST", "/api/v1/tickets", "local-alice", string(payload))
	if status != 200 || !bytes.Contains(body, []byte(`"replayed":true`)) {
		t.Fatalf("replay status=%d body=%s", status, body)
	}
	status, body = call("GET", path, "local-bob", "")
	if status != 403 || bytes.Contains(body, []byte(created.Ticket.Title)) || bytes.Contains(body, []byte(created.Ticket.TicketNo)) {
		t.Fatalf("unauthorized disclosure status=%d body=%s", status, body)
	}
	for _, token := range []string{"", "not-a-valid-token", "local-disabled"} {
		status, _ = call("GET", path, token, "")
		if status != 401 {
			t.Errorf("token=%q status=%d", token, status)
		}
	}
	var spoof map[string]any
	json.Unmarshal(payload, &spoof)
	spoof["creator_id"] = 6
	data, _ := json.Marshal(spoof)
	status, _ = call("POST", "/api/v1/tickets", "local-alice", string(data))
	if status != 400 {
		t.Fatal("client creator_id accepted")
	}
	status, _ = call("POST", "/api/v1/tickets", "local-alice", string(payload)+" {}")
	if status != 400 {
		t.Fatal("trailing JSON accepted")
	}
	tooLarge := request("REQ-too-large")
	tooLarge.Description = strings.Repeat("x", 80<<10)
	data, err := json.Marshal(tooLarge)
	if err != nil {
		t.Fatal(err)
	}
	status, body = call("POST", "/api/v1/tickets", "local-alice", string(data))
	if status != http.StatusRequestEntityTooLarge || !bytes.Contains(body, []byte("REQUEST_TOO_LARGE")) {
		t.Fatalf("oversized JSON status=%d body=%s", status, body)
	}
	f.counts(t, 1)
}
