//go:build integration && m2 && m3

package syncpipe_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	assets "ticketpulse/db"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/config"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

// Mutations affect only fresh, uniquely named MySQL/ES/CK fixtures.
func TestReconcileRealExtraConflictAndSourceChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_TEST_DSN"))
	if err != nil || cfg.DBName != "" {
		t.Fatal("real test requires owned MySQL admin DSN")
	}
	if cfg.Net == "tcp" {
		host, port, e := net.SplitHostPort(cfg.Addr)
		if e != nil || host != "127.0.0.1" || (port != "33306" && port != "33307") {
			t.Fatal("refusing non-project MySQL")
		}
	} else if cfg.Net != "unix" || (!strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m5-02-mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m5-04/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-02/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03a/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03b/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c1/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c2/mysql/mysql.sock") && !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c3/mysql/mysql.sock")) {
		t.Fatal("refusing non-project socket")
	}
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03a" && (cfg.Net != "unix" || !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03a/mysql/mysql.sock")) {
		t.Fatal("M4-03A suite prohibits every other MySQL endpoint")
	}
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if os.Getenv("TICKETPULSE_ISOLATED_SUITE") == "m4-03c3" && (cfg.Net != "unix" || !strings.HasSuffix(cfg.Addr, "/ticketpulse/.local/m4-03c3/mysql/mysql.sock")) {
		t.Fatal("M4-03C2 prohibits every other database endpoint")
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
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var uuid string
	if err = admin.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil || uuid == "" || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
		t.Fatal("MySQL ownership mismatch")
	}
	name := fmt.Sprintf("ticketpulse_test_reconcile_%d", time.Now().UnixNano())
	if _, err = admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	defer admin.ExecContext(context.Background(), "DROP DATABASE "+name)
	cfg.DBName = name
	cfg.MultiStatements = true
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, assets.Schema); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "INSERT INTO users(id,organization_id,name,role,active,auth_token_hash,created_at,updated_at) VALUES(1,1,'requester','requester',1,UNHEX(SHA2('reconcile-owned-test',256)),UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)); INSERT INTO teams(id,organization_id,name,active,created_at,updated_at) VALUES(10,1,'A',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))"); err != nil {
		t.Fatal(err)
	}
	store, err := mysqlstore.Open(ctx, cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	routing, err := config.LoadRouting("../../configs/local.json")
	if err != nil {
		t.Fatal(err)
	}
	routing.DefaultTeamID = 10
	routing.CategoryTeams = map[string]int64{}
	service, err := ticket.NewService(mysqlstore.New(store), routing)
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(ctx, 1, ticket.CreateRequest{RequestID: "reconcile-fixture", Title: "source title", Description: "searchable description", Category: ticket.CategoryOther, Priority: ticket.PriorityMedium})
	if err != nil {
		t.Fatal(err)
	}
	detail, err := service.Detail(ctx, 1, created.Ticket.ID)
	if err != nil {
		t.Fatal(err)
	}
	h := detail.History[0]
	d := search.FromTicket(detail.Ticket)
	e := syncpipe.Event{Schema: 2, Kind: "ticket", ID: "reconcile-fixture", Source: "snapshot", TicketID: d.ID, Version: d.Version, EventAt: d.UpdatedAt, Document: d, History: &h, FirstResolvedAt: detail.Ticket.FirstResolvedAt}
	fact, current, err := syncpipe.AnalysisProjection(e)
	if err != nil {
		t.Fatal(err)
	}
	es, err := search.NewElasticsearch(os.Getenv("TICKETPULSE_ES_URL"), name)
	if err != nil {
		t.Fatal(err)
	}
	if err = es.EnsureIndex(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		req, _ := http.NewRequest(http.MethodDelete, strings.TrimRight(os.Getenv("TICKETPULSE_ES_URL"), "/")+"/"+name, nil)
		res, err := http.DefaultClient.Do(req)
		if err == nil {
			res.Body.Close()
		}
	}()
	ck, err := analytics.NewClickHouse(os.Getenv("TICKETPULSE_CK_URL"), name)
	if err != nil {
		t.Fatal(err)
	}
	if err = ck.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	defer ck.Query(context.Background(), "DROP DATABASE "+name)
	if _, err = es.Put(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err = ck.InsertCurrent(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err = ck.InsertFact(ctx, *fact); err != nil {
		t.Fatal(err)
	}
	state := &syncpipe.StateStore{DB: db, Schema: name}
	options := syncpipe.ReconcileOptions{BatchSize: 1, OrganizationID: 1, MetricsRequest: analytics.Request{Start: time.Now().In(analytics.Shanghai).AddDate(0, 0, -1).Format("2006-01-02"), End: time.Now().In(analytics.Shanghai).AddDate(0, 0, 1).Format("2006-01-02")}}
	check := func(want string) syncpipe.ReconcileReport {
		r, e := syncpipe.Reconcile(ctx, state, nil, es, ck, options)
		if e != nil || r.State != want {
			data, _ := json.MarshalIndent(r, "", "  ")
			t.Fatalf("state=%s want=%s error=%v report=%s", r.State, want, e, data)
		}
		return r
	}
	check(syncpipe.ReconcileConsistent)
	if err = ck.InsertCurrent(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err = ck.InsertFact(ctx, *fact); err != nil {
		t.Fatal(err)
	}
	r := check(syncpipe.ReconcileConsistent)
	if r.CK.HistoryCount != 1 || r.CK.PhysicalHistoryRows != 2 {
		t.Fatalf("wrong dedup=%+v", r.CK)
	}
	// Full source verification covers another organization while metrics remain
	// explicitly scoped to organization 1.
	if _, err = db.ExecContext(ctx, "INSERT INTO users(id,organization_id,name,role,active,auth_token_hash,created_at,updated_at) VALUES(2,2,'other organization','requester',1,UNHEX(SHA2('reconcile-other-org',256)),UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)); INSERT INTO teams(id,organization_id,name,active,created_at,updated_at) VALUES(20,2,'B',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))"); err != nil {
		t.Fatal(err)
	}
	otherRouting := routing
	otherRouting.DefaultTeamID = 20
	otherService, err := ticket.NewService(mysqlstore.New(store), otherRouting)
	if err != nil {
		t.Fatal(err)
	}
	other, err := otherService.Create(ctx, 2, ticket.CreateRequest{RequestID: "other-org", Title: "another organization", Description: "source in other organization", Category: ticket.CategoryOther, Priority: ticket.PriorityMedium})
	if err != nil {
		t.Fatal(err)
	}
	otherDetail, err := otherService.Detail(ctx, 2, other.Ticket.ID)
	if err != nil {
		t.Fatal(err)
	}
	otherDocument := search.FromTicket(otherDetail.Ticket)
	otherHistory := otherDetail.History[0]
	otherFact, otherCurrent, err := syncpipe.AnalysisProjection(syncpipe.Event{Schema: 2, Kind: "ticket", ID: "other-org", Source: "snapshot", TicketID: otherDocument.ID, Version: 1, EventAt: otherDocument.UpdatedAt, Document: otherDocument, History: &otherHistory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = es.Put(ctx, otherDocument); err != nil {
		t.Fatal(err)
	}
	if err = ck.InsertFact(ctx, *otherFact); err != nil {
		t.Fatal(err)
	}
	if err = ck.InsertCurrent(ctx, otherCurrent); err != nil {
		t.Fatal(err)
	}
	options.OrganizationID = 0
	options.MetricsOrganizationID = 1
	r = check(syncpipe.ReconcileConsistent)
	if r.Source.Tickets != 2 || r.Source.Histories != 2 || r.Metrics.OrganizationID != 1 {
		t.Fatalf("all organization source/selected metrics invalid: %+v", r)
	}
	extra := d
	extra.ID += 100000
	extra.TicketNo = "extra"
	if _, err = es.Put(ctx, extra); err != nil {
		t.Fatal(err)
	}
	conflicting := *fact
	conflicting.EventTeamID = 20
	if err = ck.InsertFact(ctx, conflicting); err != nil {
		t.Fatal(err)
	}
	extraFact := *fact
	extraFact.TicketID = extra.ID
	extraFact.BusinessEventID = fmt.Sprintf("1:%d:1", extra.ID)
	if err = ck.InsertFact(ctx, extraFact); err != nil {
		t.Fatal(err)
	}
	r = check(syncpipe.ReconcileInconsistent)
	if r.CK.ConflictingKeys != 1 || r.CK.HistoryCount != 3 || r.ES.CurrentCount != 3 {
		t.Fatalf("extra/conflict not detected: %+v", r)
	}
	options.ConfirmSourceBoundary = func(ctx context.Context) error {
		_, err := service.Comment(ctx, 1, ticket.CommentRequest{ActionRequest: ticket.ActionRequest{TicketID: d.ID, ExpectedVersion: d.Version}, Content: "during observation"})
		return err
	}
	r = check(syncpipe.ReconcileCatchingUp)
	if !r.SourceChanged {
		t.Fatal("source digest missed a new write")
	}
	options.ConfirmSourceBoundary = nil
	if _, err = db.ExecContext(ctx, "DELETE FROM ticket_histories WHERE ticket_id=? AND version=2", d.ID); err != nil {
		t.Fatal(err)
	}
	r, err = syncpipe.Reconcile(ctx, state, nil, es, ck, options)
	if err == nil || r.State != syncpipe.ReconcileUncheckable {
		t.Fatalf("missing authoritative history must be uncheckable: %+v err=%v", r, err)
	}
}
