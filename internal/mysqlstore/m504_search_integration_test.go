//go:build integration && m2

package mysqlstore_test

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"ticketpulse/internal/ticket"
)

func TestM504MembershipCursorAndCurrentSummary(t *testing.T) {
	m := newM2(t)
	ids := []int64{}
	for i := 0; i < 3; i++ {
		req := ticket.CreateRequest{RequestID: "m504-membership-" + string(rune('a'+i)), Title: "M504 membership needle", Description: "Synthetic fixture only", Category: "network", Priority: "high"}
		ids = append(ids, m.create(t, 1, req).Ticket.ID)
	}
	m.startCapture(t)
	m.startConsumer(t)
	for _, id := range ids {
		m.projected(t, id, 1)
	}
	base := "q=membership&sort=id&order=asc&page_size=1"
	first := m.query(t, "local-it-agent", base)
	if len(first.Items) != 1 || first.Items[0].ID != ids[0] || !first.HasMore {
		t.Fatal(first)
	}
	continuation := base + "&cursor=" + url.QueryEscape(first.NextCursor)
	if _, err := m.db.Exec("UPDATE team_members SET active=0 WHERE organization_id=1 AND team_id=10 AND user_id=3"); err != nil {
		t.Fatal(err)
	}
	revoked := m.query(t, "local-it-agent", continuation)
	if len(revoked.Items) != 0 || revoked.HasMore || revoked.Total != nil || revoked.NextCursor != "" {
		t.Fatalf("membership removal leaked: %+v", revoked)
	}
	if _, err := m.db.Exec("UPDATE team_members SET active=1 WHERE organization_id=1 AND team_id=10 AND user_id=3"); err != nil {
		t.Fatal(err)
	}
	restored := m.query(t, "local-it-agent", continuation)
	if len(restored.Items) != 1 || restored.Items[0].ID != ids[1] {
		t.Fatalf("cursor did not resume current memberships: %+v", restored)
	}
	m.stopConsumer(t)
	// The comment advances source to v2; ES v1 must be omitted even for a manager.
	current := m.actionHTTP(t, "local-it-agent", "comments", ids[1], 1, map[string]any{"content": "synthetic unindexed comment"})
	stale := m.query(t, "local-supervisor", "ticket_no="+current.TicketNo)
	if len(stale.Items) != 0 || stale.Total != nil {
		t.Fatalf("stale version returned: %+v", stale)
	}
	old := m.query(t, "local-it-agent", base)
	m.actionHTTP(t, "local-supervisor", "transfer-team", ids[0], 1, map[string]any{"team_id": 20, "reason": "M504 actual transfer"})
	denied := m.query(t, "local-it-agent", "ticket_no="+old.Items[0].TicketNo)
	if len(denied.Items) != 0 || denied.Total != nil {
		t.Fatal("old team leaked after real transfer")
	}
	m.call(t, "local-it-agent", "GET", "/tickets/"+formatID(ids[0]), nil, 403)
	m.startConsumer(t)
	m.projected(t, ids[1], 2)
	m.projected(t, ids[0], 2)
	got := m.query(t, "local-device-agent", "ticket_no="+old.Items[0].TicketNo)
	if len(got.Items) != 1 || got.Items[0].Version != 2 || got.Items[0].CurrentTeamID != 20 {
		t.Fatalf("new team summary: %+v", got)
	}
	m.call(t, "local-it-agent", "GET", "/search/tickets?"+base+"&cursor="+url.QueryEscape(first.NextCursor)+"&priority=urgent", nil, 400)
	m.call(t, "local-supervisor", "GET", "/search/tickets?"+continuation, nil, 400)
	empty := m.query(t, "local-supervisor", "q=m504nonexistenttoken")
	if len(empty.Items) != 0 || empty.HasMore || empty.Total != nil {
		t.Fatal(empty)
	}
	saveM504(t, "membership-and-transfer.json", map[string]any{"ids": ids, "cursor_revoked": revoked, "cursor_restored": restored, "stale_version": stale, "new_team": got, "empty": empty})
	t.Log("M504 current membership, old-team denial, v2 summary, cursor binding, empty-result PASS")
}

func formatID(id int64) string { return strconv.FormatInt(id, 10) }

func saveM504(t *testing.T, name string, value any) {
	t.Helper()
	folder := os.Getenv("TICKETPULSE_M504_EVIDENCE_DIR")
	if folder == "" {
		return
	}
	if err := os.MkdirAll(folder, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(folder, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}
