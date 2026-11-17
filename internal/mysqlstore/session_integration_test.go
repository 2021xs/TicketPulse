//go:build integration

package mysqlstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	assets "ticketpulse/db"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/ticket"
)

type browserClient struct {
	client *http.Client
	server *httptest.Server
	csrf   string
}

func newBrowserClient(server *httptest.Server) *browserClient {
	jar, _ := cookiejar.New(nil)
	return &browserClient{client: &http.Client{Jar: jar}, server: server}
}

func (c *browserClient) send(t *testing.T, method, path string, input any, origin, csrf string) (*http.Response, []byte) {
	t.Helper()
	var payload []byte
	if input != nil {
		var err error
		payload, err = json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
	}
	r, err := http.NewRequest(method, c.server.URL+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if csrf != "" {
		r.Header.Set("X-CSRF-Token", csrf)
	}
	response, err := c.client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, body
}

func (c *browserClient) login(t *testing.T, username string) (*http.Cookie, ticket.LoginResult) {
	t.Helper()
	r, body := c.send(t, http.MethodPost, "/api/v1/auth/login", ticket.LoginRequest{Username: username, Password: "TicketPulse!2026"}, c.server.URL, "")
	if r.StatusCode != 200 {
		t.Fatalf("login status=%d body=%s", r.StatusCode, body)
	}
	var result ticket.LoginResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	c.csrf = result.CSRFToken
	for _, cookie := range r.Cookies() {
		if cookie.Name == "ticketpulse_session" {
			return cookie, result
		}
	}
	t.Fatal("login missing cookie")
	return nil, result
}

func TestPasswordCookieSessionAndCSRF(t *testing.T) {
	f := newFixture(t)
	server := collaborationServer(f)
	defer server.Close()
	c := newBrowserClient(server)
	cookie, result := c.login(t, "alice")
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Secure || cookie.Path != "/" || result.Token != "" || len(cookie.Value) != 43 || time.Until(result.ExpiresAt) < 7*time.Hour {
		t.Fatal("cookie/session security contract changed")
	}
	hash := sha256.Sum256([]byte(cookie.Value))
	var count int
	if err := f.db.QueryRow("SELECT COUNT(*) FROM user_sessions WHERE token_hash=? AND user_id=1 AND revoked_at IS NULL", hash[:]).Scan(&count); err != nil || count != 1 {
		t.Fatal("session not persisted as hash", err)
	}
	r, body := c.send(t, http.MethodGet, "/api/v1/me", nil, "", "")
	var me struct {
		ticket.Profile
		CSRFToken string `json:"csrf_token"`
	}
	if r.StatusCode != 200 || json.Unmarshal(body, &me) != nil || me.User.ID != 1 || me.CSRFToken != c.csrf || len(me.Categories) != 5 || len(me.Teams) != 0 || bytes.Contains(body, []byte(cookie.Value)) || bytes.Contains(body, []byte("password")) {
		t.Fatal("me response invalid")
	}
	for _, headers := range [][2]string{{"", ""}, {server.URL, ""}, {server.URL, "forged"}, {"http://foreign.example", c.csrf}} {
		r, body := c.send(t, http.MethodPost, "/api/v1/tickets", request("REQ-cookie-csrf"), headers[0], headers[1])
		if r.StatusCode != 403 || !bytes.Contains(body, []byte("CSRF_REJECTED")) {
			t.Fatal("cookie write bypassed CSRF", r.StatusCode)
		}
	}
	if r, _ := c.send(t, http.MethodPost, "/api/v1/tickets", request("REQ-cookie-csrf"), server.URL, c.csrf); r.StatusCode != 201 {
		t.Fatal("legitimate cookie create failed")
	}
	// Login itself requires same origin; prevents login CSRF.
	if r, _ := c.send(t, http.MethodPost, "/api/v1/auth/login", ticket.LoginRequest{Username: "alice", Password: "TicketPulse!2026"}, "http://foreign.example", ""); r.StatusCode != 403 {
		t.Fatal("login CSRF accepted")
	}
	if r, _ := c.send(t, http.MethodPost, "/api/v1/auth/logout", map[string]any{}, server.URL, c.csrf); r.StatusCode != 200 {
		t.Fatal("logout failed")
	}
	if r, _ := c.send(t, http.MethodGet, "/api/v1/me", nil, "", ""); r.StatusCode != 401 {
		t.Fatal("logout cookie still active")
	}
	if _, err := f.service.Session(context.Background(), cookie.Value); err != ticket.ErrUnauthenticated {
		t.Fatal("revoked credential reusable", err)
	}
	if id, err := f.store.Authenticate(context.Background(), "local-alice"); err != nil || id != 1 {
		t.Fatal("Bearer regression", err)
	}
}

func TestInvalidLoginExpiryDisabledAndRotation(t *testing.T) {
	f := newFixture(t)
	server := collaborationServer(f)
	defer server.Close()
	c := newBrowserClient(server)
	for _, r := range []ticket.LoginRequest{{"alice", "wrong"}, {"missing", "TicketPulse!2026"}, {"disabled", "TicketPulse!2026"}} {
		response, body := c.send(t, http.MethodPost, "/api/v1/auth/login", r, server.URL, "")
		if response.StatusCode != 401 || !bytes.Contains(body, []byte("INVALID_CREDENTIALS")) || len(response.Cookies()) != 0 {
			t.Fatal("invalid login accepted")
		}
	}
	old, _ := c.login(t, "alice")
	next, _ := c.login(t, "alice")
	if next.Value == old.Value {
		t.Fatal("login did not rotate credential")
	}
	if _, err := f.service.Session(context.Background(), old.Value); err != ticket.ErrUnauthenticated {
		t.Fatal("rotated credential still valid", err)
	}
	hash := sha256.Sum256([]byte(next.Value))
	f.exec(t, "UPDATE user_sessions SET created_at=UTC_TIMESTAMP(6)-INTERVAL 2 HOUR,expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE token_hash=?", hash[:])
	response, body := c.send(t, http.MethodGet, "/api/v1/me", nil, "", "")
	if response.StatusCode != 401 || !bytes.Contains(body, []byte("SESSION_EXPIRED")) {
		t.Fatal("expired session accepted")
	}
	cookie, _ := c.login(t, "alice")
	f.exec(t, "UPDATE users SET active=FALSE WHERE id=1")
	if _, err := f.service.Session(context.Background(), cookie.Value); err != ticket.ErrUnauthenticated {
		t.Fatal("disabled user's session active", err)
	}
	response, _ = c.send(t, http.MethodPost, "/api/v1/tickets", request("REQ-disabled-cookie"), server.URL, c.csrf)
	if response.StatusCode != 401 {
		t.Fatal("disabled cookie write allowed")
	}
}

func TestWebCandidatesAndParticipants(t *testing.T) {
	f := newFixture(t)
	current := f.create(t, 1, request("REQ-web-options")).Ticket
	manager, err := f.service.Options(context.Background(), 5, current.ID)
	if err != nil || !slices.Contains(manager.Actions, "transfer") || len(manager.Assignees) != 2 || len(manager.Teams) != 3 {
		t.Fatal("manager options incomplete", err, manager)
	}
	for _, u := range manager.Assignees {
		if u.Role != ticket.RoleAgent || !u.Active || (u.ID != 3 && u.ID != 11) {
			t.Fatal("invalid target disclosed")
		}
	}
	for _, tm := range manager.Teams {
		if tm.OrganizationID != 1 || !tm.Active || tm.ID == 99 || tm.ID == 200 {
			t.Fatal("invalid team disclosed")
		}
	}
	requester, err := f.service.Options(context.Background(), 1, current.ID)
	if err != nil || len(requester.Assignees) != 0 || len(requester.Teams) != 0 || !slices.Equal(requester.Actions, []string{"comments"}) {
		t.Fatal("requester candidate leak", err)
	}
	if _, err := f.service.Options(context.Background(), 2, current.ID); err != ticket.ErrForbidden {
		t.Fatal("candidate authorization bypassed", err)
	}
	if _, err := f.service.Options(context.Background(), 9, current.ID); err != ticket.ErrNotFound {
		t.Fatal("candidate cross-org leak", err)
	}
	claimed := f.claim(t, 3, current)
	owner, err := f.service.Options(context.Background(), 3, current.ID)
	if err != nil || !slices.Contains(owner.Actions, "start") || len(owner.Assignees) != 1 || owner.Assignees[0].ID != 11 || len(owner.Teams) != 0 || !owner.AssigneeValid {
		t.Fatal("owner options incomplete", err)
	}
	if d := f.detail(t, current.ID); len(d.Participants) != 2 || len(d.Teams) != 1 {
		t.Fatal("detail references leaked unrelated people", d.Participants, d.Teams)
	}
	changed, err := f.service.TransferTeam(context.Background(), 5, ticket.TeamTransferRequest{ActionRequest: actionRequest(claimed), TeamID: 20, Reason: "换队"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Options(context.Background(), 3, current.ID); err != ticket.ErrForbidden {
		t.Fatal("old team options still visible", err)
	}
	if _, err := f.service.Options(context.Background(), 4, changed.Ticket.ID); err != nil {
		t.Fatal("new team options unavailable", err)
	}
	f.exec(t, "INSERT INTO team_members (organization_id,team_id,user_id,active,created_at) VALUES (1,20,3,TRUE,UTC_TIMESTAMP(6))")
	profile, err := f.service.CurrentUser(context.Background(), 3)
	if err != nil || len(profile.Teams) != 2 {
		t.Fatal("multiple teams omitted", err)
	}
	f.exec(t, "UPDATE team_members SET active=FALSE WHERE user_id=3 AND team_id=10")
	profile, err = f.service.CurrentUser(context.Background(), 3)
	if err != nil || len(profile.Teams) != 1 || profile.Teams[0].ID != 20 {
		t.Fatal("current team identity stale", err)
	}
}

func TestWebMigrationAndPasswordPreservation(t *testing.T) {
	f := newFixture(t)
	current := f.processing(t, "REQ-web-migration")
	before := f.detail(t, current.ID)
	f.exec(t, "DROP TABLE user_sessions")
	f.exec(t, "DROP TABLE login_accounts")
	for _, statement := range strings.Split(assets.WebSessionsMigration, ";") {
		if strings.TrimSpace(statement) != "" {
			f.exec(t, statement)
		}
	}
	f.exec(t, assets.LoginSeed)
	if !reflect.DeepEqual(before, f.detail(t, current.ID)) {
		t.Fatal("login migration changed business data")
	}
	var original string
	if err := f.db.QueryRow("SELECT password_hash FROM login_accounts WHERE username='alice'").Scan(&original); err != nil {
		t.Fatal(err)
	}
	f.exec(t, "UPDATE login_accounts SET password_hash=? WHERE username='alice'", dummyDifferentHash())
	f.exec(t, assets.LoginSeed)
	var actual string
	if err := f.db.QueryRow("SELECT password_hash FROM login_accounts WHERE username='alice'").Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if actual != dummyDifferentHash() || actual == original || !strings.HasPrefix(original, "$2a$12$") {
		t.Fatal("seed overwrote existing password")
	}
}

func dummyDifferentHash() string {
	return "$2a$12$IF8KNCJBAzl43xgBieaAmulBVmSREoNFiTzlPTWPBL9VM4A2ydk50"
}

func TestLoginPersistenceFailureNoCookie(t *testing.T) {
	f := newFixture(t)
	f.exec(t, "CREATE TRIGGER fail_session BEFORE INSERT ON user_sessions FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected session failure'")
	server := httptest.NewServer(httpapi.New(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	c := newBrowserClient(server)
	r, body := c.send(t, http.MethodPost, "/api/v1/auth/login", ticket.LoginRequest{Username: "alice", Password: "TicketPulse!2026"}, server.URL, "")
	if r.StatusCode != 500 || len(r.Cookies()) != 0 || bytes.Contains(body, []byte("injected")) {
		t.Fatal("session write failure issued cookie or leaked DB error")
	}
}

func TestAbsoluteSessionExpiryAndSecureCookie(t *testing.T) {
	f := newFixture(t)
	server := httptest.NewTLSServer(httpapi.NewWithOptions(f.service, f.store, slog.New(slog.NewTextHandler(io.Discard, nil)), httpapi.Options{SessionTTL: time.Second, SecureCookies: true}))
	defer server.Close()
	c := newBrowserClient(server)
	c.client.Transport = server.Client().Transport
	cookie, result := c.login(t, "alice")
	if !cookie.Secure || !cookie.HttpOnly || result.ExpiresAt.IsZero() {
		t.Fatal("HTTPS cookie is not secure")
	}
	// Reconstruct the Service to prove that session validity comes from MySQL rather than process memory.
	restarted, err := ticket.NewService(f.store, f.routing)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := restarted.Session(context.Background(), cookie.Value)
	if err != nil || identity.UserID != 1 || !identity.ExpiresAt.Equal(result.ExpiresAt) {
		t.Fatal("session did not survive service reconstruction", err)
	}
	time.Sleep(time.Until(result.ExpiresAt) + 5*time.Millisecond)
	if _, err := restarted.Session(context.Background(), cookie.Value); err != ticket.ErrSessionExpired {
		t.Fatal("fixed absolute expiry was extended by reading session", err)
	}
}
