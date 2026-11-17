package httpapi

import (
	"context"
	"crypto/hmac"
	"net/http"
	"time"

	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

const sessionCookie = "ticketpulse_session"

type Options struct {
	SessionTTL     time.Duration
	SecureCookies  bool
	Search         SearchService
	SearchOptions  SearchOptionsService
	SearchObserver func(search.Diagnostics)
	Analytics      AnalyticsService
	Readiness      func(context.Context) error
	ReadIdentity   func() any
}

type SessionService interface {
	Login(context.Context, ticket.LoginRequest, time.Duration) (ticket.LoginResult, error)
	Session(context.Context, string) (ticket.SessionIdentity, error)
	Logout(context.Context, string) error
}

func sameOrigin(r *http.Request) bool {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	values := r.Header.Values("Origin")
	return len(values) == 1 && values[0] == scheme+"://"+r.Host && r.Header.Get("Sec-Fetch-Site") != "cross-site"
}

func (a *API) cookieIdentity(r *http.Request) (ticket.SessionIdentity, string, error) {
	if a.sessions == nil {
		return ticket.SessionIdentity{}, "", ticket.ErrUnauthenticated
	}
	var value string
	count := 0
	for _, cookie := range r.Cookies() {
		if cookie.Name == sessionCookie {
			value = cookie.Value
			count++
		}
	}
	if count != 1 {
		return ticket.SessionIdentity{}, "", ticket.ErrUnauthenticated
	}
	identity, err := a.sessions.Session(r.Context(), value)
	if err != nil {
		return identity, "", err
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		csrf := r.Header.Values("X-CSRF-Token")
		if !sameOrigin(r) || len(csrf) != 1 || !hmac.Equal([]byte(csrf[0]), []byte(ticket.SessionCSRF(value))) {
			return ticket.SessionIdentity{}, "", ticket.ErrCSRF
		}
	}
	return identity, value, nil
}

func (a *API) login(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) {
		a.fail(w, r, ticket.ErrCSRF)
		return
	}
	var request ticket.LoginRequest
	if !a.decodeRequest(w, r, &request, "username、password") {
		return
	}
	if a.sessions == nil {
		a.fail(w, r, ticket.ErrUnauthenticated)
		return
	}
	result, err := a.sessions.Login(r.Context(), request, a.options.SessionTTL)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if old, err := r.Cookie(sessionCookie); err == nil && old.Value != result.Token {
		if err := a.sessions.Logout(r.Context(), old.Value); err != nil && err != ticket.ErrUnauthenticated {
			a.fail(w, r, err)
			return
		}
	}
	maxAge := int((time.Until(result.ExpiresAt) + time.Second - 1) / time.Second)
	if maxAge < 1 {
		maxAge = 1
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: result.Token, Path: "/", HttpOnly: true, Secure: a.options.SecureCookies || r.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: maxAge, Expires: result.ExpiresAt})
	respond(w, http.StatusOK, result)
}

func (a *API) logout(w http.ResponseWriter, r *http.Request) {
	_, token, err := a.cookieIdentity(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if err := a.sessions.Logout(r.Context(), token); err != nil {
		a.fail(w, r, err)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Path: "/", Value: "", HttpOnly: true, Secure: a.options.SecureCookies || r.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: -1, Expires: time.Unix(1, 0)})
	respond(w, http.StatusOK, map[string]bool{"logged_out": true})
}

func (a *API) me(w http.ResponseWriter, r *http.Request) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	profile, err := a.service.CurrentUser(r.Context(), userID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	var csrf string
	var expires *time.Time
	if len(r.Header.Values("Authorization")) == 0 {
		identity, token, err := a.cookieIdentity(r)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		csrf = ticket.SessionCSRF(token)
		expires = &identity.ExpiresAt
	}
	respond(w, http.StatusOK, struct {
		ticket.Profile
		CSRFToken string     `json:"csrf_token"`
		ExpiresAt *time.Time `json:"expires_at"`
	}{profile, csrf, expires})
}
