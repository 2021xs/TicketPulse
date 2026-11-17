package ticket

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"regexp"
	"strings"
	"time"
)

var loginName = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SessionIdentity struct {
	UserID    int64
	ExpiresAt time.Time
}

type LoginResult struct {
	User      User      `json:"user"`
	CSRFToken string    `json:"csrf_token"`
	ExpiresAt time.Time `json:"expires_at"`
	Token     string    `json:"-"`
}

type Profile struct {
	User       User     `json:"user"`
	Teams      []Team   `json:"teams"`
	Categories []string `json:"categories"`
	Priorities []string `json:"priorities"`
}

func (s *Service) Login(ctx context.Context, request LoginRequest, ttl time.Duration) (LoginResult, error) {
	request.Username = strings.ToLower(strings.TrimSpace(request.Username))
	if !loginName.MatchString(request.Username) || len(request.Password) < 1 || len(request.Password) > 72 {
		return LoginResult{}, ErrInvalidCredentials
	}
	if ttl < time.Second || ttl > 24*time.Hour {
		return LoginResult{}, Invalid("会话有效期配置必须为 1 秒至 24 小时")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return LoginResult{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(secret)
	result, err := s.repo.Login(ctx, request, sha256.Sum256([]byte(token)), ttl)
	if err != nil {
		return LoginResult{}, err
	}
	result.Token = token
	result.CSRFToken = SessionCSRF(token)
	return result, nil
}

func validSessionToken(token string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	return err == nil && len(decoded) == 32 && len(token) == 43
}

func (s *Service) Session(ctx context.Context, token string) (SessionIdentity, error) {
	if !validSessionToken(token) {
		return SessionIdentity{}, ErrUnauthenticated
	}
	return s.repo.Session(ctx, sha256.Sum256([]byte(token)), time.Now().UTC())
}

func (s *Service) Logout(ctx context.Context, token string) error {
	if !validSessionToken(token) {
		return ErrUnauthenticated
	}
	return s.repo.Logout(ctx, sha256.Sum256([]byte(token)), time.Now().UTC())
}

// Bound to the secret session cookie; only same-origin JSON responses disclose this value.
func SessionCSRF(token string) string {
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte("TicketPulse-CSRF-v1"))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *Service) CurrentUser(ctx context.Context, userID int64) (Profile, error) {
	if userID <= 0 {
		return Profile{}, ErrUnauthenticated
	}
	profile, err := s.repo.CurrentUser(ctx, userID)
	if err != nil {
		return Profile{}, err
	}
	profile.Categories = append([]string{}, s.routing.Categories...)
	profile.Priorities = append([]string{}, s.routing.Priorities...)
	return profile, nil
}
