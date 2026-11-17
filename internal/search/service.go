package search

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"ticketpulse/internal/ticket"
)

type Service struct {
	auth      Authorizer
	es        *Elasticsearch
	freshness FreshnessReader
	cipher    cipher.AEAD
	target    string
	check     func(context.Context) error
}

type ServiceOption func(*Service) error

func WithTargetVersion(target string, check func(context.Context) error) ServiceOption {
	return func(s *Service) error {
		if target == "" {
			return fmt.Errorf("search target version is required")
		}
		s.target, s.check = target, check
		return nil
	}
}

func NewService(auth Authorizer, es *Elasticsearch, freshness FreshnessReader, key []byte, options ...ServiceOption) (*Service, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("search cursor key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if es == nil {
		return nil, fmt.Errorf("search projection is required")
	}
	// Legacy mode is still stable across restarts with the same key/config. All
	// new cursors carry a target; pre-M4-03A cursors must restart the search.
	hash := sha256.Sum256([]byte(es.Endpoint() + "/" + es.Index()))
	s := &Service{auth: auth, es: es, freshness: freshness, cipher: aead, target: fmt.Sprintf("unmanaged:%x", hash)}
	for _, option := range options {
		if err := option(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

type cursor struct {
	UserID  int64             `json:"u"`
	OrgID   int64             `json:"o"`
	Query   string            `json:"q"`
	Expires int64             `json:"e"`
	After   []json.RawMessage `json:"a"`
	Target  string            `json:"t"`
}

func (s *Service) encodeCursor(c cursor) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, s.cipher.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	token := s.cipher.Seal(nonce, nonce, b, []byte("ticketpulse-search-v1"))
	return base64.RawURLEncoding.EncodeToString(token), nil
}
func (s *Service) decodeCursor(token string, u ticket.User, r Request) ([]json.RawMessage, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(b) < s.cipher.NonceSize() {
		return nil, ticket.Invalid("分页游标无效，请重新搜索")
	}
	size := s.cipher.NonceSize()
	data, err := s.cipher.Open(nil, b[:size], b[size:], []byte("ticketpulse-search-v1"))
	if err != nil {
		return nil, ticket.Invalid("分页游标无效，请重新搜索")
	}
	var c cursor
	if json.Unmarshal(data, &c) != nil || c.UserID != u.ID || c.OrgID != u.OrganizationID || c.Query != r.Fingerprint() || c.Expires < time.Now().Unix() || len(c.After) == 0 {
		return nil, ticket.Invalid("分页游标已过期或与当前查询/身份不匹配")
	}
	if c.Target != s.target {
		return nil, ErrCursorTargetChanged
	}
	return c.After, nil
}

func (s *Service) Query(ctx context.Context, userID int64, r Request) (out Result, queryErr error) {
	d := Diagnostic(ctx)
	if d != nil {
		defer func() {
			d.Returned = len(out.Items)
			if queryErr != nil {
				d.ErrorCategory = ErrorCategory(queryErr)
				if queryErr == ErrTimeout || ctx.Err() != nil {
					d.TimeoutAt = d.Stage
				}
			}
		}()
		d.Stage = "validation"
	}
	if err := r.Validate(); err != nil {
		return Result{}, err
	}
	if s.check != nil {
		if d != nil {
			d.Stage = "projection_identity"
		}
		if err := s.check(ctx); err != nil {
			return Result{}, ErrUnavailable
		}
	}
	// Freshness uses a pool connection. Read it before opening the authorization
	// snapshot so concurrent searches never hold one connection while waiting
	// for another from the same bounded pool. Current authorization remains in
	// its original request-local transaction.
	freshness := Freshness{State: "unknown"}
	if s.freshness != nil {
		start := time.Now()
		if d != nil {
			d.Stage = "mysql_freshness"
		}
		f, err := s.freshness.SearchFreshness(ctx)
		if d != nil {
			d.FreshnessMS += Milliseconds(start)
		}
		if err != nil {
			return Result{}, err
		}
		freshness = f
	}
	start := time.Now()
	if d != nil {
		d.Stage = "mysql_begin_authorization"
	}
	access, err := s.auth.BeginSearch(ctx, userID)
	if d != nil {
		d.BeginMS += Milliseconds(start)
	}
	if err != nil {
		return Result{}, err
	}
	defer access.Close()
	u := access.Identity()
	var after []json.RawMessage
	if r.Cursor != "" {
		if d != nil {
			d.Stage = "cursor"
		}
		after, err = s.decodeCursor(r.Cursor, u, r)
		if err != nil {
			return Result{}, err
		}
	}
	result := Result{Items: make([]ticket.TicketSummary, 0), PageSize: r.PageSize, TotalRelation: "unavailable", Consistency: "eventual_projection_current_mysql_authorization", RefreshedAt: time.Now().UTC(), Freshness: Freshness{State: "unknown"}}
	result.Freshness = freshness
	// Scan until page_size+1 currently authorized, version-matched hits, never expose raw totals.
	// Budget exhaustion is an explicit failure; a truncated page is never labelled complete.
	scanned := 0
	const budget = 2000
	var lastReturned []json.RawMessage
	matched := 0
	for {
		if scanned >= budget {
			if d != nil {
				d.Stage = "candidate_budget"
			}
			return Result{}, ErrBudget
		}
		if d != nil {
			d.Stage = "elasticsearch"
			d.Batches++
		}
		batch := candidateBatch(scanned, matched, r.PageSize+1-matched, budget)
		candidates, err := s.es.Candidates(ctx, BuildQuery(r, u.OrganizationID, u.ID, u.IsManager(), access.TeamIDs(), after, batch))
		if err != nil {
			return Result{}, err
		}
		ids := make([]int64, 0, len(candidates))
		for _, c := range candidates {
			ids = append(ids, c.ID)
		}
		start = time.Now()
		if d != nil {
			d.Stage = "mysql_authorization"
			d.Candidates += len(candidates)
		}
		allowed, err := access.Authorized(ctx, ids)
		if d != nil {
			d.AuthorizationMS += Milliseconds(start)
			d.Allowed += len(allowed)
			if len(ids) > 0 {
				d.AuthorizationQueries++
			}
		}
		if err != nil {
			return Result{}, err
		}
		scanned += len(candidates)
		for _, c := range candidates {
			item, ok := allowed[c.ID]
			if !ok || item.Version != c.Version {
				if d != nil && ok {
					d.VersionRejected++
				}
				continue
			}
			if d != nil {
				d.Matched++
			}
			matched++
			if len(result.Items) == r.PageSize {
				result.HasMore = true
				if d != nil {
					d.Stage = "cursor"
				}
				result.NextCursor, err = s.encodeCursor(cursor{UserID: u.ID, OrgID: u.OrganizationID, Query: r.Fingerprint(), Expires: time.Now().Add(time.Hour).Unix(), After: lastReturned, Target: s.target})
				if err != nil {
					return Result{}, err
				}
				if d != nil {
					d.Stage = "complete"
				}
				return result, nil
			}
			result.Items = append(result.Items, item)
			lastReturned = c.Sort
		}
		if len(candidates) < batch {
			if d != nil {
				d.Stage = "complete"
			}
			return result, nil
		}
		after = candidates[len(candidates)-1].Sort
	}
}

// The first batch stays at 100. When few candidates survive current MySQL
// authorization and version checks, fetch a larger bounded next batch rather
// than pay twenty serial ES/SQL round trips. This only changes work grouping;
// the 2,000-candidate ceiling, lookahead and last-returned cursor are unchanged.
func candidateBatch(scanned, matched, needed, budget int) int {
	batch := 100
	if scanned > 0 && matched*4 < scanned {
		batch = 400
		if matched > 0 {
			estimate := (needed*scanned + matched - 1) / matched
			batch = ((estimate + 99) / 100) * 100
			if batch < 100 {
				batch = 100
			}
			if batch > 400 {
				batch = 400
			}
		}
	}
	if batch > budget-scanned {
		batch = budget - scanned
	}
	return batch
}
