package ticket

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// Routing is static policy; MySQL remains authoritative for team existence, activity and organization.
type Routing struct {
	Categories    []string         `json:"categories"`
	Priorities    []string         `json:"priorities"`
	CategoryTeams map[string]int64 `json:"category_teams"`
	DefaultTeamID int64            `json:"default_team_id"`
}

var keyPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)

func (r Routing) Validate() error {
	if !slices.Equal(r.Categories, []string{CategoryNetwork, CategoryAccount, CategoryDevice, CategorySoftware, CategoryOther}) {
		return fmt.Errorf("categories must match the fixed PRD v0.2 contract")
	}
	if !slices.Equal(r.Priorities, []string{PriorityUrgent, PriorityHigh, PriorityMedium, PriorityLow}) {
		return fmt.Errorf("priorities must match the fixed PRD v0.2 contract")
	}
	if r.DefaultTeamID < 0 {
		return fmt.Errorf("default_team_id must not be negative")
	}
	for category, teamID := range r.CategoryTeams {
		if !slices.Contains(r.Categories, category) || teamID <= 0 {
			return fmt.Errorf("invalid category route %q", category)
		}
	}
	return nil
}

func (r Routing) Candidates(category string) []int64 {
	ids := make([]int64, 0, 2)
	if id := r.CategoryTeams[category]; id > 0 {
		ids = append(ids, id)
	}
	if r.DefaultTeamID > 0 && !slices.Contains(ids, r.DefaultTeamID) {
		ids = append(ids, r.DefaultTeamID)
	}
	return ids
}

func (r CreateRequest) Validate(policy Routing) error {
	if !keyPattern.MatchString(r.RequestID) {
		return Invalid("request_id 必须为 1–64 个字母、数字或 _ . : -")
	}
	if !utf8.ValidString(r.Title) || strings.TrimSpace(r.Title) == "" || utf8.RuneCountInString(r.Title) < 5 || utf8.RuneCountInString(r.Title) > 120 {
		return Invalid("title 必须为 5–120 个字符且不能全部为空白")
	}
	if !utf8.ValidString(r.Description) || strings.TrimSpace(r.Description) == "" || utf8.RuneCountInString(r.Description) > 5000 {
		return Invalid("description 必须为 1–5000 个字符且不能全部为空白")
	}
	if !slices.Contains(policy.Categories, r.Category) {
		return Invalid("category 不在配置的枚举中")
	}
	if !slices.Contains(policy.Priorities, r.Priority) {
		return Invalid("priority 不在配置的枚举中")
	}
	return nil
}

// Fingerprint excludes request identity. It hashes a fixed field order and preserves content exactly.
// JSON whitespace and key order do not change the fingerprint; business whitespace does.
func (r CreateRequest) Fingerprint() [32]byte {
	body := struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		Category    string `json:"category"`
		Priority    string `json:"priority"`
	}{r.Title, r.Description, r.Category, r.Priority}
	data, _ := json.Marshal(body) // All fields are strings: Marshal cannot fail.
	return sha256.Sum256(data)
}

type Repository interface {
	Create(context.Context, int64, CreateRequest, [32]byte, []int64) (CreateResult, error)
	Detail(context.Context, int64, int64) (Detail, error)
	Claim(context.Context, int64, ActionRequest) (ActionResult, error)
	Start(context.Context, int64, ActionRequest) (ActionResult, error)
	Resolve(context.Context, int64, ResolveRequest) (ActionResult, error)
	Reopen(context.Context, int64, ActionRequest) (ActionResult, error)
	Close(context.Context, int64, ActionRequest) (ActionResult, error)
	Transfer(context.Context, int64, TransferRequest) (ActionResult, error)
	TransferTeam(context.Context, int64, TeamTransferRequest) (ActionResult, error)
	ManagementReopen(context.Context, int64, ManagementReopenRequest) (ActionResult, error)
	ManualClose(context.Context, int64, ReasonRequest) (ActionResult, error)
	Comment(context.Context, int64, CommentRequest) (ActionResult, error)
	List(context.Context, int64, ListRequest) (ListResult, error)
	Login(context.Context, LoginRequest, [32]byte, time.Duration) (LoginResult, error)
	Session(context.Context, [32]byte, time.Time) (SessionIdentity, error)
	Logout(context.Context, [32]byte, time.Time) error
	CurrentUser(context.Context, int64) (Profile, error)
	Options(context.Context, int64, int64) (Options, error)
}

type Service struct {
	repo    Repository
	routing Routing
}

func NewService(repo Repository, routing Routing) (*Service, error) {
	if err := routing.Validate(); err != nil {
		return nil, err
	}
	// Freeze policy so callers cannot mutate routes during a request or retry.
	copyPolicy := Routing{Categories: slices.Clone(routing.Categories), Priorities: slices.Clone(routing.Priorities), DefaultTeamID: routing.DefaultTeamID, CategoryTeams: map[string]int64{}}
	for k, v := range routing.CategoryTeams {
		copyPolicy.CategoryTeams[k] = v
	}
	return &Service{repo: repo, routing: copyPolicy}, nil
}

func (s *Service) Create(ctx context.Context, userID int64, request CreateRequest) (CreateResult, error) {
	if userID <= 0 {
		return CreateResult{}, ErrUnauthenticated
	}
	if err := request.Validate(s.routing); err != nil {
		return CreateResult{}, err
	}
	return s.repo.Create(ctx, userID, request, request.Fingerprint(), s.routing.Candidates(request.Category))
}

func (s *Service) Detail(ctx context.Context, userID, ticketID int64) (Detail, error) {
	if userID <= 0 {
		return Detail{}, ErrUnauthenticated
	}
	if ticketID <= 0 {
		return Detail{}, Invalid("工单 id 必须为正整数")
	}
	return s.repo.Detail(ctx, userID, ticketID)
}
