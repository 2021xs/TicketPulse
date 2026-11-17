package search

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	"ticketpulse/internal/ticket"
)

// Document is the versioned, authoritative ticket row projected into ES.
// Comments trigger a ticket version update but their text is deliberately not indexed.
type Document struct {
	ID             int64     `json:"id"`
	OrganizationID int64     `json:"organization_id"`
	TicketNo       string    `json:"ticket_no"`
	Title          string    `json:"title"`
	Description    string    `json:"description"`
	Status         string    `json:"status"`
	Priority       string    `json:"priority"`
	PriorityRank   int       `json:"priority_rank"`
	Category       string    `json:"category"`
	CreatorID      int64     `json:"creator_id"`
	AssigneeID     *int64    `json:"assignee_id"`
	InitialTeamID  int64     `json:"initial_team_id"`
	CurrentTeamID  int64     `json:"current_team_id"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	Version        int64     `json:"version"`
}

func FromTicket(t ticket.Ticket) Document {
	rank := map[string]int{ticket.PriorityLow: 1, ticket.PriorityMedium: 2, ticket.PriorityHigh: 3, ticket.PriorityUrgent: 4}[t.Priority]
	return Document{t.ID, t.OrganizationID, t.TicketNo, t.Title, t.Description, t.Status, t.Priority, rank, t.Category, t.CreatorID, t.AssigneeID, t.InitialTeamID, t.CurrentTeamID, t.CreatedAt, t.UpdatedAt, t.Version}
}

func (d Document) Validate() error {
	if d.ID <= 0 || d.OrganizationID <= 0 || d.Version <= 0 || d.CreatorID <= 0 || d.InitialTeamID <= 0 || d.CurrentTeamID <= 0 || d.TicketNo == "" || d.Title == "" || d.Description == "" || d.CreatedAt.IsZero() || d.UpdatedAt.IsZero() {
		return fmt.Errorf("incomplete ticket projection")
	}
	r := Request{Statuses: []string{d.Status}, Priorities: []string{d.Priority}, Categories: []string{d.Category}, PageSize: 20, Sort: "updated_at", Order: "desc"}
	if err := r.Validate(); err != nil {
		return fmt.Errorf("invalid projection enums")
	}
	if d.PriorityRank != map[string]int{"low": 1, "medium": 2, "high": 3, "urgent": 4}[d.Priority] || (d.AssigneeID != nil && *d.AssigneeID <= 0) {
		return fmt.Errorf("invalid projection rank or assignee")
	}
	if !supportedTime(d.CreatedAt) || !supportedTime(d.UpdatedAt) {
		return fmt.Errorf("projection timestamp outside date_nanos range")
	}
	return nil
}

type Request struct {
	Query       string     `json:"q"`
	TicketNo    string     `json:"ticket_no"`
	Statuses    []string   `json:"status"`
	Priorities  []string   `json:"priority"`
	Categories  []string   `json:"category"`
	CreatorIDs  []int64    `json:"creator_id"`
	AssigneeIDs []int64    `json:"assignee_id"`
	TeamIDs     []int64    `json:"current_team_id"`
	CreatedFrom *time.Time `json:"created_from"`
	CreatedTo   *time.Time `json:"created_to"`
	UpdatedFrom *time.Time `json:"updated_from"`
	UpdatedTo   *time.Time `json:"updated_to"`
	Sort        string     `json:"sort"`
	Order       string     `json:"order"`
	PageSize    int        `json:"page_size"`
	Cursor      string     `json:"-"`
}

func (r Request) Validate() error {
	if !utf8.ValidString(r.Query) || utf8.RuneCountInString(r.Query) > 200 || len(r.TicketNo) > 64 || r.PageSize < 1 || r.PageSize > 100 || len(r.Cursor) > 4096 {
		return ticket.Invalid("关键词最多 200 字，工单号最多 64 字节，page_size 为 1–100")
	}
	for _, filter := range []struct{ values, allowed []string }{
		{r.Statuses, []string{"pending", "in_progress", "awaiting_confirmation", "closed"}},
		{r.Priorities, []string{"urgent", "high", "medium", "low"}},
		{r.Categories, []string{"network", "account", "device", "software", "other"}},
	} {
		if len(filter.values) > 20 {
			return ticket.Invalid("同字段最多 20 个筛选值")
		}
		for _, v := range filter.values {
			if !slices.Contains(filter.allowed, v) {
				return ticket.Invalid("未知状态、优先级或类别")
			}
		}
	}
	for _, ids := range []struct {
		values []int64
		zero   bool
	}{{r.CreatorIDs, false}, {r.AssigneeIDs, true}, {r.TeamIDs, false}} {
		if len(ids.values) > 20 {
			return ticket.Invalid("同字段最多 20 个 ID")
		}
		for _, id := range ids.values {
			if id < 0 || (!ids.zero && id == 0) {
				return ticket.Invalid("人员/团队 ID 必须为正整数；assignee_id=0 表示未分配")
			}
		}
	}
	for _, window := range [][2]*time.Time{{r.CreatedFrom, r.CreatedTo}, {r.UpdatedFrom, r.UpdatedTo}} {
		for _, at := range window {
			if at != nil && !supportedTime(*at) {
				return ticket.Invalid("搜索时间须在 1970-01-01 至 2262-04-11 的 date_nanos 范围内")
			}
		}
		if window[0] != nil && window[1] != nil && !window[0].Before(*window[1]) {
			return ticket.Invalid("时间区间需满足 from < to，左闭右开")
		}
	}
	if !slices.Contains([]string{"updated_at", "created_at", "priority", "id"}, r.Sort) || (r.Order != "asc" && r.Order != "desc") {
		return ticket.Invalid("sort 为 updated_at、created_at、priority 或 id；order 为 asc 或 desc")
	}
	return nil
}

func (r Request) Fingerprint() string {
	b, _ := json.Marshal(r)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func supportedTime(at time.Time) bool {
	seconds := at.Unix()
	return seconds >= 0 && (seconds < 9223372036 || (seconds == 9223372036 && at.Nanosecond() <= 854775807))
}

type Candidate struct {
	ID      int64             `json:"id"`
	Version int64             `json:"version"`
	Sort    []json.RawMessage `json:"-"`
}

// Access lives for one request and uses the existing MySQL consistent-read contract.
type Access interface {
	Identity() ticket.User
	TeamIDs() []int64
	Authorized(context.Context, []int64) (map[int64]ticket.TicketSummary, error)
	Close() error
}
type Authorizer interface {
	BeginSearch(context.Context, int64) (Access, error)
}

type Options struct {
	Teams     []ticket.Team `json:"teams"`
	Assignees []ticket.User `json:"assignees"`
	Scope     string        `json:"scope"`
}

type Freshness struct {
	VersionID      string     `json:"version_id,omitempty"`
	ReadRevision   int64      `json:"read_revision,omitempty"`
	ConsumerGroup  string     `json:"consumer_group,omitempty"`
	StateNamespace string     `json:"state_namespace,omitempty"`
	State          string     `json:"state"`
	LastEventAt    *time.Time `json:"last_projected_event_at"`
	LastSuccessAt  *time.Time `json:"last_success_at"`
	CaughtUp       *bool      `json:"caught_up"` // nil unless end-to-end completeness has actually been established.
}
type FreshnessReader interface {
	SearchFreshness(context.Context) (Freshness, error)
}

type Result struct {
	Items         []ticket.TicketSummary `json:"items"`
	PageSize      int                    `json:"page_size"`
	HasMore       bool                   `json:"has_more"`
	NextCursor    string                 `json:"next_cursor,omitempty"`
	Total         *int64                 `json:"total"`
	TotalRelation string                 `json:"total_relation"`
	Freshness     Freshness              `json:"freshness"`
	RefreshedAt   time.Time              `json:"refreshed_at"`
	Consistency   string                 `json:"consistency"`
}

var (
	ErrCursorTargetChanged = &ticket.Error{Code: "SEARCH_CURSOR_TARGET_CHANGED", Message: "搜索目标版本已变化，请重新发起搜索"}
	ErrUnavailable         = &ticket.Error{Code: "SEARCH_UNAVAILABLE", Message: "搜索服务暂不可用，请稍后重试；基础列表与工单详情仍可使用"}
	ErrTimeout             = &ticket.Error{Code: "SEARCH_TIMEOUT", Message: "搜索超时，请缩小查询范围后重试"}
	ErrBudget              = &ticket.Error{Code: "SEARCH_TOO_BROAD", Message: "搜索无法在查询预算内完成，请增加筛选条件；未返回部分结果"}
)
