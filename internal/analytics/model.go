package analytics

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"ticketpulse/internal/ticket"
)

var Shanghai = func() *time.Location {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic(err)
	}
	return loc
}()

type Request struct {
	Start, End string
	TeamIDs    []int64
}

func (r Request) Window() (time.Time, time.Time, error) {
	start, err := time.ParseInLocation("2006-01-02", r.Start, Shanghai)
	if err != nil {
		return start, time.Time{}, ticket.Invalid("start 必须为 YYYY-MM-DD，北京时间日期")
	}
	end, err := time.ParseInLocation("2006-01-02", r.End, Shanghai)
	if err != nil {
		return start, end, ticket.Invalid("end 必须为 YYYY-MM-DD，右边界日期不含")
	}
	if !start.Before(end) || end.Sub(start) > 366*24*time.Hour || start.Year() < 1970 || end.Year() > 2261 {
		return start, end, ticket.Invalid("日期窗口须为 1–366 天，左闭右开")
	}
	if len(r.TeamIDs) > 20 {
		return start, end, ticket.Invalid("最多 20 个团队筛选值")
	}
	for _, id := range r.TeamIDs {
		if id <= 0 {
			return start, end, ticket.Invalid("team_id 必须为正整数")
		}
	}
	return start.UTC(), end.UTC(), nil
}

type Fact struct {
	OrganizationID   int64   `json:"organization_id"`
	TicketID         int64   `json:"ticket_id"`
	Version          int64   `json:"version"`
	BusinessEventID  string  `json:"business_event_id"`
	TransportEventID string  `json:"transport_event_id"`
	Action           string  `json:"action"`
	OccurredAt       string  `json:"occurred_at"`
	EventTeamID      int64   `json:"event_team_id"`
	InitialTeamID    int64   `json:"initial_team_id"`
	BeforeTeamID     *int64  `json:"before_team_id"`
	BeforeStatus     string  `json:"before_status"`
	AfterStatus      string  `json:"after_status"`
	AssigneeID       *int64  `json:"assignee_id"`
	CreatedAt        string  `json:"created_at"`
	FirstResolvedAt  *string `json:"first_resolved_at"`
}

type Current struct {
	OrganizationID  int64   `json:"organization_id"`
	TicketID        int64   `json:"ticket_id"`
	Version         int64   `json:"version"`
	Complete        int     `json:"complete"`
	CurrentTeamID   int64   `json:"current_team_id"`
	AssigneeID      *int64  `json:"assignee_id"`
	Status          string  `json:"status"`
	InitialTeamID   int64   `json:"initial_team_id"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
	FirstResolvedAt *string `json:"first_resolved_at"`
}

type Authorizer interface {
	AnalyticsAccess(context.Context, int64) (ticket.User, []ticket.Team, error)
}
type StatusReader interface {
	AnalyticsStatus(context.Context) (SyncStatus, error)
}

type SyncStatus struct {
	VersionID           string            `json:"version_id,omitempty"`
	ReadRevision        int64             `json:"read_revision,omitempty"`
	StateNamespace      string            `json:"state_namespace,omitempty"`
	Applied             int64             `json:"applied"`
	ProcessingAttempts  int64             `json:"processing_attempts"`
	State               string            `json:"state"`
	SourceWatermark     *time.Time        `json:"source_watermark_at"`
	ConfirmedThrough    *time.Time        `json:"confirmed_through"`
	LastSuccessfulWrite *time.Time        `json:"last_successful_write_at"`
	HeartbeatAt         *time.Time        `json:"heartbeat_at"`
	CaughtUp            bool              `json:"caught_up"`
	BootstrapComplete   bool              `json:"bootstrap_complete"`
	Topic               string            `json:"topic"`
	ConsumerGroup       string            `json:"consumer_group"`
	PartitionOffsets    []PartitionOffset `json:"partition_offsets"`
	Pending             uint64            `json:"pending"`
	AckPending          int               `json:"ack_pending"`
	Retrying            int64             `json:"retrying"`
	Isolated            int64             `json:"isolated"`
	Recovered           int64             `json:"recovered"`
	LastError           *string           `json:"last_error,omitempty"`
	ObservedAt          time.Time         `json:"observed_at"`
}

type PartitionOffset struct {
	Partition int32 `json:"partition"`
	Committed int64 `json:"committed_offset"`
	End       int64 `json:"log_end_offset"`
	Lag       int64 `json:"lag"`
}

type Overview struct {
	Created                       int64    `json:"created"`
	FirstResolved                 int64    `json:"first_resolved"`
	AverageFirstResolutionSeconds *float64 `json:"average_first_resolution_seconds"`
	Backlog                       int64    `json:"current_backlog"`
}
type Day struct {
	Date                          string   `json:"date"`
	Created                       int64    `json:"created"`
	FirstResolved                 int64    `json:"first_resolved"`
	AverageFirstResolutionSeconds *float64 `json:"average_first_resolution_seconds"`
}
type TeamMetrics struct {
	ID                            int64    `json:"team_id"`
	Name                          string   `json:"team_name"`
	Active                        bool     `json:"active"`
	Created                       int64    `json:"created"`
	FirstResolved                 int64    `json:"first_resolved"`
	AverageFirstResolutionSeconds *float64 `json:"average_first_resolution_seconds"`
	Backlog                       int64    `json:"current_backlog"`
	DrilldownURL                  string   `json:"backlog_drilldown_url,omitempty"`
}
type Result struct {
	Start          string        `json:"start"`
	End            string        `json:"end"`
	Timezone       string        `json:"timezone"`
	Overview       Overview      `json:"overview"`
	Daily          []Day         `json:"daily"`
	Teams          []TeamMetrics `json:"teams"`
	AvailableTeams []ticket.Team `json:"available_teams"`
	Sync           SyncStatus    `json:"sync"`
	RefreshedAt    time.Time     `json:"refreshed_at"`
	DataState      string        `json:"data_state"`
	BacklogScope   string        `json:"backlog_scope"`
}

func Drilldown(teamID int64) string {
	q := url.Values{"status": {"pending", "in_progress"}, "current_team_id": {strconv.FormatInt(teamID, 10)}, "sort": {"updated_at"}, "order": {"desc"}}
	return "/search?" + q.Encode()
}
func Timestamp(at time.Time) string { return at.UTC().Format("2006-01-02 15:04:05.000000") }
func OptionalTimestamp(at *time.Time) *string {
	if at == nil {
		return nil
	}
	value := Timestamp(*at)
	return &value
}

var (
	ErrForbidden   = &ticket.Error{Code: "ANALYTICS_FORBIDDEN", Message: "运营分析仅对当前有效主管或管理员开放"}
	ErrUnavailable = &ticket.Error{Code: "ANALYTICS_UNAVAILABLE", Message: "统计服务暂不可用，请稍后重试；工单操作不受影响"}
	ErrTimeout     = &ticket.Error{Code: "ANALYTICS_TIMEOUT", Message: "统计查询超时，请缩小日期范围后重试"}
)

func UnknownTeam(id int64) string { return fmt.Sprintf("团队 %d", id) }
