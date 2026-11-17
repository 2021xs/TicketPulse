package ticket

import "time"

const (
	StatusPending              = "pending"
	StatusInProgress           = "in_progress"
	StatusAwaitingConfirmation = "awaiting_confirmation"
	StatusClosed               = "closed"
	ActionCreated              = "created"
	ActionClaimed              = "claimed"
	ActionProcessingStarted    = "processing_started"
	ActionResolved             = "resolved"
	ActionReopened             = "reopened"
	ActionClosed               = "closed"
	ActionAssigneeTransferred  = "assignee_transferred"
	ActionTeamTransferred      = "team_transferred"
	ActionReopenedRequeued     = "reopened_requeued"
	ActionManagementReopened   = "management_reopened"
	ActionManuallyClosed       = "manually_closed"
	ActionCommented            = "commented"
	RoleRequester              = "requester"
	RoleAgent                  = "agent"
	RoleSupervisor             = "supervisor"
	RoleAdmin                  = "admin"
	CategoryNetwork            = "network"
	CategoryAccount            = "account"
	CategoryDevice             = "device"
	CategorySoftware           = "software"
	CategoryOther              = "other"
	PriorityUrgent             = "urgent"
	PriorityHigh               = "high"
	PriorityMedium             = "medium"
	PriorityLow                = "low"
)

type User struct {
	ID             int64  `json:"id"`
	OrganizationID int64  `json:"organization_id"`
	Name           string `json:"name"`
	Role           string `json:"role"`
	Active         bool   `json:"active"`
}

type Team struct {
	ID             int64  `json:"id"`
	OrganizationID int64  `json:"organization_id"`
	Name           string `json:"name"`
	Active         bool   `json:"active"`
}

type Ticket struct {
	ID              int64      `json:"id"`
	OrganizationID  int64      `json:"organization_id"`
	TicketNo        string     `json:"ticket_no"`
	Title           string     `json:"title"`
	Description     string     `json:"description"`
	ResolutionNote  *string    `json:"resolution_note"`
	Category        string     `json:"category"`
	Priority        string     `json:"priority"`
	Status          string     `json:"status"`
	CreatorID       int64      `json:"creator_id"`
	AssigneeID      *int64     `json:"assignee_id"`
	InitialTeamID   int64      `json:"initial_team_id"`
	CurrentTeamID   int64      `json:"current_team_id"`
	Version         int64      `json:"version"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	FirstResolvedAt *time.Time `json:"first_resolved_at"`
	ClosedAt        *time.Time `json:"closed_at"`
	ReopenCount     int64      `json:"reopen_count"`
}

// State preserves business state at the operation's version, independent of later edits.
type State struct {
	Title           string     `json:"title"`
	Description     string     `json:"description"`
	ResolutionNote  *string    `json:"resolution_note"`
	Category        string     `json:"category"`
	Priority        string     `json:"priority"`
	Status          string     `json:"status"`
	CreatorID       int64      `json:"creator_id"`
	AssigneeID      *int64     `json:"assignee_id"`
	InitialTeamID   int64      `json:"initial_team_id"`
	CurrentTeamID   int64      `json:"current_team_id"`
	Version         int64      `json:"version"`
	FirstResolvedAt *time.Time `json:"first_resolved_at"`
	ClosedAt        *time.Time `json:"closed_at"`
	ReopenCount     int64      `json:"reopen_count"`
}

func (t Ticket) State() State {
	return State{Title: t.Title, Description: t.Description, ResolutionNote: t.ResolutionNote, Category: t.Category, Priority: t.Priority,
		Status: t.Status, CreatorID: t.CreatorID, AssigneeID: t.AssigneeID, InitialTeamID: t.InitialTeamID,
		CurrentTeamID: t.CurrentTeamID, Version: t.Version, FirstResolvedAt: t.FirstResolvedAt, ClosedAt: t.ClosedAt, ReopenCount: t.ReopenCount}
}

type History struct {
	ID                int64     `json:"id"`
	TicketID          int64     `json:"ticket_id"`
	Action            string    `json:"action"`
	OperatorID        int64     `json:"operator_id"`
	Version           int64     `json:"version"`
	Before            *State    `json:"before"`
	After             State     `json:"after"`
	ResponsibleTeamID int64     `json:"responsible_team_id"`
	OccurredAt        time.Time `json:"occurred_at"`
	Reason            *string   `json:"reason"`
	CommentID         *int64    `json:"comment_id"`
}

type Comment struct {
	ID        int64     `json:"id"`
	TicketID  int64     `json:"ticket_id"`
	AuthorID  int64     `json:"author_id"`
	Content   string    `json:"content"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
}

type Detail struct {
	Ticket       Ticket    `json:"ticket"`
	CurrentTeam  Team      `json:"current_team"`
	Assignee     *User     `json:"assignee"`
	History      []History `json:"history"`
	Comments     []Comment `json:"comments"`
	Participants []User    `json:"participants"`
	Teams        []Team    `json:"teams"`
}

type CreateRequest struct {
	RequestID   string `json:"request_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Category    string `json:"category"`
	Priority    string `json:"priority"`
}

type CreateResult struct {
	Ticket   Ticket `json:"ticket"`
	Replayed bool   `json:"replayed"`
}
