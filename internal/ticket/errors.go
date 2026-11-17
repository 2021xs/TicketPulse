package ticket

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

var (
	ErrWritesFrozen        = &Error{"BUSINESS_WRITES_FROZEN", "业务写入已暂停维护，请稍后重试；工单详情和基础列表仍可读取"}
	ErrUnauthenticated     = &Error{"UNAUTHENTICATED", "请使用有效账号登录"}
	ErrForbidden           = &Error{"FORBIDDEN", "无权查看该工单"}
	ErrNotFound            = &Error{"TICKET_NOT_FOUND", "工单不存在"}
	ErrIdempotencyConflict = &Error{"IDEMPOTENCY_CONFLICT", "request_id 已用于其他创建内容"}
	ErrNoActiveTeam        = &Error{"NO_ACTIVE_TEAM", "当前组织没有有效的受理团队或默认团队"}
	ErrCreateResultUnknown = &Error{"CREATE_RESULT_UNKNOWN", "创建提交结果待确认，请使用相同 request_id 和内容重试"}
	ErrActionForbidden     = &Error{"ACTION_FORBIDDEN", "无权执行该工单操作"}
	ErrInvalidState        = &Error{"INVALID_STATE", "当前工单状态不允许执行该操作"}
	ErrAlreadyClaimed      = &Error{"ALREADY_CLAIMED", "工单已经被认领，请刷新详情"}
	ErrVersionConflict     = &Error{"VERSION_CONFLICT", "工单版本已变化，请刷新详情后重新操作"}
	ErrActionResultUnknown = &Error{"ACTION_RESULT_UNKNOWN", "操作提交结果待确认，请先读取详情；沿用原 expected_version 重试可能返回版本冲突"}
	ErrAssigneeUnavailable = &Error{"ASSIGNEE_UNAVAILABLE", "负责人不再是当前团队的有效 Agent"}
	ErrTeamUnavailable     = &Error{"TEAM_UNAVAILABLE", "当前团队已失效，请联系主管恢复到有效团队；待确认工单需带原因重开并改派"}
	ErrInvalidAssignee     = &Error{"INVALID_ASSIGNEE", "目标必须是当前团队的有效 Agent"}
	ErrInvalidTeam         = &Error{"INVALID_TEAM", "目标团队不存在或已失效"}
	ErrAlreadyAssigned     = &Error{"ALREADY_ASSIGNED", "目标已经是当前负责人"}
	ErrInvalidCredentials  = &Error{"INVALID_CREDENTIALS", "账号或密码错误，或账号已停用"}
	ErrSessionExpired      = &Error{"SESSION_EXPIRED", "登录已过期，请重新登录"}
	ErrCSRF                = &Error{"CSRF_REJECTED", "请求校验失败，请刷新页面后重试"}
)

func Invalid(message string) error { return &Error{"INVALID_ARGUMENT", message} }
