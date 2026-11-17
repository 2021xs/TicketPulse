package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
	"ticketpulse/web"
)

type TicketService interface {
	Create(context.Context, int64, ticket.CreateRequest) (ticket.CreateResult, error)
	Detail(context.Context, int64, int64) (ticket.Detail, error)
	Claim(context.Context, int64, ticket.ActionRequest) (ticket.ActionResult, error)
	Start(context.Context, int64, ticket.ActionRequest) (ticket.ActionResult, error)
	Resolve(context.Context, int64, ticket.ResolveRequest) (ticket.ActionResult, error)
	Reopen(context.Context, int64, ticket.ActionRequest) (ticket.ActionResult, error)
	Close(context.Context, int64, ticket.ActionRequest) (ticket.ActionResult, error)
	Transfer(context.Context, int64, ticket.TransferRequest) (ticket.ActionResult, error)
	TransferTeam(context.Context, int64, ticket.TeamTransferRequest) (ticket.ActionResult, error)
	ManagementReopen(context.Context, int64, ticket.ManagementReopenRequest) (ticket.ActionResult, error)
	ManualClose(context.Context, int64, ticket.ReasonRequest) (ticket.ActionResult, error)
	Comment(context.Context, int64, ticket.CommentRequest) (ticket.ActionResult, error)
	List(context.Context, int64, ticket.ListRequest) (ticket.ListResult, error)
	CurrentUser(context.Context, int64) (ticket.Profile, error)
	Options(context.Context, int64, int64) (ticket.Options, error)
}

type Authenticator interface {
	Authenticate(context.Context, string) (int64, error)
}

type API struct {
	service  TicketService
	auth     Authenticator
	log      *slog.Logger
	sessions SessionService
	options  Options
}

func New(service TicketService, auth Authenticator, logger *slog.Logger) http.Handler {
	return NewWithOptions(service, auth, logger, Options{SessionTTL: 8 * time.Hour})
}

func NewWithOptions(service TicketService, auth Authenticator, logger *slog.Logger, options Options) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	a := &API{service: service, auth: auth, log: logger, options: options}
	a.sessions, _ = service.(SessionService)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]string{"state": "alive"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if a.options.Readiness != nil {
			if err := a.options.Readiness(r.Context()); err != nil {
				a.log.WarnContext(r.Context(), "readiness check failed", "error", err)
				respond(w, http.StatusServiceUnavailable, map[string]string{"state": "not_ready"})
				return
			}
		}
		respond(w, http.StatusOK, map[string]string{"state": "ready"})
	})
	if options.ReadIdentity != nil {
		mux.HandleFunc("GET /api/v1/read-target", func(w http.ResponseWriter, r *http.Request) {
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
			if !profile.User.IsManager() {
				a.fail(w, r, ticket.ErrForbidden)
				return
			}
			if a.options.Readiness != nil {
				if err = a.options.Readiness(r.Context()); err != nil {
					respond(w, http.StatusServiceUnavailable, map[string]string{"state": "not_ready"})
					return
				}
			}
			respond(w, http.StatusOK, a.options.ReadIdentity())
		})
	}
	mux.HandleFunc("POST /api/v1/tickets", a.create)
	mux.HandleFunc("GET /api/v1/tickets/{id}", a.detail)
	mux.HandleFunc("POST /api/v1/tickets/claim", a.claim)
	mux.HandleFunc("POST /api/v1/tickets/start", a.start)
	mux.HandleFunc("POST /api/v1/tickets/resolve", a.resolve)
	mux.HandleFunc("POST /api/v1/tickets/reopen", a.reopen)
	mux.HandleFunc("POST /api/v1/tickets/close", a.close)
	mux.HandleFunc("POST /api/v1/tickets/transfer", a.transfer)
	mux.HandleFunc("POST /api/v1/tickets/transfer-team", a.transferTeam)
	mux.HandleFunc("POST /api/v1/tickets/management-reopen", a.managementReopen)
	mux.HandleFunc("POST /api/v1/tickets/manual-close", a.manualClose)
	mux.HandleFunc("POST /api/v1/tickets/comments", a.comment)
	mux.HandleFunc("GET /api/v1/tickets", a.list)
	mux.HandleFunc("GET /api/v1/tickets/{id}/options", a.ticketOptions)
	mux.HandleFunc("POST /api/v1/auth/login", a.login)
	mux.HandleFunc("POST /api/v1/auth/logout", a.logout)
	mux.HandleFunc("GET /api/v1/me", a.me)
	mux.HandleFunc("GET /api/v1/search/tickets", a.searchTickets)
	mux.HandleFunc("GET /api/v1/search/options", a.searchOptions)
	mux.HandleFunc("GET /api/v1/analytics", a.analytics)
	mux.Handle("GET /", web.Handler())
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *API) user(r *http.Request) (int64, error) {
	values := r.Header.Values("Authorization")
	if len(values) == 0 {
		identity, _, err := a.cookieIdentity(r)
		return identity.UserID, err
	}
	if len(values) != 1 {
		return 0, ticket.ErrUnauthenticated
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return 0, ticket.ErrUnauthenticated
	}
	return a.auth.Authenticate(r.Context(), parts[1])
}

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	var request ticket.CreateRequest
	if !a.decodeRequest(w, r, &request, "request_id、title、description、category、priority") {
		return
	}
	result, err := a.service.Create(r.Context(), userID, request)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	status := http.StatusCreated
	if result.Replayed {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/api/v1/tickets/"+strconv.FormatInt(result.Ticket.ID, 10))
	respond(w, status, result)
}

func (a *API) detail(w http.ResponseWriter, r *http.Request) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		a.fail(w, r, ticket.Invalid("工单 id 必须为正整数"))
		return
	}
	result, err := a.service.Detail(r.Context(), userID, id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	respond(w, http.StatusOK, result)
}

func (a *API) claim(w http.ResponseWriter, r *http.Request) {
	a.ticketAction(w, r, a.service.Claim)
}

func (a *API) start(w http.ResponseWriter, r *http.Request) {
	a.ticketAction(w, r, a.service.Start)
}

func (a *API) reopen(w http.ResponseWriter, r *http.Request) {
	a.ticketAction(w, r, a.service.Reopen)
}

func (a *API) close(w http.ResponseWriter, r *http.Request) {
	a.ticketAction(w, r, a.service.Close)
}

func (a *API) resolve(w http.ResponseWriter, r *http.Request) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	var request ticket.ResolveRequest
	if !a.decodeRequest(w, r, &request, "ticket_id、expected_version、resolution_note") {
		return
	}
	result, err := a.service.Resolve(r.Context(), userID, request)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	respond(w, http.StatusOK, result)
}

func (a *API) ticketAction(w http.ResponseWriter, r *http.Request,
	operation func(context.Context, int64, ticket.ActionRequest) (ticket.ActionResult, error)) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	var request ticket.ActionRequest
	if !a.decodeRequest(w, r, &request, "ticket_id、expected_version") {
		return
	}
	result, err := operation(r.Context(), userID, request)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	respond(w, http.StatusOK, result)
}

func (a *API) decodeRequest(w http.ResponseWriter, r *http.Request, request any, fields string) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		respond(w, http.StatusUnsupportedMediaType, map[string]any{"error": &ticket.Error{Code: "UNSUPPORTED_MEDIA_TYPE", Message: "请使用 application/json"}})
		return false
	}
	// Same M1-01 bound, including escaped maximum Unicode descriptions.
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(request); err != nil {
		a.badBody(w, err, fields)
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		a.badBody(w, err, fields)
		return false
	}
	return true
}

func (a *API) badBody(w http.ResponseWriter, err error, fields string) {
	status := http.StatusBadRequest
	code, message := "INVALID_ARGUMENT", "请求必须为一个 JSON 对象，且只包含 "+fields
	var maxBytes *http.MaxBytesError
	if errors.As(err, &maxBytes) {
		status = http.StatusRequestEntityTooLarge
		code = "REQUEST_TOO_LARGE"
		message = "请求体超过 64 KiB"
	}
	respond(w, status, map[string]any{"error": &ticket.Error{Code: code, Message: message}})
}

func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusInternalServerError
	public := &ticket.Error{Code: "INTERNAL_ERROR", Message: "服务暂时无法完成请求，请稍后重试"}
	if r.Method == http.MethodPost && r.URL.Path == "/api/v1/tickets" {
		public.Message = "创建结果待确认，请保留原 request_id 和内容重试"
	}
	isAction := r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/tickets/")
	if isAction {
		public = &ticket.Error{Code: "INTERNAL_ERROR", Message: "数据库处理失败，请读取详情确认最新版本；重试沿用原 expected_version"}
	}
	var business *ticket.Error
	if errors.As(err, &business) {
		public = business
		switch business.Code {
		case "INVALID_ARGUMENT":
			status = http.StatusBadRequest
		case "UNAUTHENTICATED", "INVALID_CREDENTIALS", "SESSION_EXPIRED":
			status = http.StatusUnauthorized
		case "FORBIDDEN", "ACTION_FORBIDDEN", "CSRF_REJECTED", "ANALYTICS_FORBIDDEN":
			status = http.StatusForbidden
		case "TICKET_NOT_FOUND":
			status = http.StatusNotFound
		case "IDEMPOTENCY_CONFLICT", "VERSION_CONFLICT", "ALREADY_CLAIMED", "INVALID_STATE", "ASSIGNEE_UNAVAILABLE", "TEAM_UNAVAILABLE", "ALREADY_ASSIGNED":
			status = http.StatusConflict
		case "NO_ACTIVE_TEAM", "INVALID_ASSIGNEE", "INVALID_TEAM":
			status = http.StatusUnprocessableEntity
		case "CREATE_RESULT_UNKNOWN", "ACTION_RESULT_UNKNOWN", "BUSINESS_WRITES_FROZEN":
			status = http.StatusServiceUnavailable
		case "SEARCH_CURSOR_TARGET_CHANGED":
			status = http.StatusConflict
		case "SEARCH_UNAVAILABLE", "SEARCH_TIMEOUT", "ANALYTICS_UNAVAILABLE", "ANALYTICS_TIMEOUT":
			status = http.StatusServiceUnavailable
		case "SEARCH_TOO_BROAD":
			status = http.StatusUnprocessableEntity
		}
	} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		status = http.StatusServiceUnavailable
		public = &ticket.Error{Code: "REQUEST_TIMEOUT", Message: "请求超时；创建结果待确认，请使用相同 request_id 和内容重试"}
		if isAction {
			public = &ticket.Error{Code: "REQUEST_TIMEOUT", Message: "操作超时，请先读取详情确认最新版本；重试沿用原 expected_version"}
		}
	}
	if status >= 500 {
		if d := search.Diagnostic(r.Context()); d != nil {
			a.log.ErrorContext(r.Context(), "request failed", "method", r.Method, "request_id", d.RequestID, "code", public.Code, "error_category", search.ErrorCategory(err))
		} else {
			a.log.ErrorContext(r.Context(), "request failed", "method", r.Method, "error", err)
		}
	}
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	respond(w, status, map[string]any{"error": public})
}

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
