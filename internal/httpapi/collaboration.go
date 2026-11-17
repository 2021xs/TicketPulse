package httpapi

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"ticketpulse/internal/ticket"
)

func (a *API) actionBody(w http.ResponseWriter, r *http.Request, body any, fields string, call func(context.Context, int64) (ticket.ActionResult, error)) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if !a.decodeRequest(w, r, body, fields) {
		return
	}
	result, err := call(r.Context(), userID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	respond(w, http.StatusOK, result)
}

func (a *API) transfer(w http.ResponseWriter, r *http.Request) {
	var body ticket.TransferRequest
	a.actionBody(w, r, &body, "ticket_id、expected_version、assignee_id、reason", func(ctx context.Context, userID int64) (ticket.ActionResult, error) {
		return a.service.Transfer(ctx, userID, body)
	})
}

func (a *API) transferTeam(w http.ResponseWriter, r *http.Request) {
	var body ticket.TeamTransferRequest
	a.actionBody(w, r, &body, "ticket_id、expected_version、team_id、reason", func(ctx context.Context, userID int64) (ticket.ActionResult, error) {
		return a.service.TransferTeam(ctx, userID, body)
	})
}

func (a *API) managementReopen(w http.ResponseWriter, r *http.Request) {
	var body ticket.ManagementReopenRequest
	a.actionBody(w, r, &body, "ticket_id、expected_version、team_id、reason", func(ctx context.Context, userID int64) (ticket.ActionResult, error) {
		return a.service.ManagementReopen(ctx, userID, body)
	})
}

func (a *API) manualClose(w http.ResponseWriter, r *http.Request) {
	var body ticket.ReasonRequest
	a.actionBody(w, r, &body, "ticket_id、expected_version、reason", func(ctx context.Context, userID int64) (ticket.ActionResult, error) {
		return a.service.ManualClose(ctx, userID, body)
	})
}

func (a *API) comment(w http.ResponseWriter, r *http.Request) {
	var body ticket.CommentRequest
	a.actionBody(w, r, &body, "ticket_id、expected_version、content", func(ctx context.Context, userID int64) (ticket.ActionResult, error) {
		return a.service.Comment(ctx, userID, body)
	})
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		a.fail(w, r, ticket.Invalid("列表查询参数无效"))
		return
	}
	for key, entries := range values {
		if (key != "view" && key != "page" && key != "page_size") || len(entries) != 1 || entries[0] == "" {
			a.fail(w, r, ticket.Invalid("只允许单值 view、page、page_size 参数"))
			return
		}
	}
	request := ticket.ListRequest{View: ticket.ViewCreatedByMe, Page: 1, PageSize: 20}
	if view := values.Get("view"); view != "" {
		request.View = view
	}
	for key, target := range map[string]*int{"page": &request.Page, "page_size": &request.PageSize} {
		if value := values.Get(key); value != "" {
			*target, err = strconv.Atoi(value)
			if err != nil {
				a.fail(w, r, ticket.Invalid(key+" 必须为整数"))
				return
			}
		}
	}
	result, err := a.service.List(r.Context(), userID, request)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	respond(w, http.StatusOK, result)
}
