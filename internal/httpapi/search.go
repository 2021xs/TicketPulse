package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

type SearchService interface {
	Query(context.Context, int64, search.Request) (search.Result, error)
}
type SearchOptionsService interface {
	SearchOptions(context.Context, int64) (search.Options, error)
}

func searchRequest(query url.Values) (search.Request, error) {
	r := search.Request{Sort: "updated_at", Order: "desc", PageSize: 20}
	known := map[string]bool{}
	for _, name := range []string{"q", "ticket_no", "status", "priority", "category", "creator_id", "assignee_id", "current_team_id", "created_from", "created_to", "updated_from", "updated_to", "sort", "order", "page_size", "cursor"} {
		known[name] = true
	}
	for key, values := range query {
		if !known[key] {
			return r, ticket.Invalid("未知搜索参数: " + key)
		}
		if len(values) == 0 || len(values) > 20 {
			return r, ticket.Invalid("搜索参数值数量无效")
		}
	}
	single := func(name string) (string, error) {
		values := query[name]
		if len(values) > 1 {
			return "", ticket.Invalid(name + " 只允许一个值")
		}
		if len(values) == 0 {
			return "", nil
		}
		if values[0] == "" {
			return "", ticket.Invalid(name + " 不能为空")
		}
		return values[0], nil
	}
	for _, field := range []struct {
		name   string
		target *string
	}{{"q", &r.Query}, {"ticket_no", &r.TicketNo}, {"sort", &r.Sort}, {"order", &r.Order}, {"cursor", &r.Cursor}} {
		if _, ok := query[field.name]; ok {
			v, err := single(field.name)
			if err != nil {
				return r, err
			}
			*field.target = v
		}
	}
	if _, ok := query["page_size"]; ok {
		v, err := single("page_size")
		if err != nil {
			return r, err
		}
		r.PageSize, err = strconv.Atoi(v)
		if err != nil {
			return r, ticket.Invalid("page_size 必须为整数")
		}
	}
	for _, field := range []struct {
		name   string
		target *[]string
	}{{"status", &r.Statuses}, {"priority", &r.Priorities}, {"category", &r.Categories}} {
		*field.target = query[field.name]
	}
	for _, field := range []struct {
		name   string
		target *[]int64
	}{{"creator_id", &r.CreatorIDs}, {"assignee_id", &r.AssigneeIDs}, {"current_team_id", &r.TeamIDs}} {
		for _, v := range query[field.name] {
			id, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return r, ticket.Invalid(field.name + " 必须为整数")
			}
			*field.target = append(*field.target, id)
		}
	}
	for _, field := range []struct {
		name   string
		target **time.Time
	}{{"created_from", &r.CreatedFrom}, {"created_to", &r.CreatedTo}, {"updated_from", &r.UpdatedFrom}, {"updated_to", &r.UpdatedTo}} {
		if _, ok := query[field.name]; ok {
			v, err := single(field.name)
			if err != nil {
				return r, err
			}
			at, err := time.Parse(time.RFC3339Nano, v)
			if err != nil || strings.TrimSpace(v) != v {
				return r, ticket.Invalid(field.name + " 必须为带时区的 RFC3339 时间")
			}
			at = at.UTC()
			*field.target = &at
		}
	}
	return r, r.Validate()
}

func (a *API) searchTickets(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		a.fail(w, r, err)
		return
	}
	d := &search.Diagnostics{RequestID: hex.EncodeToString(nonce[:]), Stage: "http_identity"}
	w.Header().Set("X-Request-ID", d.RequestID)
	r = r.WithContext(search.WithDiagnostics(r.Context(), d))
	var searchErr error
	defer func() {
		d.TotalMS = search.Milliseconds(start)
		d.Outcome = "success"
		if searchErr != nil {
			d.ErrorCategory = search.ErrorCategory(searchErr)
			var business *ticket.Error
			if errors.As(searchErr, &business) {
				d.Outcome = business.Code
			} else if errors.Is(searchErr, context.DeadlineExceeded) || errors.Is(searchErr, context.Canceled) {
				d.Outcome = "REQUEST_TIMEOUT"
			} else {
				d.Outcome = "internal_error"
			}
			if r.Context().Err() != nil || errors.Is(searchErr, search.ErrTimeout) || errors.Is(searchErr, context.DeadlineExceeded) || errors.Is(searchErr, context.Canceled) {
				d.TimeoutAt = d.Stage
			}
			a.log.WarnContext(r.Context(), "search diagnostic", "search", *d)
		}
		if a.options.SearchObserver != nil {
			a.options.SearchObserver(*d)
		}
	}()
	userID, err := a.user(r)
	d.IdentityMS = search.Milliseconds(start)
	if err != nil {
		searchErr = err
		a.fail(w, r, err)
		return
	}
	d.Stage = "validation"
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		searchErr = ticket.Invalid("搜索参数编码错误")
		a.fail(w, r, ticket.Invalid("搜索参数编码错误"))
		return
	}
	request, err := searchRequest(values)
	if err != nil {
		searchErr = err
		a.fail(w, r, err)
		return
	}
	if a.options.Search == nil {
		searchErr = search.ErrUnavailable
		a.fail(w, r, search.ErrUnavailable)
		return
	}
	result, err := a.options.Search.Query(r.Context(), userID, request)
	if err != nil {
		searchErr = err
		a.fail(w, r, err)
		return
	}
	respond(w, http.StatusOK, result)
}
func (a *API) searchOptions(w http.ResponseWriter, r *http.Request) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if r.URL.RawQuery != "" {
		a.fail(w, r, ticket.Invalid("搜索候选不支持额外参数"))
		return
	}
	if a.options.SearchOptions == nil {
		a.fail(w, r, search.ErrUnavailable)
		return
	}
	result, err := a.options.SearchOptions.SearchOptions(r.Context(), userID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	respond(w, http.StatusOK, result)
}
