package httpapi

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/ticket"
)

type AnalyticsService interface {
	Query(context.Context, int64, analytics.Request) (analytics.Result, error)
}

func analyticsRequest(query url.Values) (analytics.Request, error) {
	today := time.Now().In(analytics.Shanghai)
	r := analytics.Request{Start: today.AddDate(0, 0, -6).Format("2006-01-02"), End: today.AddDate(0, 0, 1).Format("2006-01-02")}
	for key, values := range query {
		if key != "start" && key != "end" && key != "team_id" {
			return r, ticket.Invalid("未知统计参数: " + key)
		}
		if len(values) == 0 || len(values) > 20 {
			return r, ticket.Invalid("统计参数数量无效")
		}
	}
	for _, key := range []string{"start", "end"} {
		if values, ok := query[key]; ok {
			if len(values) != 1 {
				return r, ticket.Invalid(key + " 只允许一个值")
			}
			if key == "start" {
				r.Start = values[0]
			} else {
				r.End = values[0]
			}
		}
	}
	for _, value := range query["team_id"] {
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return r, ticket.Invalid("team_id 必须为正整数")
		}
		r.TeamIDs = append(r.TeamIDs, id)
	}
	_, _, err := r.Window()
	return r, err
}

func (a *API) analytics(w http.ResponseWriter, r *http.Request) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	// Even the disabled-service route first enforces current manager permissions.
	profile, err := a.service.CurrentUser(r.Context(), userID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if !profile.User.IsManager() {
		a.fail(w, r, analytics.ErrForbidden)
		return
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		a.fail(w, r, ticket.Invalid("统计参数编码错误"))
		return
	}
	request, err := analyticsRequest(values)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if a.options.Analytics == nil {
		a.fail(w, r, analytics.ErrUnavailable)
		return
	}
	result, err := a.options.Analytics.Query(r.Context(), userID, request)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	respond(w, http.StatusOK, result)
}
