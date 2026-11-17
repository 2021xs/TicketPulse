package httpapi

import (
	"net/http"
	"strconv"
	"ticketpulse/internal/ticket"
)

func (a *API) ticketOptions(w http.ResponseWriter, r *http.Request) {
	userID, err := a.user(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 || r.URL.RawQuery != "" {
		a.fail(w, r, ticket.Invalid("工单 id 必须为正整数，不支持额外候选参数"))
		return
	}
	options, err := a.service.Options(r.Context(), userID, id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	respond(w, http.StatusOK, options)
}
