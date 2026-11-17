package httpapi

import (
	"net/url"
	"testing"
)

func TestAnalyticsParameters(t *testing.T) {
	for _, q := range []string{"role=admin", "start=2026-11-01&start=2026-11-02", "end=", "team_id=-1", "team_id=not-a-number", "start=2026-11-03&end=2026-11-02"} {
		values, _ := url.ParseQuery(q)
		if _, err := analyticsRequest(values); err == nil {
			t.Error("accepted", q)
		}
	}
	values, _ := url.ParseQuery("start=2026-11-01&end=2026-11-04&team_id=10&team_id=20")
	r, err := analyticsRequest(values)
	if err != nil || r.Start != "2026-11-01" || r.End != "2026-11-04" || len(r.TeamIDs) != 2 {
		t.Fatal(r, err)
	}
}
