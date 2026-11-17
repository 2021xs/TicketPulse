package syncpipe

import (
	"encoding/json"
	"testing"

	"ticketpulse/internal/analytics"
)

func TestReconcileComparisonRetainsInt64AndNull(t *testing.T) {
	if jsonEqual(json.RawMessage(`9007199254740992`), json.RawMessage(`9007199254740993`)) {
		t.Fatal("different int64 IDs were rounded into equality")
	}
	r := ReconcileReport{}
	key := reconcileKey(1, 2, 3)
	r.compare("ck_current", key, map[string]any{"assignee_id": nil, "version": int64(9007199254740993)}, map[string]any{"assignee_id": int64(5), "version": int64(9007199254740992)})
	if r.DifferenceCount != 2 || len(r.Differences) != 2 {
		t.Fatalf("differences=%+v", r)
	}
	if r.Differences[0].Field != "assignee_id" || r.Differences[1].Field != "version" {
		t.Fatal("differences should be stable and exact")
	}
}
func TestReconcileDifferenceBudgetPreservesTotal(t *testing.T) {
	r := ReconcileReport{}
	for i := 0; i < 1007; i++ {
		r.difference("es", reconcileKey(1, int64(i), 0), "missing", 1, nil)
	}
	if r.DifferenceCount != 1007 || len(r.Differences) != 1000 || !r.DifferencesTruncated {
		t.Fatalf("report=%+v", r)
	}
}
func TestReconcilePositionRequiresSameSourceAndOrderedFile(t *testing.T) {
	p := Position{ServerUUID: "source", File: "binlog.000099", Pos: 500}
	for _, tc := range []struct {
		p    Position
		want bool
	}{{p, true}, {Position{"source", "binlog.000099", 499}, false}, {Position{"source", "binlog.000100", 4}, true}, {Position{"source", "other.000100", 4}, false}, {Position{"changed", "binlog.000100", 4}, false}} {
		if got := reconcilePositionAtLeast(tc.p, p); got != tc.want {
			t.Fatalf("position=%+v got=%v want=%v", tc.p, got, tc.want)
		}
	}
}
func TestReconcileMetricComparisonIncludesExtraAndDuplicateRows(t *testing.T) {
	expected := []analytics.MetricRow{{Metric: "created", Day: "2026-11-01", TeamID: 10, Amount: 3}, {Metric: "resolved", Day: "2026-11-02", TeamID: 20, Amount: 1, Duration: 7200}}
	actual := []analytics.MetricRow{{Metric: "created", Day: "2026-11-01", TeamID: 10, Amount: 3}, {Metric: "created", Day: "2026-11-01", TeamID: 10, Amount: 3}, {Metric: "resolved", Day: "2026-11-02", TeamID: 20, Amount: 1, Duration: 7300}, {Metric: "backlog", TeamID: 20, Amount: 2}}
	r := ReconcileReport{}
	reconcileCompareMetrics(&r, expected, actual)
	if r.DifferenceCount != 3 {
		t.Fatalf("differences=%+v", r.Differences)
	}
}
func TestReconcileReopenActions(t *testing.T) {
	for _, action := range []string{"reopened", "reopened_requeued", "management_reopened"} {
		if !reconcileReopen(action) {
			t.Fatal(action)
		}
	}
	if reconcileReopen("resolved") {
		t.Fatal("resolution is not a reopening")
	}
}
