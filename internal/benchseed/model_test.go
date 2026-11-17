package benchseed

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"ticketpulse/internal/ticket"
)

func TestEveryProfileHasCompleteImmutableHistory(t *testing.T) {
	o := Options{Tickets: 1000, Seed: 20261109, BatchSize: 200}
	m := newManifest(o)
	expectedRules(o, &m)
	var base int64
	for id := int64(1); id <= 1000; id++ {
		b, err := GenerateTicket(id, o.Seed, base)
		if err != nil {
			t.Fatal(err)
		}
		if err = ValidateBundle(b); err != nil {
			t.Fatalf("ticket %d: %v", id, err)
		}
		base += int64(len(b.Comments))
		countBundle(&m, b)
		if b.Ticket.FirstResolvedAt != nil && b.Ticket.FirstResolvedAt.Sub(b.Ticket.CreatedAt).Seconds() != 9900 {
			t.Fatal("independent first-resolution duration differs")
		}
		slot := slotFor(id, o.Seed)
		profile := slot % 10
		v, c := profileCounts(profile, b.Hot)
		if b.Ticket.Version != v || int64(len(b.Comments)) != c || b.Ticket.Status != statusFor(profile) {
			t.Fatalf("profile %d disagrees", profile)
		}
	}
	want := Counts{Tickets: 1000, Histories: 9750, Comments: 2650, FirstResolved: 400, Reopened: 300, Backlog: 700}
	if m.Counts != want || m.Expected != want {
		t.Fatalf("actual=%+v independent=%+v want=%+v", m.Counts, m.Expected, want)
	}
	if m.ExpectedMetrics.Teams["10"].Created != 700 || m.ExpectedMetrics.Teams["20"].Created != 200 || m.ExpectedMetrics.Teams["90"].Created != 100 {
		t.Fatal("team skew differs")
	}
	for _, action := range []string{ticket.ActionCreated, ticket.ActionClaimed, ticket.ActionProcessingStarted, ticket.ActionCommented, ticket.ActionTeamTransferred, ticket.ActionResolved, ticket.ActionReopened, ticket.ActionClosed} {
		if m.Distributions.Action[action] == 0 {
			t.Fatal("missing action", action)
		}
	}
}
func TestSeedDeterministicIndependentOfBatchSize(t *testing.T) {
	hash := func(batch int) string {
		h := sha256.New()
		var base int64
		for first := 1; first <= 100; first += batch {
			for id := first; id < first+batch && id <= 100; id++ {
				b, err := GenerateTicket(int64(id), 20261109, base)
				if err != nil {
					t.Fatal(err)
				}
				base += int64(len(b.Comments))
				p, _ := json.Marshal(b)
				h.Write(p)
				h.Write([]byte{10})
			}
		}
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	if hash(1) != hash(37) || hash(37) != hash(200) {
		t.Fatal("batch size changed generated contents")
	}
	a, _ := GenerateTicket(1, 1, 0)
	b, _ := GenerateTicket(1, 2, 0)
	if reflect.DeepEqual(a, b) {
		t.Fatal("seed does not affect data")
	}
}
func TestValidationDetectsBrokenHistoryAndLinks(t *testing.T) {
	b, err := GenerateTicket(1, 20261109, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = ValidateBundle(b); err != nil {
		t.Fatal(err)
	}
	b.Histories[1].Before.Version = 99
	if ValidateBundle(b) == nil {
		t.Fatal("broken predecessor accepted")
	}
	b, err = GenerateTicket(1, 20261109, 0)
	if err != nil {
		t.Fatal(err)
	}
	b.Comments[0].Version = 99
	if ValidateBundle(b) == nil {
		t.Fatal("broken comment link accepted")
	}
}
func TestEstimateRequiresNoDatabaseAndRejectsUnsafeSize(t *testing.T) {
	o := Options{Tickets: 1000000, Seed: 20261109, BatchSize: 200, DryEstimate: true}
	m, err := Generate(context.Background(), nil, o)
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != "dry_estimate" || m.Counts.Tickets != 0 || m.Expected.Tickets != 1000000 || m.Expected.Histories != 9750000 || m.MeanVersions != 9.75 {
		t.Fatalf("estimate=%+v", m)
	}
	for _, o := range []Options{{Tickets: 1000001, BatchSize: 200}, {Tickets: 10, BatchSize: 0}, {Tickets: 10, BatchSize: 501}} {
		if _, err := Estimate(o); err == nil {
			t.Fatal("unsafe estimate accepted")
		}
	}
}

func TestExportedEventsReuseProductionProjectionContract(t *testing.T) {
	for id := int64(1); id <= 10; id++ {
		b, err := GenerateTicket(id, 20261109, 1000*id)
		if err != nil {
			t.Fatal(err)
		}
		events, err := Events(b, 20261109, "ticketpulse_test_m5_unit")
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != len(b.Histories)+1 {
			t.Fatal("history/current export count differs")
		}
		for _, e := range events {
			if err = e.Validate(); err != nil {
				t.Fatal(err)
			}
		}
		last := events[len(events)-1]
		if last.History != nil || last.Version != b.Ticket.Version || last.Document.Title != b.Ticket.Title {
			t.Fatal("current export differs")
		}
	}
}

func TestDryDistributionsMatchGeneratedData(t *testing.T) {
	o := Options{Tickets: 1000, Seed: 20261109, BatchSize: 200}
	planned, err := Estimate(o)
	if err != nil {
		t.Fatal(err)
	}
	actual := newManifest(o)
	var base int64
	for id := int64(1); id <= o.Tickets; id++ {
		b, err := GenerateTicket(id, o.Seed, base)
		if err != nil {
			t.Fatal(err)
		}
		base += int64(len(b.Comments))
		countBundle(&actual, b)
	}
	if !reflect.DeepEqual(planned.Distributions, actual.Distributions) {
		t.Fatalf("planned=%+v actual=%+v", planned.Distributions, actual.Distributions)
	}
}
