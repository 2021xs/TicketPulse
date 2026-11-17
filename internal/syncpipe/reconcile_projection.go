package syncpipe

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/search"
)

type reconcileIterator[T any] struct {
	page  []T
	index int
	done  bool
	load  func() ([]T, error)
}

func (it *reconcileIterator[T]) next() (T, bool, error) {
	var zero T
	if it.index == len(it.page) {
		if it.done {
			return zero, false, nil
		}
		page, err := it.load()
		if err != nil {
			return zero, false, err
		}
		it.page = page
		it.index = 0
		if len(page) == 0 {
			it.done = true
			return zero, false, nil
		}
	}
	row := it.page[it.index]
	it.index++
	return row, true, nil
}
func reconcileMySQLCurrentIterator(ctx context.Context, tx *sql.Tx, o ReconcileOptions) *reconcileIterator[reconcileCurrentRow] {
	key := analytics.ReconcileKey{}
	return &reconcileIterator[reconcileCurrentRow]{load: func() ([]reconcileCurrentRow, error) {
		page, err := reconcileCurrentPage(ctx, tx, o.OrganizationID, key, o.BatchSize)
		if len(page) > 0 {
			d := page[len(page)-1].Document
			key = reconcileKey(d.OrganizationID, d.ID, 0)
		}
		return page, err
	}}
}
func reconcileMySQLHistoryIterator(ctx context.Context, tx *sql.Tx, o ReconcileOptions) *reconcileIterator[reconcileHistoryRow] {
	key := analytics.ReconcileKey{}
	return &reconcileIterator[reconcileHistoryRow]{load: func() ([]reconcileHistoryRow, error) {
		page, err := reconcileHistoryPage(ctx, tx, o.OrganizationID, key, o.BatchSize)
		if len(page) > 0 {
			h := page[len(page)-1]
			key = reconcileKey(h.OrganizationID, h.History.TicketID, h.History.Version)
		}
		return page, err
	}}
}

func reconcileES(ctx context.Context, tx *sql.Tx, es *search.Elasticsearch, o ReconcileOptions, r *ReconcileReport) error {
	if err := es.Refresh(ctx); err != nil {
		return err
	}
	pit, err := es.OpenReconcileView(ctx)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = es.CloseReconcileView(closeCtx, pit)
	}()
	var after []json.RawMessage
	target := &reconcileIterator[search.ReconcileHit]{load: func() ([]search.ReconcileHit, error) {
		page, next, err := es.ReconcilePage(ctx, pit, o.OrganizationID, after, o.BatchSize)
		pit = next
		if len(page) > 0 {
			cursor := page[len(page)-1].Sort
			before, _ := json.Marshal(after)
			following, _ := json.Marshal(cursor)
			if string(before) == string(following) {
				return nil, fmt.Errorf("ES reconciliation cursor did not advance")
			}
			after = cursor
		}
		return page, err
	}}
	source := reconcileMySQLCurrentIterator(ctx, tx, o)
	s, sok, err := source.next()
	if err != nil {
		return err
	}
	t, tok, err := target.next()
	if err != nil {
		return err
	}
	digest := sha256.New()
	consume := func() error {
		r.ES.CurrentCount++
		reconcileHash(digest, struct {
			ID              string
			ExternalVersion int64
			Document        search.Document
		}{t.ID, t.ExternalVersion, t.Document})
		var err error
		t, tok, err = target.next()
		return err
	}
	for sok || tok {
		sk := reconcileKey(s.Document.OrganizationID, s.Document.ID, 0)
		tk := reconcileKey(t.Document.OrganizationID, t.Document.ID, 0)
		if !tok || sok && reconcileKeyCompare(sk, tk) < 0 {
			r.difference("es", sk, "missing_document", s.Document, nil)
			s, sok, err = source.next()
			if err != nil {
				return err
			}
			continue
		}
		if !sok || reconcileKeyCompare(sk, tk) > 0 {
			r.difference("es", tk, "extra_document", nil, t)
			if err = consume(); err != nil {
				return err
			}
			continue
		}
		r.compare("es", sk, s.Document, t.Document)
		if t.ExternalVersion != s.Document.Version {
			r.difference("es", sk, "_version", s.Document.Version, t.ExternalVersion)
		}
		if t.ID != strconv.FormatInt(s.Document.ID, 10) {
			r.difference("es", sk, "_id", strconv.FormatInt(s.Document.ID, 10), t.ID)
		}
		s, sok, err = source.next()
		if err != nil {
			return err
		}
		if err = consume(); err != nil {
			return err
		}
	}
	r.ES.CurrentSHA256 = reconcileSHA(digest)
	return nil
}

func reconcileCK(ctx context.Context, tx *sql.Tx, ck *analytics.ClickHouse, o ReconcileOptions, r *ReconcileReport) error {
	source := reconcileMySQLCurrentIterator(ctx, tx, o)
	key := analytics.ReconcileKey{}
	target := &reconcileIterator[analytics.ReconcileCurrent]{load: func() ([]analytics.ReconcileCurrent, error) {
		page, err := ck.ReconcileCurrentPage(ctx, o.OrganizationID, key, o.BatchSize)
		if len(page) > 0 {
			v := page[len(page)-1]
			next := reconcileKey(v.OrganizationID, v.TicketID, 0)
			if reconcileKeyCompare(next, key) <= 0 {
				return nil, fmt.Errorf("CK current cursor did not advance")
			}
			key = next
		}
		return page, err
	}}
	s, sok, err := source.next()
	if err != nil {
		return err
	}
	t, tok, err := target.next()
	if err != nil {
		return err
	}
	digest := sha256.New()
	consume := func() error {
		r.CK.CurrentCount++
		r.CK.PhysicalCurrentRows += t.PhysicalRows
		reconcileHash(digest, t.Current)
		if t.Variants != 1 {
			r.CK.ConflictingKeys++
			r.difference("ck_current", reconcileKey(t.OrganizationID, t.TicketID, t.Version), "same_key_content_variants", 1, t.Variants)
		}
		var err error
		t, tok, err = target.next()
		return err
	}
	for sok || tok {
		sk := reconcileKey(s.Document.OrganizationID, s.Document.ID, 0)
		tk := reconcileKey(t.OrganizationID, t.TicketID, 0)
		if !tok || sok && reconcileKeyCompare(sk, tk) < 0 {
			r.difference("ck_current", sk, "missing_current", reconcileCurrent(s), nil)
			s, sok, err = source.next()
			if err != nil {
				return err
			}
			continue
		}
		if !sok || reconcileKeyCompare(sk, tk) > 0 {
			r.difference("ck_current", tk, "extra_current", nil, t.Current)
			if err = consume(); err != nil {
				return err
			}
			continue
		}
		r.compare("ck_current", sk, reconcileCurrent(s), t.Current)
		s, sok, err = source.next()
		if err != nil {
			return err
		}
		if err = consume(); err != nil {
			return err
		}
	}
	r.CK.CurrentSHA256 = reconcileSHA(digest)
	return reconcileCKHistory(ctx, tx, ck, o, r)
}
func reconcileCKHistory(ctx context.Context, tx *sql.Tx, ck *analytics.ClickHouse, o ReconcileOptions, r *ReconcileReport) error {
	source := reconcileMySQLHistoryIterator(ctx, tx, o)
	key := analytics.ReconcileKey{}
	target := &reconcileIterator[analytics.ReconcileFact]{load: func() ([]analytics.ReconcileFact, error) {
		page, err := ck.ReconcileFactPage(ctx, o.OrganizationID, key, o.BatchSize)
		if len(page) > 0 {
			v := page[len(page)-1]
			next := reconcileKey(v.OrganizationID, v.TicketID, v.Version)
			if reconcileKeyCompare(next, key) <= 0 {
				return nil, fmt.Errorf("CK history cursor did not advance")
			}
			key = next
		}
		return page, err
	}}
	s, sok, err := source.next()
	if err != nil {
		return err
	}
	t, tok, err := target.next()
	if err != nil {
		return err
	}
	digest := sha256.New()
	lastTicket := analytics.ReconcileKey{}
	resolvedSeen := false
	consume := func() error {
		tk := reconcileKey(t.OrganizationID, t.TicketID, 0)
		if tk != lastTicket {
			resolvedSeen = false
			lastTicket = tk
		}
		r.CK.HistoryCount++
		r.CK.PhysicalHistoryRows += t.PhysicalRows
		t.TransportEventID = ""
		reconcileHash(digest, t.Fact)
		if t.Variants != 1 {
			r.CK.ConflictingKeys++
			r.difference("ck_history", reconcileKey(t.OrganizationID, t.TicketID, t.Version), "same_key_content_variants", 1, t.Variants)
		}
		if t.Action == "resolved" && t.AfterStatus == "awaiting_confirmation" && !resolvedSeen {
			r.CK.FirstResolved++
			resolvedSeen = true
		}
		if reconcileReopen(t.Action) {
			r.CK.Reopened++
		}
		var err error
		t, tok, err = target.next()
		return err
	}
	for sok || tok {
		sk := reconcileKey(s.OrganizationID, s.History.TicketID, s.History.Version)
		tk := reconcileKey(t.OrganizationID, t.TicketID, t.Version)
		if !tok || sok && reconcileKeyCompare(sk, tk) < 0 {
			expected, err := reconcileHistoryFact(s)
			if err != nil {
				return err
			}
			r.difference("ck_history", sk, "missing_fact", expected, nil)
			s, sok, err = source.next()
			if err != nil {
				return err
			}
			continue
		}
		if !sok || reconcileKeyCompare(sk, tk) > 0 {
			t.TransportEventID = ""
			r.difference("ck_history", tk, "extra_fact", nil, t.Fact)
			if err = consume(); err != nil {
				return err
			}
			continue
		}
		expected, err := reconcileHistoryFact(s)
		if err != nil {
			return err
		}
		actual := t.Fact
		actual.TransportEventID = ""
		r.compare("ck_history", sk, expected, actual)
		s, sok, err = source.next()
		if err != nil {
			return err
		}
		if err = consume(); err != nil {
			return err
		}
	}
	r.CK.HistorySHA256 = reconcileSHA(digest)
	if r.CK.HistoryCount != r.Source.Histories {
		r.difference("ck_history", analytics.ReconcileKey{}, "canonical_event_count", r.Source.Histories, r.CK.HistoryCount)
	}
	if r.CK.FirstResolved != r.Source.FirstResolved {
		r.difference("ck_history", analytics.ReconcileKey{}, "first_resolved_count", r.Source.FirstResolved, r.CK.FirstResolved)
	}
	if r.CK.Reopened != r.Source.Reopened {
		r.difference("ck_history", analytics.ReconcileKey{}, "reopened_count", r.Source.Reopened, r.CK.Reopened)
	}
	return nil
}
