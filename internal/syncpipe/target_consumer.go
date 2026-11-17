package syncpipe

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"ticketpulse/internal/projection"
)

type TargetConsumerConfig struct {
	Control *HandoffControl
	Handoff projection.Handoff
	Version projection.Version
	// Optional observation for real assignment/revoke/commit test evidence.
	Observe    func(string, any)
	lost       atomic.Bool
	membership atomic.Pointer[targetMembership]
	writer     atomic.Pointer[projection.WriterLease]
}
type targetMembership struct {
	member     string
	generation int32
}

func (t *TargetConsumerConfig) currentMembership() (string, int32) {
	if value := t.membership.Load(); value != nil {
		return value.member, value.generation
	}
	return "", -1
}
func (p *HandoffControl) ConsumerTarget(ctx context.Context, id string) (*TargetConsumerConfig, error) {
	h, v, err := p.load(ctx, id)
	if err != nil {
		return nil, err
	}
	if h.Phase != projection.Consuming && h.Phase != projection.Verified {
		return nil, fmt.Errorf("HANDOFF_NOT_CONSUMING: candidate consumption is not authorized")
	}
	if err = p.checkIdentities(ctx, v); err != nil {
		return nil, err
	}
	return &TargetConsumerConfig{Control: p, Handoff: h, Version: v}, nil
}
func (t *TargetConsumerConfig) Check(ctx context.Context) error {
	if t.lost.Load() {
		return fmt.Errorf("%w: Kafka group session lost; stop before target writes", projection.ErrWriterOwnership)
	}
	h, v, err := t.Control.load(ctx, t.Handoff.ID)
	if err != nil {
		return err
	}
	if h.Phase != projection.Consuming && h.Phase != projection.Verified {
		return fmt.Errorf("HANDOFF_NOT_CONSUMING")
	}
	if v.Fingerprint() != t.Version.Fingerprint() || h.Group("es") != t.Handoff.Group("es") || h.CKGroup != t.Handoff.CKGroup {
		return fmt.Errorf("%w: candidate identity changed", projection.ErrTargetMismatch)
	}
	if err = t.Control.checkIdentities(ctx, v); err != nil {
		return err
	}
	for _, component := range []string{"es", "ck"} {
		o, err := projection.ReadOwner(ctx, t.Control.Store.DB, projection.VersionTargetKey(v, component), "")
		if err != nil || o.Kind != "consumer_group" || o.Owner != h.Group(component) || o.HandoffID == nil || *o.HandoffID != h.ID || o.VersionID == nil || *o.VersionID != v.ID {
			return fmt.Errorf("%w: candidate %s owner mismatch: %v", projection.ErrWriterOwnership, component, err)
		}
	}
	return nil
}
func (p *HandoffControl) committedProgress(ctx context.Context, h projection.Handoff, v projection.Version, component string) ([]PartitionProgress, error) {
	starts, err := p.Queue.Admin.ListStartOffsets(ctx, p.Queue.Topic)
	if err != nil {
		return nil, err
	}
	ends, err := p.Queue.Admin.ListEndOffsets(ctx, p.Queue.Topic)
	if err != nil {
		return nil, err
	}
	offsets, err := p.rawOffsets(ctx, h.Group(component))
	if err != nil {
		return nil, err
	}
	result := []PartitionProgress{}
	for _, part := range v.Contract.Kafka.Partitions {
		start, a := starts.Lookup(p.Queue.Topic, part)
		end, b := ends.Lookup(p.Queue.Topic, part)
		o, c := offsets.Lookup(p.Queue.Topic, part)
		if !a || !b || start.Err != nil || end.Err != nil {
			return nil, fmt.Errorf("HANDOFF_OFFSET_UNAVAILABLE: Kafka range incomplete")
		}
		if !c || o.Err != nil || o.At < 0 {
			return nil, fmt.Errorf("HANDOFF_OFFSET_MISSING: %s partition %d lacks a committed offset", component, part)
		}
		if o.At < h.PinnedOffsets[part] || o.At < start.Offset || o.At > end.Offset {
			return nil, fmt.Errorf("HANDOFF_OFFSET_OUT_OF_RANGE: %s/%d commit %d, pinned %d, retained [%d,%d]", component, part, o.At, h.PinnedOffsets[part], start.Offset, end.Offset)
		}
		result = append(result, PartitionProgress{part, start.Offset, end.Offset, o.At, end.Offset - o.At})
	}
	return result, nil
}
func (t *TargetConsumerConfig) Acquire(ctx context.Context, component string) (*StateStore, *projection.WriterLease, error) {
	if err := t.Check(ctx); err != nil {
		return nil, nil, err
	}
	if _, err := t.Control.committedProgress(ctx, t.Handoff, t.Version, component); err != nil {
		return nil, nil, err
	}
	w, err := projection.AcquireWriter(ctx, t.Control.Store.DB, projection.WriterClaim{Key: projection.VersionTargetKey(t.Version, component), Component: component, Kind: "consumer_group", Owner: t.Handoff.Group(component)}, false)
	if err != nil {
		return nil, nil, err
	}
	s := t.Control.Store.ForConsumer(component)
	s.VersionID = t.Version.ID
	s.GroupID = t.Handoff.Group(component)
	s.Writer = w
	if err = t.Check(ctx); err != nil {
		w.Close()
		return nil, nil, err
	}
	t.writer.Store(w)
	return s, w, nil
}

// Legacy workers retain their normal groups, but cannot bypass a candidate's
// managed target owner merely by omitting the handoff startup parameter.
func rejectUnmanagedCandidate(ctx context.Context, store *StateStore, component, endpoint, namespace string) error {
	owner, err := projection.ReadOwner(ctx, store.DB, projection.TargetKey(component, endpoint, namespace), "")
	if projection.MissingTable(err) {
		installed, failure := projection.HandoffInstalled(ctx, store.DB)
		if failure != nil {
			return failure
		}
		if installed {
			return fmt.Errorf("HANDOFF_METADATA_UNAVAILABLE: installed target ownership table unavailable")
		}
		return nil
	}
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: target is managed by %s/%s; handoff consumer admission required", projection.ErrWriterOwnership, owner.Kind, owner.Owner)
}
func (t *TargetConsumerConfig) groupOptions(component string) []kgo.Opt {
	observe := func(stage string, data any) {
		if t.Observe != nil {
			t.Observe(stage, data)
		}
	}
	return []kgo.Opt{
		kgo.OnPartitionsAssigned(func(_ context.Context, c *kgo.Client, ps map[string][]int32) {
			member, gen := c.GroupMetadata()
			t.membership.Store(&targetMembership{member, gen})
			observe("assigned", map[string]any{"component": component, "partitions": ps, "member": member, "generation": gen})
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, ps map[string][]int32) {
			observe("revoked", map[string]any{"component": component, "partitions": ps})
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, ps map[string][]int32) {
			t.lost.Store(true)
			observe("lost", map[string]any{"component": component, "partitions": ps})
		}),
		kgo.OnOffsetsFetched(func(ctx context.Context, _ *kgo.Client, _ *kmsg.OffsetFetchResponse) error {
			if err := t.Check(ctx); err != nil {
				return err
			}
			_, err := t.Control.committedProgress(ctx, t.Handoff, t.Version, component)
			return err
		}),
	}
}

// TargetMetadata is a diagnostic read; an old ACTIVE heartbeat cannot supply
// candidate readiness. Missing candidate metadata stays unknown.
func (t *TargetConsumerConfig) TargetMetadata(ctx context.Context, component string) (map[string]any, error) {
	h, _, err := t.Control.load(ctx, t.Handoff.ID)
	if err != nil {
		return nil, err
	}
	o, err := projection.ReadOwner(ctx, t.Control.Store.DB, projection.VersionTargetKey(t.Version, component), "")
	if err != nil {
		return nil, err
	}
	var state string
	var beat *time.Time
	err = t.Control.Store.DB.QueryRowContext(ctx, "SELECT state,heartbeat_at FROM sync_projection_worker_state WHERE version_id=? AND component=? AND group_id=?", t.Version.ID, component, h.Group(component)).Scan(&state, &beat)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return map[string]any{"version_id": t.Version.ID, "handoff_phase": h.Phase, "consumer_group": h.Group(component), "owner": o, "state": state, "heartbeat_at": beat}, nil
}
