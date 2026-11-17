package syncpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/syncpipe/linkerrors"
	"ticketpulse/internal/ticket"
)

type CKConsumer struct {
	Store           *StateStore
	Queue           *Queue
	CK              *analytics.ClickHouse
	RedeliveryDelay time.Duration
	Check           func(context.Context) error
	Target          *TargetConsumerConfig
}

func AnalysisProjection(e Event) (*analytics.Fact, analytics.Current, error) {
	if err := e.Validate(); err != nil {
		return nil, analytics.Current{}, err
	}
	if e.IsWatermark() {
		return nil, analytics.Current{}, nil
	}
	d := e.Document
	first := e.FirstResolvedAt
	complete := 0
	if e.Schema == 2 {
		complete = 1
	}
	var fact *analytics.Fact
	if h := e.History; h != nil {
		if h.After.Version != e.Version || h.After.CurrentTeamID != h.ResponsibleTeamID || h.After.InitialTeamID != d.InitialTeamID || h.After.CreatorID != d.CreatorID || h.After.Status != d.Status || !h.OccurredAt.Equal(d.UpdatedAt) || h.TicketID != d.ID || h.Version != d.Version {
			return nil, analytics.Current{}, fmt.Errorf("history does not match committed ticket version/team/time")
		}
		if h.Before != nil && h.Before.Version != h.Version-1 {
			return nil, analytics.Current{}, fmt.Errorf("history predecessor version is invalid")
		}
		if h.Action == ticket.ActionCreated && (h.Version != 1 || h.Before != nil || h.After.Status != ticket.StatusPending || h.ResponsibleTeamID != d.InitialTeamID) {
			return nil, analytics.Current{}, fmt.Errorf("invalid creation fact")
		}
		if h.Action == ticket.ActionResolved && (h.Before == nil || h.Before.Status != ticket.StatusInProgress || h.After.Status != ticket.StatusAwaitingConfirmation || h.After.FirstResolvedAt == nil) {
			return nil, analytics.Current{}, fmt.Errorf("invalid resolution fact")
		}
		first = h.After.FirstResolvedAt
		complete = 1
		if first != nil && first.Before(d.CreatedAt) {
			return nil, analytics.Current{}, fmt.Errorf("resolution precedes creation")
		}
		f := analytics.Fact{OrganizationID: d.OrganizationID, TicketID: d.ID, Version: d.Version, BusinessEventID: fmt.Sprintf("%d:%d:%d", d.OrganizationID, d.ID, d.Version), TransportEventID: e.ID, Action: h.Action, OccurredAt: analytics.Timestamp(h.OccurredAt), EventTeamID: h.ResponsibleTeamID, InitialTeamID: h.After.InitialTeamID, AfterStatus: h.After.Status, AssigneeID: h.After.AssigneeID, CreatedAt: analytics.Timestamp(d.CreatedAt), FirstResolvedAt: analytics.OptionalTimestamp(first)}
		if h.Before != nil {
			team := h.Before.CurrentTeamID
			f.BeforeTeamID = &team
			f.BeforeStatus = h.Before.Status
		}
		fact = &f
	}
	current := analytics.Current{OrganizationID: d.OrganizationID, TicketID: d.ID, Version: d.Version, Complete: complete, CurrentTeamID: d.CurrentTeamID, AssigneeID: d.AssigneeID, Status: d.Status, InitialTeamID: d.InitialTeamID, CreatedAt: analytics.Timestamp(d.CreatedAt), UpdatedAt: analytics.Timestamp(d.UpdatedAt), FirstResolvedAt: analytics.OptionalTimestamp(first)}
	return fact, current, nil
}

func (c *CKConsumer) Run(ctx context.Context) (err error) {
	store := c.Store.ForConsumer("ck")
	check := c.Check
	if c.Target == nil {
		if err := rejectUnmanagedCandidate(ctx, store, "ck", c.CK.Endpoint(), c.CK.Database()); err != nil {
			return err
		}
	}
	if c.Target != nil {
		if c.CK.Endpoint() != c.Target.Version.Contract.CK.Endpoint || c.CK.Database() != c.Target.Version.Contract.CK.Database || c.Queue.Target != c.Target || c.Queue.Topic != c.Target.Version.Contract.Kafka.Topic || c.Queue.Partitions != int32(len(c.Target.Version.Contract.Kafka.Partitions)) || !reflect.DeepEqual(c.Queue.Brokers, c.Target.Control.Queue.Brokers) {
			return fmt.Errorf("%w: actual CK writer/queue differs from handoff", projection.ErrTargetMismatch)
		}
		var lease *projection.WriterLease
		store, lease, err = c.Target.Acquire(ctx, "ck")
		if err != nil {
			return err
		}
		defer lease.Close()
		check = func(ctx context.Context) error {
			if err := c.Target.Check(ctx); err != nil {
				return err
			}
			return lease.Check(ctx)
		}
	}
	if check != nil {
		if err := check(ctx); err != nil {
			return err
		}
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		state := "stopped"
		if err != nil && ctx.Err() == nil {
			state = "failed"
		}
		_ = store.Heartbeat(shutdown, "ck", state, nil, err)
	}()
	if check == nil {
		if err := store.VerifyQueue(ctx, c.Queue); err != nil {
			return err
		}
	}
	if check == nil {
		if err := c.CK.Ensure(ctx); err != nil {
			return err
		}
	}
	initialized := c.Target != nil // completeness comes from the sealed M4 report.
	if c.Target == nil {
		if err := store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_kafka_analytics_bootstrap WHERE pipeline='ck-v1')").Scan(&initialized); err != nil {
			return err
		}
	}
	if !initialized {
		if check != nil {
			return fmt.Errorf("managed CK requires an existing analytics bootstrap; no takeover/bootstrap allowed")
		}
		for {
			checkpoint, err := store.Checkpoint(ctx)
			if err == nil && checkpoint.BootstrapComplete {
				break
			}
			if err != nil && err != sql.ErrNoRows {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		if _, _, err := AnalyticsSnapshot(ctx, store, c.Queue); err != nil {
			return err
		}
	}
	workQueue := *c.Queue
	if c.Target == nil {
		observer, e := RegisterLegacyObservation(ctx, store, c.Queue, "ck", nil, c.CK)
		if e != nil {
			return e
		}
		workQueue.LegacyObservation = observer
		if observer != nil {
			defer observer.Close()
		}
	}
	if err := store.Heartbeat(ctx, "ck", "running", nil, nil); err != nil {
		return err
	}
	stop := store.KeepAlive(ctx, "ck")
	defer stop()
	return workQueue.Consume(ctx, "ck", c.RedeliveryDelay, func(ctx context.Context, record *kgo.Record) error {
		if check != nil {
			if err := check(ctx); err != nil {
				return err
			}
		}
		if c.Target != nil {
			member, gen := c.Target.currentMembership()
			if err := store.GroupMembership(ctx, member, gen); err != nil {
				return err
			}
		}
		ref := Reference(record)
		var event Event
		problem := json.Unmarshal(record.Value, &event)
		var fact *analytics.Fact
		var current analytics.Current
		if problem == nil {
			fact, current, problem = AnalysisProjection(event)
		}
		if problem == nil && c.Target != nil && event.Commit != nil && event.Commit.ServerUUID != c.Target.Version.Contract.Source.ServerUUID {
			return fmt.Errorf("%w: record source UUID differs", projection.ErrTargetMismatch)
		}
		if problem != nil {
			if err := store.Delivery(ctx, ref, event, "isolated", 1, fmt.Errorf("invalid analysis input: %w", problem), record.Value); err != nil {
				return err
			}
			_ = store.Heartbeat(ctx, "ck", "degraded", nil, problem)
			return nil
		}
		problem = Retry(ctx, RetryPolicy{MaxAttempts: 3, BaseBackoff: 100 * time.Millisecond}, func() error {
			if err := store.Delivery(ctx, ref, event, "processing", 1, nil, nil); err != nil {
				return linkerrors.System("ck_progress", err)
			}
			if event.IsWatermark() {
				return nil
			}
			classify := func(err error) error {
				if err == nil {
					return nil
				}
				var write *analytics.WriteError
				if errors.As(err, &write) && write.Permanent() {
					return linkerrors.Permanent("ck_projection", err)
				}
				return linkerrors.Transient("ck_unavailable", err)
			}
			if fact != nil {
				if check != nil {
					if err := check(ctx); err != nil {
						return err
					}
				}
				if err := c.CK.InsertFact(ctx, *fact); err != nil {
					return classify(err)
				}
			}
			if check != nil {
				if err := check(ctx); err != nil {
					return err
				}
			}
			return classify(c.CK.InsertCurrent(ctx, current))
		}, nil)
		if problem != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if linkerrors.IsPermanent(problem) {
				if err := store.Delivery(ctx, ref, event, "isolated", 0, problem, record.Value); err != nil {
					return err
				}
				_ = store.Heartbeat(ctx, "ck", "degraded", nil, problem)
				return nil
			}
			if err := store.Delivery(ctx, ref, event, "retrying", 0, problem, nil); err != nil {
				return err
			}
			_ = store.Heartbeat(ctx, "ck", "retrying", nil, problem)
			return problem
		}
		if event.IsWatermark() {
			if err := store.ApplyWatermark(ctx, "ck", ref, event); err != nil {
				return err
			}
		}
		if err := store.Delivery(ctx, ref, event, "applied", 0, nil, nil); err != nil {
			return err
		}
		if err := store.FinishRecoveries(ctx, ref); err != nil {
			return err
		}
		if !event.IsWatermark() {
			return store.Heartbeat(ctx, "ck", "running", &event.EventAt, nil)
		}
		return nil
	}, check)
}
