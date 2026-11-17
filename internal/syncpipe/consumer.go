package syncpipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"reflect"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe/linkerrors"
	"time"
)

type Consumer struct {
	Store           *StateStore
	Queue           *Queue
	ES              *search.Elasticsearch
	RedeliveryDelay time.Duration
	Check           func(context.Context) error
	Target          *TargetConsumerConfig
}

func (c *Consumer) Run(ctx context.Context) (err error) {
	store := c.Store
	check := c.Check
	if c.Target == nil {
		if err := rejectUnmanagedCandidate(ctx, store, "es", c.ES.Endpoint(), c.ES.Index()); err != nil {
			return err
		}
	}
	if c.Target != nil {
		if c.ES.Endpoint() != c.Target.Version.Contract.ES.Endpoint || c.ES.Index() != c.Target.Version.Contract.ES.Index || c.Queue.Target != c.Target || c.Queue.Topic != c.Target.Version.Contract.Kafka.Topic || c.Queue.Partitions != int32(len(c.Target.Version.Contract.Kafka.Partitions)) || !reflect.DeepEqual(c.Queue.Brokers, c.Target.Control.Queue.Brokers) {
			return fmt.Errorf("%w: actual ES writer/queue differs from handoff", projection.ErrTargetMismatch)
		}
		var lease *projection.WriterLease
		store, lease, err = c.Target.Acquire(ctx, "es")
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
		_ = store.Heartbeat(shutdown, "es", state, nil, err)
	}()
	if check == nil {
		if err := store.VerifyQueue(ctx, c.Queue); err != nil {
			return err
		}
	}
	if check == nil {
		if err := c.ES.EnsureIndex(ctx); err != nil {
			return err
		}
	}
	workQueue := *c.Queue
	if c.Target == nil {
		observer, e := RegisterLegacyObservation(ctx, store, c.Queue, "es", c.ES, nil)
		if e != nil {
			return e
		}
		workQueue.LegacyObservation = observer
		if observer != nil {
			defer observer.Close()
		}
	}
	if err := store.Heartbeat(ctx, "es", "running", nil, nil); err != nil {
		return err
	}
	stop := store.KeepAlive(ctx, "es")
	defer stop()
	return workQueue.Consume(ctx, "es", c.RedeliveryDelay, func(ctx context.Context, record *kgo.Record) error {
		if check != nil {
			if err := check(ctx); err != nil {
				return err
			}
		}
		if c.Target != nil {
			member, gen := c.Queue.Target.currentMembership()
			if err := store.GroupMembership(ctx, member, gen); err != nil {
				return err
			}
		}
		ref := Reference(record)
		var event Event
		problem := json.Unmarshal(record.Value, &event)
		if problem == nil {
			problem = event.Validate()
		}
		if problem == nil && c.Target != nil && event.Commit != nil && event.Commit.ServerUUID != c.Target.Version.Contract.Source.ServerUUID {
			return fmt.Errorf("%w: record source UUID differs", projection.ErrTargetMismatch)
		}
		if problem != nil {
			if err := store.Delivery(ctx, ref, event, "isolated", 1, fmt.Errorf("invalid event: %w", problem), record.Value); err != nil {
				return err
			}
			_ = store.Heartbeat(ctx, "es", "degraded", nil, problem)
			return nil
		}
		if event.IsWatermark() {
			if err := store.ApplyWatermark(ctx, "es", ref, event); err != nil {
				return err
			}
			return store.Delivery(ctx, ref, event, "applied", 1, nil, nil)
		}
		duplicate := false
		problem = Retry(ctx, RetryPolicy{MaxAttempts: 3, BaseBackoff: 100 * time.Millisecond}, func() error {
			if err := store.Delivery(ctx, ref, event, "processing", 1, nil, nil); err != nil {
				return linkerrors.System("progress_write", err)
			}
			if check != nil {
				if err := check(ctx); err != nil {
					return err
				}
			}
			old, err := c.ES.Put(ctx, event.Document)
			duplicate = old
			if err == nil {
				return nil
			}
			var write *search.WriteError
			if errors.As(err, &write) && write.Permanent() {
				return linkerrors.Permanent("es_projection", err)
			}
			return linkerrors.Transient("es_unavailable", err)
		}, nil)
		if problem != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if linkerrors.IsPermanent(problem) {
				if err := store.Delivery(ctx, ref, event, "isolated", 0, problem, record.Value); err != nil {
					return err
				}
				_ = store.Heartbeat(ctx, "es", "degraded", nil, problem)
				return nil
			}
			if err := store.Delivery(ctx, ref, event, "retrying", 0, problem, nil); err != nil {
				return err
			}
			_ = store.Heartbeat(ctx, "es", "retrying", nil, problem)
			return problem
		}
		state := "applied"
		if duplicate {
			state = "duplicate_or_old"
		}
		if err := store.Delivery(ctx, ref, event, state, 0, nil, nil); err != nil {
			return err
		}
		if err := store.FinishRecoveries(ctx, ref); err != nil {
			return err
		}
		return store.Heartbeat(ctx, "es", "running", &event.EventAt, nil)
	}, check)
}
