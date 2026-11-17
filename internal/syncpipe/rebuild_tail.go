package syncpipe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/search"
)

func (r *Rebuilder) tailClient(ctx context.Context, j RebuildJob) (*kgo.Client, error) {
	topics, err := r.Queue.Admin.ListTopics(ctx, r.Queue.Topic)
	if err != nil {
		return nil, err
	}
	detail, ok := topics[r.Queue.Topic]
	if !ok || detail.Err != nil || detail.ID.String() != j.TopicID || len(detail.Partitions) != int(r.Queue.Partitions) {
		return nil, fmt.Errorf("REBUILD_KAFKA_CHANGED: topic identity/partitions differ")
	}
	starts, err := r.Queue.Admin.ListStartOffsets(ctx, r.Queue.Topic)
	if err != nil {
		return nil, err
	}
	ends, err := r.Queue.Admin.ListEndOffsets(ctx, r.Queue.Topic)
	if err != nil {
		return nil, err
	}
	assigned := map[int32]kgo.Offset{}
	if len(j.NextOffsets) != int(r.Queue.Partitions) {
		return nil, fmt.Errorf("rebuild partition progress incomplete")
	}
	for p, n := range j.NextOffsets {
		start, ok := starts.Lookup(r.Queue.Topic, p)
		if !ok || start.Err != nil {
			return nil, fmt.Errorf("rebuild retained offset unavailable")
		}
		end, ok := ends.Lookup(r.Queue.Topic, p)
		if !ok || end.Err != nil {
			return nil, fmt.Errorf("rebuild end offset unavailable")
		}
		if n < start.Offset {
			return nil, fmt.Errorf("REBUILD_KAFKA_OFFSET_EXPIRED: frozen overlap is no longer retained; create a new MySQL rebuild job")
		}
		if n > end.Offset {
			return nil, fmt.Errorf("rebuild offset exceeds Kafka end")
		}
		assigned[p] = kgo.NewOffset().At(n)
	}
	return kgo.NewClient(kgo.SeedBrokers(r.Queue.Brokers...), kgo.ClientID("ticketpulse-rebuild-"+j.ID), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{r.Queue.Topic: assigned}), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchMaxWait(100*time.Millisecond), kgo.FetchMaxBytes(4<<20), kgo.RequestTimeoutOverhead(2*time.Second), kgo.RequestRetries(3))
}
func (r *Rebuilder) barrier(ctx context.Context, j *RebuildJob) error {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	id := hex.EncodeToString(raw)
	tx, err := r.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO sync_source_watermark(source_name,barrier_id,issued_at) VALUES('tickets-v1',?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE barrier_id=VALUES(barrier_id),issued_at=VALUES(issued_at)`, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sync_rebuild_jobs SET barrier_id=?,updated_at=UTC_TIMESTAMP(6) WHERE job_id=?`, id, j.ID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sync_rebuild_offsets SET barrier_id=NULL WHERE job_id=?`, j.ID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	j.BarrierID = id
	return r.log(ctx, j.ID, "catchup", "source_barrier_issued", map[string]any{"barrier_id": id, "next_offsets": j.NextOffsets})
}
func (r *Rebuilder) catchup(ctx context.Context, j *RebuildJob, es *search.Elasticsearch, ck *analytics.ClickHouse, follow bool) error {
	if err := r.verifyIdentities(ctx, *j, es, ck); err != nil {
		return err
	}
	client, err := r.tailClient(ctx, *j)
	if err != nil {
		return err
	}
	defer client.Close()
	if err = r.barrier(ctx, j); err != nil {
		return err
	}
	confirmed := map[int32]bool{}
	for ctx.Err() == nil {
		if err = r.checkWriter(ctx, j.ID); err != nil {
			return err
		}
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetched := client.PollRecords(pollCtx, j.Options.BatchSize)
		cancel()
		// A recreated/deleted topic can first surface as UNKNOWN_TOPIC_ID from
		// the fetch client. Check persisted identity before classifying that error.
		if err = r.verifyIdentities(ctx, *j, es, ck); err != nil {
			return err
		}
		if err := fetched.Err(); err != nil && fetched.NumRecords() == 0 {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("rebuild increment read failed: %w", err)
		}
		for _, record := range fetched.Records() {
			if err = r.checkWriter(ctx, j.ID); err != nil {
				return err
			}
			if record.Offset < j.NextOffsets[record.Partition] {
				continue
			}
			if record.Offset != j.NextOffsets[record.Partition] {
				return fmt.Errorf("REBUILD_KAFKA_OFFSET_GAP: partition %d expected %d received %d", record.Partition, j.NextOffsets[record.Partition], record.Offset)
			}
			var event Event
			if err = json.Unmarshal(record.Value, &event); err != nil {
				return fmt.Errorf("rebuild increment decode: %w", err)
			}
			if event.Commit != nil && event.Commit.ServerUUID != j.SourceUUID {
				return fmt.Errorf("REBUILD_SOURCE_CHANGED: increment belongs to another MySQL UUID")
			}
			if err = r.apply(ctx, es, ck, event); err != nil {
				_ = r.log(ctx, j.ID, "catchup", "record_failed", map[string]any{"record": Reference(record), "event_id": event.ID, "error": err.Error()})
				return err
			}
			marker := event.IsWatermark() && event.Source == "binlog" && event.ID == j.BarrierID
			var barrier any
			if marker {
				barrier = j.BarrierID
				confirmed[record.Partition] = true
			}
			for _, lease := range r.writerLeases {
				if err = lease.Check(ctx); err != nil {
					return err
				}
			}
			_, err = r.Store.DB.ExecContext(ctx, `UPDATE sync_rebuild_offsets SET next_offset=?,barrier_id=COALESCE(?,barrier_id) WHERE job_id=? AND partition_id=?`, record.Offset+1, barrier, j.ID, record.Partition)
			if err != nil {
				return err
			}
			j.NextOffsets[record.Partition] = record.Offset + 1
		}
		if fetched.NumRecords() > 0 {
			if err = r.log(ctx, j.ID, "catchup", "batch_applied", map[string]any{"next_offsets": j.NextOffsets, "source_barrier": j.BarrierID, "confirmed_partitions": len(confirmed)}); err != nil {
				return err
			}
		}
		if !follow && len(confirmed) == int(r.Queue.Partitions) {
			return r.log(ctx, j.ID, "catchup", "source_barrier_confirmed", map[string]any{"barrier_id": j.BarrierID, "next_offsets": j.NextOffsets})
		}
	}
	return fmt.Errorf("REBUILD_CATCHUP_TIMEOUT_OR_INTERRUPTION: %w", ctx.Err())
}

// Continue applying increments to an approved candidate without changing any
// live consumer group. A completed job is a point-in-time proof, not a daemon.
func (r *Rebuilder) Follow(ctx context.Context, id string) (err error) {
	operation := *r
	operation.writerLeases = nil
	r = &operation
	release, err := r.lock(ctx, id)
	if err != nil {
		return err
	}
	defer release()
	j, err := r.Status(ctx, id)
	if err != nil {
		return err
	}
	closeTargets, err := r.acquireTargets(ctx, j)
	if err != nil {
		return err
	}
	defer closeTargets()
	defer func() {
		save, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		state := "failed"
		if ctx.Err() != nil {
			state = "interrupted"
		}
		_ = r.state(save, id, state, "catchup", err)
	}()
	if j.Applied != j.Total || j.Total == 0 && j.SourceSHA256 == "" {
		return fmt.Errorf("follow requires an applied frozen snapshot")
	}
	es, ck, err := r.targets(ctx, &j)
	if err != nil {
		return err
	}
	if err = r.state(ctx, id, "following", "catchup", nil); err != nil {
		return err
	}
	return r.catchup(ctx, &j, es, ck, true)
}
