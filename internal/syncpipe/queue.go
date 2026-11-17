package syncpipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"ticketpulse/internal/syncpipe/linkerrors"
)

// Offsets are zero-based; committed offsets point to the next record.
type RecordRef struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func Reference(r *kgo.Record) RecordRef { return RecordRef{r.Topic, r.Partition, r.Offset} }

type PartitionProgress struct {
	Partition int32 `json:"partition"`
	Start     int64 `json:"log_start_offset"`
	End       int64 `json:"log_end_offset"`
	Committed int64 `json:"committed_offset"`
	Lag       int64 `json:"lag"`
}
type Queue struct {
	Client            *kgo.Client
	Admin             *kadm.Client
	Brokers           []string
	Topic             string
	Partitions        int32
	ReplicationFactor int16
	MinInSyncReplicas int
	Target            *TargetConsumerConfig  // nil keeps the original ACTIVE group behavior.
	LegacyObservation *LegacyProcessObserver // optional 015 identity evidence; never a writer lease.
}

func ConnectQueue(endpoint, schema string) (*Queue, error) {
	q, err := NewQueue(strings.Split(endpoint, ","), "ticketpulse."+schema+".tickets.v1", configuredPartitions())
	if err != nil {
		return nil, err
	}
	replicas, err := configuredReplication()
	if err != nil {
		q.Close()
		return nil, err
	}
	q.ReplicationFactor, q.MinInSyncReplicas = replicas.Factor, replicas.MinInSync
	return q, nil
}
func ConnectStatusQueue(endpoint, schema string) (*Queue, error) {
	return ConnectQueue(endpoint, schema)
}
func configuredPartitions() int32 {
	value := os.Getenv("TICKETPULSE_KAFKA_PARTITIONS")
	if value == "" {
		return 3
	}
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return -1
	}
	return int32(n)
}
func NewQueue(brokers []string, topic string, partitions int32) (*Queue, error) {
	if len(brokers) == 0 || partitions < 1 || partitions > 64 || !regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,249}$`).MatchString(topic) {
		return nil, fmt.Errorf("invalid Kafka brokers/topic/partitions")
	}
	for i, b := range brokers {
		brokers[i] = strings.TrimSpace(b)
		if brokers[i] == "" || strings.ContainsAny(brokers[i], " /?#@") {
			return nil, fmt.Errorf("Kafka bootstrap broker must be host:port")
		}
	}
	keyed := kgo.StickyKeyPartitioner(nil)
	partitioner := kgo.BasicConsistentPartitioner(func(topic string) func(*kgo.Record, int) int {
		p := keyed.ForTopic(topic)
		return func(r *kgo.Record, n int) int {
			if r.Partition >= 0 {
				return int(r.Partition)
			}
			return p.Partition(r, n)
		}
	})
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...), kgo.ClientID("ticketpulse-cdc"), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordPartitioner(partitioner), kgo.RecordRetries(5), kgo.UnknownTopicRetries(3), kgo.RecordDeliveryTimeout(10 * time.Second), kgo.ProduceRequestTimeout(3 * time.Second), kgo.RequestTimeoutOverhead(2 * time.Second), kgo.RequestRetries(3), kgo.MaxBufferedRecords(1000), kgo.MaxBufferedBytes(8 << 20)}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	// Idempotent production is enabled by franz-go by default; never disable it.
	return &Queue{Client: client, Admin: kadm.NewClient(client), Brokers: brokers, Topic: topic, Partitions: partitions, ReplicationFactor: 1, MinInSyncReplicas: 1}, nil
}
func (q *Queue) Close() { q.Client.Close() }
func (q *Queue) Ensure(ctx context.Context) error {
	if err := (TopicReplication{q.ReplicationFactor, q.MinInSyncReplicas}).Validate(); err != nil {
		return err
	}
	configs := map[string]*string{"cleanup.policy": kadm.StringPtr("delete"), "retention.ms": kadm.StringPtr("-1"), "retention.bytes": kadm.StringPtr("-1"), "min.insync.replicas": kadm.StringPtr(strconv.Itoa(q.MinInSyncReplicas)), "unclean.leader.election.enable": kadm.StringPtr("false"), "max.message.bytes": kadm.StringPtr("524288")}
	result, err := q.Admin.CreateTopics(ctx, q.Partitions, q.ReplicationFactor, configs, q.Topic)
	if err != nil {
		return err
	}
	if topic, ok := result[q.Topic]; !ok || (topic.Err != nil && !errors.Is(topic.Err, kerr.TopicAlreadyExists)) {
		return fmt.Errorf("create Kafka event topic: %+v", result)
	}
	// CreateTopics can be acknowledged before every Broker has applied the
	// new Topic metadata. Wait for existence before validating its immutable
	// partition/replica configuration; an absent Topic is not repartitioning.
	metadataCtx, metadataCancel := context.WithTimeout(ctx, 10*time.Second)
	defer metadataCancel()
	var detail kadm.TopicDetail
	for {
		topics, err := q.Admin.ListTopics(metadataCtx, q.Topic)
		if err != nil {
			return err
		}
		var found bool
		detail, found = topics[q.Topic]
		if found && detail.Err == nil {
			break
		}
		if found && !errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
			return detail.Err
		}
		select {
		case <-metadataCtx.Done():
			return fmt.Errorf("Kafka topic metadata not ready: %w", metadataCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if len(detail.Partitions) != int(q.Partitions) {
		return fmt.Errorf("Kafka topic partition count differs; stop and explicitly review repartitioning")
	}
	for _, p := range detail.Partitions {
		if len(p.Replicas) != int(q.ReplicationFactor) {
			return fmt.Errorf("Kafka topic replication factor differs; explicit replica reassignment required")
		}
	}
	configCtx, configCancel := context.WithTimeout(ctx, 10*time.Second)
	defer configCancel()
	var settings kadm.ResourceConfig
	for {
		actual, err := q.Admin.DescribeTopicConfigs(configCtx, q.Topic)
		if err != nil {
			return err
		}
		settings, err = actual.On(q.Topic, nil)
		if err == nil && settings.Err == nil {
			break
		}
		if err == nil {
			err = settings.Err
		}
		if !errors.Is(err, kerr.UnknownTopicOrPartition) {
			return fmt.Errorf("Kafka topic configuration unavailable: %w", err)
		}
		select {
		case <-configCtx.Done():
			return fmt.Errorf("Kafka topic configuration not ready: %w", configCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	for _, key := range []string{"cleanup.policy", "retention.ms", "retention.bytes", "unclean.leader.election.enable", "min.insync.replicas"} {
		valid := false
		for _, config := range settings.Configs {
			if config.Key == key && config.Value != nil && *config.Value == *configs[key] {
				valid = true
			}
		}
		if !valid {
			return fmt.Errorf("Kafka topic durability/retention config differs: %s", key)
		}
	}
	// Topic creation metadata can precede the broker's leader readiness. Verify
	// offsets at every leader before the first snapshot attempts a publication.
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		ends, err := q.Admin.ListEndOffsets(readyCtx, q.Topic)
		ready := err == nil && len(ends[q.Topic]) == int(q.Partitions)
		for _, offset := range ends[q.Topic] {
			if offset.Err != nil {
				ready = false
			}
		}
		if ready {
			break
		}
		select {
		case <-readyCtx.Done():
			return fmt.Errorf("Kafka partition leaders not ready: %w", readyCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil
}
func (q *Queue) Publish(ctx context.Context, e Event) (RecordRef, error) {
	if err := e.Validate(); err != nil {
		return RecordRef{}, err
	}
	if e.IsWatermark() {
		return RecordRef{}, fmt.Errorf("use per-partition PublishBarrier for a source watermark")
	}
	if e.EventType == "" {
		if e.History != nil {
			e.EventType = e.History.Action
		} else if e.IsWatermark() {
			e.EventType = "watermark"
		} else {
			e.EventType = "ticket_snapshot"
		}
	}
	data, err := json.Marshal(e)
	if err != nil {
		return RecordRef{}, err
	}
	return q.publish(ctx, data, e.ID, strconv.FormatInt(e.TicketID, 10), -1)
}
func (q *Queue) PublishBytes(ctx context.Context, data []byte, id string) (RecordRef, error) {
	var e Event
	_ = json.Unmarshal(data, &e)
	return q.publish(ctx, data, id, strconv.FormatInt(e.TicketID, 10), -1)
}
func (q *Queue) publish(ctx context.Context, data []byte, id, key string, partition int32) (RecordRef, error) {
	if len(data) > 512<<10 {
		return RecordRef{}, fmt.Errorf("Kafka event exceeds 512 KiB")
	}
	publishCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	record := &kgo.Record{Topic: q.Topic, Partition: partition, Key: []byte(key), Value: data, Headers: []kgo.RecordHeader{{Key: "event_id", Value: []byte(id)}}}
	type confirmation struct {
		ref RecordRef
		err error
	}
	confirmed := make(chan confirmation, 1)
	q.Client.Produce(publishCtx, record, func(r *kgo.Record, err error) {
		confirmed <- confirmation{Reference(r), err}
	})
	select {
	case result := <-confirmed:
		if result.err != nil {
			return RecordRef{}, fmt.Errorf("Kafka publication not confirmed: %w", result.err)
		}
		return result.ref, nil
	case <-publishCtx.Done():
		// An idempotent in-flight batch can outlive RecordDeliveryTimeout while
		// Kafka's outcome is ambiguous. Bound the caller without aborting its
		// sequence or treating a possibly persisted record as acknowledged. A
		// late callback fits the buffered channel; CDC safely replays its last
		// durable checkpoint and never advances on this unconfirmed result.
		return RecordRef{}, fmt.Errorf("Kafka publication not confirmed (outcome may be unknown): %w", publishCtx.Err())
	}
}

// A marker is written to every partition after prior CDC publications.
func (q *Queue) PublishBarrier(ctx context.Context, e Event) (map[int32]int64, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	if !e.IsWatermark() {
		return nil, fmt.Errorf("not a watermark")
	}
	if e.EventType == "" {
		if e.History != nil {
			e.EventType = e.History.Action
		} else if e.IsWatermark() {
			e.EventType = "watermark"
		} else {
			e.EventType = "ticket_snapshot"
		}
	}
	data, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	offsets := map[int32]int64{}
	for p := int32(0); p < q.Partitions; p++ {
		r, err := q.publish(ctx, data, e.ID+":"+fmt.Sprint(p), "watermark", p)
		if err != nil {
			return nil, err
		}
		offsets[p] = r.Offset + 1
	}
	return offsets, nil
}
func (q *Queue) Group(component string) string {
	if q.Target != nil {
		return q.Target.Handoff.Group(component)
	}
	return q.Topic + "." + component + ".v1"
}
func (q *Queue) ConsumerClient(component string) (*kgo.Client, error) {
	if component != "es" && component != "ck" {
		return nil, fmt.Errorf("unknown projection consumer")
	}
	reset := kgo.NoResetOffset().AtStart()
	if q.Target != nil {
		reset = kgo.NewOffset().AtCommitted()
	}
	opts := []kgo.Opt{kgo.SeedBrokers(q.Brokers...), kgo.ClientID("ticketpulse-" + component), kgo.ConsumerGroup(q.Group(component)), kgo.ConsumeTopics(q.Topic), kgo.ConsumeResetOffset(reset), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.SessionTimeout(10 * time.Second), kgo.RebalanceTimeout(60 * time.Second), kgo.FetchMaxWait(250 * time.Millisecond), kgo.FetchMaxBytes(4 << 20), kgo.FetchMaxPartitionBytes(1 << 20), kgo.RequestTimeoutOverhead(2 * time.Second), kgo.RequestRetries(3)}
	if q.Target != nil {
		opts = append(opts, q.Target.groupOptions(component)...)
	}
	return kgo.NewClient(opts...)
}
func (q *Queue) Progress(ctx context.Context, component string) ([]PartitionProgress, error) {
	if q.Target != nil {
		return q.Target.Control.committedProgress(ctx, q.Target.Handoff, q.Target.Version, component)
	}
	ends, err := q.Admin.ListEndOffsets(ctx, q.Topic)
	if err != nil {
		return nil, err
	}
	starts, err := q.Admin.ListStartOffsets(ctx, q.Topic)
	if err != nil {
		return nil, err
	}
	committed, err := q.Admin.FetchOffsets(ctx, q.Group(component))
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return nil, err
	}
	progress := []PartitionProgress{}
	for p := int32(0); p < q.Partitions; p++ {
		end, ok := ends.Lookup(q.Topic, p)
		if !ok || end.Err != nil {
			return nil, fmt.Errorf("Kafka end offset unavailable")
		}
		start, _ := starts.Lookup(q.Topic, p)
		if start.Err != nil {
			return nil, start.Err
		}
		offset, ok := committed.Lookup(q.Topic, p)
		next := start.Offset
		if ok && offset.At >= 0 {
			if offset.Err != nil {
				return nil, offset.Err
			}
			next = offset.At
		}
		if next < start.Offset {
			return nil, fmt.Errorf("KAFKA_OFFSET_EXPIRED: retained log no longer covers committed offset")
		}
		lag := end.Offset - next
		if lag < 0 {
			return nil, fmt.Errorf("Kafka committed offset exceeds log end")
		}
		progress = append(progress, PartitionProgress{p, start.Offset, end.Offset, next, lag})
	}
	return progress, nil
}
func (q *Queue) DeleteTestTopic(ctx context.Context) error {
	if !strings.Contains(q.Topic, "ticketpulse_test_") {
		return fmt.Errorf("refusing to delete non-test Kafka topic")
	}
	_, _ = q.Admin.DeleteGroups(ctx, q.Group("es"), q.Group("ck"))
	responses, err := q.Admin.DeleteTopics(ctx, q.Topic)
	if err != nil {
		return err
	}
	if response := responses[q.Topic]; response.Err != nil && !errors.Is(response.Err, kerr.UnknownTopicOrPartition) {
		return response.Err
	}
	return nil
}

// Read one retained record for local diagnostics and protocol verification.
// It has no consumer group and cannot change projection offsets.
func (q *Queue) ReadRecord(ctx context.Context, ref RecordRef) ([]byte, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(q.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{ref.Topic: {ref.Partition: kgo.NewOffset().At(ref.Offset)}}), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		return nil, err
	}
	defer client.Close()
	fetched := client.PollRecords(ctx, 1)
	if err := fetched.Err(); err != nil {
		return nil, err
	}
	records := fetched.Records()
	if len(records) != 1 || Reference(records[0]) != ref {
		return nil, fmt.Errorf("Kafka record not available at requested offset")
	}
	return records[0].Value, nil
}

// Bounded batch; partitions process concurrently, records within a partition in
// order. Only a failed partition rewinds; no successful commit passes its hole.
func (q *Queue) Consume(ctx context.Context, component string, delay time.Duration, process func(context.Context, *kgo.Record) error, checks ...func(context.Context) error) error {
	check := func(ctx context.Context) error {
		if q.Target != nil {
			writer := q.Target.writer.Load()
			if writer == nil || writer.Claim.Component != component {
				return fmt.Errorf("candidate queue requires a registered component writer")
			}
			if err := writer.Check(ctx); err != nil {
				return err
			}
		}
		for _, fn := range checks {
			if fn != nil {
				if err := fn(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := check(ctx); err != nil {
		return err
	}
	client, err := q.ConsumerClient(component)
	if err != nil {
		return err
	}
	defer client.Close()
	if delay <= 0 {
		delay = 2 * time.Second
	}
	paused := map[int32]time.Time{}
	for ctx.Err() == nil {
		if err := check(ctx); err != nil {
			return err
		}
		for partition, until := range paused {
			if !time.Now().Before(until) {
				client.ResumeFetchPartitions(map[string][]int32{q.Topic: {partition}})
				delete(paused, partition)
			}
		}
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetched := client.PollRecords(pollCtx, 16)
		cancel()
		if q.LegacyObservation != nil {
			member, generation := client.GroupMetadata()
			if err := q.LegacyObservation.Membership(ctx, member, generation); err != nil {
				client.AllowRebalance()
				return err
			}
		}
		if err := fetched.Err(); err != nil && fetched.NumRecords() == 0 {
			client.AllowRebalance()
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		// Bound blocked rebalance time below the 60s group timeout. On timeout,
		// return/rejoin without confirming unfinished records.
		if err := check(ctx); err != nil {
			client.AllowRebalance()
			return err
		}
		batchCtx, batchCancel := context.WithTimeout(ctx, 45*time.Second)
		type failure struct {
			record *kgo.Record
			err    error
		}
		failures := make(chan failure, q.Partitions)
		var group sync.WaitGroup
		fetched.EachPartition(func(p kgo.FetchTopicPartition) {
			group.Add(1)
			go func() {
				defer group.Done()
				for _, r := range p.Records {
					if err := process(batchCtx, r); err != nil {
						failures <- failure{r, err}
						break
					}
					if err := check(batchCtx); err != nil {
						failures <- failure{r, err}
						break
					}
					commitCtx, cancel := context.WithTimeout(batchCtx, 8*time.Second)
					if q.Target != nil && q.Target.Observe != nil {
						q.Target.Observe("before_record_commit", Reference(r))
					}
					err := client.CommitRecords(commitCtx, r)
					cancel()
					if q.Target != nil && q.Target.Observe != nil {
						q.Target.Observe("record_commit_result", map[string]any{"record": Reference(r), "error": fmt.Sprint(err)})
					}
					if err != nil {
						failures <- failure{r, fmt.Errorf("Kafka offset commit failed: %w", err)}
						break
					}
				}
			}()
		})
		group.Wait()
		batchCancel()
		close(failures)
		for failed := range failures {
			if !linkerrors.IsTransient(failed.err) {
				client.AllowRebalance()
				return failed.err
			}
			client.SetOffsets(map[string]map[int32]kgo.EpochOffset{q.Topic: {failed.record.Partition: {Epoch: failed.record.LeaderEpoch, Offset: failed.record.Offset}}})
			client.PauseFetchPartitions(map[string][]int32{q.Topic: {failed.record.Partition}})
			paused[failed.record.Partition] = time.Now().Add(delay)
		}
		client.AllowRebalance()
	}
	return ctx.Err()
}
func Replay(ctx context.Context, store *StateStore, q *Queue, original RecordRef) (RecordRef, error) {
	data, err := store.ReplayPayload(ctx, original)
	if err != nil {
		return RecordRef{}, err
	}
	id := fmt.Sprintf("replay:%d:%d:%d", original.Partition, original.Offset, time.Now().UnixNano())
	recovery, err := q.PublishBytes(ctx, data, id)
	if err != nil {
		return RecordRef{}, err
	}
	return recovery, store.RecordRecovery(ctx, original, recovery)
}
