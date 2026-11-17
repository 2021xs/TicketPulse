package syncpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"ticketpulse/internal/analytics"
)

type AnalysisStatus struct {
	Store *StateStore
	Queue *Queue
}

func (a *AnalysisStatus) AnalyticsStatus(ctx context.Context) (analytics.SyncStatus, error) {
	if a.Queue.Target != nil {
		return (&TargetStatus{Target: a.Queue.Target}).AnalyticsStatus(ctx)
	}
	result := analytics.SyncStatus{State: "unknown", ObservedAt: time.Now().UTC(), Topic: a.Queue.Topic, ConsumerGroup: a.Queue.Group("ck"), PartitionOffsets: []analytics.PartitionOffset{}}
	progress, err := a.Queue.Progress(ctx, "ck")
	if err != nil {
		result.State = "broker_unavailable"
		return result, nil
	}
	var sourceID string
	err = a.Store.DB.QueryRowContext(ctx, "SELECT barrier_id,issued_at FROM sync_source_watermark WHERE source_name='tickets-v1'").Scan(&sourceID, &result.SourceWatermark)
	if err != nil && err != sql.ErrNoRows {
		return result, err
	}
	err = a.Store.DB.QueryRowContext(ctx, "SELECT state,heartbeat_at,last_error FROM sync_worker_state WHERE component='ck'").Scan(&result.State, &result.HeartbeatAt, &result.LastError)
	if err != nil && err != sql.ErrNoRows {
		return result, err
	}
	var bootstrapBytes []byte
	var bootstrapTopic string
	err = a.Store.DB.QueryRowContext(ctx, "SELECT topic,end_offsets FROM sync_kafka_analytics_bootstrap WHERE pipeline='ck-v1'").Scan(&bootstrapTopic, &bootstrapBytes)
	if err != nil && err != sql.ErrNoRows {
		return result, err
	}
	bootstrap := map[int32]int64{}
	if len(bootstrapBytes) > 0 {
		if err := json.Unmarshal(bootstrapBytes, &bootstrap); err != nil {
			return result, err
		}
	}
	result.BootstrapComplete = len(bootstrap) == int(a.Queue.Partitions) && bootstrapTopic == a.Queue.Topic
	allMarkers := sourceID != ""
	allCommittedMarkers := true
	for _, p := range progress {
		result.Pending += uint64(p.Lag)
		result.PartitionOffsets = append(result.PartitionOffsets, analytics.PartitionOffset{Partition: p.Partition, Committed: p.Committed, End: p.End, Lag: p.Lag})
		if p.Committed < bootstrap[p.Partition] {
			result.BootstrapComplete = false
		}
		var appliedID string
		var at time.Time
		var offset int64
		err = a.Store.DB.QueryRowContext(ctx, "SELECT barrier_id,issued_at,record_offset FROM sync_kafka_watermarks WHERE component='ck' AND partition_id=? AND topic=?", p.Partition, a.Queue.Topic).Scan(&appliedID, &at, &offset)
		if err != nil && err != sql.ErrNoRows {
			return result, err
		}
		if err == nil && p.Committed > offset {
			if result.ConfirmedThrough == nil || at.Before(*result.ConfirmedThrough) {
				copy := at
				result.ConfirmedThrough = &copy
			}
		} else {
			allCommittedMarkers = false
			allMarkers = false
		}
		if appliedID != sourceID {
			allMarkers = false
		}
	}
	if !allCommittedMarkers {
		result.ConfirmedThrough = nil
	}
	rows, err := a.Store.DB.QueryContext(ctx, "SELECT status,COUNT(*),SUM(attempts) FROM sync_kafka_ck_deliveries GROUP BY status")
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var state string
		var count, attempts int64
		if err := rows.Scan(&state, &count, &attempts); err != nil {
			rows.Close()
			return result, err
		}
		result.ProcessingAttempts += attempts
		switch state {
		case "applied":
			result.Applied = count
		case "retrying":
			result.Retrying = count
		case "isolated":
			result.Isolated = count
		case "recovered":
			result.Recovered = count
		case "processing":
			result.AckPending = int(count)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()
	err = a.Store.DB.QueryRowContext(ctx, "SELECT MAX(last_seen_at) FROM sync_kafka_ck_deliveries WHERE status='applied' AND ticket_id IS NOT NULL").Scan(&result.LastSuccessfulWrite)
	if err != nil {
		return result, err
	}
	fresh := result.HeartbeatAt != nil && time.Since(*result.HeartbeatAt) < 10*time.Second && result.SourceWatermark != nil && time.Since(*result.SourceWatermark) < 10*time.Second
	if !fresh {
		result.State = "stalled"
	} else if result.Isolated > 0 {
		result.State = "degraded"
	}
	result.CaughtUp = fresh && result.State == "running" && result.BootstrapComplete && allMarkers && result.Pending == 0 && result.Isolated == 0 && result.Retrying == 0 && result.AckPending == 0
	if !result.CaughtUp && result.State == "running" {
		result.State = "catching_up"
	}
	return result, nil
}
