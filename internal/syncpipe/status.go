package syncpipe

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Status struct {
	Database         string              `json:"database"`
	Checkpoint       *Checkpoint         `json:"cdc_checkpoint"`
	SourcePosition   Position            `json:"source_position"`
	Workers          []WorkerStatus      `json:"workers"`
	Deliveries       map[string]int64    `json:"deliveries"`
	Attempts         int64               `json:"processing_attempts"`
	StreamMessages   uint64              `json:"retained_messages"`
	Topic            string              `json:"topic"`
	ConsumerGroup    string              `json:"es_consumer_group"`
	Partitions       []PartitionProgress `json:"partitions"`
	Pending          uint64              `json:"es_pending"`
	AckPending       int                 `json:"es_ack_pending"`
	Redeliveries     int                 `json:"redeliveries"`
	LastAppliedEvent *time.Time          `json:"last_applied_event_at"`
	LastAppliedAt    *time.Time          `json:"last_applied_at"`
	LastAppliedLagMS *float64            `json:"last_applied_lag_ms"`
	CheckedAt        time.Time           `json:"checked_at"`
	Completeness     string              `json:"completeness"`
}
type WorkerStatus struct {
	Component string     `json:"component"`
	State     string     `json:"state"`
	Heartbeat time.Time  `json:"heartbeat_at"`
	EventAt   *time.Time `json:"last_event_at"`
	LastError *string    `json:"last_error"`
}

func ReadStatus(ctx context.Context, store *StateStore, q *Queue) (Status, error) {
	if q.Target != nil {
		return Status{}, fmt.Errorf("candidate status requires version-scoped TargetStatus.Diagnostics")
	}
	result := Status{Database: store.Schema, Workers: []WorkerStatus{}, Deliveries: map[string]int64{}, CheckedAt: time.Now().UTC(), Completeness: "unknown: queue depth alone does not prove end-to-end synchronization"}
	cp, err := store.Checkpoint(ctx)
	if err != nil && err != sql.ErrNoRows {
		return result, err
	}
	if err == nil {
		result.Checkpoint = &cp
	}
	result.SourcePosition, err = CurrentPosition(ctx, store.DB)
	if err != nil {
		return result, err
	}
	rows, err := store.DB.QueryContext(ctx, "SELECT component,state,heartbeat_at,last_event_at,last_error FROM sync_worker_state ORDER BY component")
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var worker WorkerStatus
		if err := rows.Scan(&worker.Component, &worker.State, &worker.Heartbeat, &worker.EventAt, &worker.LastError); err != nil {
			rows.Close()
			return result, err
		}
		if time.Since(worker.Heartbeat) > 10*time.Second {
			worker.State = "stalled"
		}
		result.Workers = append(result.Workers, worker)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()
	rows, err = store.DB.QueryContext(ctx, "SELECT status,COUNT(*),SUM(attempts) FROM sync_kafka_es_deliveries GROUP BY status")
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var status string
		var count, attempts int64
		if err := rows.Scan(&status, &count, &attempts); err != nil {
			rows.Close()
			return result, err
		}
		result.Deliveries[status] = count
		result.Attempts += attempts
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()
	if result.Deliveries["isolated"] > 0 {
		for i := range result.Workers {
			if result.Workers[i].Component == "es" && result.Workers[i].State == "running" {
				result.Workers[i].State = "degraded"
			}
		}
	}
	var lag sql.NullFloat64
	err = store.DB.QueryRowContext(ctx, "SELECT event_at,last_seen_at,TIMESTAMPDIFF(MICROSECOND,event_at,last_seen_at)/1000.0 FROM sync_kafka_es_deliveries WHERE status='applied' ORDER BY last_seen_at DESC LIMIT 1").Scan(&result.LastAppliedEvent, &result.LastAppliedAt, &lag)
	if err != nil && err != sql.ErrNoRows {
		return result, err
	}
	if lag.Valid {
		result.LastAppliedLagMS = &lag.Float64
	}
	result.Topic = q.Topic
	result.ConsumerGroup = q.Group("es")
	result.Partitions, err = q.Progress(ctx, "es")
	if err != nil {
		return result, err
	}
	for _, p := range result.Partitions {
		result.StreamMessages += uint64(p.End - p.Start)
		result.Pending += uint64(p.Lag)
	}
	return result, nil
}
