package main

import (
	"context"
	"database/sql"
	"fmt"
	"ticketpulse/internal/syncpipe"
	"time"
)

func migrateKafka(ctx context.Context, store *syncpipe.StateStore, q *syncpipe.Queue, encode func(any) error) error {
	// The same named lock excludes both the old capture and new Kafka capture.
	release, err := store.Lock(ctx, "cdc")
	if err != nil {
		return err
	}
	defer release()
	if err := store.VerifyQueue(ctx, q); err != nil {
		return err
	}
	existing, err := store.Checkpoint(ctx)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	source := &syncpipe.Capture{Store: store, Queue: q}
	if err == nil {
		if err := source.VerifySource(ctx, existing); err != nil {
			return err
		}
	} else {
		old := syncpipe.Checkpoint{}
		err = store.DB.QueryRowContext(ctx, "SELECT server_uuid,binlog_file,binlog_pos,bootstrap_complete FROM sync_cdc_checkpoint WHERE source_name='tickets-v1'").Scan(&old.ServerUUID, &old.File, &old.Pos, &old.BootstrapComplete)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == sql.ErrNoRows {
			old.Position, err = syncpipe.CurrentPosition(ctx, store.DB)
			if err != nil {
				return err
			}
		}
		if err := source.VerifySource(ctx, old); err != nil {
			return err
		}
		// Retained legacy boundary covers writes made while installing/switching.
		// Kafka bootstrap reuses that boundary and allows safe snapshot duplicates.
		if err := store.SaveCheckpoint(ctx, old.Position, false); err != nil {
			return err
		}
	}
	checkpoint, err := syncpipe.Bootstrap(ctx, store, q)
	if err != nil {
		return err
	}
	if !checkpoint.BootstrapComplete {
		return fmt.Errorf("Kafka bootstrap incomplete")
	}
	return encode(map[string]any{"migration": "legacy_binlog_boundary_to_kafka", "checkpoint": checkpoint, "topic": q.Topic, "partitions": q.Partitions, "legacy_control_rows_preserved": true, "business_data_unchanged": true, "at": time.Now().UTC()})
}
