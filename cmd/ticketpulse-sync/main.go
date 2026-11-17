package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/mysqlstore"
	projectioncontrol "ticketpulse/internal/projection"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
)

func main() {
	if err := run(); err != nil {
		slog.Error("TicketPulse sync stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "all", "all, capture, consume, ck, snapshot, analytics-snapshot, status, analytics-status, replay, cleanup-test, migrate-kafka")
	projection := flag.String("consumer", "es", "es or ck for replay")
	handoff := flag.String("handoff", "", "M4-03B candidate handoff; only consume/ck/status/analytics-status")
	partition := flag.Int("partition", -1, "isolated Kafka partition to replay")
	offset := flag.Int64("offset", -1, "isolated Kafka offset to replay")
	id := flag.Uint("server-id", 1001, "unique MySQL replica server ID")
	flag.Parse()
	if *id < 2 || uint64(*id) > uint64(1<<32-1) {
		return fmt.Errorf("server-id must be 2–4294967295 and unique among consumers")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn := os.Getenv("TICKETPULSE_SYNC_DSN")
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.DBName == "" {
		return fmt.Errorf("valid TICKETPULSE_SYNC_DSN with database is required")
	}
	db, err := mysqlstore.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if *projection != "es" && *projection != "ck" {
		return fmt.Errorf("consumer must be es or ck")
	}
	store := &syncpipe.StateStore{DB: db, Schema: cfg.DBName}
	q, err := syncpipe.ConnectQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
	if err != nil {
		return err
	}
	defer q.Close()
	if *handoff != "" {
		return runHandoff(ctx, store, q, *handoff, *mode)
	}
	var managed *syncpipe.ManagedProjection
	var managedCK *analytics.ClickHouse
	var managedES *search.Elasticsearch
	if version := os.Getenv("TICKETPULSE_TARGET_VERSION"); version != "" {
		// Managed operation never creates targets or accepts bootstrap/replay/
		// cleanup/migration commands. Existing standard groups only, no handoff.
		if *mode != "all" && *mode != "capture" && *mode != "consume" && *mode != "ck" && *mode != "status" && *mode != "analytics-status" {
			return fmt.Errorf("managed mode only permits existing workers and read-only status")
		}
		managedES, err = search.NewElasticsearch(os.Getenv("TICKETPULSE_ES_URL"), os.Getenv("TICKETPULSE_SEARCH_INDEX"))
		if err != nil {
			return err
		}
		managedCK, err = analytics.NewClickHouse(os.Getenv("TICKETPULSE_CK_URL"), os.Getenv("TICKETPULSE_CK_DATABASE"))
		if err != nil {
			return err
		}
		control := &syncpipe.ProjectionControl{Store: store, Queue: q, ES: managedES, CK: managedCK}
		managed, err = control.Managed(ctx, version)
		if err != nil {
			return err
		}
		checkpoint, err := store.Checkpoint(ctx)
		if err != nil || !checkpoint.BootstrapComplete {
			return fmt.Errorf("managed mode requires an existing CDC checkpoint; no bootstrap allowed")
		}
	}
	if managed == nil && *mode != "status" && *mode != "analytics-status" && *mode != "cleanup-test" {
		if err := q.Ensure(ctx); err != nil {
			return err
		}
	}
	var es *search.Elasticsearch
	if *mode == "all" || *mode == "consume" {
		es = managedES
		if es == nil {
			es, err = search.NewElasticsearch(os.Getenv("TICKETPULSE_ES_URL"), os.Getenv("TICKETPULSE_SEARCH_INDEX"))
		}
		if err != nil {
			return err
		}
	}
	encode := func(value any) error {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	}
	switch *mode {
	case "analytics-status":
		state, err := (&syncpipe.AnalysisStatus{Store: store, Queue: q}).AnalyticsStatus(ctx)
		if err != nil {
			return err
		}
		return encode(state)
	case "analytics-snapshot":
		count, seq, err := syncpipe.AnalyticsSnapshot(ctx, store, q)
		if err != nil {
			return err
		}
		return encode(map[string]any{"published": count, "end_offsets": seq, "cdc_checkpoint_unchanged": true})
	case "cleanup-test":
		if !strings.HasPrefix(cfg.DBName, "ticketpulse_test_search_") {
			return fmt.Errorf("cleanup-test only accepts owned random search fixture schemas")
		}
		if err := q.DeleteTestTopic(ctx); err != nil {
			return err
		}
		return encode(map[string]any{"removed_test_topic": q.Topic})
	case "migrate-kafka":
		return migrateKafka(ctx, store, q, encode)
	case "status":
		status, err := syncpipe.ReadStatus(ctx, store, q)
		if err != nil {
			return err
		}
		return encode(status)
	case "snapshot":
		count, err := syncpipe.Snapshot(ctx, store, q)
		if err != nil {
			return err
		}
		return encode(map[string]any{"snapshot_published": count, "cdc_checkpoint_unchanged": true})
	case "replay":
		if *partition < 0 || *offset < 0 {
			return fmt.Errorf("replay requires -partition and -offset")
		}
		seq, err := syncpipe.Replay(ctx, store.ForConsumer(*projection), q, syncpipe.RecordRef{Topic: q.Topic, Partition: int32(*partition), Offset: *offset})
		if err != nil {
			return err
		}
		return encode(map[string]any{"original_record": syncpipe.RecordRef{Topic: q.Topic, Partition: int32(*partition), Offset: *offset}, "recovery_record": seq})
	case "all", "capture", "consume", "ck":
	default:
		return fmt.Errorf("unsupported mode")
	}
	capture := &syncpipe.Capture{Store: store, Queue: q, Config: syncpipe.CaptureConfig{Address: os.Getenv("TICKETPULSE_CDC_ADDR"), User: os.Getenv("TICKETPULSE_CDC_USER"), Password: os.Getenv("TICKETPULSE_CDC_PASSWORD"), ServerID: uint32(*id)}, Log: slog.Default()}
	consumer := &syncpipe.Consumer{Store: store, Queue: q, ES: es}
	if managed != nil {
		capture.Check = managed.Check
		consumer.Check = managed.Check
	}
	done := make(chan error, 3)
	workers := 0
	if *mode == "all" || *mode == "capture" {
		workers++
		go func() { done <- supervise(ctx, store, "cdc", capture.Run) }()
	}
	if *mode == "all" || *mode == "consume" {
		workers++
		go func() { done <- supervise(ctx, store, "es", consumer.Run) }()
	}
	if (*mode == "all" && os.Getenv("TICKETPULSE_CK_URL") != "") || *mode == "ck" {
		ck := managedCK
		if ck == nil {
			ck, err = analytics.NewClickHouse(os.Getenv("TICKETPULSE_CK_URL"), os.Getenv("TICKETPULSE_CK_DATABASE"))
		}
		if err != nil {
			return err
		}
		consumer := &syncpipe.CKConsumer{Store: store, Queue: q, CK: ck}
		if managed != nil {
			consumer.Check = managed.Check
		}
		workers++
		go func() { done <- supervise(ctx, store, "ck", consumer.Run) }()
	}
	var final error
	for i := 0; i < workers; i++ {
		err := <-done
		if err != nil && ctx.Err() == nil {
			final = err
			stop()
		}
	}
	return final
}

func supervise(ctx context.Context, store *syncpipe.StateStore, component string, run func(context.Context) error) error {
	for ctx.Err() == nil {
		err := run(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("worker exited unexpectedly")
		}
		// Never silently reset a missing checkpoint/log or continue through incompatible data.
		if errors.Is(err, projectioncontrol.ErrTargetMismatch) || strings.Contains(err.Error(), "CDC_BINLOG_EXPIRED") || strings.Contains(err.Error(), "already running") || strings.Contains(err.Error(), "decode committed") || strings.Contains(err.Error(), "config differs") || strings.Contains(err.Error(), "physical deletion") || strings.Contains(err.Error(), "Kafka topic identity") || strings.Contains(err.Error(), "KAFKA_OFFSET_EXPIRED") || strings.Contains(err.Error(), "KAFKA_MIGRATION_REQUIRED") {
			return err
		}
		slog.Error("sync worker will retry", "component", component, "error", err)
		_ = store.Heartbeat(ctx, component, "retrying", nil, err)
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}
