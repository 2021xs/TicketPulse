package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "TicketPulse rebuild:", err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "status", "create, run, status, follow, check, logs, migrate")
	id := flag.String("job", "", "immutable rebuild job ID")
	target := flag.String("target", "both", "es, ck or both")
	batch := flag.Int("batch-size", 100, "bounded source and projection batch, 1–500")
	wait := flag.Duration("wait", 30*time.Second, "finite catch-up deadline")
	org := flag.Int64("organization", 0, "metrics organization; check without a job also filters row scope (0 checks all rows)")
	start := flag.String("start", "", "optional analytics Beijing start date")
	end := flag.String("end", "", "optional analytics Beijing end date, exclusive")
	pageTrace := flag.String("page-trace", "", "new JSONL source page evidence file; optional")
	flag.Parse()
	if *target != "es" && *target != "ck" && *target != "both" {
		return fmt.Errorf("target must be es, ck or both")
	}
	if *wait <= 0 || *wait > 5*time.Minute {
		return fmt.Errorf("wait must be >0 and <=5m")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn := os.Getenv("TICKETPULSE_SYNC_DSN")
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.DBName == "" {
		return fmt.Errorf("valid TICKETPULSE_SYNC_DSN is required")
	}
	db, err := mysqlstore.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	var traceError error
	encode := func(value any) error {
		if traceError != nil {
			return fmt.Errorf("page trace failed: %w", traceError)
		}
		out := json.NewEncoder(os.Stdout)
		out.SetIndent("", "  ")
		return out.Encode(value)
	}
	if *mode == "migrate" {
		if err = syncpipe.EnsureRebuildSchema(ctx, db); err != nil {
			return err
		}
		return encode(map[string]any{"migrations": []string{"009_rebuild", "010_reconcile_index"}, "business_data_changed": false})
	}
	queue, err := syncpipe.ConnectQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
	if err != nil {
		return err
	}
	defer queue.Close()
	rebuilder := &syncpipe.Rebuilder{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: queue, ESURL: os.Getenv("TICKETPULSE_ES_URL"), CKURL: os.Getenv("TICKETPULSE_CK_URL"), BatchSize: *batch, CatchupTimeout: *wait}
	if *pageTrace != "" {
		file, problem := os.OpenFile(*pageTrace, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if problem != nil {
			return problem
		}
		defer file.Close()
		encoder := json.NewEncoder(file)
		rebuilder.ObservePage = func(v syncpipe.ReconcilePageObservation) {
			if traceError == nil {
				traceError = encoder.Encode(v)
			}
		}
	}

	switch *mode {
	case "create":
		job, err := rebuilder.Create(ctx, syncpipe.RebuildOptions{ID: *id, Target: *target, BatchSize: *batch, OrganizationID: *org, MetricsRequest: analytics.Request{Start: *start, End: *end}})
		if err != nil {
			return err
		}
		return encode(job)
	case "run":
		job, problem := rebuilder.Run(ctx, *id)
		if err = encode(job); err != nil {
			return err
		}
		return problem
	case "status":
		job, err := rebuilder.Status(ctx, *id)
		if err != nil {
			return err
		}
		return encode(job)
	case "follow":
		return rebuilder.Follow(ctx, *id)
	case "logs":
		rows, err := db.QueryContext(ctx, `SELECT id,phase,outcome,details,occurred_at FROM sync_rebuild_log WHERE job_id=? ORDER BY id`, *id)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var seq int64
			var phase, outcome string
			var details json.RawMessage
			var at time.Time
			if err = rows.Scan(&seq, &phase, &outcome, &details, &at); err != nil {
				return err
			}
			if err = encode(map[string]any{"id": seq, "phase": phase, "outcome": outcome, "details": details, "occurred_at": at}); err != nil {
				return err
			}
		}
		return rows.Err()
	case "check":
		if *id != "" {
			report, problem := rebuilder.Check(ctx, *id)
			if err = encode(report); err != nil {
				return err
			}
			return problem
		}
		var es *search.Elasticsearch
		var ck *analytics.ClickHouse
		if *target != "ck" {
			es, err = search.NewElasticsearch(rebuilder.ESURL, os.Getenv("TICKETPULSE_SEARCH_INDEX"))
			if err != nil {
				return err
			}
		}
		if *target != "es" {
			ck, err = analytics.NewClickHouse(rebuilder.CKURL, os.Getenv("TICKETPULSE_CK_DATABASE"))
			if err != nil {
				return err
			}
		}
		report, problem := syncpipe.Reconcile(ctx, rebuilder.Store, queue, es, ck, syncpipe.ReconcileOptions{ObservePage: rebuilder.ObservePage, BatchSize: *batch, LagTimeout: *wait, OrganizationID: *org, MetricsRequest: analytics.Request{Start: *start, End: *end}})
		if err = encode(report); err != nil {
			return err
		}
		if problem != nil {
			return problem
		}
		if report.State != syncpipe.ReconcileConsistent {
			return fmt.Errorf("reconciliation state: %s", report.State)
		}
		return nil
	default:
		return fmt.Errorf("unsupported rebuild mode")
	}
}
