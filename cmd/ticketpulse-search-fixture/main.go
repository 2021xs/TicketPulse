// Search-only fixture import. The frozen generator is reused unchanged.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/benchseed"
	"ticketpulse/internal/syncpipe"
)

func run() error {
	n := flag.Int64("tickets", 10000, "frozen v1 fixture count: 10000/100000")
	manifest := flag.String("manifest", "", "new manifest output")
	initialize := flag.Bool("initialize-cdc", false, "install future-write checkpoint after private bulk setup")
	flag.Parse()
	if (*n != 10000 && *n != 100000) || (!*initialize && *manifest == "") {
		return fmt.Errorf("10000/100000 and new manifest required")
	}
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_BENCH_DSN"))
	if err != nil || cfg.Net != "unix" || !regexp.MustCompile("^ticketpulse_test_m5_m504_[a-z0-9_]+$").MatchString(cfg.DBName) ||
		filepath.Base(cfg.Addr) != "mysql.sock" || filepath.Base(filepath.Dir(cfg.Addr)) != "mysql" || filepath.Base(filepath.Dir(filepath.Dir(cfg.Addr))) != "m5-04" {
		return fmt.Errorf("owned M5-04 fixture DSN required")
	}
	cfg.ParseTime, cfg.Loc = true, time.UTC
	cfg.ReadTimeout, cfg.WriteTimeout, cfg.Timeout = 20*time.Second, 20*time.Second, 5*time.Second
	cfg.Params = map[string]string{"time_zone": "'+00:00'", "sql_mode": "'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'"}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if *initialize {
		var uuid string
		if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
			return fmt.Errorf("source UUID mismatch")
		}
		q, err := syncpipe.ConnectQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
		if err != nil {
			return err
		}
		defer q.Close()
		if err = q.Ensure(ctx); err != nil {
			return err
		}
		state := &syncpipe.StateStore{DB: db, Schema: cfg.DBName}
		if err = state.VerifyQueue(ctx, q); err != nil {
			return err
		}
		var exists int
		if err = db.QueryRowContext(ctx, "SELECT count(*) FROM sync_kafka_cdc_checkpoint").Scan(&exists); err != nil || exists != 0 {
			return fmt.Errorf("refusing existing CDC checkpoint")
		}
		position, err := syncpipe.CurrentPosition(ctx, db)
		if err != nil {
			return err
		}
		if err = state.SaveCheckpoint(ctx, position, true); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"fixture_bulk_complete": true, "future_write_checkpoint": position, "topic": q.Topic})
	}
	file, err := os.OpenFile(*manifest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	m, problem := benchseed.Generate(ctx, db, benchseed.Options{Tickets: *n, Seed: 20261109, BatchSize: 500, Schema: cfg.DBName, ExpectedUUID: os.Getenv("TICKETPULSE_TEST_SERVER_UUID")})
	if err := json.NewEncoder(file).Encode(m); err != nil {
		return err
	}
	return problem
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
