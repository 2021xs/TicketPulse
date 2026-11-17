package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/benchseed"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "TicketPulse bench seed:", err)
		os.Exit(1)
	}
}
func run() error {
	tickets := flag.Int64("tickets", 10000, "fixture ticket count, max 1000000; this is a SQL import")
	seed := flag.Int64("seed", 20261109, "deterministic data seed")
	batch := flag.Int("batch", 200, "whole-ticket transaction size, 1–500")
	deadline := flag.Duration("deadline", 5*time.Minute, "finite import deadline, >0 and <=30m")
	dry := flag.Bool("dry-estimate", false, "exact rule counts and sampled payload estimate, no DB connection or import")
	manifest := flag.String("manifest", "", "optional JSON manifest output file")
	eventsOutput := flag.String("events-output", "", "optional new JSONL file of committed source history/current events; no output in dry estimate")
	flag.Parse()
	if *deadline <= 0 || *deadline > 30*time.Minute {
		return fmt.Errorf("deadline must be >0 and <=30m")
	}
	o := benchseed.Options{Tickets: *tickets, Seed: *seed, BatchSize: *batch, DryEstimate: *dry}
	o.EventsOutput = *eventsOutput
	if err := o.Validate(); err != nil {
		return err
	}
	var result benchseed.Manifest
	var problem error
	if *dry {
		result, problem = benchseed.Estimate(o)
	} else {
		cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_BENCH_DSN"))
		if err != nil {
			return fmt.Errorf("valid TICKETPULSE_BENCH_DSN is required")
		}
		if !regexp.MustCompile("^ticketpulse_test_m5_[a-z0-9_]+$").MatchString(cfg.DBName) || len(cfg.DBName) > 64 {
			return fmt.Errorf("only ticketpulse_test_m5_ isolated schemas may be seeded")
		}
		switch cfg.Net {
		case "tcp":
			host, port, e := net.SplitHostPort(cfg.Addr)
			if e != nil || host != "127.0.0.1" || port != "33306" {
				return fmt.Errorf("refusing non-project MySQL endpoint")
			}
		case "unix":
			if !strings.HasSuffix(filepath.ToSlash(filepath.Clean(cfg.Addr)), "/ticketpulse/.local/mysql.sock") {
				return fmt.Errorf("refusing non-project MySQL socket")
			}
		default:
			return fmt.Errorf("explicit isolated MySQL tcp/unix endpoint required")
		}
		expected := os.Getenv("TICKETPULSE_TEST_SERVER_UUID")
		if expected == "" {
			return fmt.Errorf("TICKETPULSE_TEST_SERVER_UUID is required")
		}
		o.ExpectedUUID = expected
		o.Schema = cfg.DBName
		cfg.ParseTime = true
		cfg.Loc = time.UTC
		cfg.MultiStatements = false
		cfg.ClientFoundRows = false
		cfg.Timeout = 5 * time.Second
		cfg.ReadTimeout = 20 * time.Second
		cfg.WriteTimeout = 20 * time.Second
		cfg.Params = map[string]string{"time_zone": "'+00:00'", "sql_mode": "'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'"}
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			return err
		}
		defer db.Close()
		db.SetMaxOpenConns(2)
		db.SetMaxIdleConns(2)
		var eventsFile *os.File
		var eventsWriter *bufio.Writer
		if *eventsOutput != "" {
			if *eventsOutput == *manifest {
				return fmt.Errorf("events and manifest paths must differ")
			}
			eventsFile, err = os.OpenFile(*eventsOutput, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			defer eventsFile.Close()
			eventsWriter = bufio.NewWriterSize(eventsFile, 1<<20)
			o.EventsWriter = eventsWriter
		}
		signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ctx, cancel := context.WithTimeout(signalCtx, *deadline)
		defer cancel()
		result, problem = benchseed.Generate(ctx, db, o)
		if eventsWriter != nil {
			if err = eventsWriter.Flush(); err == nil {
				err = eventsFile.Sync()
			}
			if err != nil {
				result.Status = "failed"
				result.Failure = "SQL rows preserved; event export flush failed"
				problem = err
			}
		}
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if *manifest != "" {
		if err = os.WriteFile(*manifest, data, 0600); err != nil {
			return err
		}
	}
	if _, err = os.Stdout.Write(data); err != nil {
		return err
	}
	return problem
}
