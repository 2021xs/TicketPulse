// M5-03 fixture import reuses the frozen generator and real CK projection.
// It never seeds the demonstration namespace or changes the synchronization protocol.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/benchseed"
	"ticketpulse/internal/syncpipe"
)

type projector struct {
	ctx    context.Context
	ck     *analytics.ClickHouse
	facts  []analytics.Fact
	states []analytics.Current
	Facts  int64 `json:"facts"`
	States int64 `json:"states"`
}

func (p *projector) flush() error {
	if err := fixtureInsert(p.ctx, "ticket_events", p.facts); err != nil {
		return err
	}
	if err := fixtureInsert(p.ctx, "ticket_states", p.states); err != nil {
		return err
	}
	p.Facts += int64(len(p.facts))
	p.States += int64(len(p.states))
	p.facts, p.states = p.facts[:0], p.states[:0]
	return nil
}

func (p *projector) Write(data []byte) (int, error) {
	var event syncpipe.Event
	if err := json.Unmarshal(data, &event); err != nil {
		return 0, err
	}
	fact, current, err := syncpipe.AnalysisProjection(event)
	if err != nil {
		return 0, err
	}
	if fact != nil {
		p.facts = append(p.facts, *fact)
	}
	p.states = append(p.states, current)
	if len(p.states) == 20000 {
		if err := p.flush(); err != nil {
			return 0, err
		}
	}
	return len(data), nil
}

// Fixture setup writes larger bounded INSERTs to avoid thousands of tiny Parts.
// This is not the production consumer or a CDC throughput measurement. Replays
// after unknown INSERT outcomes are retained as physical duplicates.
func fixtureInsert[T any](ctx context.Context, table string, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	var data bytes.Buffer
	enc := json.NewEncoder(&data)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	params := url.Values{"database": {os.Getenv("TICKETPULSE_CK_DATABASE")},
		"query": {"INSERT INTO " + table + " FORMAT JSONEachRow"}, "wait_end_of_query": {"1"},
		"async_insert": {"0"}, "input_format_skip_unknown_fields": {"0"},
		"max_execution_time": {"4"}, "max_threads": {"2"}}
	client := &http.Client{Timeout: 6 * time.Second}
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, os.Getenv("TICKETPULSE_CK_URL")+"/?"+params.Encode(), bytes.NewReader(data.Bytes()))
		if err != nil {
			return err
		}
		res, err := client.Do(req)
		if err == nil {
			payload, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
			res.Body.Close()
			if res.StatusCode == 200 && readErr == nil && len(payload) == 0 {
				return nil
			}
			last = fmt.Errorf("fixture INSERT HTTP %d code %s: %s", res.StatusCode, res.Header.Get("X-ClickHouse-Exception-Code"), payload)
		} else {
			last = err
		}
		fmt.Fprintln(os.Stderr, "fixture retry; preserve duplicate outcomes:", last)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Second):
		}
	}
	return last
}

func run() error {
	n := flag.Int64("tickets", 10000, "frozen v1 fixture count: 10000 or 100000")
	manifest := flag.String("manifest", "", "new evidence file")
	flag.Parse()
	if (*n != 10000 && *n != 100000) || *manifest == "" {
		return fmt.Errorf("10000/100000 and new manifest required")
	}
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_BENCH_DSN"))
	if err != nil || !regexp.MustCompile(`^ticketpulse_test_m5_m503_[a-z0-9_]+$`).MatchString(cfg.DBName) || cfg.Net != "unix" {
		return fmt.Errorf("owned M5-03 fixture DSN required")
	}
	ckdb := os.Getenv("TICKETPULSE_CK_DATABASE")
	if !regexp.MustCompile(`^ticketpulse_m503_[a-z0-9_]+$`).MatchString(ckdb) || os.Getenv("TICKETPULSE_CK_URL") != "http://127.0.0.1:18125" {
		return fmt.Errorf("owned independent M5-03 ClickHouse required")
	}
	out, err := os.OpenFile(*manifest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
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
	ck, err := analytics.NewClickHouse(os.Getenv("TICKETPULSE_CK_URL"), ckdb)
	if err != nil {
		return err
	}
	if err = ck.Ensure(ctx); err != nil {
		return err
	}
	for _, table := range []string{"ticket_events", "ticket_states"} {
		data, problem := ck.Query(ctx, "SELECT count() FROM "+table)
		if problem != nil || string(data) != "0\n" {
			return fmt.Errorf("refusing nonempty CK fixture: %s %v", table, problem)
		}
	}
	p := &projector{ctx: ctx, ck: ck}
	m, problem := benchseed.Generate(ctx, db, benchseed.Options{Tickets: *n, Seed: 20261109, BatchSize: 500,
		Schema: cfg.DBName, ExpectedUUID: os.Getenv("TICKETPULSE_TEST_SERVER_UUID"), EventsWriter: p})
	if problem == nil {
		problem = p.flush()
	}
	result := map[string]any{"dataset": m, "projection": p, "ck_database": ckdb,
		"canonical_sql_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(analytics.Canonical))),
		"meaning":              "SQL and CK fixture import, not Kafka or HTTP throughput"}
	if problem != nil {
		result["failure"] = problem.Error()
	}
	if err = json.NewEncoder(out).Encode(result); err != nil {
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
