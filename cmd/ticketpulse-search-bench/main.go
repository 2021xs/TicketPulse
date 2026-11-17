// Isolated search benchmark. Uses the production API, SQL pool and search
// service without requiring ClickHouse or changing demonstration services.
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/config"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
)

func run() error {
	listen := flag.String("listen", "127.0.0.1:18087", "owned benchmark listener")
	output := flag.String("diagnostics", "", "new private JSONL evidence file")
	flag.Parse()
	if !strings.HasPrefix(*listen, "127.0.0.1:") || *output == "" {
		return fmt.Errorf("loopback and new diagnostic output required")
	}
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_DSN"))
	if err != nil || cfg.Net != "unix" || !regexp.MustCompile(`^ticketpulse_test_m5_m504_[a-z0-9_]+$`).MatchString(cfg.DBName) {
		return fmt.Errorf("owned M5-04 source schema required")
	}
	key, err := hex.DecodeString(os.Getenv("TICKETPULSE_SEARCH_CURSOR_KEY"))
	if err != nil || len(key) != 32 {
		return fmt.Errorf("private 32-byte cursor key required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := mysqlstore.Open(ctx, cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	var uuid string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
		return fmt.Errorf("source UUID mismatch")
	}
	routing, err := config.LoadRouting("configs/local.json")
	if err != nil {
		return err
	}
	store := mysqlstore.New(db)
	service, err := ticket.NewService(store, routing)
	if err != nil {
		return err
	}
	es, err := search.NewElasticsearch(os.Getenv("TICKETPULSE_ES_URL"), os.Getenv("TICKETPULSE_SEARCH_INDEX"))
	if err != nil {
		return err
	}
	searchService, err := search.NewService(store, es, store, key)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	buffer := bufio.NewWriterSize(file, 1<<20)
	defer buffer.Flush()
	encoder := json.NewEncoder(buffer)
	var lock sync.Mutex
	var requests, candidates, authQueries, items int64
	var logErr error
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	api := httpapi.NewWithOptions(service, store, logger, httpapi.Options{SessionTTL: 8 * time.Hour,
		Search: searchService, SearchOptions: store, SearchObserver: func(d search.Diagnostics) {
			lock.Lock()
			defer lock.Unlock()
			requests++
			candidates += int64(d.Candidates)
			authQueries += int64(d.AuthorizationQueries)
			items += int64(d.Returned)
			if err := encoder.Encode(d); err != nil {
				logErr = err
			}
		}})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/bench", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		if err := buffer.Flush(); err != nil {
			logErr = err
		}
		json.NewEncoder(w).Encode(map[string]any{"pool": db.Stats(), "search": map[string]int64{"requests": requests, "candidates": candidates, "authorization_queries": authQueries, "items": items}, "diagnostic_write_failed": logErr != nil})
	})
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.Handle("/", api)
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
