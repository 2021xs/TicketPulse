// Isolated benchmark server using the real HTTP handlers and stores. Diagnostic
// state is local to this process and never enabled on the demonstration app.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/config"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
	"time"
)

type observer struct {
	sync.Mutex
	Calls, Candidates, Allowed int64
	AuthorizationNS            int64
}
type observedAuthorizer struct {
	store *mysqlstore.Store
	stats *observer
}
type observedAccess struct {
	search.Access
	stats *observer
}

func (a observedAuthorizer) BeginSearch(ctx context.Context, id int64) (search.Access, error) {
	access, err := a.store.BeginSearch(ctx, id)
	if err != nil {
		return nil, err
	}
	return observedAccess{access, a.stats}, nil
}
func (a observedAccess) Authorized(ctx context.Context, ids []int64) (map[int64]ticket.TicketSummary, error) {
	start := time.Now()
	result, err := a.Access.Authorized(ctx, ids)
	a.stats.Lock()
	a.stats.Calls++
	a.stats.Candidates += int64(len(ids))
	a.stats.Allowed += int64(len(result))
	a.stats.AuthorizationNS += time.Since(start).Nanoseconds()
	a.stats.Unlock()
	return result, err
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	listen := flag.String("listen", "127.0.0.1:18085", "isolated benchmark app")
	diagnostics := flag.Bool("diagnostic-log", false, "write isolated HTTP error logs")
	flag.Parse()
	if !strings.HasPrefix(*listen, "127.0.0.1:") {
		return fmt.Errorf("loopback benchmark listener required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_DSN"))
	if err != nil || !(strings.HasPrefix(cfg.DBName, "ticketpulse_test_m5_") || strings.HasPrefix(cfg.DBName, "ticketpulse_test_m502_")) {
		return fmt.Errorf("owned M5 database required")
	}
	db, err := mysqlstore.Open(ctx, cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	var uuid string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
		return fmt.Errorf("owned MySQL UUID required")
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
	ck, err := analytics.NewClickHouse(os.Getenv("TICKETPULSE_CK_URL"), os.Getenv("TICKETPULSE_CK_DATABASE"))
	if err != nil {
		return err
	}
	q, err := syncpipe.ConnectStatusQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
	if err != nil {
		return err
	}
	defer q.Close()
	es, err := search.NewElasticsearch(os.Getenv("TICKETPULSE_ES_URL"), os.Getenv("TICKETPULSE_SEARCH_INDEX"))
	if err != nil {
		return err
	}
	key := make([]byte, 32)
	rand.Read(key)
	stats := &observer{}
	searchService, err := search.NewService(observedAuthorizer{store, stats}, es, store, key)
	if err != nil {
		return err
	}
	diagnosticWriter := ioDiscard{}
	logger := slog.New(slog.NewTextHandler(diagnosticWriter, nil))
	if *diagnostics {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	api := httpapi.NewWithOptions(service, store, logger, httpapi.Options{SessionTTL: 8 * time.Hour, Search: searchService, SearchOptions: store, Analytics: &analytics.Service{Auth: store, CK: ck, Status: &syncpipe.AnalysisStatus{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: q}}})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/bench", func(w http.ResponseWriter, r *http.Request) {
		stats.Lock()
		defer stats.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"pool": db.Stats(), "search_authorization": map[string]any{"calls": stats.Calls, "candidates": stats.Candidates, "allowed": stats.Allowed, "duration_ns": stats.AuthorizationNS}})
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

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
