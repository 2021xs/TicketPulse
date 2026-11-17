package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/config"
	"ticketpulse/internal/httpapi"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/readruntime"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
)

func main() {
	if err := run(); err != nil {
		slog.Error("TicketPulse stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", "127.0.0.1:18080", "HTTP listen address")
	routingPath := flag.String("routing", "configs/local.json", "static routing policy")
	sessionTTL := flag.Duration("session-ttl", 8*time.Hour, "absolute cookie session lifetime (1s to 24h)")
	secureCookies := flag.Bool("secure-cookies", false, "require HTTPS for session cookies")
	shutdownTimeout := flag.Duration("shutdown-timeout", 5*time.Second, "bounded shutdown wait (100ms to 30s); timeout closes remaining connections")
	flag.Parse()
	if *sessionTTL < time.Second || *sessionTTL > 24*time.Hour {
		return fmt.Errorf("session-ttl must be between 1s and 24h")
	}
	if *shutdownTimeout < 100*time.Millisecond || *shutdownTimeout > 30*time.Second {
		return fmt.Errorf("invalid shutdown timeout")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := mysqlstore.Open(ctx, os.Getenv("TICKETPULSE_DSN"))
	if err != nil {
		return err
	}
	defer db.Close()
	routing, err := config.LoadRouting(*routingPath)
	if err != nil {
		return err
	}
	store := mysqlstore.New(db)
	if os.Getenv("TICKETPULSE_REQUIRE_WRITE_GATE") == "true" {
		store.RequireBusinessGate()
		if _, err := projection.ReadGate(ctx, db); err != nil {
			return err
		}
	}
	service, err := ticket.NewService(store, routing)
	if err != nil {
		return err
	}
	options := httpapi.Options{SessionTTL: *sessionTTL, SecureCookies: *secureCookies}
	var ckClient *analytics.ClickHouse
	var esClient *search.Elasticsearch
	var statusQueue *syncpipe.Queue
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_DSN"))
	if err != nil {
		return err
	}
	options.Readiness = db.PingContext
	readEnabled, err := projection.ReadControlEnabled(ctx, db)
	if err != nil {
		return err
	}
	if readEnabled && os.Getenv("TICKETPULSE_TARGET_VERSION") == "" {
		return fmt.Errorf("enabled read selector requires explicit managed target expectations")
	}
	if endpoint := os.Getenv("TICKETPULSE_CK_URL"); endpoint != "" {
		ck, err := analytics.NewClickHouse(endpoint, os.Getenv("TICKETPULSE_CK_DATABASE"))
		if err != nil {
			return err
		}
		cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_DSN"))
		if err != nil {
			return err
		}
		queue, err := syncpipe.ConnectStatusQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
		if err != nil {
			return err
		}
		defer queue.Close()
		ckClient, statusQueue = ck, queue
		options.Analytics = &analytics.Service{Auth: store, CK: ck, Status: &syncpipe.AnalysisStatus{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: queue}}
	}
	if endpoint := os.Getenv("TICKETPULSE_ES_URL"); endpoint != "" {
		es, err := search.NewElasticsearch(endpoint, os.Getenv("TICKETPULSE_SEARCH_INDEX"))
		if err != nil {
			return err
		}
		esClient = es
		key := make([]byte, 32)
		if configured := os.Getenv("TICKETPULSE_CURSOR_KEY"); configured != "" {
			key, err = hex.DecodeString(configured)
			if err != nil || len(key) != 32 {
				return fmt.Errorf("TICKETPULSE_CURSOR_KEY must be 64 hex characters")
			}
		} else {
			if _, err := rand.Read(key); err != nil {
				return err
			}
		}
		options.Search, err = search.NewService(store, es, store, key)
		if err != nil {
			return err
		}
		options.SearchOptions = store
	}
	var selected *syncpipe.SelectedAPI
	var apiIdentity projection.APIIdentity
	if version := os.Getenv("TICKETPULSE_TARGET_VERSION"); version != "" {
		control := &syncpipe.ProjectionControl{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: statusQueue, ES: esClient, CK: ckClient}
		store.RequireBusinessGate()
		// Explicit managed targets require a stable operator key so target
		// invalidation is testable independently of process restarts.
		key, err := hex.DecodeString(os.Getenv("TICKETPULSE_CURSOR_KEY"))
		if err != nil || len(key) != 32 {
			return fmt.Errorf("managed projection requires TICKETPULSE_CURSOR_KEY (64 hex characters)")
		}
		if readEnabled {
			selected, err = control.ResolveReadSelection(ctx, version)
			if err != nil {
				return err
			}
			var freshness search.FreshnessReader = store
			var status analytics.StatusReader = &syncpipe.AnalysisStatus{Store: control.Store, Queue: statusQueue}
			if selected.Destination.Handoff != nil {
				targetStatus, e := selected.TargetStatus()
				if e != nil {
					return e
				}
				freshness, status = targetStatus, targetStatus
			}
			options.Search, err = search.NewService(store, selected.Destination.ES, &syncpipe.ReadFreshness{Selected: selected, Reader: freshness}, key, search.WithTargetVersion(selected.CursorTarget(), selected.CheckReadSelection))
			if err != nil {
				return err
			}
			options.Analytics = &analytics.Service{Auth: store, CK: selected.Destination.CK, Status: &syncpipe.ReadAnalyticsStatus{Selected: selected, Reader: status}, Check: selected.CheckReadSelection}
			options.Readiness = selected.CheckReadSelection
			var nonce [16]byte
			if _, err = rand.Read(nonce[:]); err != nil {
				return err
			}
			process, e := readruntime.Inspect(ctx, os.Getpid(), *listen)
			if e != nil {
				return e
			}
			apiIdentity = projection.APIIdentity{BootID: hex.EncodeToString(nonce[:]), Process: process, VersionID: selected.Selection.VersionID, ContractHash: selected.Selection.ContractSHA256, ReadRevision: selected.Selection.ReadRevision, HandoffID: selected.Selection.HandoffID, CKDatabaseUUID: selected.Destination.CKDatabaseUUID, StateNamespace: selected.Destination.StateNamespace, Groups: selected.Destination.Groups}
			if _, err = (&projection.ReadControlStore{DB: db}).RegisterAPI(ctx, apiIdentity); err != nil {
				return err
			}
			selected.BootID = apiIdentity.BootID
			options.ReadIdentity = func() any { return apiIdentity }
		} else {
			managed, e := control.Managed(ctx, version)
			if e != nil {
				return e
			}
			options.Readiness = managed.Check
			options.Search, err = search.NewService(store, esClient, store, key, search.WithTargetVersion(managed.Version.CursorTarget(), managed.Check))
			if err != nil {
				return err
			}
			options.Analytics.(*analytics.Service).Check = managed.Check
		}
	}
	handler := httpapi.NewWithOptions(service, store, slog.Default(), options)
	if selected != nil {
		underlying := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-TicketPulse-Read-Version", apiIdentity.VersionID)
			w.Header().Set("X-TicketPulse-Read-Revision", fmt.Sprint(apiIdentity.ReadRevision))
			w.Header().Set("X-TicketPulse-Boot-ID", apiIdentity.BootID)
			w.Header().Set("X-TicketPulse-Status-Scope", apiIdentity.StateNamespace)
			underlying.ServeHTTP(w, r)
		})
	}
	server := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	var heartbeatCancel context.CancelFunc
	var heartbeatDone chan struct{}
	if selected != nil {
		boots := &projection.ReadControlStore{DB: db}
		if err = boots.APIHeartbeat(ctx, apiIdentity.BootID, "RUNNING", nil); err != nil {
			return err
		}
		beatCtx, cancel := context.WithCancel(context.Background())
		heartbeatCancel = cancel
		heartbeatDone = make(chan struct{})
		go func() {
			defer close(heartbeatDone)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-beatCtx.Done():
					return
				case <-ticker.C:
					check, cancel := context.WithTimeout(beatCtx, 2*time.Second)
					err := boots.APIHeartbeat(check, apiIdentity.BootID, "RUNNING", nil)
					cancel()
					if err != nil {
						stop()
						return
					}
				}
			}
		}()
		defer func() {
			heartbeatCancel()
			<-heartbeatDone
			final, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = boots.APIHeartbeat(final, apiIdentity.BootID, "EXITED", nil)
		}()
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	slog.Info("TicketPulse listening", "address", *listen)
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if selected != nil {
			heartbeatCancel()
			<-heartbeatDone
			final, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = (&projection.ReadControlStore{DB: db}).APIHeartbeat(final, apiIdentity.BootID, "DRAINING", nil)
			cancel()
		}
		shutdown, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		err := server.Shutdown(shutdown)
		if err != nil {
			_ = server.Close()
		}
		if selected != nil {
			final, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = (&projection.ReadControlStore{DB: db}).APIHeartbeat(final, apiIdentity.BootID, "DRAINING", map[string]any{"shutdown_complete": err == nil, "forced_connections_closed": err != nil, "observed_at": time.Now().UTC()})
			cancel()
		}
		return err
	}
}
