// Read-only formal CK reconciliation of the owned stationary M5-03 dataset.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/syncpipe"
)

func run() error {
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_DSN"))
	if err != nil || !strings.HasPrefix(cfg.DBName, "ticketpulse_test_m5_m503_") || cfg.Net != "unix" {
		return fmt.Errorf("owned M5-03 source required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db, err := mysqlstore.Open(ctx, cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	var uuid string
	if err := db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
		return fmt.Errorf("source UUID mismatch")
	}
	if os.Getenv("TICKETPULSE_CK_URL") != "http://127.0.0.1:18125" || !strings.HasPrefix(os.Getenv("TICKETPULSE_CK_DATABASE"), "ticketpulse_m503_") {
		return fmt.Errorf("owned M5-03 CK required")
	}
	ck, err := analytics.NewClickHouse(os.Getenv("TICKETPULSE_CK_URL"), os.Getenv("TICKETPULSE_CK_DATABASE"))
	if err != nil {
		return err
	}
	report, err := syncpipe.Reconcile(ctx, &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, nil, nil, ck,
		syncpipe.ReconcileOptions{BatchSize: 500, OrganizationID: 1, MetricsOrganizationID: 1,
			MetricsRequest: analytics.Request{Start: "2026-11-01", End: "2026-11-09"}})
	if encErr := json.NewEncoder(os.Stdout).Encode(report); encErr != nil {
		return encErr
	}
	if err != nil {
		return err
	}
	if report.State != syncpipe.ReconcileConsistent || report.DifferenceCount != 0 {
		return fmt.Errorf("formal CK reconciliation is %s, differences=%d", report.State, report.DifferenceCount)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
