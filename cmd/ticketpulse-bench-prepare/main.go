// Benchmark-only bulk fixture loading; rates are never business or CDC TPS.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
	"ticketpulse/internal/ticket"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func post(ctx context.Context, endpoint string, payload []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("fixture projection HTTP=%d: %.160s", response.StatusCode, data)
	}
	return data, nil
}
func run() error {
	batch := flag.Int("batch", 500, "fixture batch")
	deadline := flag.Duration("deadline", 10*time.Minute, "fixture budget")
	flag.Parse()
	if *batch < 1 || *batch > 1000 {
		return fmt.Errorf("invalid batch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *deadline)
	defer cancel()
	started := time.Now()
	cfg, err := mysql.ParseDSN(os.Getenv("TICKETPULSE_BENCH_DSN"))
	if err != nil || !(strings.HasPrefix(cfg.DBName, "ticketpulse_test_m5_") || strings.HasPrefix(cfg.DBName, "ticketpulse_test_m502_")) {
		return fmt.Errorf("owned M5 test database required")
	}
	db, err := mysqlstore.Open(ctx, cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	var uuid string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil || uuid != os.Getenv("TICKETPULSE_TEST_SERVER_UUID") {
		return fmt.Errorf("source identity mismatch")
	}
	esURL, ckURL := os.Getenv("TICKETPULSE_ES_URL"), os.Getenv("TICKETPULSE_CK_URL")
	index, namespace := cfg.DBName+"_tickets_v1", cfg.DBName+"_analytics_v1"
	es, err := search.NewElasticsearch(esURL, index)
	if err != nil {
		return err
	}
	if err = es.EnsureIndex(ctx); err != nil {
		return err
	}
	ck, err := analytics.NewClickHouse(ckURL, namespace)
	if err != nil {
		return err
	}
	if err = ck.Ensure(ctx); err != nil {
		return err
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
	cp, err := syncpipe.CurrentPosition(ctx, db)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var sourceCount int64
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM tickets").Scan(&sourceCount); err != nil {
		return err
	}
	var factData, currentData, esData bytes.Buffer
	facts, currents := int64(0), int64(0)
	insert := func(table string, data *bytes.Buffer) error {
		if data.Len() == 0 {
			return nil
		}
		params := url.Values{"database": {namespace}, "query": {"INSERT INTO " + table + " FORMAT JSONEachRow"}, "async_insert": {"0"}, "wait_end_of_query": {"1"}}
		_, err := post(ctx, ckURL+"/?"+params.Encode(), data.Bytes())
		data.Reset()
		return err
	}
	flush := func() error {
		if err := insert("ticket_events", &factData); err != nil {
			return err
		}
		if err := insert("ticket_states", &currentData); err != nil {
			return err
		}
		if esData.Len() > 0 {
			data, err := post(ctx, esURL+"/_bulk", esData.Bytes())
			if err != nil {
				return err
			}
			var result struct {
				Errors bool `json:"errors"`
			}
			if err = json.Unmarshal(data, &result); err != nil || result.Errors {
				return fmt.Errorf("fixture ES bulk failed: %.300s", data)
			}
			esData.Reset()
		}
		return nil
	}
	appendRow := func(buffer *bytes.Buffer, value any) {
		data, _ := json.Marshal(value)
		buffer.Write(data)
		buffer.WriteByte(10)
	}
	lastID, lastVersion := int64(0), int64(0)
	for {
		rows, err := tx.QueryContext(ctx, `SELECT h.id,h.ticket_id,h.action,h.operator_id,h.version,h.before_state,h.after_state,h.responsible_team_id,h.occurred_at,h.reason,h.comment_id,t.organization_id,t.ticket_no,t.created_at FROM ticket_histories h JOIN tickets t ON t.id=h.ticket_id AND t.organization_id=h.organization_id WHERE h.ticket_id>? OR (h.ticket_id=? AND h.version>?) ORDER BY h.ticket_id,h.version LIMIT ?`, lastID, lastID, lastVersion, *batch)
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			var h ticket.History
			var before, after []byte
			var org int64
			var number string
			var created time.Time
			if err = rows.Scan(&h.ID, &h.TicketID, &h.Action, &h.OperatorID, &h.Version, &before, &after, &h.ResponsibleTeamID, &h.OccurredAt, &h.Reason, &h.CommentID, &org, &number, &created); err != nil {
				rows.Close()
				return err
			}
			if before != nil {
				h.Before = &ticket.State{}
				if err = json.Unmarshal(before, h.Before); err != nil {
					rows.Close()
					return err
				}
			}
			if err = json.Unmarshal(after, &h.After); err != nil {
				rows.Close()
				return err
			}
			s := h.After
			d := search.FromTicket(ticket.Ticket{ID: h.TicketID, OrganizationID: org, TicketNo: number, Title: s.Title, Description: s.Description, Category: s.Category, Priority: s.Priority, Status: s.Status, CreatorID: s.CreatorID, AssigneeID: s.AssigneeID, InitialTeamID: s.InitialTeamID, CurrentTeamID: s.CurrentTeamID, Version: h.Version, CreatedAt: created, UpdatedAt: h.OccurredAt})
			fact, _, err := syncpipe.AnalysisProjection(syncpipe.Event{Schema: 2, Kind: "ticket", ID: fmt.Sprintf("benchmark-load:%d:%d", h.TicketID, h.Version), Source: "snapshot", TicketID: h.TicketID, Version: h.Version, EventAt: h.OccurredAt, Document: d, History: &h, FirstResolvedAt: s.FirstResolvedAt})
			if err != nil {
				rows.Close()
				return err
			}
			appendRow(&factData, fact)
			facts++
			count++
			lastID = h.TicketID
			lastVersion = h.Version
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if err = flush(); err != nil {
			return err
		}
		if count < *batch {
			break
		}
	}
	lastID = 0
	for {
		rows, err := tx.QueryContext(ctx, `SELECT id,organization_id,ticket_no,title,description,status,priority,category,creator_id,assignee_id,initial_team_id,current_team_id,created_at,updated_at,version,first_resolved_at FROM tickets WHERE id>? ORDER BY id LIMIT ?`, lastID, *batch)
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			var d search.Document
			var first *time.Time
			if err = rows.Scan(&d.ID, &d.OrganizationID, &d.TicketNo, &d.Title, &d.Description, &d.Status, &d.Priority, &d.Category, &d.CreatorID, &d.AssigneeID, &d.InitialTeamID, &d.CurrentTeamID, &d.CreatedAt, &d.UpdatedAt, &d.Version, &first); err != nil {
				rows.Close()
				return err
			}
			d.PriorityRank = map[string]int{"low": 1, "medium": 2, "high": 3, "urgent": 4}[d.Priority]
			_, current, err := syncpipe.AnalysisProjection(syncpipe.Event{Schema: 2, Kind: "ticket", ID: fmt.Sprintf("benchmark-current:%d:%d", d.ID, d.Version), Source: "snapshot", TicketID: d.ID, Version: d.Version, EventAt: d.UpdatedAt, Document: d, FirstResolvedAt: first})
			if err != nil {
				rows.Close()
				return err
			}
			appendRow(&currentData, current)
			appendRow(&esData, map[string]any{"index": map[string]any{"_index": index, "_id": fmt.Sprint(d.ID), "version": d.Version, "version_type": "external"}})
			appendRow(&esData, d)
			currents++
			count++
			lastID = d.ID
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if err = flush(); err != nil {
			return err
		}
		if count < *batch {
			break
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = state.SaveCheckpoint(ctx, cp, true); err != nil {
		return err
	}
	offsets := map[int32]int64{}
	ends, err := q.Admin.ListEndOffsets(ctx, q.Topic)
	if err != nil {
		return err
	}
	for p := int32(0); p < q.Partitions; p++ {
		end, ok := ends.Lookup(q.Topic, p)
		if !ok || end.Err != nil {
			return fmt.Errorf("fixture offset unavailable")
		}
		offsets[p] = end.Offset
	}
	encoded, _ := json.Marshal(offsets)
	if _, err = db.ExecContext(ctx, `INSERT INTO sync_kafka_analytics_bootstrap(pipeline,topic,end_offsets,published_at) VALUES('ck-v1',?,?,UTC_TIMESTAMP(6))`, q.Topic, encoded); err != nil {
		return err
	}
	if err = es.Refresh(ctx); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"type": "fixture_bulk_load_not_api_or_cdc_throughput", "tickets": currents, "facts": facts, "duration_seconds": time.Since(started).Seconds(), "checkpoint_before_source_read": cp, "source_tickets": sourceCount, "index": index, "clickhouse_database": namespace})
}
