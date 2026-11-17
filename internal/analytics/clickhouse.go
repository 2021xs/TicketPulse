package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	assets "ticketpulse/db"
)

type ClickHouse struct {
	endpoint, database string
	client             *http.Client
}

func NewClickHouse(endpoint, database string) (*ClickHouse, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" || !regexp.MustCompile(`^[a-z0-9][a-z0-9_]{0,127}$`).MatchString(database) {
		return nil, fmt.Errorf("invalid ClickHouse endpoint/namespace")
	}
	return &ClickHouse{strings.TrimRight(endpoint, "/"), database, &http.Client{Timeout: 6 * time.Second}}, nil
}

type WriteError struct {
	HTTPStatus int
	Code       int
}

func (e *WriteError) Error() string {
	return fmt.Sprintf("ClickHouse request failed: HTTP=%d code=%d", e.HTTPStatus, e.Code)
}
func (e *WriteError) Permanent() bool {
	return e.Code == 16 || e.Code == 47 || e.Code == 53 || e.Code == 62 || e.Code == 69 || e.Code == 70 || e.Code == 81 || e.Code == 117 || e.Code == 497 || e.HTTPStatus == 401 || e.HTTPStatus == 403
}

func (c *ClickHouse) request(ctx context.Context, query string, params map[string]string, data []byte, database bool) ([]byte, error) {
	values := url.Values{"wait_end_of_query": {"1"}, "max_execution_time": {"4"}, "max_threads": {"2"}, "output_format_json_quote_64bit_integers": {"0"}, "input_format_skip_unknown_fields": {"0"}, "async_insert": {"0"}}
	if database {
		values.Set("database", c.database)
	}
	for key, value := range params {
		values.Set("param_"+key, value)
	}
	var body []byte
	if data != nil {
		values.Set("query", query)
		body = data
	} else {
		body = []byte(query)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/?"+values.Encode(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(res.Body, (8<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > 8<<20 {
		return nil, fmt.Errorf("ClickHouse response exceeds complete-result budget")
	}
	if res.StatusCode != 200 {
		code, _ := strconv.Atoi(res.Header.Get("X-ClickHouse-Exception-Code"))
		return nil, &WriteError{res.StatusCode, code}
	}
	return payload, nil
}
func (c *ClickHouse) Ensure(ctx context.Context) error {
	if _, err := c.request(ctx, "CREATE DATABASE IF NOT EXISTS "+c.database, nil, nil, false); err != nil {
		return err
	}
	for _, sql := range strings.Split(assets.ClickHouseSchema, ";") {
		if strings.TrimSpace(sql) == "" {
			continue
		}
		if _, err := c.request(ctx, sql, nil, nil, true); err != nil {
			return err
		}
	}
	return nil
}
func (c *ClickHouse) InsertFact(ctx context.Context, f Fact) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	_, err = c.request(ctx, "INSERT INTO ticket_events (organization_id,ticket_id,version,business_event_id,transport_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at) FORMAT JSONEachRow", nil, append(data, '\n'), true)
	return err
}
func (c *ClickHouse) InsertCurrent(ctx context.Context, current Current) error {
	data, err := json.Marshal(current)
	if err != nil {
		return err
	}
	_, err = c.request(ctx, "INSERT INTO ticket_states (organization_id,ticket_id,version,complete,current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at) FORMAT JSONEachRow", nil, append(data, '\n'), true)
	return err
}

// Version minima/maxima are idempotent on equal business-version replays. Each
// ticket is reduced once; arrayJoin emits metrics from those same first/current
// facts, without a creation join or repeated CTE scans. Date/team filters run
// after first/latest selection so old late versions and transfers keep M3 scope.
// Exact distinct business contents are compared with distinct version keys. A
// mismatch emits a conflict row and fails the complete query, including conflicts
// outside the selected date/team. Nullable fields remain inside tuples. Transport
// IDs and ingestion time are deliberately excluded from business equality.
// Memory settings use the existing ORDER BY prefix and bound buffered aggregate
// blocks; they do not limit admitted requests, alter windows, or cache results.
const Canonical = `WITH
firsts AS (
 SELECT ticket_id,
 argMinIf(tuple(occurred_at,initial_team_id),version,action='created') AS created,
 argMinIf(tuple(occurred_at,event_team_id),version,action='resolved' AND after_status='awaiting_confirmation') AS resolved,
 countIf(action='created')>0 AS has_created,
 countIf(action='resolved' AND after_status='awaiting_confirmation')>0 AS has_resolved,
 uniqExact(tuple(version,business_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at))!=uniqExact(version) AS conflicting
 FROM ticket_events WHERE organization_id={org:UInt64}
 GROUP BY organization_id,ticket_id
),
event_rows AS (
 SELECT arrayJoin([
 tuple('created',created.1,created.2,has_created,toInt64(0)),
 tuple('resolved',resolved.1,resolved.2,has_created AND has_resolved,dateDiff('microsecond',created.1,resolved.1)),
 tuple('conflict',toDateTime64(0,6,'UTC'),toUInt64(0),conflicting,toInt64(0))
 ]) AS value FROM firsts
),
latest AS (
 SELECT ticket_id,argMax(tuple(current_team_id,status),tuple(version,complete)) AS current,
 uniqExact(tuple(version,complete,current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at))!=uniqExact(tuple(version,complete)) AS conflicting
 FROM ticket_states WHERE organization_id={org:UInt64}
 GROUP BY organization_id,ticket_id
),
state_rows AS (
 SELECT arrayJoin([
 tuple('backlog',current.1,toUInt64(current.2 IN ('pending','in_progress') AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},current.1)))),
 tuple('observed',toUInt64(0),toUInt64(1)),
 tuple('conflict',toUInt64(0),toUInt64(conflicting))
 ]) AS value FROM latest
 UNION ALL
 SELECT tuple('observed',toUInt64(0),toUInt64(0)) AS value
),
metrics AS (
 SELECT value.1 AS metric,if(value.1='conflict','',toString(toDate(value.2,'Asia/Shanghai'))) AS day,value.3 AS team_id,count() AS amount,sum(value.5)/1000000.0 AS duration
 FROM event_rows WHERE value.4 AND (value.1='conflict' OR (value.2>={start:DateTime64(6,'UTC')} AND value.2<{end:DateTime64(6,'UTC')}
 AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},value.3))))
 GROUP BY metric,day,team_id
 UNION ALL
 SELECT value.1 AS metric,'' AS day,value.2 AS team_id,sum(value.3) AS amount,toFloat64(0) AS duration
 FROM state_rows WHERE value.3>0 OR value.1='observed' GROUP BY metric,team_id
)
SELECT metric,day,team_id,amount,duration FROM metrics WHERE throwIf(metric='conflict','conflicting ClickHouse business version')=0 ORDER BY metric,day,team_id
SETTINGS optimize_aggregation_in_order=1,aggregation_in_order_max_block_bytes=131072,max_block_size=1024,read_in_order_use_buffering=0,max_threads=1,max_final_threads=1
FORMAT JSONEachRow`

type MetricRow struct {
	Metric   string  `json:"metric"`
	Day      string  `json:"day"`
	TeamID   int64   `json:"team_id"`
	Amount   int64   `json:"amount"`
	Duration float64 `json:"duration"`
}

func (c *ClickHouse) Metrics(ctx context.Context, org int64, r Request) ([]MetricRow, error) {
	start, end, err := r.Window()
	if err != nil {
		return nil, err
	}
	teams, _ := json.Marshal(r.TeamIDs)
	if string(teams) == "null" {
		teams = []byte("[]")
	}
	params := map[string]string{"org": strconv.FormatInt(org, 10), "start": Timestamp(start), "end": Timestamp(end), "teams": string(teams)}
	data, err := c.request(ctx, Canonical, params, nil, true)
	if err != nil {
		var network net.Error
		var write *WriteError
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil || (errors.As(err, &network) && network.Timeout()) || (errors.As(err, &write) && write.Code == 159) {
			return nil, ErrTimeout
		}
		return nil, ErrUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	rows := []MetricRow{}
	for {
		var row MetricRow
		err := decoder.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, ErrUnavailable
		}
		if row.Metric != "created" && row.Metric != "resolved" && row.Metric != "backlog" && row.Metric != "observed" {
			return nil, ErrUnavailable
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (c *ClickHouse) Query(ctx context.Context, sql string) ([]byte, error) {
	return c.request(ctx, sql, nil, nil, true)
}
