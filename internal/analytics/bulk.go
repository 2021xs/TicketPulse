package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

// InsertFacts/InsertCurrents keep synchronous inserts and list every column.
// A successful first table followed by a failed second table can be replayed;
// canonical business keys, rather than physical rows, provide deduplication.
func (c *ClickHouse) InsertFacts(ctx context.Context, facts []Fact) error {
	return insertBatchRows(ctx, c, "INSERT INTO ticket_events (organization_id,ticket_id,version,business_event_id,transport_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at) FORMAT JSONEachRow", facts)
}
func (c *ClickHouse) InsertCurrents(ctx context.Context, currents []Current) error {
	return insertBatchRows(ctx, c, "INSERT INTO ticket_states (organization_id,ticket_id,version,complete,current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at) FORMAT JSONEachRow", currents)
}
func insertBatchRows[T any](ctx context.Context, c *ClickHouse, query string, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) > 500 {
		return fmt.Errorf("ClickHouse batch exceeds 500 row budget")
	}
	var payload bytes.Buffer
	flush := func() error {
		if payload.Len() == 0 {
			return nil
		}
		response, err := c.request(ctx, query, nil, payload.Bytes(), true)
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(response)) != 0 {
			return fmt.Errorf("ClickHouse insert returned unexpected response body")
		}
		payload.Reset()
		return nil
	}
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return err
		}
		if len(data)+1 > 4<<20 {
			return fmt.Errorf("ClickHouse row exceeds 4 MiB request budget")
		}
		if payload.Len()+len(data)+1 > 4<<20 {
			if err = flush(); err != nil {
				return err
			}
		}
		payload.Write(data)
		payload.WriteByte(10)
	}
	return flush()
}
