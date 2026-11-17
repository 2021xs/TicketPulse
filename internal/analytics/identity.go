package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

func (c *ClickHouse) Endpoint() string { return c.endpoint }
func (c *ClickHouse) Database() string { return c.database }

type TableIdentity struct {
	Events string `json:"ticket_events_uuid"`
	States string `json:"ticket_states_uuid"`
}

func (i TableIdentity) String() string {
	return "ticket_events:" + i.Events + ",ticket_states:" + i.States
}

// Query the system namespace so absence can be distinguished from an HTTP
// failure. A partially recreated pair of tables is an error, never a success.
func (c *ClickHouse) Identity(ctx context.Context) (identity TableIdentity, exists bool, err error) {
	data, err := c.request(ctx, "SELECT name,toString(uuid) AS uuid FROM system.tables WHERE database={database:String} AND name IN ('ticket_states','ticket_events') ORDER BY name FORMAT JSONEachRow", map[string]string{"database": c.database}, nil, false)
	if err != nil {
		return identity, false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	count := 0
	for {
		var row struct {
			Name string `json:"name"`
			UUID string `json:"uuid"`
		}
		err = decoder.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return identity, false, err
		}
		count++
		if row.Name == "ticket_events" && identity.Events == "" {
			identity.Events = row.UUID
		} else if row.Name == "ticket_states" && identity.States == "" {
			identity.States = row.UUID
		} else {
			return identity, false, fmt.Errorf("unexpected CK identity rows")
		}
	}
	if count == 0 {
		return identity, false, nil
	}
	if count != 2 || identity.Events == "" || identity.States == "" || identity.Events == "00000000-0000-0000-0000-000000000000" || identity.States == "00000000-0000-0000-0000-000000000000" {
		return identity, false, fmt.Errorf("incomplete CK target tables")
	}
	return identity, true, nil
}
