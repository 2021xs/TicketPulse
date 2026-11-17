package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

type ReconcileKey struct {
	OrganizationID int64 `json:"organization_id"`
	TicketID       int64 `json:"ticket_id"`
	Version        int64 `json:"version"`
}
type ReconcileCurrent struct {
	Current
	Variants     int64 `json:"variants"`
	PhysicalRows int64 `json:"physical_rows"`
}
type ReconcileFact struct {
	Fact
	Variants     int64 `json:"variants"`
	PhysicalRows int64 `json:"physical_rows"`
}

func decodeReconcileRows[T any](data []byte, size int) ([]T, error) {
	result := make([]T, 0)
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var row T
		err := decoder.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, row)
		if len(result) > size {
			return nil, fmt.Errorf("ClickHouse reconciliation response exceeds batch")
		}
	}
	return result, nil
}
func reconcileParams(org int64, after ReconcileKey, size int) (map[string]string, error) {
	if org < 0 || size < 1 || size > 1000 {
		return nil, fmt.Errorf("invalid ClickHouse reconciliation batch")
	}
	return map[string]string{"org": strconv.FormatInt(org, 10), "after_org": strconv.FormatInt(after.OrganizationID, 10), "after_id": strconv.FormatInt(after.TicketID, 10), "after_version": strconv.FormatInt(after.Version, 10), "size": strconv.Itoa(size)}, nil
}

// The complete tuple is selected with the production (version,complete) order.
// uniqExact also checks conflicting contents at equal version/complete; NULLs
// stay inside tuples instead of being silently skipped by per-column argMax.
func (c *ClickHouse) ReconcileCurrentPage(ctx context.Context, org int64, after ReconcileKey, size int) ([]ReconcileCurrent, error) {
	params, err := reconcileParams(org, after, size)
	if err != nil {
		return nil, err
	}
	data, err := c.request(ctx, `WITH versions AS (
 SELECT organization_id,ticket_id,version,complete,
 argMin(tuple(current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at),ingested_at) AS content,
 uniqExact(tuple(current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at)) AS variants,count() AS physical_rows
 FROM ticket_states WHERE ({org:UInt64}=0 OR organization_id={org:UInt64}) AND tuple(organization_id,ticket_id)>tuple({after_org:UInt64},{after_id:UInt64})
 GROUP BY organization_id,ticket_id,version,complete
), latest AS (
 SELECT organization_id,ticket_id,argMax(tuple(version,complete,content),tuple(version,complete)) AS current,max(variants) AS variants,sum(physical_rows) AS physical_rows
 FROM versions GROUP BY organization_id,ticket_id ORDER BY organization_id,ticket_id LIMIT {size:UInt32}
) SELECT organization_id,ticket_id,current.1 AS version,current.2 AS complete,
 current.3.1 AS current_team_id,current.3.2 AS assignee_id,current.3.3 AS status,current.3.4 AS initial_team_id,
 toString(current.3.5) AS created_at,toString(current.3.6) AS updated_at,toString(current.3.7) AS first_resolved_at,variants,physical_rows
 FROM latest ORDER BY organization_id,ticket_id FORMAT JSONEachRow`, params, nil, true)
	if err != nil {
		return nil, err
	}
	return decodeReconcileRows[ReconcileCurrent](data, size)
}

// Transport event IDs and ingestion time may differ on replay. Every persisted
// business field must agree for an organization/ticket/version key. A tuple
// sample is used only alongside its exact variant count, never to hide conflict.
func (c *ClickHouse) ReconcileFactPage(ctx context.Context, org int64, after ReconcileKey, size int) ([]ReconcileFact, error) {
	params, err := reconcileParams(org, after, size)
	if err != nil {
		return nil, err
	}
	data, err := c.request(ctx, `WITH canonical AS (
 SELECT organization_id,ticket_id,version,
 argMin(tuple(business_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at),transport_event_id) AS fact,
 uniqExact(tuple(business_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at)) AS variants,count() AS physical_rows
 FROM ticket_events WHERE ({org:UInt64}=0 OR organization_id={org:UInt64})
 AND tuple(organization_id,ticket_id,version)>tuple({after_org:UInt64},{after_id:UInt64},{after_version:UInt64})
 GROUP BY organization_id,ticket_id,version ORDER BY organization_id,ticket_id,version LIMIT {size:UInt32}
) SELECT organization_id,ticket_id,version,fact.1 AS business_event_id,fact.2 AS action,toString(fact.3) AS occurred_at,
 fact.4 AS event_team_id,fact.5 AS initial_team_id,fact.6 AS before_team_id,fact.7 AS before_status,fact.8 AS after_status,
 fact.9 AS assignee_id,toString(fact.10) AS created_at,toString(fact.11) AS first_resolved_at,variants,physical_rows
 FROM canonical ORDER BY organization_id,ticket_id,version FORMAT JSONEachRow`, params, nil, true)
	if err != nil {
		return nil, err
	}
	return decodeReconcileRows[ReconcileFact](data, size)
}
func (c *ClickHouse) ReconcileTarget() string { return c.endpoint + "/" + c.database }
