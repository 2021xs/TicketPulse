package syncpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
)

// TargetStatus reads only this candidate's namespaces. It is also usable as a
// FreshnessReader for a future managed Search; M4-03B does not wire it to ACTIVE.
type TargetStatus struct{ Target *TargetConsumerConfig }

func (s *TargetStatus) status(ctx context.Context, component string) (analytics.SyncStatus, error) {
	t := s.Target
	result := analytics.SyncStatus{State: "unknown", ObservedAt: time.Now().UTC(), Topic: t.Control.Queue.Topic, ConsumerGroup: t.Handoff.Group(component), PartitionOffsets: []analytics.PartitionOffset{}}
	if err := t.Check(ctx); err != nil {
		return result, err
	}
	h, v, err := t.Control.load(ctx, t.Handoff.ID)
	if err != nil {
		return result, err
	}
	progress, err := t.Control.committedProgress(ctx, h, v, component)
	if err != nil {
		result.State = "broker_unavailable"
		result.LastError = strPtr(err.Error())
		return result, nil
	}
	var sourceID string
	err = t.Control.Store.DB.QueryRowContext(ctx, `SELECT barrier_id,issued_at FROM sync_source_watermark WHERE source_name='tickets-v1'`).Scan(&sourceID, &result.SourceWatermark)
	if err != nil && err != sql.ErrNoRows {
		return result, err
	}
	var storedGroup string
	var storedEpoch int64
	err = t.Control.Store.DB.QueryRowContext(ctx, `SELECT state,heartbeat_at,last_error,group_id,owner_epoch FROM sync_projection_worker_state WHERE version_id=? AND component=?`, v.ID, component).Scan(&result.State, &result.HeartbeatAt, &result.LastError, &storedGroup, &storedEpoch)
	if err != nil && err != sql.ErrNoRows {
		return result, err
	}
	owner, err := projection.ReadOwner(ctx, t.Control.Store.DB, projection.VersionTargetKey(v, component), "")
	if err != nil {
		return result, err
	}
	writerLive := false
	if owner.Token != nil && owner.ConnectionID != nil {
		var connection sql.NullInt64
		if err = t.Control.Store.DB.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", projection.TargetLockName(owner.Key)).Scan(&connection); err != nil {
			return result, err
		}
		writerLive = connection.Valid && connection.Int64 == *owner.ConnectionID && storedGroup == h.Group(component) && storedEpoch == owner.Epoch
	}
	result.BootstrapComplete = h.FinalReportSHA256 != nil
	allMarkers := sourceID != ""
	for _, p := range progress {
		result.Pending += uint64(p.Lag)
		result.PartitionOffsets = append(result.PartitionOffsets, analytics.PartitionOffset{Partition: p.Partition, Committed: p.Committed, End: p.End, Lag: p.Lag})
		var id string
		var issued time.Time
		var offset int64
		err = t.Control.Store.DB.QueryRowContext(ctx, `SELECT barrier_id,issued_at,record_offset FROM sync_projection_watermarks WHERE version_id=? AND component=? AND partition_id=? AND topic=?`, v.ID, component, p.Partition, t.Control.Queue.Topic).Scan(&id, &issued, &offset)
		if err != nil && err != sql.ErrNoRows {
			return result, err
		}
		if err != nil || p.Committed <= offset || id != sourceID {
			allMarkers = false
		} else if result.ConfirmedThrough == nil || issued.Before(*result.ConfirmedThrough) {
			value := issued
			result.ConfirmedThrough = &value
		}
	}
	if !allMarkers {
		result.ConfirmedThrough = nil
	}
	rows, err := t.Control.Store.DB.QueryContext(ctx, `SELECT status,COUNT(*),SUM(attempts) FROM sync_projection_deliveries WHERE version_id=? AND component=? GROUP BY status`, v.ID, component)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var status string
		var count, attempts int64
		if err = rows.Scan(&status, &count, &attempts); err != nil {
			rows.Close()
			return result, err
		}
		result.ProcessingAttempts += attempts
		switch status {
		case "applied", "duplicate_or_old":
			result.Applied += count
		case "isolated":
			result.Isolated = count
		case "retrying":
			result.Retrying = count
		case "processing":
			result.AckPending = int(count)
		case "recovered":
			result.Recovered = count
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	err = t.Control.Store.DB.QueryRowContext(ctx, `SELECT MAX(last_seen_at) FROM sync_projection_deliveries WHERE version_id=? AND component=? AND status IN ('applied','duplicate_or_old') AND ticket_id IS NOT NULL`, v.ID, component).Scan(&result.LastSuccessfulWrite)
	if err != nil {
		return result, err
	}
	fresh := writerLive && result.HeartbeatAt != nil && time.Since(*result.HeartbeatAt) < 10*time.Second && result.SourceWatermark != nil && time.Since(*result.SourceWatermark) < 10*time.Second
	if !fresh {
		result.State = "stalled"
	} else if result.Isolated > 0 {
		result.State = "degraded"
	}
	result.CaughtUp = fresh && result.State == "running" && result.BootstrapComplete && allMarkers && result.Pending == 0 && result.Isolated == 0 && result.Retrying == 0 && result.AckPending == 0
	if !result.CaughtUp && result.State == "running" {
		result.State = "catching_up"
	}
	return result, nil
}
func strPtr(s string) *string { return &s }
func (s *TargetStatus) AnalyticsStatus(ctx context.Context) (analytics.SyncStatus, error) {
	return s.status(ctx, "ck")
}
func (s *TargetStatus) SearchFreshness(ctx context.Context) (search.Freshness, error) {
	status, err := s.status(ctx, "es")
	if err != nil {
		return search.Freshness{}, err
	}
	var eventAt *time.Time
	err = s.Target.Control.Store.DB.QueryRowContext(ctx, `SELECT last_event_at FROM sync_projection_worker_state WHERE version_id=? AND component='es'`, s.Target.Version.ID).Scan(&eventAt)
	if err != nil && err != sql.ErrNoRows {
		return search.Freshness{}, err
	}
	caught := status.CaughtUp
	return search.Freshness{State: status.State, LastEventAt: eventAt, LastSuccessAt: status.LastSuccessfulWrite, CaughtUp: &caught}, nil
}
func (s *TargetStatus) Diagnostics(ctx context.Context) (map[string]any, error) {
	es, err := s.status(ctx, "es")
	if err != nil {
		return nil, err
	}
	ck, err := s.status(ctx, "ck")
	if err != nil {
		return nil, err
	}
	owners := map[string]any{}
	for _, component := range []string{"es", "ck"} {
		o, err := s.Target.TargetMetadata(ctx, component)
		if err != nil {
			return nil, err
		}
		owners[component] = o
	}
	return map[string]any{"version_id": s.Target.Version.ID, "handoff_id": s.Target.Handoff.ID, "es": es, "ck": ck, "owners": owners, "api_active_changed": false}, nil
}

// Verify pins a new marker, waits for both new groups to apply and commit it,
// and performs a fresh full content check. It never trusts the old M4 marker.
func (p *HandoffControl) Verify(ctx context.Context, id string) (result projection.Handoff, err error) {
	defer func() { p.failed(id, err) }()
	release, err := p.operationLock(ctx, id)
	if err != nil {
		return result, err
	}
	defer release()
	h, v, err := p.load(ctx, id)
	if err != nil {
		return h, err
	}
	if h.Phase == projection.Verified {
		if err = p.checkIdentities(ctx, v); err != nil {
			return h, err
		}
		return h, nil
	}
	if h.Phase != projection.Consuming {
		return h, fmt.Errorf("verification requires CONSUMING")
	}
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return h, err
	}
	defer tx.Rollback()
	if err = p.checkFrozen(ctx, tx, h); err != nil {
		return h, err
	}
	locked, err := (&projection.HandoffStore{DB: p.Store.DB}).Lock(ctx, tx, id)
	if err != nil {
		return h, err
	}
	if locked.Phase != projection.Consuming {
		return h, fmt.Errorf("handoff phase changed before verification")
	}
	target, err := p.ConsumerTarget(ctx, id)
	if err != nil {
		return h, err
	}
	if err = p.Store.IssueWatermark(ctx); err != nil {
		return h, err
	}
	var marker string
	var issued time.Time
	if err = p.Store.DB.QueryRowContext(ctx, `SELECT barrier_id,issued_at FROM sync_source_watermark WHERE source_name='tickets-v1'`).Scan(&marker, &issued); err != nil {
		return h, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sync_projection_handoffs SET verification_marker=?,updated_at=UTC_TIMESTAMP(6) WHERE handoff_id=? AND phase='CONSUMING'`, marker, id); err != nil {
		return h, err
	}
	markerOffsets := map[string]map[int32]int64{}
	for {
		complete := true
		for _, component := range []string{"es", "ck"} {
			if err = target.Check(ctx); err != nil {
				return h, err
			}
			progress, e := p.committedProgress(ctx, h, v, component)
			if e != nil {
				return h, e
			}
			confirmed := map[int32]int64{}
			for _, part := range progress {
				var offset int64
				var state string
				e = p.Store.DB.QueryRowContext(ctx, `SELECT record_offset,status FROM sync_projection_deliveries WHERE version_id=? AND component=? AND topic=? AND partition_id=? AND event_id=? ORDER BY record_offset ASC LIMIT 1`, v.ID, component, p.Queue.Topic, part.Partition, marker).Scan(&offset, &state)
				if e == sql.ErrNoRows {
					complete = false
					continue
				}
				if e != nil {
					return h, e
				}
				if state != "applied" || part.Committed <= offset {
					complete = false
					continue
				}
				confirmed[part.Partition] = offset
			}
			markerOffsets[component] = confirmed
			o, e := projection.ReadOwner(ctx, p.Store.DB, projection.VersionTargetKey(v, component), "")
			if e != nil {
				return h, e
			}
			var connection sql.NullInt64
			if e = p.Store.DB.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", projection.TargetLockName(o.Key)).Scan(&connection); e != nil {
				return h, e
			}
			if o.Token == nil || o.ConnectionID == nil || !connection.Valid || connection.Int64 != *o.ConnectionID {
				complete = false
			}
			var outstanding int
			if e = p.Store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_projection_deliveries WHERE version_id=? AND component=? AND status NOT IN ('applied','duplicate_or_old','recovered')`, v.ID, component).Scan(&outstanding); e != nil {
				return h, e
			}
			if outstanding != 0 {
				complete = false
			}
		}
		if complete {
			break
		}
		select {
		case <-ctx.Done():
			return h, fmt.Errorf("HANDOFF_VERIFY_TIMEOUT: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	report, err := Reconcile(ctx, p.Store, nil, p.ES, p.CK, ReconcileOptions{BatchSize: 250, ConfirmSourceBoundary: func(ctx context.Context) error { return target.Check(ctx) }})
	if err != nil {
		return h, err
	}
	if report.State != ReconcileConsistent || report.SourceChanged || report.DifferenceCount != 0 || report.SourceAfter == nil {
		return h, fmt.Errorf("handoff verification not complete consistent")
	}
	if err = p.checkIdentities(ctx, v); err != nil {
		return h, err
	}
	// Do not verify while a failed worker/isolated record appeared during audit.
	for _, component := range []string{"es", "ck"} {
		var outstanding int
		if err = p.Store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_projection_deliveries WHERE version_id=? AND component=? AND status NOT IN ('applied','duplicate_or_old','recovered')`, v.ID, component).Scan(&outstanding); err != nil {
			return h, err
		}
		if outstanding != 0 {
			return h, fmt.Errorf("candidate has outstanding records after full audit")
		}
		owner, e := projection.ReadOwner(ctx, p.Store.DB, projection.VersionTargetKey(v, component), "")
		if e != nil {
			return h, e
		}
		var connection sql.NullInt64
		if e = p.Store.DB.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", projection.TargetLockName(owner.Key)).Scan(&connection); e != nil {
			return h, e
		}
		if owner.Kind != "consumer_group" || owner.Owner != h.Group(component) || owner.Token == nil || owner.ConnectionID == nil || !connection.Valid || connection.Int64 != *owner.ConnectionID {
			return h, fmt.Errorf("writer stopped during final audit")
		}
		var state, group string
		var epoch int64
		var beat time.Time
		if e = p.Store.DB.QueryRowContext(ctx, `SELECT state,group_id,owner_epoch,heartbeat_at FROM sync_projection_worker_state WHERE version_id=? AND component=?`, v.ID, component).Scan(&state, &group, &epoch, &beat); e != nil {
			return h, e
		}
		if state != "running" || group != h.Group(component) || epoch != owner.Epoch || time.Since(beat) > 10*time.Second {
			return h, fmt.Errorf("candidate writer state is not current")
		}
		groups, e := p.Queue.Admin.DescribeGroups(ctx, h.Group(component))
		if e != nil {
			return h, e
		}
		g, exists := groups[h.Group(component)]
		if !exists || g.Err != nil || g.State != "Stable" || len(g.Members) != 1 {
			return h, fmt.Errorf("candidate group is not a single Stable member")
		}
	}
	data, _ := json.Marshal(report)
	hash, err := projection.HashJSON(data)
	if err != nil {
		return h, err
	}
	for component, positions := range markerOffsets {
		for part, offset := range positions {
			if _, err = tx.ExecContext(ctx, `UPDATE sync_projection_handoff_partitions SET verification_offset=? WHERE handoff_id=? AND component=? AND partition_id=?`, offset, id, component, part); err != nil {
				return h, err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sync_projection_handoffs SET phase='VERIFIED',verification_report=?,verification_report_sha256=?,verified_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6),revision=revision+1,last_error=NULL WHERE handoff_id=? AND phase='CONSUMING'`, data, hash, id); err != nil {
		return h, err
	}
	if err = p.log(ctx, tx, id, "verified", map[string]any{"marker": marker, "issued_at": issued, "marker_offsets": markerOffsets, "report_sha256": hash, "api_active_changed": false}); err != nil {
		return h, err
	}
	if err = tx.Commit(); err != nil {
		return h, fmt.Errorf("verify commit outcome unknown; read durable handoff: %w", err)
	}
	result, _, err = p.load(ctx, id)
	return
}
