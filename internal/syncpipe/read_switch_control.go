package syncpipe

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"ticketpulse/internal/projection"
)

// Read control changes only its independent selector and operation metadata.
// Immutable enrollment, B ownership and all Kafka offsets remain unchanged.
type ReadSwitchControl struct {
	*ProjectionControl
	Observe func(string, any) error
}

func (p *ReadSwitchControl) observe(stage string, value any) error {
	if p.Observe != nil {
		return p.Observe(stage, value)
	}
	return nil
}
func (p *ReadSwitchControl) readStore() *projection.ReadControlStore {
	return &projection.ReadControlStore{DB: p.Store.DB}
}
func (p *ReadSwitchControl) lock(ctx context.Context) (func(), error) {
	return p.Store.Lock(ctx, "read-control")
}
func readCAS(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("READ_SWITCH_CAS_FAILED")
	}
	return nil
}

func (p *ReadSwitchControl) InitializeReadSelection(ctx context.Context, id string) (projection.ReadSelection, error) {
	release, err := p.lock(ctx)
	if err != nil {
		return projection.ReadSelection{}, err
	}
	defer release()
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.ReadSelection{}, err
	}
	defer tx.Rollback()
	if _, err = projection.LockFrozen(ctx, tx); err != nil {
		return projection.ReadSelection{}, err
	}
	managed, err := p.Managed(ctx, id)
	if err != nil {
		return projection.ReadSelection{}, err
	}
	selection, err := p.readStore().Initialize(ctx, tx, managed.Version)
	if err != nil {
		return selection, err
	}
	if err = tx.Commit(); err != nil {
		return selection, fmt.Errorf("read initialization outcome unknown; read selection before retry: %w", err)
	}
	return p.readStore().Selection(ctx)
}

func (p *ReadSwitchControl) BeginReadSwitch(ctx context.Context, id, direction, destination string, expectedReadRevision int64, reverseOf ...string) (projection.ReadSwitch, error) {
	return p.beginReadSwitch(ctx, id, direction, destination, expectedReadRevision, reverseOf)
}

type ReadSwitchStatus struct {
	Enabled              bool                      `json:"enabled"`
	Gate                 projection.Gate           `json:"gate"`
	Selection            projection.ReadSelection  `json:"selection"`
	Operation            *projection.ReadSwitch    `json:"operation,omitempty"`
	ProofIsHistorical    bool                      `json:"proof_requires_live_revalidation"`
	APIReadTargetChanged bool                      `json:"api_read_target_changed"`
	Execution            *projection.ReadExecution `json:"execution,omitempty"`
}

func (p *ReadSwitchControl) ReadSwitchStatus(ctx context.Context, id string) (ReadSwitchStatus, error) {
	result := ReadSwitchStatus{ProofIsHistorical: true}
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	result.Enabled, err = projection.ReadControlEnabled(ctx, tx)
	if err != nil {
		return result, err
	}
	result.Gate, err = projection.ReadGate(ctx, tx)
	if err != nil {
		return result, err
	}
	if !result.Enabled {
		return result, nil
	}
	// Use the same snapshot for selection, operation and gate.
	result.Selection, err = p.readStore().ReadSelectionIn(ctx, tx)
	if err != nil {
		return result, err
	}
	if id == "" && result.Selection.CurrentOperationID != nil {
		id = *result.Selection.CurrentOperationID
	}
	if id != "" {
		o, e := p.readStore().ReadOperationIn(ctx, tx, id)
		if e != nil {
			return result, e
		}
		result.Operation = &o
		var runtimeInstalled, executions int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_control_log WHERE operation='install_read_runtime'").Scan(&runtimeInstalled); err != nil {
			return result, err
		}
		if runtimeInstalled > 0 {
			if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_read_execution WHERE operation_id=?", id).Scan(&executions); err != nil {
				return result, err
			}
		}
		if executions > 0 || o.Stage == projection.ReadCommitted || o.Stage == projection.ReadAPIReady || o.Stage == projection.ReadResumed {
			e, err := p.readStore().ExecutionIn(ctx, tx, id)
			if err != nil {
				return result, err
			}
			result.Execution = &e
		}
	}
	result.APIReadTargetChanged = result.Selection.ReadRevision > 1
	return result, tx.Commit()
}

func (p *ReadSwitchControl) CancelPlannedReadSwitch(ctx context.Context, id string) (projection.ReadSwitch, error) {
	release, err := p.lock(ctx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	defer release()
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	selected, err := p.readStore().LockSelection(ctx, tx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	o, err := p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return o, err
	}
	if o.Stage == projection.ReadCancelled {
		if g.FreezeID == nil || *g.FreezeID != o.FreezeID || g.Revision != o.FreezeRevision || selected.VersionID != o.FromVersionID || selected.ContractSHA256 != o.FromContractSHA256 || selected.ReadRevision != o.ExpectedReadRevision || !reflect.DeepEqual(selected.CurrentOperationID, o.RollbackOf) {
			return o, fmt.Errorf("cancelled intent no longer matches current freeze/selection")
		}
		if o.RollbackOf != nil {
			prior, e := p.readStore().LockOperation(ctx, tx, *o.RollbackOf)
			if e != nil {
				return o, e
			}
			if prior.Stage == projection.ReadCommitted || prior.Stage == projection.ReadAPIReady {
				// An explicitly cancelled recovery restores the still-unfinished
				// predecessor. Retrying reports that committed cancellation,
				// without granting Resume or cancelling the predecessor.
				pending, e := activeReadOperationCount(ctx, tx)
				if e != nil || pending != 1 || prior.SupersededBy != nil || prior.FreezeID != o.FreezeID || prior.FreezeRevision != o.FreezeRevision || prior.ToVersionID != o.FromVersionID || prior.ExpectedReadRevision+1 != o.ExpectedReadRevision {
					return o, fmt.Errorf("cancelled recovery predecessor differs")
				}
				var audit []byte
				if e = tx.QueryRowContext(ctx, "SELECT details FROM sync_projection_control_log WHERE operation='read_switch_cancelled' AND reference_id=?", o.ID).Scan(&audit); e != nil {
					return o, e
				}
				raw, _ := json.Marshal(o)
				expected, e1 := projection.HashJSON(raw)
				actual, e2 := projection.HashJSON(audit)
				if e1 != nil || e2 != nil || expected != actual {
					return o, fmt.Errorf("cancelled recovery audit differs")
				}
				return o, nil
			}
		}
		if err = projection.CheckReadResume(ctx, tx, g); err != nil {
			if errors.Is(err, projection.ErrReadCancelResume) {
				return o, nil
			}
			return o, err
		}
		return o, nil
	}
	if err = projection.MatchPlanned(g, selected, o); err != nil {
		return o, err
	}
	pending, e2 := activeReadOperationCount(ctx, tx)
	if e2 != nil || pending != 1 {
		return o, fmt.Errorf("read cancellation has conflicting operations: %v", err)
	}
	// Re-read both immutable contracts before permitting the only C1 escape.
	for id, hash := range map[string]string{o.FromVersionID: o.FromContractSHA256, o.ToVersionID: o.ToContractSHA256} {
		v, e := (&projection.VersionStore{DB: p.Store.DB}).Get(ctx, id)
		if e != nil || v.Fingerprint() != hash {
			return o, fmt.Errorf("cancel contract differs: %v", e)
		}
	}
	if err = readCAS(tx.ExecContext(ctx, `UPDATE sync_projection_read_switches SET stage='CANCELLED',operation_revision=operation_revision+1,cancelled_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6),last_error=NULL WHERE operation_id=? AND stage='PLANNED' AND operation_revision=?`, o.ID, o.Revision)); err != nil {
		return o, err
	}
	if o.RollbackOf != nil {
		parent, e := p.readStore().LockOperation(ctx, tx, *o.RollbackOf)
		if e != nil {
			return o, e
		}
		if parent.SupersededBy != nil && *parent.SupersededBy == o.ID {
			if err = readCAS(tx.ExecContext(ctx, "UPDATE sync_projection_read_switches SET superseded_by=NULL,operation_revision=operation_revision+1,updated_at=UTC_TIMESTAMP(6) WHERE operation_id=? AND superseded_by=?", parent.ID, o.ID)); err != nil {
				return o, err
			}
			if err = projection.LogReadControl(ctx, tx, "read_switch_reverse_cancelled", parent.ID, map[string]any{"reverse_operation_id": o.ID}); err != nil {
				return o, err
			}
		}
	}
	if err = readCAS(tx.ExecContext(ctx, `UPDATE sync_projection_read_selection SET current_operation_id=?,updated_at=UTC_TIMESTAMP(6) WHERE selector_id=1 AND current_operation_id=? AND version_id=? AND contract_sha256=? AND read_revision=?`, o.RollbackOf, o.ID, o.FromVersionID, o.FromContractSHA256, o.ExpectedReadRevision)); err != nil {
		return o, err
	}
	cancelled, err := p.readStore().LockOperation(ctx, tx, o.ID)
	if err != nil {
		return o, err
	}
	if err = projection.LogReadControl(ctx, tx, "read_switch_cancelled", o.ID, cancelled); err != nil {
		return o, err
	}
	if err = tx.Commit(); err != nil {
		return o, fmt.Errorf("cancel outcome unknown; read operation and selection before retry: %w", err)
	}
	return p.readStore().Operation(ctx, id)
}

// Marker and intent are committed together. Replaying validation retains this
// exact marker even if the latest periodic watermark has already changed.
func (p *ReadSwitchControl) ensureReadMarker(ctx context.Context, id string) (projection.ReadSwitch, error) {
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	selected, err := p.readStore().LockSelection(ctx, tx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	o, err := p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return o, err
	}
	if err = projection.MatchPlanned(g, selected, o); err != nil {
		return o, err
	}
	if o.MarkerID != nil {
		return o, nil
	}
	raw := make([]byte, 16)
	if _, err = rand.Read(raw); err != nil {
		return o, err
	}
	marker := hex.EncodeToString(raw)
	if _, err = tx.ExecContext(ctx, `INSERT INTO sync_source_watermark(source_name,barrier_id,issued_at) VALUES('tickets-v1',?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE barrier_id=VALUES(barrier_id),issued_at=VALUES(issued_at)`, marker); err != nil {
		return o, err
	}
	var issued time.Time
	if err = tx.QueryRowContext(ctx, "SELECT issued_at FROM sync_source_watermark WHERE source_name='tickets-v1' AND barrier_id=?", marker).Scan(&issued); err != nil {
		return o, err
	}
	if err = readCAS(tx.ExecContext(ctx, `UPDATE sync_projection_read_switches SET marker_id=?,marker_issued_at=?,operation_revision=operation_revision+1,updated_at=UTC_TIMESTAMP(6),proof=NULL,proof_sha256=NULL,validated_at=NULL,last_error=NULL WHERE operation_id=? AND stage='PLANNED' AND operation_revision=?`, marker, issued, o.ID, o.Revision)); err != nil {
		return o, err
	}
	if err = projection.LogReadControl(ctx, tx, "read_switch_marker", o.ID, map[string]any{"marker_id": marker, "issued_at": issued}); err != nil {
		return o, err
	}
	if err = p.observe("before_marker_commit", map[string]any{"operation_id": id, "marker_id": marker}); err != nil {
		return o, err
	}
	if err = tx.Commit(); err != nil {
		return o, fmt.Errorf("marker outcome unknown; reload exact operation before retry: %w", err)
	}
	if err = p.observe("after_marker_commit", map[string]any{"operation_id": id, "marker_id": marker}); err != nil {
		return o, err
	}
	return p.readStore().Operation(ctx, id)
}

type ReadMarkerEvidence struct {
	Partition    int32 `json:"partition"`
	RecordOffset int64 `json:"record_offset"`
	Event        Event `json:"event"`
}
type ReadSwitchProof struct {
	OperationID    string                           `json:"operation_id"`
	FreezeID       string                           `json:"freeze_id"`
	FreezeRevision int64                            `json:"freeze_revision"`
	FromVersionID  string                           `json:"from_version_id"`
	ToVersionID    string                           `json:"to_version_id"`
	FromHash       string                           `json:"from_contract_sha256"`
	ToHash         string                           `json:"to_contract_sha256"`
	ReadRevision   int64                            `json:"read_revision"`
	RollbackOf     *string                          `json:"rollback_of,omitempty"`
	MarkerID       string                           `json:"marker_id"`
	MarkerIssuedAt time.Time                        `json:"marker_issued_at"`
	Source         projection.Source                `json:"source"`
	Topic          projection.Kafka                 `json:"topic"`
	ES             projection.ES                    `json:"es_identity"`
	CK             projection.CK                    `json:"ck_identity"`
	CKDatabaseUUID string                           `json:"ck_database_uuid"`
	MarkerRecords  map[string][]ReadMarkerEvidence  `json:"marker_records"`
	Before         map[string]ReadComponentEvidence `json:"before_audit"`
	After          map[string]ReadComponentEvidence `json:"after_audit"`
	Checkpoint     Checkpoint                       `json:"cdc_checkpoint_observed"`
	Report         ReconcileReport                  `json:"reconcile_report"`
	ReportSHA256   string                           `json:"reconcile_report_sha256"`
	ValidatedAt    time.Time                        `json:"validated_at"`
}

func (p *ReadSwitchControl) failedReadSwitch(id string, cause error) {
	if cause == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.Store.DB.ExecContext(ctx, `UPDATE sync_projection_read_switches SET proof=NULL,proof_sha256=NULL,validated_at=NULL,last_error=?,operation_revision=operation_revision+1,updated_at=UTC_TIMESTAMP(6) WHERE operation_id=? AND stage='PLANNED'`, safeText(cause.Error(), 1000), id)
}

func (p *ReadSwitchControl) ValidateReadSwitchDestination(ctx context.Context, id string) (result projection.ReadSwitch, err error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	release, err := p.lock(ctx)
	if err != nil {
		return result, err
	}
	defer release()
	defer func() { p.failedReadSwitch(id, err) }()
	initial, err := p.readStore().Operation(ctx, id)
	if err != nil {
		return initial, err
	}
	d, err := p.ResolveReadDestination(ctx, initial.ToVersionID)
	if err != nil {
		return initial, err
	}
	if d.Version.Fingerprint() != initial.ToContractSHA256 {
		return initial, fmt.Errorf("read destination contract changed")
	}
	if d.CKDatabaseUUID != initial.CKDatabaseUUID {
		return initial, fmt.Errorf("%w: CK database UUID differs from PLANNED", projection.ErrTargetMismatch)
	}
	// Fail before publishing a marker for a missing/expired offset or invalid writer.
	for _, component := range []string{"es", "ck"} {
		if _, err = p.waitReadWriter(ctx, d, component); err != nil {
			return initial, err
		}
	}
	o, err := p.ensureReadMarker(ctx, id)
	if err != nil {
		return o, err
	}
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return o, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return o, err
	}
	selected, err := p.readStore().LockSelection(ctx, tx)
	if err != nil {
		return o, err
	}
	o, err = p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return o, err
	}
	if err = projection.MatchPlanned(g, selected, o); err != nil {
		return o, err
	}
	from, err := (&projection.VersionStore{DB: p.Store.DB}).Get(ctx, o.FromVersionID)
	if err != nil || from.Fingerprint() != o.FromContractSHA256 {
		return o, fmt.Errorf("source read contract changed: %v", err)
	}
	proof, err := p.proveReadDestination(ctx, d, o)
	if err != nil {
		return o, err
	}
	var data []byte
	data, err = json.Marshal(proof)
	if err != nil {
		return o, err
	}
	hash, err := projection.HashJSON(data)
	if err != nil {
		return o, err
	}
	if err = p.observe("before_proof_save", proof); err != nil {
		return o, err
	}
	if err = readCAS(tx.ExecContext(ctx, `UPDATE sync_projection_read_switches SET proof=?,proof_sha256=?,validated_at=?,operation_revision=operation_revision+1,updated_at=UTC_TIMESTAMP(6),last_error=NULL WHERE operation_id=? AND stage='PLANNED' AND operation_revision=?`, data, hash, proof.ValidatedAt, o.ID, o.Revision)); err != nil {
		return o, err
	}
	if err = projection.LogReadControl(ctx, tx, "read_switch_proved", o.ID, map[string]any{"proof_sha256": hash, "proof": proof}); err != nil {
		return o, err
	}
	if err = p.observe("before_proof_commit", proof); err != nil {
		return o, err
	}
	if err = tx.Commit(); err != nil {
		return o, fmt.Errorf("proof commit outcome unknown; reload and live revalidate: %w", err)
	}
	if err = p.observe("after_proof_commit", proof); err != nil {
		return o, err
	}
	return p.readStore().Operation(ctx, id)
}

// Applied delivery + raw committed offsets are followed by an actual retained
// record read. This proves a real CDC publication, not a synthetic watermark row.
func (p *ReadSwitchControl) waitReadMarker(ctx context.Context, d *ReadDestination, o projection.ReadSwitch) (map[string]ReadComponentEvidence, map[string][]ReadMarkerEvidence, Checkpoint, error) {
	for {
		if err := d.CheckIdentities(ctx); err != nil {
			return nil, nil, Checkpoint{}, err
		}
		components := map[string]ReadComponentEvidence{}
		records := map[string][]ReadMarkerEvidence{}
		complete := true
		var sourcePosition *Position
		for _, component := range []string{"es", "ck"} {
			e, err := p.waitReadWriter(ctx, d, component)
			if err != nil {
				return nil, nil, Checkpoint{}, err
			}
			components[component] = e
			for _, part := range e.Progress {
				var offset int64
				var state string
				if d.Handoff != nil {
					err = p.Store.DB.QueryRowContext(ctx, `SELECT record_offset,status FROM sync_projection_deliveries WHERE version_id=? AND component=? AND topic=? AND partition_id=? AND event_id=? ORDER BY record_offset LIMIT 1`, d.Version.ID, component, p.Queue.Topic, part.Partition, *o.MarkerID).Scan(&offset, &state)
				} else {
					table := "sync_kafka_es_deliveries"
					if component == "ck" {
						table = "sync_kafka_ck_deliveries"
					}
					err = p.Store.DB.QueryRowContext(ctx, "SELECT record_offset,status FROM "+table+" WHERE topic=? AND partition_id=? AND event_id=? ORDER BY record_offset LIMIT 1", p.Queue.Topic, part.Partition, *o.MarkerID).Scan(&offset, &state)
				}
				if err == sql.ErrNoRows {
					complete = false
					continue
				}
				if err != nil {
					return nil, nil, Checkpoint{}, err
				}
				if state != "applied" || part.Committed <= offset {
					complete = false
					continue
				}
				if offset < part.Start || offset >= part.End {
					return nil, nil, Checkpoint{}, fmt.Errorf("READ_MARKER_EXPIRED: exact marker not retained")
				}
				data, err := p.Queue.ReadRecord(ctx, RecordRef{Topic: p.Queue.Topic, Partition: part.Partition, Offset: offset})
				if err != nil {
					return nil, nil, Checkpoint{}, err
				}
				var event Event
				if json.Unmarshal(data, &event) != nil || event.Validate() != nil || !event.IsWatermark() || event.Source != "binlog" || event.ID != *o.MarkerID || event.Commit == nil || event.Commit.ServerUUID != d.Version.Contract.Source.ServerUUID || !event.EventAt.Equal(*o.MarkerIssuedAt) {
					return nil, nil, Checkpoint{}, fmt.Errorf("READ_MARKER_MISMATCH: exact CDC event differs")
				}
				if sourcePosition != nil && *sourcePosition != *event.Commit {
					return nil, nil, Checkpoint{}, fmt.Errorf("READ_MARKER_MISMATCH: source transaction differs across partitions")
				}
				copy := *event.Commit
				sourcePosition = &copy
				records[component] = append(records[component], ReadMarkerEvidence{Partition: part.Partition, RecordOffset: offset, Event: event})
			}
		}
		cp, err := p.Store.Checkpoint(ctx)
		if err != nil {
			return nil, nil, cp, err
		}
		if complete && sourcePosition != nil && cp.BootstrapComplete && reconcilePositionAtLeast(cp.Position, *sourcePosition) {
			return components, records, cp, nil
		}
		select {
		case <-ctx.Done():
			return nil, nil, cp, fmt.Errorf("READ_MARKER_TIMEOUT: not all partitions applied/committed exact source marker: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (p *ReadSwitchControl) waitReadWriter(ctx context.Context, d *ReadDestination, component string) (ReadComponentEvidence, error) {
	for {
		observed, err := d.ObserveWriter(ctx, component)
		if !errors.Is(err, errReadPending) {
			return observed, err
		}
		select {
		case <-ctx.Done():
			return observed, fmt.Errorf("READ_MARKER_TIMEOUT: pending delivery did not drain: %w", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (p *ReadSwitchControl) proveReadDestination(ctx context.Context, d *ReadDestination, o projection.ReadSwitch) (ReadSwitchProof, error) {
	var err error
	proof := ReadSwitchProof{OperationID: o.ID, FreezeID: o.FreezeID, FreezeRevision: o.FreezeRevision, FromVersionID: o.FromVersionID, ToVersionID: o.ToVersionID, FromHash: o.FromContractSHA256, ToHash: o.ToContractSHA256, ReadRevision: o.ExpectedReadRevision, MarkerID: *o.MarkerID, MarkerIssuedAt: *o.MarkerIssuedAt, Source: d.Version.Contract.Source, Topic: d.Version.Contract.Kafka, ES: d.Version.Contract.ES, CK: d.Version.Contract.CK, MarkerRecords: map[string][]ReadMarkerEvidence{}, Before: map[string]ReadComponentEvidence{}, After: map[string]ReadComponentEvidence{}}
	proof.CKDatabaseUUID = o.CKDatabaseUUID
	proof.RollbackOf = o.RollbackOf
	proof.Before, proof.MarkerRecords, proof.Checkpoint, err = p.waitReadMarker(ctx, d, o)
	if err != nil {
		return proof, err
	}
	if err = p.observe("marker_confirmed", proof); err != nil {
		return proof, err
	}
	proof.Report, err = Reconcile(ctx, p.Store, nil, d.ES, d.CK, ReconcileOptions{BatchSize: 250, ConfirmSourceBoundary: func(ctx context.Context) error { return d.CheckIdentities(ctx) }})
	if err != nil {
		return proof, err
	}
	if proof.Report.State != ReconcileConsistent || proof.Report.SourceChanged || proof.Report.DifferenceCount != 0 || proof.Report.SourceAfter == nil || proof.Report.ES == nil || proof.Report.CK == nil || proof.Report.ES.ConflictingKeys != 0 || proof.Report.CK.ConflictingKeys != 0 {
		return proof, fmt.Errorf("READ_RECONCILE_FAILED: full destination is not consistent")
	}
	if err = d.CheckIdentities(ctx); err != nil {
		return proof, err
	}
	for _, component := range []string{"es", "ck"} {
		after, e := p.waitReadWriter(ctx, d, component)
		if e != nil {
			return proof, e
		}
		if !sameReadWriter(proof.Before[component], after) {
			return proof, fmt.Errorf("READ_WRITER_CHANGED: %s changed during audit", component)
		}
		for i, position := range after.Progress {
			before := proof.Before[component].Progress[i]
			if position.Committed < before.Committed {
				return proof, fmt.Errorf("READ_OFFSET_REGRESSED")
			}
		}
		proof.After[component] = after
	}
	proof.ValidatedAt = time.Now().UTC()
	data, err := json.Marshal(proof.Report)
	if err != nil {
		return proof, err
	}
	proof.ReportSHA256, err = projection.HashJSON(data)
	if err != nil {
		return proof, err
	}
	return proof, nil
}
