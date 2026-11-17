package syncpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
)

// HandoffControl never changes ACTIVE, aliases, or the shared CDC checkpoint.
// Observe records real protocol operations; fault tests may stop at these
// boundaries, but no observer can manufacture successful Kafka responses.
type HandoffControl struct {
	*ProjectionControl
	Observe        func(string, any) error
	CatchupTimeout time.Duration
}

func (p *HandoffControl) observe(stage string, v any) error {
	if p.Observe != nil {
		return p.Observe(stage, v)
	}
	return nil
}
func (p *HandoffControl) log(ctx context.Context, tx *sql.Tx, id, operation string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if tx != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO sync_projection_handoff_log(handoff_id,operation,details,occurred_at) VALUES(?,?,?,UTC_TIMESTAMP(6))`, id, operation, b)
	} else {
		_, err = p.Store.DB.ExecContext(ctx, `INSERT INTO sync_projection_handoff_log(handoff_id,operation,details,occurred_at) VALUES(?,?,?,UTC_TIMESTAMP(6))`, id, operation, b)
	}
	return err
}
func (p *HandoffControl) failed(id string, cause error) {
	if cause == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.Store.DB.ExecContext(ctx, `UPDATE sync_projection_handoffs SET last_error=?,updated_at=UTC_TIMESTAMP(6) WHERE handoff_id=?`, safeText(cause.Error(), 1000), id)
}
func (p *HandoffControl) load(ctx context.Context, id string) (projection.Handoff, projection.Version, error) {
	h, err := (&projection.HandoffStore{DB: p.Store.DB}).Get(ctx, id)
	if err != nil {
		return h, projection.Version{}, err
	}
	v, err := (&projection.VersionStore{DB: p.Store.DB}).Get(ctx, h.VersionID)
	if err == nil {
		err = h.Validate(v)
	}
	return h, v, err
}
func (p *HandoffControl) checkFrozen(ctx context.Context, tx *sql.Tx, h projection.Handoff) error {
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return err
	}
	if g.FreezeID == nil || *g.FreezeID != h.FreezeID || g.Revision != h.FreezeRevision {
		return fmt.Errorf("HANDOFF_FREEZE_CHANGED: freeze identity/revision differs")
	}
	return nil
}
func (p *HandoffControl) checkIdentities(ctx context.Context, v projection.Version) error {
	c, err := p.identities(ctx, p.ES, p.CK)
	if err != nil {
		return fmt.Errorf("%w: %v", projection.ErrTargetMismatch, err)
	}
	if c.Source != v.Contract.Source || !reflect.DeepEqual(c.Kafka, v.Contract.Kafka) || c.ES != v.Contract.ES || c.CK != v.Contract.CK {
		return fmt.Errorf("%w: handoff source/topic/targets differ", projection.ErrTargetMismatch)
	}
	cp, err := p.Store.Checkpoint(ctx)
	if err != nil || !cp.BootstrapComplete || cp.ServerUUID != c.Source.ServerUUID {
		return fmt.Errorf("handoff CDC checkpoint/source unavailable: %v", err)
	}
	return nil
}

// Begin makes the stop request durable before the follower drains. DRAINING is
// the single necessary precursor to PINNED: a crash here cannot admit a new M4
// writer or accidentally resume business while the final check is unfinished.
func (p *HandoffControl) Begin(ctx context.Context, id, versionID string) (projection.Handoff, error) {
	if !projection.ValidID(id) {
		return projection.Handoff{}, fmt.Errorf("invalid handoff ID")
	}
	v, err := (&projection.VersionStore{DB: p.Store.DB}).Get(ctx, versionID)
	if err != nil {
		return projection.Handoff{}, err
	}
	if v.Phase != projection.Prepared || v.Contract.Candidate == nil {
		return projection.Handoff{}, fmt.Errorf("handoff requires PREPARED candidate")
	}
	if err = p.checkIdentities(ctx, v); err != nil {
		return projection.Handoff{}, err
	}
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.Handoff{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return projection.Handoff{}, err
	}
	existing, e := (&projection.HandoffStore{DB: p.Store.DB}).Lock(ctx, tx, id)
	if e == nil {
		if existing.VersionID != v.ID || existing.ContractSHA256 != v.Fingerprint() || existing.FreezeID != *g.FreezeID || existing.FreezeRevision != g.Revision {
			return existing, fmt.Errorf("immutable handoff identity differs")
		}
		tx.Rollback()
		h, _, err := p.load(ctx, id)
		return h, err
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return existing, e
	}
	// Both physical targets must already be owned by this M4 task. Installing
	// 012 before running/rechecking the candidate establishes these rows.
	for _, component := range []string{"es", "ck"} {
		o, e := projection.ReadOwner(ctx, tx, projection.VersionTargetKey(v, component), " FOR UPDATE")
		if e != nil || o.Kind != "rebuild_job" || o.Owner != v.Contract.Candidate.JobID {
			return projection.Handoff{}, fmt.Errorf("candidate writer provenance unavailable for %s: %v", component, e)
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sync_projection_handoffs(handoff_id,version_id,contract_sha256,job_id,freeze_id,freeze_revision,phase,es_group,ck_group,created_at,updated_at) VALUES(?,?,?,?,?,?,'DRAINING',?,?,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, id, v.ID, v.Fingerprint(), v.Contract.Candidate.JobID, *g.FreezeID, g.Revision, projection.TargetGroup(v.Contract.Kafka.Topic, v.ID, "es"), projection.TargetGroup(v.Contract.Kafka.Topic, v.ID, "ck"))
	if err != nil {
		return projection.Handoff{}, err
	}
	if err = p.log(ctx, tx, id, "drain_requested", map[string]any{"version_id": v.ID, "freeze_revision": g.Revision}); err != nil {
		return projection.Handoff{}, err
	}
	if err = tx.Commit(); err != nil {
		return projection.Handoff{}, fmt.Errorf("drain result unknown; inspect handoff: %w", err)
	}
	h, _, err := p.load(ctx, id)
	return h, err
}

func (p *HandoffControl) Pin(ctx context.Context, id string) (result projection.Handoff, err error) {
	defer func() { p.failed(id, err) }()
	h, v, err := p.load(ctx, id)
	if err != nil {
		return h, err
	}
	if h.Phase != projection.Draining {
		if err = p.checkIdentities(ctx, v); err != nil {
			return h, err
		}
		return h, nil
	}
	r := &Rebuilder{Store: p.Store, Queue: p.Queue, ESURL: p.ES.Endpoint(), CKURL: p.CK.Endpoint(), CatchupTimeout: p.CatchupTimeout, handoffID: id}
	release, err := r.lock(ctx, h.JobID)
	if err != nil {
		return h, fmt.Errorf("follower not drained; retry after it releases job lock: %w", err)
	}
	defer release()
	job, err := r.Status(ctx, h.JobID)
	if err != nil {
		return h, err
	}
	closeTargets, err := r.acquireTargets(ctx, job)
	if err != nil {
		return h, err
	}
	defer closeTargets()
	if len(r.writerLeases) != 2 {
		return h, fmt.Errorf("handoff requires installed target writer protection")
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
	if locked.Phase != projection.Draining {
		return h, fmt.Errorf("handoff phase changed")
	}
	if err = p.checkIdentities(ctx, v); err != nil {
		return h, err
	}
	if err = r.verifySnapshot(ctx, job); err != nil {
		return h, err
	}
	report, err := r.checkLocked(ctx, h.JobID)
	if err != nil {
		return h, err
	}
	if report.State != ReconcileConsistent || report.SourceChanged || report.DifferenceCount != 0 || report.SourceAfter == nil {
		return h, fmt.Errorf("final candidate report is not complete consistent")
	}
	job, err = r.Status(ctx, h.JobID)
	if err != nil {
		return h, err
	}
	if job.BarrierID == "" || len(job.NextOffsets) != int(p.Queue.Partitions) {
		return h, fmt.Errorf("final marker/position vector incomplete")
	}
	for _, partition := range v.Contract.Kafka.Partitions {
		var n int64
		var marker sql.NullString
		if err = tx.QueryRowContext(ctx, `SELECT next_offset,barrier_id FROM sync_rebuild_offsets WHERE job_id=? AND partition_id=? FOR SHARE`, h.JobID, partition).Scan(&n, &marker); err != nil {
			return h, err
		}
		if !marker.Valid || marker.String != job.BarrierID || n != job.NextOffsets[partition] {
			return h, fmt.Errorf("final durable marker/offset differs at partition %d", partition)
		}
	}
	if err = p.retained(ctx, job.NextOffsets); err != nil {
		return h, err
	}
	if err = p.checkIdentities(ctx, v); err != nil {
		return h, err
	}
	data, _ := json.Marshal(report)
	hash, err := projection.HashJSON(data)
	if err != nil {
		return h, err
	}
	offsets, _ := json.Marshal(job.NextOffsets)
	if err = p.observe("before_pin_commit", map[string]any{"report": report, "pinned": job.NextOffsets, "marker": job.BarrierID}); err != nil {
		return h, err
	}
	for _, w := range r.writerLeases {
		if err = w.Transition(ctx, tx, "sealed", id, id, v.ID); err != nil {
			return h, err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE sync_projection_handoffs SET phase='PINNED',pinned_offsets=?,final_marker=?,final_report=?,final_report_sha256=?,revision=revision+1,last_error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE handoff_id=? AND phase='DRAINING'`, offsets, job.BarrierID, data, hash, id)
	if err != nil {
		return h, err
	}
	for _, component := range []string{"es", "ck"} {
		for _, partition := range v.Contract.Kafka.Partitions {
			if _, err = tx.ExecContext(ctx, `INSERT INTO sync_projection_handoff_partitions(handoff_id,component,partition_id) VALUES(?,?,?)`, id, component, partition); err != nil {
				return h, err
			}
		}
	}
	if err = p.log(ctx, tx, id, "pinned", map[string]any{"next_offsets": job.NextOffsets, "marker": job.BarrierID, "report_sha256": hash}); err != nil {
		return h, err
	}
	if err = tx.Commit(); err != nil {
		return h, fmt.Errorf("pin result unknown; inspect durable handoff: %w", err)
	}
	if err = p.observe("after_pin_commit", job.NextOffsets); err != nil {
		return h, err
	}
	result, _, err = p.load(ctx, id)
	return
}

func (p *HandoffControl) operationLock(ctx context.Context, id string) (func(), error) {
	key := projection.TargetKey("handoff", p.Store.Schema, id)[:32]
	return (&StateStore{DB: p.Store.DB, Schema: "handoff"}).Lock(ctx, key)
}
func (p *HandoffControl) groupsEmpty(ctx context.Context, h projection.Handoff) error {
	for _, group := range []string{h.ESGroup, h.CKGroup} {
		groups, err := p.Queue.Admin.DescribeGroups(ctx, group)
		if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
			return err
		}
		g, ok := groups[group]
		if ok {
			if g.Err != nil && !errors.Is(g.Err, kerr.GroupIDNotFound) {
				return g.Err
			}
			if len(g.Members) != 0 || (g.Err == nil && g.State != "Empty" && g.State != "Dead") {
				return fmt.Errorf("HANDOFF_GROUP_NOT_EMPTY: %s state %s", group, g.State)
			}
		}
	}
	return nil
}
func (p *HandoffControl) rawOffsets(ctx context.Context, group string) (kadm.OffsetResponses, error) {
	offsets, err := p.Queue.Admin.FetchOffsets(ctx, group)
	if errors.Is(err, kerr.GroupIDNotFound) {
		return kadm.OffsetResponses{}, nil
	}
	if err == nil {
		err = offsets.Error()
	}
	return offsets, err
}

// Initialize never runs after CONSUMING. Each commit has durable intent before
// network I/O; partial/unknown replies are recovered only by exact raw readback.
func (p *HandoffControl) Initialize(ctx context.Context, id string) (result projection.Handoff, err error) {
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
	if h.Phase == projection.Consuming || h.Phase == projection.Verified {
		return h, fmt.Errorf("HANDOFF_ALREADY_CONSUMING: offset initialization is permanently disabled")
	}
	if h.Phase != projection.Pinned && h.Phase != projection.OffsetsReady {
		return h, fmt.Errorf("handoff is not pinned")
	}
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return h, err
	}
	defer tx.Rollback()
	if err = p.checkFrozen(ctx, tx, h); err != nil {
		return h, err
	}
	lockedHandoff, err := (&projection.HandoffStore{DB: p.Store.DB}).Lock(ctx, tx, id)
	if err != nil {
		return h, err
	}
	if lockedHandoff.Phase != projection.Pinned && lockedHandoff.Phase != projection.OffsetsReady {
		return h, fmt.Errorf("handoff phase changed before initialization")
	}
	if lockedHandoff.Phase != h.Phase {
		return h, fmt.Errorf("handoff changed while acquiring initialization protection; retry read-only")
	}
	if err = p.checkIdentities(ctx, v); err != nil {
		return h, err
	}
	if err = p.retained(ctx, h.PinnedOffsets); err != nil {
		return h, err
	}
	if err = p.groupsEmpty(ctx, h); err != nil {
		return h, err
	}
	for _, component := range []string{"es", "ck"} {
		raw, err := p.rawOffsets(ctx, h.Group(component))
		if err != nil {
			return h, err
		}
		for topic, ps := range raw {
			for part, o := range ps {
				_, known := h.PinnedOffsets[part]
				if !known || topic != v.Contract.Kafka.Topic || h.PinnedOffsets[part] != o.At || o.At < 0 {
					return h, fmt.Errorf("new group has unrelated prior position")
				}
			}
		}
		for _, partition := range v.Contract.Kafka.Partitions {
			n := h.PinnedOffsets[partition]
			old, exists := raw.Lookup(p.Queue.Topic, partition)
			if exists && old.At >= 0 {
				var intents int
				if err = p.Store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_projection_handoff_log WHERE handoff_id=? AND operation='offset_commit_intent' AND JSON_UNQUOTE(JSON_EXTRACT(details,'$.group'))=? AND JSON_EXTRACT(details,'$.partition')=? AND JSON_EXTRACT(details,'$.offset')=?`, id, h.Group(component), partition, n).Scan(&intents); err != nil {
					return h, err
				}
				if intents == 0 || old.Metadata != id+":"+v.ID {
					return h, fmt.Errorf("new group commit lacks this handoff's initialization provenance")
				}
			}
			if !exists || old.At < 0 {
				if h.Phase == projection.OffsetsReady {
					return h, fmt.Errorf("initialized group commit disappeared; no silent reinitialization")
				}
				if err = p.groupsEmpty(ctx, h); err != nil {
					return h, err
				}
				request := map[string]any{"group": h.Group(component), "topic_uuid": v.Contract.Kafka.TopicUUID, "topic": p.Queue.Topic, "partition": partition, "offset": n, "leader_epoch": -1, "generation": -1, "member_id": "", "metadata": id + ":" + v.ID}
				if err = p.log(ctx, nil, id, "offset_commit_intent", request); err != nil {
					return h, err
				}
				if err = p.observe("before_initial_commit", request); err != nil {
					return h, err
				}
				offsets := kadm.Offsets{}
				offsets.Add(kadm.Offset{Topic: p.Queue.Topic, Partition: partition, At: n, LeaderEpoch: -1, Metadata: id + ":" + v.ID})
				commitCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
				responses, commitErr := p.Queue.Admin.CommitOffsets(commitCtx, h.Group(component), offsets)
				cancel()
				response := map[string]any{"request": request, "responses": responses, "error": fmt.Sprint(commitErr)}
				if err = p.log(ctx, nil, id, "offset_commit_response", response); err != nil {
					return h, err
				}
				if err = p.observe("after_initial_commit", response); err != nil {
					return h, err
				}
				// A failed call can nevertheless have stored the offset. Do
				// not report ready in this attempt; the next retry reads back.
				if commitErr != nil {
					return h, fmt.Errorf("initial commit outcome unknown: %w", commitErr)
				}
				if err = responses.Error(); err != nil {
					return h, fmt.Errorf("initial commit partition failed: %w", err)
				}
				reply, ok := responses.Lookup(p.Queue.Topic, partition)
				if !ok || reply.Err != nil {
					return h, fmt.Errorf("initial commit partition response missing")
				}
			}
			read, err := p.rawOffsets(ctx, h.Group(component))
			if err != nil {
				return h, err
			}
			readback, ok := read.Lookup(p.Queue.Topic, partition)
			if !ok || readback.Err != nil || readback.At != n {
				return h, fmt.Errorf("initial offset readback differs at %s/%d", component, partition)
			}
			proof := map[string]any{"component": component, "partition": partition, "offset": readback.At, "group": h.Group(component)}
			if err = p.log(ctx, nil, id, "offset_readback", proof); err != nil {
				return h, err
			}
			if err = p.observe("initial_readback", proof); err != nil {
				return h, err
			}
			_, err = p.Store.DB.ExecContext(ctx, `UPDATE sync_projection_handoff_partitions SET initial_offset=?,confirmed_at=UTC_TIMESTAMP(6) WHERE handoff_id=? AND component=? AND partition_id=?`, n, id, component, partition)
			if err != nil {
				return h, err
			}
		}
	}
	if err = p.groupsEmpty(ctx, h); err != nil {
		return h, err
	}
	if err = p.checkIdentities(ctx, v); err != nil {
		return h, err
	}
	if err = p.retained(ctx, h.PinnedOffsets); err != nil {
		return h, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sync_projection_handoffs SET phase='OFFSETS_READY',revision=revision+1,last_error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE handoff_id=? AND phase IN ('PINNED','OFFSETS_READY')`, id); err != nil {
		return h, err
	}
	if err = p.log(ctx, tx, id, "offsets_ready", h.PinnedOffsets); err != nil {
		return h, err
	}
	if err = tx.Commit(); err != nil {
		return h, fmt.Errorf("offset-ready result unknown; inspect handoff: %w", err)
	}
	if err = p.observe("after_offsets_ready_commit", map[string]any{"handoff_id": id, "phase": projection.OffsetsReady}); err != nil {
		return h, err
	}
	result, _, err = p.load(ctx, id)
	return
}

func (p *HandoffControl) OpenConsumption(ctx context.Context, id string) (result projection.Handoff, err error) {
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
	if h.Phase == projection.Consuming || h.Phase == projection.Verified {
		if err = p.checkIdentities(ctx, v); err != nil {
			return h, err
		}
		return h, nil
	}
	if h.Phase != projection.OffsetsReady {
		return h, fmt.Errorf("consumption requires OFFSETS_READY")
	}
	if err = p.groupsEmpty(ctx, h); err != nil {
		return h, err
	}
	if err = p.checkIdentities(ctx, v); err != nil {
		return h, err
	}
	for _, component := range []string{"es", "ck"} {
		raw, err := p.rawOffsets(ctx, h.Group(component))
		if err != nil {
			return h, err
		}
		for _, part := range v.Contract.Kafka.Partitions {
			o, ok := raw.Lookup(p.Queue.Topic, part)
			if !ok || o.Err != nil || o.At != h.PinnedOffsets[part] {
				return h, fmt.Errorf("initial offset no longer exact")
			}
		}
	}
	if err = p.retained(ctx, h.PinnedOffsets); err != nil {
		return h, err
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
	if locked.Phase != projection.OffsetsReady {
		return h, fmt.Errorf("handoff phase changed")
	}
	for _, component := range []string{"es", "ck"} {
		key := projection.VersionTargetKey(v, component)
		o, err := projection.ReadOwner(ctx, tx, key, " FOR UPDATE")
		if err != nil {
			return h, err
		}
		var held sql.NullInt64
		if err = tx.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", projection.TargetLockName(key)).Scan(&held); err != nil {
			return h, err
		}
		if o.Kind != "sealed" || o.Owner != h.ID || o.Token != nil || held.Valid {
			return h, fmt.Errorf("sealed target still has an unproved writer")
		}
		_, err = tx.ExecContext(ctx, `UPDATE sync_projection_target_owners SET owner_kind='consumer_group',owner_id=?,owner_epoch=owner_epoch+1,updated_at=UTC_TIMESTAMP(6) WHERE target_key=?`, h.Group(component), key)
		if err != nil {
			return h, err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sync_projection_handoffs SET phase='CONSUMING',revision=revision+1,last_error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE handoff_id=?`, id); err != nil {
		return h, err
	}
	if err = p.log(ctx, tx, id, "consuming_authorized", map[string]any{"es_group": h.ESGroup, "ck_group": h.CKGroup}); err != nil {
		return h, err
	}
	if err = tx.Commit(); err != nil {
		return h, fmt.Errorf("consume authorization unknown; inspect handoff before launching")
	}
	result, _, err = p.load(ctx, id)
	return
}

// CandidateTargets is a read-only helper for an operator constructing a
// controller from a persisted version; it does not create either target.
func CandidateTargets(ctx context.Context, db *sql.DB, versionID string) (*search.Elasticsearch, *analytics.ClickHouse, error) {
	v, err := (&projection.VersionStore{DB: db}).Get(ctx, versionID)
	if err != nil {
		return nil, nil, err
	}
	es, err := search.NewElasticsearch(v.Contract.ES.Endpoint, v.Contract.ES.Index)
	if err != nil {
		return nil, nil, err
	}
	ck, err := analytics.NewClickHouse(v.Contract.CK.Endpoint, v.Contract.CK.Database)
	return es, ck, err
}
