package syncpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
)

// ProjectionControl reuses M4 identity, job locks and the full reconciliation.
// It has no target promotion, offset mutation or candidate writer takeover.
type ProjectionControl struct {
	Store *StateStore
	Queue *Queue
	ES    *search.Elasticsearch
	CK    *analytics.ClickHouse
}

func (p *ProjectionControl) identities(ctx context.Context, es *search.Elasticsearch, ck *analytics.ClickHouse) (c projection.Contract, err error) {
	c.SchemaVersion = 1
	if p.Store == nil || p.Store.DB == nil || p.Queue == nil || es == nil || ck == nil {
		return c, fmt.Errorf("managed projections require MySQL, Kafka, ES and CK")
	}
	if err = p.Store.DB.QueryRowContext(ctx, "SELECT @@server_uuid,DATABASE()").Scan(&c.Source.ServerUUID, &c.Source.Schema); err != nil {
		return c, err
	}
	if c.Source.Schema != p.Store.Schema {
		return c, fmt.Errorf("source/status schema differs")
	}
	topics, err := p.Queue.Admin.ListTopics(ctx, p.Queue.Topic)
	if err != nil {
		return c, err
	}
	detail, ok := topics[p.Queue.Topic]
	if !ok || detail.Err != nil || len(detail.Partitions) != int(p.Queue.Partitions) {
		return c, fmt.Errorf("Kafka topic identity/partitions unavailable")
	}
	c.Kafka = projection.Kafka{Topic: p.Queue.Topic, TopicUUID: detail.ID.String(), Partitions: make([]int32, p.Queue.Partitions)}
	for i := int32(0); i < p.Queue.Partitions; i++ {
		partition, ok := detail.Partitions[i]
		if !ok || partition.Err != nil {
			return c, fmt.Errorf("Kafka partition identity unavailable")
		}
		c.Kafka.Partitions[i] = i
	}
	// Read-only saved pipeline check. A readiness probe must not register a topic.
	var topic, id string
	var count int32
	if err = p.Store.DB.QueryRowContext(ctx, "SELECT topic,topic_id,partition_count FROM sync_kafka_pipeline WHERE pipeline_name='tickets-v1'").Scan(&topic, &id, &count); err != nil {
		return c, err
	}
	if topic != c.Kafka.Topic || id != c.Kafka.TopicUUID || count != p.Queue.Partitions {
		return c, fmt.Errorf("saved Kafka pipeline differs")
	}
	esID, exists, err := es.Identity(ctx)
	if err != nil {
		return c, err
	}
	if !exists {
		return c, fmt.Errorf("ES index is absent")
	}
	ckID, exists, err := ck.Identity(ctx)
	if err != nil {
		return c, err
	}
	if !exists {
		return c, fmt.Errorf("CK target tables are absent")
	}
	c.ES = projection.ES{Endpoint: es.Endpoint(), Index: es.Index(), UUID: esID}
	c.CK = projection.CK{Endpoint: ck.Endpoint(), Database: ck.Database(), EventsUUID: ckID.Events, StatesUUID: ckID.States}
	c.ESWriter = p.groupBinding("es")
	c.CKWriter = p.groupBinding("ck")
	return c, nil
}
func (p *ProjectionControl) groupBinding(component string) projection.Binding {
	return projection.Binding{Kind: "consumer_group", Owner: p.Queue.Group(component), ProgressTable: "__consumer_offsets", StatusComponent: component}
}

func (p *ProjectionControl) existingProgress(ctx context.Context, sourceUUID string) error {
	cp, err := p.Store.Checkpoint(ctx)
	if err != nil || !cp.BootstrapComplete || cp.ServerUUID != sourceUUID {
		return fmt.Errorf("existing CDC bootstrap/checkpoint unavailable or differs: %v", err)
	}
	var topic string
	if err = p.Store.DB.QueryRowContext(ctx, "SELECT topic FROM sync_kafka_analytics_bootstrap WHERE pipeline='ck-v1'").Scan(&topic); err != nil || topic != p.Queue.Topic {
		return fmt.Errorf("existing CK bootstrap unavailable or differs: %v", err)
	}
	for _, component := range []string{"es", "ck"} {
		offsets, err := p.Queue.Admin.FetchOffsets(ctx, p.Queue.Group(component))
		if err != nil {
			return err
		}
		for partition := int32(0); partition < p.Queue.Partitions; partition++ {
			o, ok := offsets.Lookup(p.Queue.Topic, partition)
			if !ok || o.Err != nil || o.At < 0 {
				return fmt.Errorf("existing %s group lacks a committed partition %d", component, partition)
			}
		}
		if _, err = p.Queue.Progress(ctx, component); err != nil {
			return err
		}
	}
	return nil
}

func (p *ProjectionControl) fresh(ctx context.Context, es *search.Elasticsearch, ck *analytics.ClickHouse) (json.RawMessage, error) {
	report, err := Reconcile(ctx, p.Store, nil, es, ck, ReconcileOptions{BatchSize: 250})
	if err != nil {
		return nil, err
	}
	if report.State != ReconcileConsistent || report.SourceChanged || report.SourceAfter == nil || report.DifferenceCount != 0 || report.ES == nil || report.CK == nil {
		return nil, fmt.Errorf("fresh frozen validation is %s, differences=%d", report.State, report.DifferenceCount)
	}
	data, err := json.Marshal(report)
	return data, err
}

func (p *ProjectionControl) finish(ctx context.Context, tx *sql.Tx, v projection.Version) (projection.Version, error) {
	store := &projection.VersionStore{DB: p.Store.DB}
	if err := store.Insert(ctx, tx, v); err != nil {
		return v, err
	}
	if err := tx.Commit(); err != nil {
		return v, fmt.Errorf("registration result unknown; inspect durable version: %w", err)
	}
	saved, err := store.Get(ctx, v.ID)
	if err != nil {
		return saved, err
	}
	if saved.Phase != v.Phase || saved.Fingerprint() != v.Fingerprint() {
		return saved, fmt.Errorf("registered version differs")
	}
	return saved, nil
}

// EnrollExisting creates the first ACTIVE contract for targets already in use.
// It freezes neither the source nor consumers on its own and writes no offset.
// Caller must first freeze and let existing writers catch up. Content equality
// and source/topic/target identities are rechecked while resume is blocked.
func (p *ProjectionControl) EnrollExisting(ctx context.Context, id string) (projection.Version, error) {
	if !projection.ValidID(id) {
		return projection.Version{}, fmt.Errorf("invalid version ID")
	}
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.Version{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return projection.Version{}, err
	}
	c, err := p.identities(ctx, p.ES, p.CK)
	if err != nil {
		return projection.Version{}, err
	}
	// Existing enrollment must not turn an M4 candidate into an ACTIVE record
	// by pairing its contents with unrelated standard consumer-group offsets.
	if strings.HasPrefix(c.ES.Index, c.Source.Schema+"_rebuild_") || strings.HasPrefix(c.CK.Database, c.Source.Schema+"_rebuild_") {
		return projection.Version{}, fmt.Errorf("M4 candidates can only be PREPARED; initial enrollment cannot promote them")
	}
	var candidates int
	err = p.Store.DB.QueryRowContext(ctx, "SELECT count(*) FROM sync_rebuild_jobs WHERE es_index=? OR ck_database=?", c.ES.Index, c.CK.Database).Scan(&candidates)
	if err != nil && !projection.MissingTable(err) {
		return projection.Version{}, err
	}
	if candidates != 0 {
		return projection.Version{}, fmt.Errorf("M4 candidates can only be PREPARED; initial enrollment cannot promote them")
	}
	if err = p.existingProgress(ctx, c.Source.ServerUUID); err != nil {
		return projection.Version{}, err
	}
	report, err := p.fresh(ctx, p.ES, p.CK)
	if err != nil {
		return projection.Version{}, err
	}
	after, err := p.identities(ctx, p.ES, p.CK)
	if err != nil || !reflect.DeepEqual(c, after) {
		return projection.Version{}, fmt.Errorf("projection identity changed during enrollment: %v", err)
	}
	v, err := projection.NewCheckedVersion(id, projection.Active, c, report, &g.Revision)
	if err != nil {
		return v, err
	}
	return p.finish(ctx, tx, v)
}

func (p *ProjectionControl) retained(ctx context.Context, next map[int32]int64) error {
	starts, err := p.Queue.Admin.ListStartOffsets(ctx, p.Queue.Topic)
	if err != nil {
		return err
	}
	ends, err := p.Queue.Admin.ListEndOffsets(ctx, p.Queue.Topic)
	if err != nil {
		return err
	}
	if len(next) != int(p.Queue.Partitions) {
		return fmt.Errorf("candidate position set differs")
	}
	for partition := int32(0); partition < p.Queue.Partitions; partition++ {
		n, ok := next[partition]
		start, a := starts.Lookup(p.Queue.Topic, partition)
		end, b := ends.Lookup(p.Queue.Topic, partition)
		if !ok || !a || !b || start.Err != nil || end.Err != nil || n < start.Offset || n > end.Offset {
			return fmt.Errorf("candidate partition %d is outside retained log", partition)
		}
	}
	return nil
}

func (p *ProjectionControl) Prepare(ctx context.Context, id, jobID string) (projection.Version, error) {
	if !projection.ValidID(id) || !projection.ValidID(jobID) {
		return projection.Version{}, fmt.Errorf("invalid version/job ID")
	}
	r := &Rebuilder{Store: p.Store, Queue: p.Queue, ESURL: p.ES.Endpoint(), CKURL: p.CK.Endpoint()}
	// Same connection-scoped M4 job lock: an active follow cannot be prepared.
	release, err := r.lock(ctx, jobID)
	if err != nil {
		return projection.Version{}, err
	}
	defer release()
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.Version{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return projection.Version{}, err
	}
	j, err := r.Status(ctx, jobID)
	if err != nil {
		return projection.Version{}, err
	}
	if j.Target != "both" || j.Status != "completed" || j.Applied != j.Total || j.Report == nil || j.Report.State != ReconcileConsistent || j.SourceSHA256 == "" {
		return projection.Version{}, fmt.Errorf("candidate requires a completed both-target M4 job")
	}
	if err = r.verifySnapshot(ctx, j); err != nil {
		return projection.Version{}, err
	}
	es, err := search.NewElasticsearch(p.ES.Endpoint(), j.Index)
	if err != nil {
		return projection.Version{}, err
	}
	ck, err := analytics.NewClickHouse(p.CK.Endpoint(), j.Database)
	if err != nil {
		return projection.Version{}, err
	}
	if err = r.verifyIdentities(ctx, j, es, ck); err != nil {
		return projection.Version{}, err
	}
	if err = p.retained(ctx, j.NextOffsets); err != nil {
		return projection.Version{}, err
	}
	c, err := p.identities(ctx, es, ck)
	if err != nil {
		return projection.Version{}, err
	}
	report, err := p.fresh(ctx, es, ck)
	if err != nil {
		return projection.Version{}, err
	}
	if err = r.verifyIdentities(ctx, j, es, ck); err != nil {
		return projection.Version{}, err
	}
	after, err := p.identities(ctx, es, ck)
	if err != nil || !reflect.DeepEqual(c, after) {
		return projection.Version{}, fmt.Errorf("candidate identity changed during preparation: %v", err)
	}
	if err = p.retained(ctx, j.NextOffsets); err != nil {
		return projection.Version{}, err
	}
	original, _ := json.Marshal(j.Report)
	originalHash, err := projection.HashJSON(original)
	if err != nil {
		return projection.Version{}, err
	}
	c.Candidate = &projection.Candidate{JobID: jobID, SourceSHA256: j.SourceSHA256, M4ReportSHA256: originalHash, NextOffsets: j.NextOffsets}
	esNext, ckNext := c.ESWriter, c.CKWriter
	c.NextESWriter, c.NextCKWriter = &esNext, &ckNext
	c.ESWriter = projection.Binding{Kind: "rebuild_job", Owner: jobID, ProgressTable: "sync_rebuild_offsets", StatusComponent: jobID}
	c.CKWriter = c.ESWriter
	v, err := projection.NewCheckedVersion(id, projection.Prepared, c, report, &g.Revision)
	if err != nil {
		return v, err
	}
	return p.finish(ctx, tx, v)
}

type ManagedProjection struct {
	Control *ProjectionControl
	Version projection.Version
}

func (p *ProjectionControl) Managed(ctx context.Context, id string) (*ManagedProjection, error) {
	v, err := (&projection.VersionStore{DB: p.Store.DB}).Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("%w: version unavailable: %v", projection.ErrTargetMismatch, err)
	}
	m := &ManagedProjection{Control: p, Version: v}
	return m, m.Check(ctx)
}

// Check is read-only and always re-observes live identities. A saved report is
// provenance, never a substitute for the runtime check or proof of freshness.
func (m *ManagedProjection) Check(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %v", projection.ErrTargetMismatch, err)
		}
	}()
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	p := m.Control
	store := &projection.VersionStore{DB: p.Store.DB}
	v, err := store.Get(checkCtx, m.Version.ID)
	if err != nil {
		return err
	}
	if v.Phase != projection.Active || v.Fingerprint() != m.Version.Fingerprint() {
		return fmt.Errorf("configured version is not the pinned ACTIVE contract")
	}
	g, err := projection.ReadGate(checkCtx, p.Store.DB)
	if err != nil {
		return err
	}
	if !g.Valid() {
		return fmt.Errorf("invalid durable business gate")
	}
	c, err := p.identities(checkCtx, p.ES, p.CK)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(v.Contract, c) {
		return fmt.Errorf("configured source/topic/read/write targets differ from ACTIVE version")
	}
	if err = p.existingProgress(checkCtx, c.Source.ServerUUID); err != nil {
		return err
	}
	for _, component := range []string{"es", "ck"} {
		if err = store.VerifyBinding(checkCtx, v, component); err != nil {
			return err
		}
	}
	return nil
}
