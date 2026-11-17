package syncpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	assets "ticketpulse/db"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe/linkerrors"
)

type RebuildOptions struct {
	ID             string            `json:"id"`
	Target         string            `json:"target"`
	BatchSize      int               `json:"batch_size"`
	OrganizationID int64             `json:"organization_id"`
	MetricsRequest analytics.Request `json:"metrics_request"`
}
type RebuildJob struct {
	ID             string            `json:"job_id"`
	Target         string            `json:"target"`
	Options        RebuildOptions    `json:"options"`
	Index          string            `json:"es_index"`
	Database       string            `json:"ck_database"`
	Status         string            `json:"status"`
	WorkerActive   bool              `json:"worker_active"`
	Phase          string            `json:"phase"`
	SourceUUID     string            `json:"source_uuid"`
	SourcePosition Position          `json:"source_position"`
	TopicID        string            `json:"topic_id"`
	StartOffsets   map[int32]int64   `json:"start_offsets"`
	NextOffsets    map[int32]int64   `json:"next_offsets"`
	Identity       map[string]string `json:"target_identity"`
	SourceSHA256   string            `json:"source_sha256"`
	Total          int64             `json:"total"`
	Applied        int64             `json:"applied"`
	Attempts       int               `json:"attempts"`
	BarrierID      string            `json:"barrier_id"`
	Report         *ReconcileReport  `json:"report,omitempty"`
	LastError      *string           `json:"last_error,omitempty"`
	UpdatedAt      time.Time         `json:"updated_at"`
	CompletedAt    *time.Time        `json:"completed_at"`
}
type Rebuilder struct {
	Store          *StateStore
	Queue          *Queue
	ESURL, CKURL   string
	BatchSize      int
	CatchupTimeout time.Duration
	// Optional observer after a real durable batch. No synthetic success path.
	AfterBatch  func(RebuildJob) error
	ObservePage func(ReconcilePageObservation)
	// Owned only by the active Run/Check/Follow call. The handoff controller uses
	// the same locks and admissions for its final bounded check.
	writerLeases []*projection.WriterLease
	handoffID    string
}

// Explicit additive migration, separate from running a job. It installs control
// tables and the history audit index; business rows and projections are unchanged.
func EnsureRebuildSchema(ctx context.Context, db *sql.DB) error {
	for _, statement := range strings.Split(assets.RebuildMigration, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	// Existing sources receive the additive audit index only through migrate.
	var indexed int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='ticket_histories' AND index_name='idx_history_reconcile'").Scan(&indexed); err != nil {
		return err
	}
	if indexed == 0 {
		if _, err := db.ExecContext(ctx, assets.ReconcileIndexMigration); err != nil {
			return err
		}
	}
	return nil
}
func (r *Rebuilder) log(ctx context.Context, id, phase, outcome string, details any) error {
	data, err := json.Marshal(details)
	if err != nil {
		return err
	}
	_, err = r.Store.DB.ExecContext(ctx, `INSERT INTO sync_rebuild_log(job_id,phase,outcome,details,occurred_at) VALUES(?,?,?,?,UTC_TIMESTAMP(6))`, id, phase, outcome, data)
	return err
}
func (r *Rebuilder) Create(ctx context.Context, o RebuildOptions) (RebuildJob, error) {
	if !regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`).MatchString(o.ID) || !regexp.MustCompile(`^[a-z0-9_]+$`).MatchString(r.Store.Schema) {
		return RebuildJob{}, fmt.Errorf("invalid rebuild job/schema identity")
	}
	if o.Target != "both" && o.Target != "es" && o.Target != "ck" {
		return RebuildJob{}, fmt.Errorf("target must be es, ck or both")
	}
	if o.BatchSize == 0 {
		o.BatchSize = r.BatchSize
	}
	if o.BatchSize == 0 {
		o.BatchSize = 100
	}
	if o.BatchSize < 1 || o.BatchSize > 500 {
		return RebuildJob{}, fmt.Errorf("batch size must be 1–500")
	}
	if o.OrganizationID < 0 {
		return RebuildJob{}, fmt.Errorf("organization must be nonnegative")
	}
	if o.MetricsRequest.Start != "" || o.MetricsRequest.End != "" || len(o.MetricsRequest.TeamIDs) != 0 {
		if o.Target == "es" {
			return RebuildJob{}, fmt.Errorf("metrics check requires a CK rebuild target")
		}
		if _, _, err := o.MetricsRequest.Window(); err != nil {
			return RebuildJob{}, err
		}
		if o.OrganizationID <= 0 {
			return RebuildJob{}, fmt.Errorf("metrics require an organization")
		}
	}
	existing, err := r.Status(ctx, o.ID)
	if err == nil {
		a, _ := json.Marshal(o)
		b, _ := json.Marshal(existing.Options)
		if string(a) != string(b) {
			return existing, fmt.Errorf("job immutable options differ")
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return RebuildJob{}, err
	}
	base := r.Store.Schema + "_rebuild_" + o.ID
	if len(base) > 120 {
		return RebuildJob{}, fmt.Errorf("rebuild target namespace too long")
	}
	data, _ := json.Marshal(o)
	_, err = r.Store.DB.ExecContext(ctx, `INSERT INTO sync_rebuild_jobs(job_id,target,options,es_index,ck_database,status,phase,created_at,updated_at) VALUES(?,?,?,?,?,'created','freeze',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, o.ID, o.Target, data, base+"_tickets", base)
	if err != nil {
		return RebuildJob{}, err
	}
	if err = r.log(ctx, o.ID, "freeze", "created", o); err != nil {
		return RebuildJob{}, err
	}
	return r.Status(ctx, o.ID)
}
func (r *Rebuilder) Status(ctx context.Context, id string) (RebuildJob, error) {
	var j RebuildJob
	var options, pos, starts, identity, report []byte
	err := r.Store.DB.QueryRowContext(ctx, `SELECT job_id,target,options,es_index,ck_database,status,phase,COALESCE(source_uuid,''),source_position,COALESCE(topic_id,''),start_offsets,target_identity,COALESCE(source_sha256,''),total,applied,attempts,COALESCE(barrier_id,''),report,last_error,updated_at,completed_at FROM sync_rebuild_jobs WHERE job_id=?`, id).Scan(&j.ID, &j.Target, &options, &j.Index, &j.Database, &j.Status, &j.Phase, &j.SourceUUID, &pos, &j.TopicID, &starts, &identity, &j.SourceSHA256, &j.Total, &j.Applied, &j.Attempts, &j.BarrierID, &report, &j.LastError, &j.UpdatedAt, &j.CompletedAt)
	if err != nil {
		return j, err
	}
	if err = r.Store.DB.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?) IS NOT NULL", r.lockName(id)).Scan(&j.WorkerActive); err != nil {
		return j, err
	}
	for _, v := range []struct {
		data   []byte
		target any
	}{{options, &j.Options}, {pos, &j.SourcePosition}, {starts, &j.StartOffsets}, {identity, &j.Identity}, {report, &j.Report}} {
		if len(v.data) > 0 {
			if err = json.Unmarshal(v.data, v.target); err != nil {
				return j, err
			}
		}
	}
	j.NextOffsets = map[int32]int64{}
	rows, err := r.Store.DB.QueryContext(ctx, `SELECT partition_id,next_offset FROM sync_rebuild_offsets WHERE job_id=? ORDER BY partition_id`, id)
	if err != nil {
		return j, err
	}
	defer rows.Close()
	for rows.Next() {
		var p int32
		var n int64
		if err = rows.Scan(&p, &n); err != nil {
			return j, err
		}
		j.NextOffsets[p] = n
	}
	return j, rows.Err()
}
func (r *Rebuilder) state(ctx context.Context, id, status, phase string, problem error) error {
	var message any
	if problem != nil {
		message = safeText(problem.Error(), 1000)
	}
	_, err := r.Store.DB.ExecContext(ctx, `UPDATE sync_rebuild_jobs SET status=?,phase=?,last_error=?,updated_at=UTC_TIMESTAMP(6),completed_at=IF(?='completed',UTC_TIMESTAMP(6),NULL) WHERE job_id=?`, status, phase, message, status, id)
	if err != nil {
		return err
	}
	return r.log(ctx, id, phase, status, map[string]any{"error": message})
}
func (r *Rebuilder) Run(ctx context.Context, id string) (job RebuildJob, err error) {
	operation := *r
	operation.writerLeases = nil
	r = &operation
	release, err := r.lock(ctx, id)
	if err != nil {
		return job, err
	}
	defer release()
	job, err = r.Status(ctx, id)
	if err != nil {
		return job, err
	}
	closeTargets, err := r.acquireTargets(ctx, job)
	if err != nil {
		return job, err
	}
	defer closeTargets()
	defer func() {
		if err != nil {
			save, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			status := "failed"
			if ctx.Err() != nil {
				status = "interrupted"
			}
			phase := job.Phase
			if current, readErr := r.Status(save, id); readErr == nil {
				phase = current.Phase
			}
			_ = r.state(save, id, status, phase, err)
			if current, e := r.Status(save, id); e == nil {
				job = current
			}
		}
	}()
	if _, err = r.Store.DB.ExecContext(ctx, `UPDATE sync_rebuild_jobs SET attempts=attempts+1 WHERE job_id=?`, id); err != nil {
		return job, err
	}
	if job.Phase == "freeze" {
		if err = r.state(ctx, id, "freezing", "freeze", nil); err != nil {
			return job, err
		}
		var frozen RebuildJob
		frozen, err = r.freeze(ctx, job)
		if err != nil {
			return job, err
		}
		job = frozen
	}
	if err = r.verifySnapshot(ctx, job); err != nil {
		return job, err
	}
	var es *search.Elasticsearch
	var ck *analytics.ClickHouse
	es, ck, err = r.targets(ctx, &job)
	if err != nil {
		return job, err
	}
	if err = r.state(ctx, id, "running", "snapshot", nil); err != nil {
		return job, err
	}
	job.Phase = "snapshot"
	for job.Applied < job.Total {
		var batch []struct {
			seq   int64
			event Event
		}
		rows, e := r.Store.DB.QueryContext(ctx, `SELECT sequence_id,payload FROM sync_rebuild_items WHERE job_id=? AND sequence_id>? ORDER BY sequence_id LIMIT ?`, id, job.Applied, job.Options.BatchSize)
		if e != nil {
			return job, e
		}
		for rows.Next() {
			var item struct {
				seq   int64
				event Event
			}
			var data []byte
			if e = rows.Scan(&item.seq, &data); e == nil {
				e = json.Unmarshal(data, &item.event)
			}
			if e != nil {
				rows.Close()
				return job, e
			}
			batch = append(batch, item)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return job, e
		}
		if len(batch) == 0 {
			return job, fmt.Errorf("frozen snapshot cursor has a gap")
		}
		if err = r.verifyIdentities(ctx, job, es, ck); err != nil {
			return job, err
		}
		if err = r.checkWriter(ctx, id); err != nil {
			return job, err
		}
		events := make([]Event, 0, len(batch))
		for _, item := range batch {
			events = append(events, item.event)
		}
		if err = r.applyBatch(ctx, es, ck, events); err != nil {
			_ = r.log(ctx, id, "snapshot", "batch_failed", map[string]any{"first_sequence": batch[0].seq, "last_sequence": batch[len(batch)-1].seq, "error": err.Error()})
			return job, err
		}
		job.Applied = batch[len(batch)-1].seq
		if err = r.checkWriter(ctx, id); err != nil {
			return job, err
		}
		tx, e := r.Store.DB.BeginTx(ctx, nil)
		if e != nil {
			return job, e
		}
		_, e = tx.ExecContext(ctx, `UPDATE sync_rebuild_jobs SET applied=?,updated_at=UTC_TIMESTAMP(6) WHERE job_id=?`, job.Applied, id)
		if e == nil {
			details, _ := json.Marshal(map[string]any{"applied": job.Applied, "total": job.Total})
			_, e = tx.ExecContext(ctx, `INSERT INTO sync_rebuild_log(job_id,phase,outcome,details,occurred_at) VALUES(?,'snapshot','batch_applied',?,UTC_TIMESTAMP(6))`, id, details)
		}
		if e != nil {
			tx.Rollback()
			return job, e
		}
		if e = tx.Commit(); e != nil {
			return job, e
		}
		if r.AfterBatch != nil {
			if err = r.AfterBatch(job); err != nil {
				return job, err
			}
		}
	}
	timeout := r.CatchupTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	catchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		job.Phase = "catchup"
		if err = r.state(catchCtx, id, "catching_up", job.Phase, nil); err != nil {
			return job, err
		}
		if err = r.catchup(catchCtx, &job, es, ck, false); err != nil {
			return job, err
		}
		job.Phase = "reconcile"
		if err = r.state(catchCtx, id, "checking", job.Phase, nil); err != nil {
			return job, err
		}
		var report ReconcileReport
		report, err = Reconcile(catchCtx, r.Store, nil, es, ck, ReconcileOptions{ObservePage: r.ObservePage, BatchSize: job.Options.BatchSize, MetricsOrganizationID: job.Options.OrganizationID, MetricsRequest: job.Options.MetricsRequest, ConfirmSourceBoundary: func(boundary context.Context) error { return r.catchup(boundary, &job, es, ck, false) }})
		job.Report = &report
		data, e := json.Marshal(report)
		if e != nil {
			return job, e
		}
		saveCtx, saveCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, e = r.Store.DB.ExecContext(saveCtx, `UPDATE sync_rebuild_jobs SET report=?,updated_at=UTC_TIMESTAMP(6) WHERE job_id=?`, data, id)
		saveCancel()
		if e != nil {
			return job, e
		}
		if err != nil {
			return job, err
		}
		if report.State == "consistent" {
			if err = r.state(catchCtx, id, "completed", "completed", nil); err != nil {
				return job, err
			}
			return r.Status(catchCtx, id)
		}
		if report.State != "catching_up" {
			return job, fmt.Errorf("REBUILD_RECONCILIATION_%s: inspect saved report", strings.ToUpper(report.State))
		}
		select {
		case <-catchCtx.Done():
			return job, fmt.Errorf("REBUILD_CATCHUP_TIMEOUT: %w", catchCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func (r *Rebuilder) apply(ctx context.Context, es *search.Elasticsearch, ck *analytics.ClickHouse, e Event) error {
	if err := e.Validate(); err != nil {
		return fmt.Errorf("invalid rebuild input: %w", err)
	}
	if e.IsWatermark() {
		return nil
	}
	var fact *analytics.Fact
	var current analytics.Current
	var err error
	if ck != nil {
		fact, current, err = AnalysisProjection(e)
		if err != nil {
			return err
		}
	}
	classify := func(err error) error {
		if err == nil {
			return nil
		}
		var ew *search.WriteError
		var cw *analytics.WriteError
		if (errors.As(err, &ew) && ew.Permanent()) || (errors.As(err, &cw) && cw.Permanent()) {
			return linkerrors.Permanent("rebuild_projection", err)
		}
		return linkerrors.Transient("rebuild_projection", err)
	}
	return Retry(ctx, RetryPolicy{MaxAttempts: 3, BaseBackoff: 100 * time.Millisecond}, func() error {
		if es != nil {
			for _, w := range r.writerLeases {
				if err := w.Check(ctx); err != nil {
					return err
				}
			}
			if _, err := es.Put(ctx, e.Document); err != nil {
				return classify(err)
			}
		}
		if ck != nil {
			if fact != nil {
				for _, w := range r.writerLeases {
					if err := w.Check(ctx); err != nil {
						return err
					}
				}
				if err := ck.InsertFact(ctx, *fact); err != nil {
					return classify(err)
				}
			}
			for _, w := range r.writerLeases {
				if err := w.Check(ctx); err != nil {
					return err
				}
			}
			if err := ck.InsertCurrent(ctx, current); err != nil {
				return classify(err)
			}
		}
		return nil
	}, nil)
}

// Snapshot transport alone is batched. Frozen inputs, source boundary and the
// all-targets-success-before-cursor contract remain unchanged. Tail events still
// use the individual apply path and independent partition progress.
func (r *Rebuilder) applyBatch(ctx context.Context, es *search.Elasticsearch, ck *analytics.ClickHouse, events []Event) error {
	if len(events) > 500 {
		return fmt.Errorf("rebuild snapshot batch exceeds 500 events")
	}
	documents := make([]search.Document, 0, len(events))
	facts := make([]analytics.Fact, 0, len(events))
	currents := make([]analytics.Current, 0, len(events))
	for _, event := range events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("invalid rebuild batch input: %w", err)
		}
		if event.IsWatermark() {
			continue
		}
		if es != nil {
			documents = append(documents, event.Document)
		}
		if ck != nil {
			fact, current, err := AnalysisProjection(event)
			if err != nil {
				return err
			}
			if fact != nil {
				facts = append(facts, *fact)
			}
			currents = append(currents, current)
		}
	}
	classify := func(err error) error {
		if err == nil {
			return nil
		}
		var ew *search.WriteError
		var cw *analytics.WriteError
		if errors.As(err, &ew) && ew.Permanent() || errors.As(err, &cw) && cw.Permanent() {
			return linkerrors.Permanent("rebuild_batch_projection", err)
		}
		return linkerrors.Transient("rebuild_batch_projection", err)
	}
	return Retry(ctx, RetryPolicy{MaxAttempts: 3, BaseBackoff: 100 * time.Millisecond}, func() error {
		if es != nil {
			for _, w := range r.writerLeases {
				if err := w.Check(ctx); err != nil {
					return err
				}
			}
			if err := es.PutBatch(ctx, documents); err != nil {
				return classify(err)
			}
		}
		if ck != nil {
			for _, w := range r.writerLeases {
				if err := w.Check(ctx); err != nil {
					return err
				}
			}
			if err := ck.InsertFacts(ctx, facts); err != nil {
				return classify(err)
			}
			for _, w := range r.writerLeases {
				if err := w.Check(ctx); err != nil {
					return err
				}
			}
			if err := ck.InsertCurrents(ctx, currents); err != nil {
				return classify(err)
			}
		}
		return nil
	}, nil)
}
