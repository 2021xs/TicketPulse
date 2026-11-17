package syncpipe

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
)

func (r *Rebuilder) lock(ctx context.Context, id string) (func(), error) {
	// GET_LOCK names have a 64-byte bound; a long test schema and job ID must
	// remain isolated without truncating into another job's lock name.
	key := fmt.Sprintf("%x", sha256.Sum256([]byte(r.Store.Schema+":"+id)))[:32]
	return (&StateStore{DB: r.Store.DB, Schema: "rebuild"}).Lock(ctx, key)
}
func (r *Rebuilder) lockName(id string) string {
	key := fmt.Sprintf("%x", sha256.Sum256([]byte(r.Store.Schema+":"+id)))[:32]
	return "tp:rebuild:" + key
}
func (r *Rebuilder) Check(ctx context.Context, id string) (report ReconcileReport, err error) {
	operation := *r
	operation.writerLeases = nil
	r = &operation
	release, err := r.lock(ctx, id)
	if err != nil {
		return report, err
	}
	defer release()
	return r.checkLocked(ctx, id)
}

// Caller holds the M4 job lock. Keeping this path internal lets final
// reconciliation and sealing share one uninterrupted target writer lease.
func (r *Rebuilder) checkLocked(ctx context.Context, id string) (report ReconcileReport, err error) {
	j, err := r.Status(ctx, id)
	if err != nil {
		return report, err
	}
	if len(r.writerLeases) == 0 {
		closeTargets, err := r.acquireTargets(ctx, j)
		if err != nil {
			return report, err
		}
		defer closeTargets()
	}
	defer func() {
		if err != nil {
			save, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			status := "failed"
			if ctx.Err() != nil {
				status = "interrupted"
			}
			_ = r.state(save, id, status, "reconcile", err)
		}
	}()
	if j.Applied != j.Total || j.SourceSHA256 == "" {
		return report, fmt.Errorf("check requires the entire frozen snapshot applied")
	}
	es, ck, err := r.targets(ctx, &j)
	if err != nil {
		return report, err
	}
	timeout := r.CatchupTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	report, err = Reconcile(checkCtx, r.Store, nil, es, ck, ReconcileOptions{ObservePage: r.ObservePage, BatchSize: j.Options.BatchSize, MetricsOrganizationID: j.Options.OrganizationID, MetricsRequest: j.Options.MetricsRequest, ConfirmSourceBoundary: func(boundary context.Context) error { return r.catchup(boundary, &j, es, ck, false) }})
	data, _ := json.Marshal(report)
	saveCtx, saveCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer saveCancel()
	if _, saveErr := r.Store.DB.ExecContext(saveCtx, `UPDATE sync_rebuild_jobs SET report=?,updated_at=UTC_TIMESTAMP(6) WHERE job_id=?`, data, id); saveErr != nil {
		return report, saveErr
	}
	if err != nil {
		return report, err
	}
	if report.State != ReconcileConsistent {
		_ = r.state(ctx, id, "checking", "reconcile", nil)
		return report, fmt.Errorf("reconciliation state: %s", report.State)
	}
	return report, r.state(ctx, id, "completed", "completed", nil)
}
