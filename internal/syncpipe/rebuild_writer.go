package syncpipe

import (
	"context"
	"database/sql"
	"fmt"

	"ticketpulse/internal/projection"
)

func (r *Rebuilder) handoffAdmission(ctx context.Context, id string) error {
	var handoff, phase string
	err := r.Store.DB.QueryRowContext(ctx, "SELECT handoff_id,phase FROM sync_projection_handoffs WHERE job_id=?", id).Scan(&handoff, &phase)
	if projection.MissingTable(err) {
		installed, failure := projection.HandoffInstalled(ctx, r.Store.DB)
		if failure != nil {
			return failure
		}
		if installed {
			return fmt.Errorf("HANDOFF_METADATA_UNAVAILABLE: installed handoff admission table unavailable")
		}
		return nil
	}
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if handoff != r.handoffID || phase != projection.Draining {
		return fmt.Errorf("REBUILD_HANDOFF_SEALED: task is draining or sealed by handoff %s", handoff)
	}
	return nil
}
func (r *Rebuilder) acquireTargets(ctx context.Context, j RebuildJob) (func(), error) {
	if err := r.handoffAdmission(ctx, j.ID); err != nil {
		return nil, err
	}
	claims := []projection.WriterClaim{}
	if j.Target != "ck" {
		claims = append(claims, projection.WriterClaim{Key: projection.TargetKey("es", r.ESURL, j.Index), Component: "es", Kind: "rebuild_job", Owner: j.ID})
	}
	if j.Target != "es" {
		claims = append(claims, projection.WriterClaim{Key: projection.TargetKey("ck", r.CKURL, j.Database), Component: "ck", Kind: "rebuild_job", Owner: j.ID})
	}
	old := r.writerLeases
	r.writerLeases = nil
	for _, claim := range claims {
		lease, err := projection.AcquireWriter(ctx, r.Store.DB, claim, true)
		if err != nil {
			for _, w := range r.writerLeases {
				w.Close()
			}
			r.writerLeases = old
			return nil, err
		}
		if lease != nil {
			r.writerLeases = append(r.writerLeases, lease)
		}
	}
	return func() {
		for _, w := range r.writerLeases {
			w.Close()
		}
		r.writerLeases = old
	}, nil
}
func (r *Rebuilder) checkWriter(ctx context.Context, id string) error {
	if err := r.handoffAdmission(ctx, id); err != nil {
		return err
	}
	for _, w := range r.writerLeases {
		if err := w.Check(ctx); err != nil {
			return err
		}
	}
	return nil
}
