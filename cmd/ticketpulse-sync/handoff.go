package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"ticketpulse/internal/projection"
	"ticketpulse/internal/syncpipe"
)

func runHandoff(ctx context.Context, store *syncpipe.StateStore, q *syncpipe.Queue, id, mode string) error {
	if mode != "consume" && mode != "ck" && mode != "status" && mode != "analytics-status" {
		return fmt.Errorf("candidate handoff permits only one standard component or read-only status; no capture/bootstrap/replay")
	}
	if os.Getenv("TICKETPULSE_TARGET_VERSION") != "" {
		return fmt.Errorf("candidate handoff cannot use the ACTIVE API target option")
	}
	h, err := (&projection.HandoffStore{DB: store.DB}).Get(ctx, id)
	if err != nil {
		return err
	}
	es, ck, err := syncpipe.CandidateTargets(ctx, store.DB, h.VersionID)
	if err != nil {
		return err
	}
	// Explicit process config is checked, never silently redirected.
	if os.Getenv("TICKETPULSE_ES_URL") != es.Endpoint() || os.Getenv("TICKETPULSE_SEARCH_INDEX") != es.Index() || os.Getenv("TICKETPULSE_CK_URL") != ck.Endpoint() || os.Getenv("TICKETPULSE_CK_DATABASE") != ck.Database() {
		return fmt.Errorf("%w: process targets differ from handoff version", projection.ErrTargetMismatch)
	}
	control := &syncpipe.HandoffControl{ProjectionControl: &syncpipe.ProjectionControl{Store: store, Queue: q, ES: es, CK: ck}}
	target, err := control.ConsumerTarget(ctx, id)
	if err != nil {
		return err
	}
	candidateQueue := *q
	candidateQueue.Target = target
	if mode == "status" || mode == "analytics-status" {
		status := &syncpipe.TargetStatus{Target: target}
		var result any
		if mode == "status" {
			result, err = status.Diagnostics(ctx)
		} else {
			result, err = status.AnalyticsStatus(ctx)
		}
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	// No automatic retry after ownership/identity loss. Restart is explicit and
	// reloads current Kafka commits; a live stale process is never preempted.
	if mode == "consume" {
		err = (&syncpipe.Consumer{Store: store, Queue: &candidateQueue, ES: es, Target: target}).Run(ctx)
	} else {
		err = (&syncpipe.CKConsumer{Store: store, Queue: &candidateQueue, CK: ck, Target: target}).Run(ctx)
	}
	if ctx.Err() != nil {
		return nil
	}
	return err
}
