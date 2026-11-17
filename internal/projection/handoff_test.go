package projection

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func handoffFixture(t *testing.T) (Version, Handoff) {
	t.Helper()
	v := exampleVersion(t)
	v.Phase = Prepared
	candidate := Candidate{JobID: "job_gold", SourceSHA256: strings.Repeat("a", 64), M4ReportSHA256: strings.Repeat("b", 64), NextOffsets: map[int32]int64{0: 7, 1: 4, 2: 0}}
	es, ck := v.Contract.ESWriter, v.Contract.CKWriter
	v.Contract.NextESWriter = &es
	v.Contract.NextCKWriter = &ck
	v.Contract.Candidate = &candidate
	v.Contract.ESWriter = Binding{Kind: "rebuild_job", Owner: candidate.JobID, ProgressTable: "sync_rebuild_offsets", StatusComponent: candidate.JobID}
	v.Contract.CKWriter = v.Contract.ESWriter
	if err := v.Validate(); err != nil {
		t.Fatal(err)
	}
	hash, err := HashJSON(v.ValidationReport)
	if err != nil {
		t.Fatal(err)
	}
	marker := "final_marker"
	now := time.Now().UTC()
	h := Handoff{ID: "handoff_gold", VersionID: v.ID, ContractSHA256: v.Fingerprint(), JobID: candidate.JobID, FreezeID: "freeze_gold", FreezeRevision: 1, Phase: OffsetsReady, PinnedOffsets: candidate.NextOffsets, FinalMarker: &marker, FinalReport: v.ValidationReport, FinalReportSHA256: &hash, ESGroup: TargetGroup(v.Contract.Kafka.Topic, v.ID, "es"), CKGroup: TargetGroup(v.Contract.Kafka.Topic, v.ID, "ck"), Revision: 3}
	for _, component := range []string{"es", "ck"} {
		for part, n := range h.PinnedOffsets {
			n := n
			h.Partitions = append(h.Partitions, HandoffPartition{Component: component, Partition: part, InitialOffset: &n, ConfirmedAt: &now})
		}
	}
	return v, h
}
func TestHandoffRejectsWrongVersionGroupAndIncompleteProof(t *testing.T) {
	v, h := handoffFixture(t)
	if err := h.Validate(v); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Handoff){
		"active_group":           func(h *Handoff) { h.ESGroup = v.Contract.Kafka.Topic + ".es.v1" },
		"another_version":        func(h *Handoff) { h.VersionID = "other_version" },
		"stale_contract":         func(h *Handoff) { h.ContractSHA256 = strings.Repeat("c", 64) },
		"missing_partition":      func(h *Handoff) { delete(h.PinnedOffsets, 1) },
		"missing_confirmation":   func(h *Handoff) { h.Partitions = h.Partitions[:len(h.Partitions)-1] },
		"duplicate_confirmation": func(h *Handoff) { h.Partitions[1] = h.Partitions[0] },
		"negative_offset":        func(h *Handoff) { h.PinnedOffsets[0] = -1 },
		"forged_consistent_hash": func(h *Handoff) {
			var report map[string]any
			json.Unmarshal(h.FinalReport, &report)
			report["state"] = "catching_up"
			h.FinalReport, _ = json.Marshal(report)
			hash, _ := HashJSON(h.FinalReport)
			h.FinalReportSHA256 = &hash
		},
	} {
		t.Run(name, func(t *testing.T) {
			b, _ := json.Marshal(h)
			var changed Handoff
			json.Unmarshal(b, &changed)
			mutate(&changed)
			if err := changed.Validate(v); err == nil {
				t.Fatal("unsafe handoff accepted")
			}
		})
	}
}
func TestDynamicHandoffStateNeverChangesTargetCursor(t *testing.T) {
	v, h := handoffFixture(t)
	before := v.CursorTarget()
	now := time.Now().UTC()
	marker := "verified_marker"
	h.Phase = Verified
	h.Revision++
	h.VerifiedAt = &now
	h.VerificationMarker = &marker
	h.VerificationReport = h.FinalReport
	h.VerificationReportSHA256 = h.FinalReportSHA256
	for i := range h.Partitions {
		n := h.PinnedOffsets[h.Partitions[i].Partition] + 2
		h.Partitions[i].VerificationOffset = &n
	}
	if err := h.Validate(v); err != nil {
		t.Fatal(err)
	}
	if v.CursorTarget() != before {
		t.Fatal("handoff progress changed cursor identity")
	}
	h.Partitions[0].VerificationOffset = nil
	if err := h.Validate(v); err == nil {
		t.Fatal("verified without all partition markers")
	}
}
func TestWriterNeverInfersLiveOrRemoteDeathFromFreeLock(t *testing.T) {
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	token := "worker_token"
	pid := int64(os.Getpid())
	owner := TargetOwner{Token: &token, Host: &host, PID: &pid}
	if previousWorkerDead(owner, host) {
		t.Fatal("live writer preempted")
	}
	other := host + "-remote"
	owner.Host = &other
	if previousWorkerDead(owner, host) {
		t.Fatal("remote writer declared dead")
	}
	owner.Token = nil
	if !previousWorkerDead(owner, host) {
		t.Fatal("clean release not reusable")
	}
	if TargetKey("es", "http://127.0.0.1:19206/", "idx") != TargetKey("es", "http://127.0.0.1:19206", "idx") {
		t.Fatal("endpoint slash split target lock")
	}
}
