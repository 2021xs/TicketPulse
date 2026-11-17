package projection

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	assets "ticketpulse/db"
)

const (
	Draining     = "DRAINING"
	Pinned       = "PINNED"
	OffsetsReady = "OFFSETS_READY"
	Consuming    = "CONSUMING"
	Verified     = "VERIFIED"
)

// Dynamic handoff state is deliberately separate from Version.Contract and its
// cursor fingerprint. Source/topic/target identities come from that exact version.
type Handoff struct {
	ID                       string             `json:"handoff_id"`
	VersionID                string             `json:"version_id"`
	ContractSHA256           string             `json:"contract_sha256"`
	JobID                    string             `json:"job_id"`
	FreezeID                 string             `json:"freeze_id"`
	FreezeRevision           int64              `json:"freeze_revision"`
	Phase                    string             `json:"phase"`
	PinnedOffsets            map[int32]int64    `json:"pinned_next_offsets"`
	FinalMarker              *string            `json:"final_source_marker"`
	FinalReport              json.RawMessage    `json:"final_report"`
	FinalReportSHA256        *string            `json:"final_report_sha256"`
	ESGroup                  string             `json:"es_group"`
	CKGroup                  string             `json:"ck_group"`
	VerificationMarker       *string            `json:"verification_marker"`
	VerificationReport       json.RawMessage    `json:"verification_report"`
	VerificationReportSHA256 *string            `json:"verification_report_sha256"`
	Revision                 int64              `json:"revision"`
	LastError                *string            `json:"last_error"`
	CreatedAt                time.Time          `json:"created_at"`
	UpdatedAt                time.Time          `json:"updated_at"`
	VerifiedAt               *time.Time         `json:"verified_at"`
	Partitions               []HandoffPartition `json:"partition_confirmations"`
	Owners                   []TargetOwner      `json:"target_owners"`
}
type HandoffPartition struct {
	Component          string     `json:"component"`
	Partition          int32      `json:"partition"`
	InitialOffset      *int64     `json:"initial_offset"`
	ConfirmedAt        *time.Time `json:"confirmed_at"`
	VerificationOffset *int64     `json:"verification_marker_offset"`
}

func TargetGroup(topic, versionID, component string) string {
	return topic + "." + component + ".target." + versionID
}
func (h Handoff) Group(component string) string {
	if component == "ck" {
		return h.CKGroup
	}
	return h.ESGroup
}
func (h Handoff) Validate(v Version) error {
	if !ValidID(h.ID) || h.VersionID != v.ID || h.ContractSHA256 != v.Fingerprint() || v.Phase != Prepared || v.Contract.Candidate == nil || h.JobID != v.Contract.Candidate.JobID || h.FreezeID == "" || h.FreezeRevision < 1 || h.Revision < 1 || h.Group("es") != TargetGroup(v.Contract.Kafka.Topic, v.ID, "es") || h.CKGroup != TargetGroup(v.Contract.Kafka.Topic, v.ID, "ck") {
		return fmt.Errorf("HANDOFF_CONTRACT_MISMATCH: invalid version/source/job/group linkage")
	}
	switch h.Phase {
	case Draining:
		return nil
	case Pinned, OffsetsReady, Consuming, Verified:
	default:
		return fmt.Errorf("invalid handoff phase")
	}
	if len(h.PinnedOffsets) != len(v.Contract.Kafka.Partitions) || h.FinalMarker == nil || *h.FinalMarker == "" || h.FinalReportSHA256 == nil {
		return fmt.Errorf("incomplete pinned handoff")
	}
	for _, p := range v.Contract.Kafka.Partitions {
		if n, ok := h.PinnedOffsets[p]; !ok || n < 0 {
			return fmt.Errorf("incomplete handoff partition %d", p)
		}
	}
	hash, err := HashJSON(h.FinalReport)
	if err != nil || hash != *h.FinalReportSHA256 {
		return fmt.Errorf("handoff final report hash differs")
	}
	if err := validateHandoffReport(h.FinalReport, v); err != nil {
		return err
	}
	if h.Phase != Pinned {
		if len(h.Partitions) != 2*len(v.Contract.Kafka.Partitions) {
			return fmt.Errorf("handoff initial confirmations incomplete")
		}
		seen := map[string]bool{}
		for _, part := range h.Partitions {
			key := fmt.Sprintf("%s:%d", part.Component, part.Partition)
			n, exists := h.PinnedOffsets[part.Partition]
			if seen[key] || !exists || (part.Component != "es" && part.Component != "ck") || part.InitialOffset == nil || *part.InitialOffset != n || part.ConfirmedAt == nil {
				return fmt.Errorf("handoff initial partition confirmation differs")
			}
			seen[key] = true
		}
	}
	if h.Phase == Verified {
		if h.VerifiedAt == nil || h.VerificationMarker == nil || h.VerificationReportSHA256 == nil {
			return fmt.Errorf("incomplete verified handoff")
		}
		hash, err = HashJSON(h.VerificationReport)
		if err != nil || hash != *h.VerificationReportSHA256 {
			return fmt.Errorf("handoff verification report hash differs")
		}
		if err := validateHandoffReport(h.VerificationReport, v); err != nil {
			return err
		}
		for _, part := range h.Partitions {
			if part.VerificationOffset == nil || *part.VerificationOffset < h.PinnedOffsets[part.Partition] {
				return fmt.Errorf("verification marker precedes handoff boundary")
			}
		}
	}
	return nil
}
func validateHandoffReport(data json.RawMessage, v Version) error {
	var report struct {
		State           string           `json:"state"`
		SourceChanged   bool             `json:"source_changed"`
		DifferenceCount int64            `json:"difference_count"`
		SourceAfter     *json.RawMessage `json:"source_after"`
		StartedAt       time.Time        `json:"started_at"`
		CompletedAt     time.Time        `json:"completed_at"`
		ES              *struct {
			Target string `json:"target"`
		} `json:"es"`
		CK *struct {
			Target string `json:"target"`
		} `json:"ck"`
	}
	if json.Unmarshal(data, &report) != nil || report.State != "consistent" || report.SourceChanged || report.DifferenceCount != 0 || report.SourceAfter == nil || report.StartedAt.IsZero() || report.CompletedAt.Before(report.StartedAt) || report.ES == nil || report.CK == nil || report.ES.Target != v.Contract.ES.Endpoint+"/"+v.Contract.ES.Index || report.CK.Target != v.Contract.CK.Endpoint+"/"+v.Contract.CK.Database {
		return fmt.Errorf("handoff requires a complete consistent report for these exact targets")
	}
	return nil
}
func EnsureHandoffSchema(ctx context.Context, db *sql.DB) error {
	if err := EnsureSchema(ctx, db); err != nil {
		return err
	}
	for _, statement := range strings.Split(assets.ProjectionHandoffMigration, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	// The preexisting control log retains installation provenance. A missing
	// handoff table must not downgrade Resume to M4-03A compatibility.
	_, err := db.ExecContext(ctx, `INSERT INTO sync_projection_control_log(operation,reference_id,details,occurred_at) SELECT 'install_handoff_schema','012_projection_handoff',JSON_OBJECT('schema',12),UTC_TIMESTAMP(6) WHERE NOT EXISTS(SELECT 1 FROM sync_projection_control_log existing WHERE existing.operation='install_handoff_schema')`)
	return err
}

type HandoffStore struct{ DB *sql.DB }

// Preserve legacy admission only when the source has never installed handoff
// control. A missing table after installation is uncertainty, not permission to
// bypass a durable seal. Installation provenance survives in the 011 log.
func HandoffInstalled(ctx context.Context, q rowQuery) (bool, error) {
	var installed int
	err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_control_log WHERE operation='install_handoff_schema'").Scan(&installed)
	if err != nil && !MissingTable(err) {
		return false, err
	}
	if installed == 0 {
		err = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN ('sync_projection_handoffs','sync_projection_target_owners','sync_projection_handoff_partitions','sync_projection_handoff_log','sync_projection_deliveries','sync_projection_worker_state','sync_projection_watermarks')").Scan(&installed)
		if err != nil {
			return false, err
		}
	}
	return installed > 0, nil
}

func readHandoff(ctx context.Context, q rowQuery, id string, lock string) (h Handoff, err error) {
	var pinned, finalReport, verificationReport []byte
	err = q.QueryRowContext(ctx, `SELECT handoff_id,version_id,contract_sha256,job_id,freeze_id,freeze_revision,phase,pinned_offsets,final_marker,final_report,final_report_sha256,es_group,ck_group,verification_marker,verification_report,verification_report_sha256,revision,last_error,created_at,updated_at,verified_at FROM sync_projection_handoffs WHERE handoff_id=?`+lock, id).Scan(&h.ID, &h.VersionID, &h.ContractSHA256, &h.JobID, &h.FreezeID, &h.FreezeRevision, &h.Phase, &pinned, &h.FinalMarker, &finalReport, &h.FinalReportSHA256, &h.ESGroup, &h.CKGroup, &h.VerificationMarker, &verificationReport, &h.VerificationReportSHA256, &h.Revision, &h.LastError, &h.CreatedAt, &h.UpdatedAt, &h.VerifiedAt)
	h.FinalReport, h.VerificationReport = finalReport, verificationReport
	if err == nil && len(pinned) > 0 {
		err = json.Unmarshal(pinned, &h.PinnedOffsets)
	}
	return
}
func (s *HandoffStore) Get(ctx context.Context, id string) (Handoff, error) {
	h, err := readHandoff(ctx, s.DB, id, "")
	if err != nil {
		return h, err
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT component,partition_id,initial_offset,confirmed_at,verification_offset FROM sync_projection_handoff_partitions WHERE handoff_id=? ORDER BY component,partition_id`, id)
	if err != nil {
		return h, err
	}
	defer rows.Close()
	for rows.Next() {
		var p HandoffPartition
		if err = rows.Scan(&p.Component, &p.Partition, &p.InitialOffset, &p.ConfirmedAt, &p.VerificationOffset); err != nil {
			return h, err
		}
		h.Partitions = append(h.Partitions, p)
	}
	if err = rows.Err(); err != nil {
		return h, err
	}
	rows.Close()
	owners, err := s.DB.QueryContext(ctx, "SELECT target_key FROM sync_projection_target_owners WHERE handoff_id=? ORDER BY component", id)
	if err != nil {
		return h, err
	}
	keys := []string{}
	for owners.Next() {
		var key string
		if err = owners.Scan(&key); err != nil {
			owners.Close()
			return h, err
		}
		keys = append(keys, key)
	}
	err = owners.Err()
	owners.Close()
	if err != nil {
		return h, err
	}
	for _, key := range keys {
		o, err := ReadOwner(ctx, s.DB, key, "")
		if err != nil {
			return h, err
		}
		o.Token = nil
		h.Owners = append(h.Owners, o)
	}
	return h, nil
}
func (s *HandoffStore) Lock(ctx context.Context, tx *sql.Tx, id string) (Handoff, error) {
	return readHandoff(ctx, tx, id, " FOR UPDATE")
}
