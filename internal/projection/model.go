// Package projection holds the durable M4-03A contract. It does not promote
// targets, write projections, migrate offsets, or coordinate consumer ownership.
package projection

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"time"
)

const (
	Active   = "ACTIVE"
	Prepared = "PREPARED"
)

var idPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
var schemaPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_]{0,63}$`)
var indexPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,200}$`)
var databasePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_]{0,127}$`)
var hashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func ValidID(id string) bool { return idPattern.MatchString(id) }

func validEndpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.Path == ""
}
func validBinding(b Binding, kind, owner, component string) bool {
	table := "__consumer_offsets"
	if kind == "rebuild_job" {
		table = "sync_rebuild_offsets"
	}
	return b.Kind == kind && b.Owner == owner && b.ProgressTable == table && b.StatusComponent == component
}

type Source struct {
	ServerUUID string `json:"server_uuid"`
	Schema     string `json:"schema"`
}
type Kafka struct {
	Topic      string  `json:"topic"`
	TopicUUID  string  `json:"topic_uuid"`
	Partitions []int32 `json:"partitions"`
}
type ES struct {
	Endpoint string `json:"endpoint"`
	Index    string `json:"index"`
	UUID     string `json:"index_uuid"`
}
type CK struct {
	Endpoint   string `json:"endpoint"`
	Database   string `json:"database"`
	EventsUUID string `json:"ticket_events_uuid"`
	StatesUUID string `json:"ticket_states_uuid"`
}

// Binding names the existing writer and its progress/status namespace.
// Prepared candidates remain owned by rebuild offsets; their later Kafka
// group handoff is expressly unverified, even after a content check passes.
type Binding struct {
	Kind            string `json:"kind"`
	Owner           string `json:"owner"`
	ProgressTable   string `json:"progress_table"`
	StatusComponent string `json:"status_component"`
}
type Candidate struct {
	JobID          string          `json:"job_id"`
	SourceSHA256   string          `json:"frozen_source_sha256"`
	M4ReportSHA256 string          `json:"m4_report_sha256"`
	NextOffsets    map[int32]int64 `json:"candidate_next_offsets"`
}
type Contract struct {
	SchemaVersion   int        `json:"schema_version"`
	Source          Source     `json:"mysql_source"`
	Kafka           Kafka      `json:"kafka"`
	ES              ES         `json:"es"`
	CK              CK         `json:"ck"`
	ESWriter        Binding    `json:"es_writer"`
	CKWriter        Binding    `json:"ck_writer"`
	NextESWriter    *Binding   `json:"next_es_writer,omitempty"`
	NextCKWriter    *Binding   `json:"next_ck_writer,omitempty"`
	Candidate       *Candidate `json:"candidate,omitempty"`
	HandoffVerified bool       `json:"handoff_verified"`
}
type Version struct {
	ID               string          `json:"version_id"`
	Phase            string          `json:"phase"`
	Contract         Contract        `json:"contract"`
	ValidationReport json.RawMessage `json:"validation_report,omitempty"`
	ReportSHA256     *string         `json:"report_sha256,omitempty"`
	FreezeRevision   *int64          `json:"freeze_revision,omitempty"`
	CheckedAt        time.Time       `json:"checked_at"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

func (v Version) Validate() error {
	c := v.Contract
	if !ValidID(v.ID) || (v.Phase != Active && v.Phase != Prepared) || c.SchemaVersion != 1 || c.Source.ServerUUID == "" || c.Source.Schema == "" || c.Kafka.Topic == "" || c.Kafka.TopicUUID == "" || len(c.Kafka.Partitions) < 1 || len(c.Kafka.Partitions) > 64 || c.ES.Endpoint == "" || c.ES.Index == "" || c.ES.UUID == "" || c.CK.Endpoint == "" || c.CK.Database == "" || c.CK.EventsUUID == "" || c.CK.StatesUUID == "" {
		return fmt.Errorf("incomplete managed projection contract")
	}
	for i, p := range c.Kafka.Partitions {
		if p != int32(i) {
			return fmt.Errorf("projection partition set must be contiguous")
		}
	}
	if !schemaPattern.MatchString(c.Source.Schema) || !indexPattern.MatchString(c.ES.Index) || !databasePattern.MatchString(c.CK.Database) || !validEndpoint(c.ES.Endpoint) || !validEndpoint(c.CK.Endpoint) || c.Kafka.Topic != "ticketpulse."+c.Source.Schema+".tickets.v1" {
		return fmt.Errorf("invalid managed projection names/endpoints")
	}
	if c.HandoffVerified {
		return fmt.Errorf("M4-03A cannot verify a position handoff")
	}
	if v.FreezeRevision == nil || *v.FreezeRevision < 1 || len(v.ValidationReport) == 0 || v.ReportSHA256 == nil || !hashPattern.MatchString(*v.ReportSHA256) {
		return fmt.Errorf("projection registration requires fresh frozen validation")
	}
	hash, err := HashJSON(v.ValidationReport)
	if err != nil || hash != *v.ReportSHA256 {
		return fmt.Errorf("projection validation report hash differs")
	}
	var report struct {
		State           string           `json:"state"`
		StartedAt       time.Time        `json:"started_at"`
		CompletedAt     time.Time        `json:"completed_at"`
		SourceChanged   bool             `json:"source_changed"`
		SourceAfter     *json.RawMessage `json:"source_after"`
		DifferenceCount int64            `json:"difference_count"`
		ES              *struct {
			Target string `json:"target"`
		} `json:"es"`
		CK *struct {
			Target string `json:"target"`
		} `json:"ck"`
	}
	if json.Unmarshal(v.ValidationReport, &report) != nil || report.State != "consistent" || report.SourceChanged || report.SourceAfter == nil || report.DifferenceCount != 0 || report.ES == nil || report.CK == nil || report.ES.Target != c.ES.Endpoint+"/"+c.ES.Index || report.CK.Target != c.CK.Endpoint+"/"+c.CK.Database || report.StartedAt.IsZero() || report.CompletedAt.Before(report.StartedAt) || report.CompletedAt.IsZero() {
		return fmt.Errorf("projection requires a complete consistent validation for these exact targets")
	}
	if v.Phase == Active {
		if c.Candidate != nil || c.NextESWriter != nil || c.NextCKWriter != nil || !validBinding(c.ESWriter, "consumer_group", c.Kafka.Topic+".es.v1", "es") || !validBinding(c.CKWriter, "consumer_group", c.Kafka.Topic+".ck.v1", "ck") {
			return fmt.Errorf("ACTIVE is limited to initial existing-target enrollment")
		}
	} else {
		if c.Candidate == nil || !ValidID(c.Candidate.JobID) || c.ESWriter.Kind != "rebuild_job" || c.CKWriter.Kind != "rebuild_job" || c.ESWriter.Owner != c.Candidate.JobID || c.CKWriter.Owner != c.Candidate.JobID || c.NextESWriter == nil || c.NextCKWriter == nil || v.FreezeRevision == nil || len(v.ValidationReport) == 0 || v.ReportSHA256 == nil || len(c.Candidate.NextOffsets) != len(c.Kafka.Partitions) {
			return fmt.Errorf("PREPARED requires a frozen candidate and fresh validation")
		}
		if !validBinding(c.ESWriter, "rebuild_job", c.Candidate.JobID, c.Candidate.JobID) || !validBinding(c.CKWriter, "rebuild_job", c.Candidate.JobID, c.Candidate.JobID) || !validBinding(*c.NextESWriter, "consumer_group", c.Kafka.Topic+".es.v1", "es") || !validBinding(*c.NextCKWriter, "consumer_group", c.Kafka.Topic+".ck.v1", "ck") || !hashPattern.MatchString(c.Candidate.SourceSHA256) || !hashPattern.MatchString(c.Candidate.M4ReportSHA256) {
			return fmt.Errorf("invalid candidate ownership/report linkage")
		}
		for _, p := range c.Kafka.Partitions {
			if n, ok := c.Candidate.NextOffsets[p]; !ok || n < 0 {
				return fmt.Errorf("invalid candidate partition position")
			}
		}
	}
	return nil
}
