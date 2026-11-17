package syncpipe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/readruntime"
	"ticketpulse/internal/search"
)

// Observational identity only: no lease, fencing, offset initialization or
// consumer group mutation. Legacy state/deliveries remain in their old tables.
type LegacyIdentity struct {
	BootID    string              `json:"boot_id"`
	Component string              `json:"component"`
	Group     string              `json:"group_id"`
	Source    projection.Source   `json:"source"`
	Kafka     projection.Kafka    `json:"kafka"`
	Endpoint  string              `json:"endpoint"`
	Namespace string              `json:"target_namespace"`
	UUID      string              `json:"target_uuid"`
	Process   readruntime.Process `json:"process"`
}
type LegacyProcessObserver struct {
	Store    *StateStore
	Identity LegacyIdentity
}
type LegacyWriterEvidence struct {
	Identity     LegacyIdentity `json:"identity"`
	IdentityHash string         `json:"identity_sha256"`
	MemberID     string         `json:"member_id"`
	Generation   int32          `json:"generation_id"`
	Heartbeat    time.Time      `json:"heartbeat_at"`
}

func RegisterLegacyObservation(ctx context.Context, s *StateStore, q *Queue, component string, es *search.Elasticsearch, ck *analytics.ClickHouse) (*LegacyProcessObserver, error) {
	installed, err := projection.ReadRollbackInstalled(ctx, s.DB)
	if err != nil {
		return nil, err
	}
	if !installed {
		return nil, nil
	}
	if component != "es" && component != "ck" {
		return nil, fmt.Errorf("invalid legacy observer component")
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	i := LegacyIdentity{BootID: hex.EncodeToString(nonce[:]), Component: component, Group: q.Group(component)}
	if err = s.DB.QueryRowContext(ctx, "SELECT @@server_uuid,DATABASE()").Scan(&i.Source.ServerUUID, &i.Source.Schema); err != nil {
		return nil, err
	}
	if i.Source.Schema != s.Schema {
		return nil, fmt.Errorf("legacy observation source mismatch")
	}
	topics, err := q.Admin.ListTopics(ctx, q.Topic)
	if err != nil {
		return nil, err
	}
	detail, ok := topics[q.Topic]
	if !ok || detail.Err != nil || len(detail.Partitions) != int(q.Partitions) {
		return nil, fmt.Errorf("legacy observation Topic unavailable")
	}
	i.Kafka = projection.Kafka{Topic: q.Topic, TopicUUID: detail.ID.String()}
	for p := int32(0); p < q.Partitions; p++ {
		if _, ok = detail.Partitions[p]; !ok {
			return nil, fmt.Errorf("legacy partition absent")
		}
		i.Kafka.Partitions = append(i.Kafka.Partitions, p)
	}
	if component == "es" {
		if es == nil {
			return nil, fmt.Errorf("legacy ES observer target missing")
		}
		var exists bool
		i.UUID, exists, err = es.Identity(ctx)
		i.Endpoint, i.Namespace = es.Endpoint(), es.Index()
		if err != nil || !exists {
			return nil, fmt.Errorf("legacy ES identity absent: %v", err)
		}
	} else {
		if ck == nil {
			return nil, fmt.Errorf("legacy CK observer target missing")
		}
		value, exists, e := ck.Identity(ctx)
		err = e
		i.UUID = value.String()
		i.Endpoint, i.Namespace = ck.Endpoint(), ck.Database()
		if err != nil || !exists {
			return nil, fmt.Errorf("legacy CK identity absent: %v", err)
		}
	}
	i.Process, err = readruntime.Inspect(ctx, os.Getpid(), "")
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	hash, err := projection.HashJSON(raw)
	if err != nil {
		return nil, err
	}
	if _, err = s.DB.ExecContext(ctx, `INSERT INTO sync_projection_legacy_boots(boot_id,component,group_id,identity,identity_sha256,state,heartbeat_at,created_at) VALUES(?,?,?,?,?,'STARTING',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, i.BootID, component, i.Group, raw, hash); err != nil {
		return nil, err
	}
	return &LegacyProcessObserver{Store: s, Identity: i}, nil
}
func (o *LegacyProcessObserver) Membership(ctx context.Context, member string, generation int32) error {
	state := "STARTING"
	if member != "" && generation >= 0 {
		state = "RUNNING"
	}
	result, err := o.Store.DB.ExecContext(ctx, `UPDATE sync_projection_legacy_boots SET member_id=?,generation_id=?,state=?,heartbeat_at=UTC_TIMESTAMP(6) WHERE boot_id=? AND state<>'STOPPED'`, member, generation, state, o.Identity.BootID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("legacy observation missing: %v", err)
	}
	return nil
}
func (o *LegacyProcessObserver) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	o.Store.DB.ExecContext(ctx, "UPDATE sync_projection_legacy_boots SET state='STOPPED',heartbeat_at=UTC_TIMESTAMP(6) WHERE boot_id=?", o.Identity.BootID)
}

func (d *ReadDestination) LegacyProcess(ctx context.Context, component, member string) (LegacyWriterEvidence, error) {
	rows, err := d.control.Store.DB.QueryContext(ctx, `SELECT identity,identity_sha256,member_id,generation_id,state,heartbeat_at FROM sync_projection_legacy_boots WHERE component=? AND group_id=? ORDER BY created_at`, component, d.Groups[component])
	if err != nil {
		return LegacyWriterEvidence{}, err
	}
	var evidence []LegacyWriterEvidence
	var states []string
	for rows.Next() {
		var raw []byte
		var e LegacyWriterEvidence
		var state string
		if err = rows.Scan(&raw, &e.IdentityHash, &e.MemberID, &e.Generation, &state, &e.Heartbeat); err != nil {
			rows.Close()
			return e, err
		}
		h, failure := projection.HashJSON(raw)
		if failure != nil || h != e.IdentityHash || json.Unmarshal(raw, &e.Identity) != nil {
			rows.Close()
			return e, fmt.Errorf("READ_LEGACY_PROCESS_CORRUPTED")
		}
		evidence = append(evidence, e)
		states = append(states, state)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return LegacyWriterEvidence{}, err
	}
	var live []LegacyWriterEvidence
	for n, e := range evidence {
		actual, failure := readruntime.Inspect(ctx, e.Identity.Process.PID, "")
		if failure != nil || !readruntime.Same(e.Identity.Process, actual) {
			if readruntime.Exited(ctx, e.Identity.Process) == nil {
				continue
			}
			return e, fmt.Errorf("READ_LEGACY_PROCESS_UNPROVEN: %v", failure)
		}
		if states[n] != "RUNNING" || e.MemberID != member || e.Generation < 0 || time.Since(e.Heartbeat) > 5*time.Second || time.Until(e.Heartbeat) > time.Second {
			return e, fmt.Errorf("READ_LEGACY_PROCESS_UNPROVEN: current PID/member/heartbeat differs")
		}
		i := e.Identity
		v := d.Version.Contract
		expectedEndpoint, expectedNamespace, expectedUUID := v.ES.Endpoint, v.ES.Index, v.ES.UUID
		if component == "ck" {
			expectedEndpoint, expectedNamespace, expectedUUID = v.CK.Endpoint, v.CK.Database, (analytics.TableIdentity{Events: v.CK.EventsUUID, States: v.CK.StatesUUID}).String()
		}
		if i.Component != component || i.Group != d.Groups[component] || i.Source != v.Source || !reflect.DeepEqual(i.Kafka, v.Kafka) || i.Endpoint != expectedEndpoint || i.Namespace != expectedNamespace || i.UUID != expectedUUID {
			return e, fmt.Errorf("READ_LEGACY_PROCESS_TARGET_MISMATCH")
		}
		live = append(live, e)
	}
	if len(live) != 1 {
		return LegacyWriterEvidence{}, fmt.Errorf("READ_LEGACY_PROCESS_UNPROVEN: require one actual writer, found %d", len(live))
	}
	return live[0], nil
}
