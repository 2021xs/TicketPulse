package syncpipe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
)

var errReadPending = errors.New("READ_DELIVERY_PENDING")

// ReadDestination binds both data targets and both status namespaces to one
// immutable version. Resolving it does not join a group or acquire a writer.
type ReadDestination struct {
	Version        projection.Version    `json:"version"`
	Handoff        *projection.Handoff   `json:"handoff,omitempty"`
	Groups         map[string]string     `json:"groups"`
	StateNamespace string                `json:"state_namespace"`
	CKDatabaseUUID string                `json:"ck_database_uuid"`
	ES             *search.Elasticsearch `json:"-"`
	CK             *analytics.ClickHouse `json:"-"`
	control        *ProjectionControl
}

func (p *ProjectionControl) ResolveReadDestination(ctx context.Context, id string) (*ReadDestination, error) {
	v, err := (&projection.VersionStore{DB: p.Store.DB}).Get(ctx, id)
	if err != nil {
		return nil, err
	}
	es, err := search.NewElasticsearch(v.Contract.ES.Endpoint, v.Contract.ES.Index)
	if err != nil {
		return nil, err
	}
	ck, err := analytics.NewClickHouse(v.Contract.CK.Endpoint, v.Contract.CK.Database)
	if err != nil {
		return nil, err
	}
	d := &ReadDestination{Version: v, ES: es, CK: ck, Groups: map[string]string{}, control: p}
	d.CKDatabaseUUID, err = readCKDatabaseUUID(ctx, ck)
	if err != nil {
		return nil, err
	}
	if v.Phase == projection.Active {
		d.StateNamespace = "legacy"
		d.Groups["es"], d.Groups["ck"] = v.Contract.ESWriter.Owner, v.Contract.CKWriter.Owner
		for _, component := range []string{"es", "ck"} {
			if err = (&projection.VersionStore{DB: p.Store.DB}).VerifyBinding(ctx, v, component); err != nil {
				return nil, err
			}
		}
	} else {
		var hid string
		if err = p.Store.DB.QueryRowContext(ctx, "SELECT handoff_id FROM sync_projection_handoffs WHERE version_id=?", id).Scan(&hid); err != nil {
			return nil, fmt.Errorf("read destination requires verified handoff: %w", err)
		}
		h, err := (&projection.HandoffStore{DB: p.Store.DB}).Get(ctx, hid)
		if err != nil {
			return nil, err
		}
		if err = h.Validate(v); err != nil {
			return nil, err
		}
		if h.Phase != projection.Verified {
			return nil, fmt.Errorf("read destination handoff is not VERIFIED")
		}
		d.Handoff = &h
		d.StateNamespace = id
		d.Groups["es"], d.Groups["ck"] = h.ESGroup, h.CKGroup
	}
	return d, d.CheckIdentities(ctx)
}
func (d *ReadDestination) handoffControl() *HandoffControl {
	return &HandoffControl{ProjectionControl: &ProjectionControl{Store: d.control.Store, Queue: d.control.Queue, ES: d.ES, CK: d.CK}}
}
func (d *ReadDestination) CheckIdentities(ctx context.Context) error {
	current, err := (&projection.VersionStore{DB: d.control.Store.DB}).Get(ctx, d.Version.ID)
	if err != nil || current.Fingerprint() != d.Version.Fingerprint() {
		return fmt.Errorf("%w: destination contract changed: %v", projection.ErrTargetMismatch, err)
	}
	if current.Contract.Source != d.Version.Contract.Source || current.Contract.Kafka.Topic != d.control.Queue.Topic {
		return fmt.Errorf("%w: destination source/topic differs", projection.ErrTargetMismatch)
	}
	if err = d.handoffControl().checkIdentities(ctx, d.Version); err != nil {
		return err
	}
	databaseUUID, err := readCKDatabaseUUID(ctx, d.CK)
	if err != nil || databaseUUID != d.CKDatabaseUUID {
		return fmt.Errorf("%w: CK database UUID changed: %v", projection.ErrTargetMismatch, err)
	}
	if d.Handoff != nil {
		h, err := (&projection.HandoffStore{DB: d.control.Store.DB}).Get(ctx, d.Handoff.ID)
		if err != nil {
			return err
		}
		if err = h.Validate(current); err != nil {
			return err
		}
		if h.Phase != projection.Verified || h.ESGroup != d.Groups["es"] || h.CKGroup != d.Groups["ck"] {
			return fmt.Errorf("read handoff state/groups changed")
		}
	}
	return nil
}

// Supplemental C1 provenance; the frozen A/B Contract still contains its
// original database name and table UUIDs without modification.
func readCKDatabaseUUID(ctx context.Context, ck *analytics.ClickHouse) (string, error) {
	data, err := ck.QueryGlobal(ctx, "SELECT toString(uuid) FROM system.databases WHERE name='"+ck.Database()+"' AND engine='Atomic' FORMAT TabSeparated")
	if err != nil {
		return "", err
	}
	uuid := strings.TrimSpace(string(data))
	if len(uuid) != 36 || uuid == "00000000-0000-0000-0000-000000000000" {
		return "", fmt.Errorf("CK database UUID unavailable")
	}
	return uuid, nil
}

type ReadComponentEvidence struct {
	Group       string                  `json:"group_id"`
	Namespace   string                  `json:"state_namespace"`
	RawOffsets  kadm.OffsetResponses    `json:"raw_fetch_offsets"`
	Progress    []PartitionProgress     `json:"positions"`
	Owner       *projection.TargetOwner `json:"owner,omitempty"`
	Legacy      *LegacyWriterEvidence   `json:"legacy_process,omitempty"`
	WriterLive  bool                    `json:"writer_live"`
	WorkerState string                  `json:"worker_state"`
	Heartbeat   time.Time               `json:"heartbeat_at"`
	GroupState  string                  `json:"group_state"`
	MemberID    string                  `json:"member_id"`
	ClientHost  string                  `json:"client_host"`
	Assigned    []int32                 `json:"assigned_partitions"`
	ObservedAt  time.Time               `json:"observed_at"`
}

// RawOffsetProgress never uses Queue.Progress's legacy log-start fallback.
// Both branches require a real committed next offset for every partition.
func (d *ReadDestination) RawOffsetProgress(ctx context.Context, component string) (kadm.OffsetResponses, []PartitionProgress, error) {
	group, ok := d.Groups[component]
	if !ok {
		return nil, nil, fmt.Errorf("unknown read component")
	}
	q := d.control.Queue
	offsets, err := d.handoffControl().rawOffsets(ctx, group)
	if err != nil {
		return nil, nil, err
	}
	starts, err := q.Admin.ListStartOffsets(ctx, q.Topic)
	if err != nil {
		return nil, nil, err
	}
	ends, err := q.Admin.ListEndOffsets(ctx, q.Topic)
	if err != nil {
		return nil, nil, err
	}
	var progress []PartitionProgress
	for _, p := range d.Version.Contract.Kafka.Partitions {
		start, a := starts.Lookup(q.Topic, p)
		end, b := ends.Lookup(q.Topic, p)
		o, c := offsets.Lookup(q.Topic, p)
		if !a || !b || start.Err != nil || end.Err != nil {
			return nil, nil, fmt.Errorf("READ_OFFSET_UNAVAILABLE: partition %d range missing", p)
		}
		if !c || o.Err != nil || o.At < 0 {
			return nil, nil, fmt.Errorf("READ_OFFSET_MISSING: %s/%d has no committed offset", component, p)
		}
		if o.At < start.Offset || o.At > end.Offset || d.Handoff != nil && o.At < d.Handoff.PinnedOffsets[p] {
			return nil, nil, fmt.Errorf("READ_OFFSET_OUT_OF_RANGE: %s/%d committed %d retained [%d,%d]", component, p, o.At, start.Offset, end.Offset)
		}
		progress = append(progress, PartitionProgress{Partition: p, Start: start.Offset, End: end.Offset, Committed: o.At, Lag: end.Offset - o.At})
	}
	return offsets, progress, nil
}

func (d *ReadDestination) ObserveWriter(ctx context.Context, component string) (ReadComponentEvidence, error) {
	e := ReadComponentEvidence{Group: d.Groups[component], Namespace: d.StateNamespace, ObservedAt: time.Now().UTC()}
	db := d.control.Store.DB
	var err error
	e.RawOffsets, e.Progress, err = d.RawOffsetProgress(ctx, component)
	if err != nil {
		return e, err
	}
	var pending, failed int
	if d.Handoff != nil {
		var group string
		var epoch int64
		if err = db.QueryRowContext(ctx, `SELECT state,heartbeat_at,group_id,owner_epoch FROM sync_projection_worker_state WHERE version_id=? AND component=?`, d.Version.ID, component).Scan(&e.WorkerState, &e.Heartbeat, &group, &epoch); err != nil {
			return e, err
		}
		owner, err := projection.ReadOwner(ctx, db, projection.VersionTargetKey(d.Version, component), "")
		if err != nil {
			return e, err
		}
		var held sql.NullInt64
		if err = db.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", projection.TargetLockName(owner.Key)).Scan(&held); err != nil {
			return e, err
		}
		if owner.Kind != "consumer_group" || owner.Owner != e.Group || owner.HandoffID == nil || *owner.HandoffID != d.Handoff.ID || owner.VersionID == nil || *owner.VersionID != d.Version.ID || owner.Token == nil || owner.ConnectionID == nil || !held.Valid || held.Int64 != *owner.ConnectionID || group != e.Group || epoch != owner.Epoch {
			return e, fmt.Errorf("READ_WRITER_UNAVAILABLE: %s owner/epoch/lock mismatch", component)
		}
		e.Owner = &owner
		e.WriterLive = true
		err = db.QueryRowContext(ctx, `SELECT COALESCE(SUM(status IN ('processing','retrying')),0),COALESCE(SUM(status NOT IN ('applied','duplicate_or_old','recovered','processing','retrying')),0) FROM sync_projection_deliveries WHERE version_id=? AND component=?`, d.Version.ID, component).Scan(&pending, &failed)
	} else {
		if err = db.QueryRowContext(ctx, "SELECT state,heartbeat_at FROM sync_worker_state WHERE component=?", component).Scan(&e.WorkerState, &e.Heartbeat); err != nil {
			return e, err
		}
		table := "sync_kafka_es_deliveries"
		if component == "ck" {
			table = "sync_kafka_ck_deliveries"
		}
		err = db.QueryRowContext(ctx, "SELECT COALESCE(SUM(status IN ('processing','retrying')),0),COALESCE(SUM(status NOT IN ('applied','duplicate_or_old','recovered','processing','retrying')),0) FROM "+table).Scan(&pending, &failed)
		// Legacy has no B lease. This proves a live single group + heartbeat,
		// not a request-level or cross-process backend fencing guarantee.
	}
	if err != nil {
		return e, err
	}
	if failed != 0 {
		return e, fmt.Errorf("READ_UNRESOLVED_DELIVERIES: %s has %d isolated/unknown records", component, failed)
	}
	if pending != 0 {
		return e, fmt.Errorf("%w: %s has %d processing/retrying records", errReadPending, component, pending)
	}
	groups, err := d.control.Queue.Admin.DescribeGroups(ctx, e.Group)
	if err != nil {
		return e, err
	}
	g, ok := groups[e.Group]
	if !ok || g.Err != nil || g.State != "Stable" || len(g.Members) != 1 {
		return e, fmt.Errorf("READ_WRITER_UNAVAILABLE: %s is not one Stable member", component)
	}
	e.GroupState = g.State
	e.MemberID = g.Members[0].MemberID
	e.ClientHost = g.Members[0].ClientHost
	assigned := g.AssignedPartitions()
	parts, ok := assigned[d.Version.Contract.Kafka.Topic]
	if !ok || len(assigned) != 1 || len(parts) != len(d.Version.Contract.Kafka.Partitions) {
		return e, fmt.Errorf("READ_WRITER_UNAVAILABLE: %s partition assignment differs", component)
	}
	for _, part := range d.Version.Contract.Kafka.Partitions {
		if _, ok := parts[part]; !ok {
			return e, fmt.Errorf("READ_WRITER_UNAVAILABLE: %s partition %d not assigned", component, part)
		}
		e.Assigned = append(e.Assigned, part)
	}
	if d.Handoff == nil {
		installed, e2 := projection.ReadRollbackInstalled(ctx, db)
		if e2 != nil {
			return e, e2
		}
		if installed {
			if host := net.ParseIP(strings.TrimPrefix(e.ClientHost, "/")); host == nil || !host.IsLoopback() {
				return e, fmt.Errorf("READ_LEGACY_PROCESS_UNPROVEN: current member is outside the controlled local host")
			}
			value, e2 := d.LegacyProcess(ctx, component, e.MemberID)
			if e2 != nil {
				return e, e2
			}
			e.Legacy = &value
		}
	}
	if e.WorkerState != "running" || time.Since(e.Heartbeat) > 10*time.Second || time.Until(e.Heartbeat) > time.Second {
		return e, fmt.Errorf("READ_WRITER_UNAVAILABLE: %s heartbeat/state is not current", component)
	}
	if d.Handoff == nil {
		e.WriterLive = true
	}
	return e, nil
}
func sameReadWriter(a, b ReadComponentEvidence) bool {
	if a.Group != b.Group || a.Namespace != b.Namespace || a.MemberID != b.MemberID || a.WriterLive != b.WriterLive || a.ClientHost != b.ClientHost || !reflect.DeepEqual(a.Assigned, b.Assigned) {
		return false
	}
	if a.Owner == nil || b.Owner == nil {
		if a.Owner != nil || b.Owner != nil {
			return false
		}
		if a.Legacy == nil || b.Legacy == nil {
			return a.Legacy == nil && b.Legacy == nil
		}
		return a.Legacy.IdentityHash == b.Legacy.IdentityHash && a.Legacy.Generation == b.Legacy.Generation && a.Legacy.MemberID == b.Legacy.MemberID
	}
	return a.Owner.Key == b.Owner.Key && a.Owner.Kind == b.Owner.Kind && a.Owner.Owner == b.Owner.Owner && a.Owner.Epoch == b.Owner.Epoch && reflect.DeepEqual(a.Owner.Token, b.Owner.Token) && reflect.DeepEqual(a.Owner.ConnectionID, b.Owner.ConnectionID)
}
