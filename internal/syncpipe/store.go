package syncpipe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"ticketpulse/internal/projection"
)

type Checkpoint struct {
	Position
	BootstrapComplete bool `json:"bootstrap_complete"`
}
type StateStore struct {
	DB        *sql.DB
	Schema    string
	Consumer  string // empty/es uses the M2 table; ck uses its independent delivery table.
	VersionID string // empty preserves the original M2/M3 progress namespace.
	GroupID   string
	Writer    *projection.WriterLease
}

func (s *StateStore) ForConsumer(component string) *StateStore {
	copy := *s
	copy.Consumer = component
	return &copy
}
func (s *StateStore) deliveryTable() string {
	if s.Consumer == "ck" {
		return "sync_kafka_ck_deliveries"
	}
	return "sync_kafka_es_deliveries"
}

// MySQL named lock is scoped to this connection, not a workflow framework.
// Capture and initialization use one named lock; Kafka groups coordinate consumers.
func (s *StateStore) Lock(ctx context.Context, component string) (func(), error) {
	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var acquired int
	name := "tp:" + s.Schema + ":" + component
	if len(name) > 64 {
		conn.Close()
		return nil, fmt.Errorf("sync schema name too long")
	}
	err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?,0)", name).Scan(&acquired)
	if err != nil || acquired != 1 {
		conn.Close()
		return nil, fmt.Errorf("sync %s is already running or lock failed", component)
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", name)
		_ = conn.Close()
	}, nil
}

func (s *StateStore) Checkpoint(ctx context.Context) (Checkpoint, error) {
	var cp Checkpoint
	err := s.DB.QueryRowContext(ctx, "SELECT server_uuid,binlog_file,binlog_pos,bootstrap_complete FROM sync_kafka_cdc_checkpoint WHERE source_name='tickets-v1'").Scan(&cp.ServerUUID, &cp.File, &cp.Pos, &cp.BootstrapComplete)
	return cp, err
}

func (s *StateStore) SaveCheckpoint(ctx context.Context, p Position, bootstrap bool) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO sync_kafka_cdc_checkpoint(source_name,server_uuid,binlog_file,binlog_pos,bootstrap_complete,updated_at) VALUES('tickets-v1',?,?,?,?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE server_uuid=VALUES(server_uuid),binlog_file=VALUES(binlog_file),binlog_pos=VALUES(binlog_pos),bootstrap_complete=VALUES(bootstrap_complete),updated_at=VALUES(updated_at)`, p.ServerUUID, p.File, p.Pos, bootstrap)
	return err
}

func (s *StateStore) Heartbeat(ctx context.Context, component, state string, eventAt *time.Time, lastError error) error {
	if s.VersionID != "" {
		return s.targetHeartbeat(ctx, component, state, eventAt, lastError)
	}
	var problem any
	if lastError != nil {
		problem = safeText(lastError.Error(), 1000)
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO sync_worker_state(component,state,heartbeat_at,last_event_at,last_error) VALUES(?,?,UTC_TIMESTAMP(6),?,?) ON DUPLICATE KEY UPDATE state=VALUES(state),heartbeat_at=VALUES(heartbeat_at),last_event_at=CASE WHEN last_event_at IS NULL THEN VALUES(last_event_at) WHEN VALUES(last_event_at)>last_event_at THEN VALUES(last_event_at) ELSE last_event_at END,last_error=VALUES(last_error)`, component, state, eventAt, problem)
	return err
}

// attemptDelta counts actual downstream attempts, never terminal writes.
func (s *StateStore) Delivery(ctx context.Context, ref RecordRef, event Event, status string, attemptDelta int, problem error, payload []byte) error {
	if s.VersionID != "" {
		return s.targetDelivery(ctx, ref, event, status, attemptDelta, problem, payload)
	}
	var message any
	if problem != nil {
		message = safeText(problem.Error(), 1000)
	}
	var id, version, eventAt any
	if event.TicketID > 0 {
		id = event.TicketID
	}
	if event.Version > 0 {
		version = event.Version
	}
	if !event.EventAt.IsZero() && event.EventAt.Year() >= 1000 && event.EventAt.Year() <= 9999 {
		eventAt = event.EventAt
	}
	eventID := event.ID
	if eventID == "" {
		eventID = fmt.Sprintf("invalid:%d:%d", ref.Partition, ref.Offset)
	}
	eventID = safeText(eventID, 255)
	_, err := s.DB.ExecContext(ctx, `INSERT INTO `+s.deliveryTable()+`(topic,partition_id,record_offset,event_id,ticket_id,ticket_version,status,attempts,first_seen_at,last_seen_at,event_at,last_error,isolated_payload) VALUES(?,?,?,?,?,?,?,?,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),?,?,?) ON DUPLICATE KEY UPDATE status=VALUES(status),attempts=attempts+VALUES(attempts),last_seen_at=VALUES(last_seen_at),last_error=VALUES(last_error),isolated_payload=COALESCE(VALUES(isolated_payload),isolated_payload)`, ref.Topic, ref.Partition, ref.Offset, eventID, id, version, status, attemptDelta, eventAt, message, payload)
	return err
}
func (s *StateStore) ReplayPayload(ctx context.Context, ref RecordRef) ([]byte, error) {
	if s.VersionID != "" {
		return s.targetReplayPayload(ctx, ref)
	}
	var data []byte
	err := s.DB.QueryRowContext(ctx, "SELECT isolated_payload FROM "+s.deliveryTable()+" WHERE topic=? AND partition_id=? AND record_offset=? AND status='isolated'", ref.Topic, ref.Partition, ref.Offset).Scan(&data)
	return data, err
}

func safeText(value string, maxBytes int) string {
	value = strings.ToValidUTF8(value, "?")
	if len(value) > maxBytes {
		value = value[:maxBytes]
		for !utf8.ValidString(value) {
			value = value[:len(value)-1]
		}
	}
	return value
}
func (s *StateStore) RecordRecovery(ctx context.Context, original, recovery RecordRef) error {
	if s.VersionID != "" {
		return s.targetRecordRecovery(ctx, original, recovery)
	}
	if original.Topic != recovery.Topic {
		return fmt.Errorf("recovery topic differs")
	}
	_, err := s.DB.ExecContext(ctx, "UPDATE "+s.deliveryTable()+" SET recovery_partition=?,recovery_offset=?,last_seen_at=UTC_TIMESTAMP(6) WHERE topic=? AND partition_id=? AND record_offset=? AND status='isolated'", recovery.Partition, recovery.Offset, original.Topic, original.Partition, original.Offset)
	if err != nil {
		return err
	}
	return s.FinishRecoveries(ctx, recovery)
}
func (s *StateStore) FinishRecoveries(ctx context.Context, ref RecordRef) error {
	if s.VersionID != "" {
		return s.targetFinishRecoveries(ctx, ref)
	}
	table := s.deliveryTable()
	_, err := s.DB.ExecContext(ctx, `UPDATE `+table+` original JOIN `+table+` replay ON replay.topic=original.topic AND replay.partition_id=original.recovery_partition AND replay.record_offset=original.recovery_offset SET original.status='recovered' WHERE original.status='isolated' AND replay.topic=? AND replay.partition_id=? AND replay.record_offset=? AND replay.status IN ('applied','duplicate_or_old')`, ref.Topic, ref.Partition, ref.Offset)
	return err
}

// Idle polling touches liveness without erasing a persisted retry/isolation state.
func (s *StateStore) KeepAlive(ctx context.Context, component string) func() {
	if s.VersionID != "" && component != s.Consumer {
		// Preserve the legacy stop-function signature, but do not start a
		// heartbeat loop for a namespace this registered writer does not own.
		return func() {}
	}
	beatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-beatCtx.Done():
				return
			case <-tick.C:
				if s.VersionID != "" {
					if s.writerCheck(beatCtx) == nil {
						_, _ = s.DB.ExecContext(beatCtx, "UPDATE sync_projection_worker_state SET heartbeat_at=UTC_TIMESTAMP(6) WHERE version_id=? AND component=? AND group_id=? AND owner_epoch=?", s.VersionID, component, s.GroupID, s.Writer.Epoch)
					}
					continue
				}
				_, _ = s.DB.ExecContext(beatCtx, "UPDATE sync_worker_state SET heartbeat_at=UTC_TIMESTAMP(6) WHERE component=?", component)
			}
		}
	}()
	return func() { cancel(); <-done }
}
