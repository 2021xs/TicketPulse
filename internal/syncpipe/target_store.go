package syncpipe

import (
	"context"
	"fmt"
	"time"

	"ticketpulse/internal/projection"
)

func (s *StateStore) writerCheck(ctx context.Context) error {
	if s.VersionID == "" {
		return nil
	}
	if s.Writer == nil {
		return fmt.Errorf("%w: version progress writes require a target writer lease", projection.ErrWriterOwnership)
	}
	if s.Writer.Claim.Component != s.Consumer || s.Writer.Claim.Owner != s.GroupID || s.Writer.VersionID != s.VersionID {
		return fmt.Errorf("%w: progress namespace differs from registered writer", projection.ErrWriterOwnership)
	}
	return s.Writer.Check(ctx)
}
func (s *StateStore) targetHeartbeat(ctx context.Context, component, state string, eventAt *time.Time, problem error) error {
	if component != s.Consumer {
		return fmt.Errorf("target heartbeat component mismatch")
	}
	if err := s.writerCheck(ctx); err != nil {
		return err
	}
	var message any
	if problem != nil {
		message = safeText(problem.Error(), 1000)
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO sync_projection_worker_state(version_id,component,group_id,owner_epoch,state,heartbeat_at,last_event_at,last_error) VALUES(?,?,?,?,?,UTC_TIMESTAMP(6),?,?) ON DUPLICATE KEY UPDATE group_id=VALUES(group_id),owner_epoch=VALUES(owner_epoch),state=VALUES(state),heartbeat_at=VALUES(heartbeat_at),last_event_at=CASE WHEN last_event_at IS NULL THEN VALUES(last_event_at) WHEN VALUES(last_event_at)>last_event_at THEN VALUES(last_event_at) ELSE last_event_at END,last_error=VALUES(last_error)`, s.VersionID, component, s.GroupID, s.Writer.Epoch, state, eventAt, message)
	return err
}
func (s *StateStore) targetDelivery(ctx context.Context, ref RecordRef, event Event, status string, delta int, problem error, payload []byte) error {
	if err := s.writerCheck(ctx); err != nil {
		return err
	}
	var message, id, version, eventAt any
	if problem != nil {
		message = safeText(problem.Error(), 1000)
	}
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
	_, err := s.DB.ExecContext(ctx, `INSERT INTO sync_projection_deliveries(version_id,component,topic,partition_id,record_offset,event_id,ticket_id,ticket_version,status,attempts,first_seen_at,last_seen_at,event_at,last_error,isolated_payload) VALUES(?,?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),?,?,?) ON DUPLICATE KEY UPDATE status=VALUES(status),attempts=attempts+VALUES(attempts),last_seen_at=VALUES(last_seen_at),last_error=VALUES(last_error),isolated_payload=COALESCE(VALUES(isolated_payload),isolated_payload)`, s.VersionID, s.Consumer, ref.Topic, ref.Partition, ref.Offset, safeText(eventID, 255), id, version, status, delta, eventAt, message, payload)
	return err
}
func (s *StateStore) targetReplayPayload(ctx context.Context, ref RecordRef) ([]byte, error) {
	var payload []byte
	err := s.DB.QueryRowContext(ctx, `SELECT isolated_payload FROM sync_projection_deliveries WHERE version_id=? AND component=? AND topic=? AND partition_id=? AND record_offset=? AND status='isolated'`, s.VersionID, s.Consumer, ref.Topic, ref.Partition, ref.Offset).Scan(&payload)
	return payload, err
}
func (s *StateStore) targetRecordRecovery(ctx context.Context, original, recovery RecordRef) error {
	if original.Topic != recovery.Topic {
		return fmt.Errorf("recovery topic differs")
	}
	if err := s.writerCheck(ctx); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE sync_projection_deliveries SET recovery_partition=?,recovery_offset=?,last_seen_at=UTC_TIMESTAMP(6) WHERE version_id=? AND component=? AND topic=? AND partition_id=? AND record_offset=? AND status='isolated'`, recovery.Partition, recovery.Offset, s.VersionID, s.Consumer, original.Topic, original.Partition, original.Offset)
	if err != nil {
		return err
	}
	return s.targetFinishRecoveries(ctx, recovery)
}
func (s *StateStore) targetFinishRecoveries(ctx context.Context, ref RecordRef) error {
	if err := s.writerCheck(ctx); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE sync_projection_deliveries original JOIN sync_projection_deliveries replay ON replay.version_id=original.version_id AND replay.component=original.component AND replay.topic=original.topic AND replay.partition_id=original.recovery_partition AND replay.record_offset=original.recovery_offset SET original.status='recovered' WHERE original.version_id=? AND original.component=? AND original.status='isolated' AND replay.topic=? AND replay.partition_id=? AND replay.record_offset=? AND replay.status IN ('applied','duplicate_or_old')`, s.VersionID, s.Consumer, ref.Topic, ref.Partition, ref.Offset)
	return err
}
func (s *StateStore) targetWatermark(ctx context.Context, component string, ref RecordRef, event Event) error {
	if err := s.writerCheck(ctx); err != nil {
		return err
	}
	if component != s.Consumer {
		return fmt.Errorf("watermark component differs")
	}
	if event.Source != "binlog" {
		return nil
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO sync_projection_watermarks(version_id,component,partition_id,topic,barrier_id,issued_at,record_offset,binlog_file,binlog_pos,applied_at) VALUES(?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE barrier_id=IF(VALUES(record_offset)>record_offset,VALUES(barrier_id),barrier_id),issued_at=GREATEST(issued_at,VALUES(issued_at)),binlog_file=IF(VALUES(record_offset)>record_offset,VALUES(binlog_file),binlog_file),binlog_pos=IF(VALUES(record_offset)>record_offset,VALUES(binlog_pos),binlog_pos),record_offset=GREATEST(record_offset,VALUES(record_offset)),applied_at=VALUES(applied_at)`, s.VersionID, component, ref.Partition, ref.Topic, event.ID, event.EventAt, ref.Offset, event.Commit.File, event.Commit.Pos)
	return err
}
func (s *StateStore) GroupMembership(ctx context.Context, member string, generation int32) error {
	if s.VersionID == "" {
		return nil
	}
	if err := s.writerCheck(ctx); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE sync_projection_worker_state SET member_id=?,generation=? WHERE version_id=? AND component=? AND owner_epoch=?`, member, generation, s.VersionID, s.Consumer, s.Writer.Epoch)
	return err
}
