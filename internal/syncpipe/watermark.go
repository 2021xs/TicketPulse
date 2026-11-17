package syncpipe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// A source marker is committed through the same MySQL log as tickets, never sent from HTTP.
func (s *StateStore) IssueWatermark(ctx context.Context) error {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO sync_source_watermark(source_name,barrier_id,issued_at) VALUES('tickets-v1',?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE barrier_id=VALUES(barrier_id),issued_at=VALUES(issued_at)`, hex.EncodeToString(random))
	return err
}
func (s *StateStore) ApplyWatermark(ctx context.Context, component string, ref RecordRef, e Event) error {
	if s.VersionID != "" {
		return s.targetWatermark(ctx, component, ref, e)
	}
	if e.Source != "binlog" {
		return nil
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO sync_kafka_watermarks(component,partition_id,topic,barrier_id,issued_at,record_offset,binlog_file,binlog_pos,applied_at) VALUES(?,?,?,?,?,?,?,?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE barrier_id=IF(VALUES(record_offset)>record_offset,VALUES(barrier_id),barrier_id),issued_at=GREATEST(issued_at,VALUES(issued_at)),binlog_file=IF(VALUES(record_offset)>record_offset,VALUES(binlog_file),binlog_file),binlog_pos=IF(VALUES(record_offset)>record_offset,VALUES(binlog_pos),binlog_pos),record_offset=GREATEST(record_offset,VALUES(record_offset)),applied_at=VALUES(applied_at)`, component, ref.Partition, ref.Topic, e.ID, e.EventAt, ref.Offset, e.Commit.File, e.Commit.Pos)
	return err
}

func firstResolved(value any) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	at, err := rowTime(value)
	if err != nil {
		return nil, err
	}
	return &at, nil
}
