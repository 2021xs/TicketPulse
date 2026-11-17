package syncpipe

import (
	"context"
	"database/sql"
	"fmt"
)

// A recreated Kafka topic invalidates offsets. Never silently resume a CDC
// checkpoint against a fresh empty log of the same name.
func (s *StateStore) VerifyQueue(ctx context.Context, q *Queue) error {
	topics, err := q.Admin.ListTopics(ctx, q.Topic)
	if err != nil {
		return err
	}
	detail, ok := topics[q.Topic]
	if !ok || detail.Err != nil {
		return fmt.Errorf("Kafka topic metadata unavailable")
	}
	id := detail.ID.String()
	var topic, storedID string
	var partitions int32
	err = s.DB.QueryRowContext(ctx, "SELECT topic,topic_id,partition_count FROM sync_kafka_pipeline WHERE pipeline_name='tickets-v1'").Scan(&topic, &storedID, &partitions)
	if err == sql.ErrNoRows {
		_, err = s.DB.ExecContext(ctx, "INSERT IGNORE INTO sync_kafka_pipeline(pipeline_name,topic,topic_id,partition_count,created_at) VALUES('tickets-v1',?,?,?,UTC_TIMESTAMP(6))", q.Topic, id, q.Partitions)
		if err != nil {
			return err
		}
		err = s.DB.QueryRowContext(ctx, "SELECT topic,topic_id,partition_count FROM sync_kafka_pipeline WHERE pipeline_name='tickets-v1'").Scan(&topic, &storedID, &partitions)
	}
	if err != nil {
		return err
	}
	if topic != q.Topic || storedID != id || partitions != q.Partitions {
		return fmt.Errorf("Kafka topic identity/config differs from saved pipeline; explicit migration or re-bootstrap required")
	}
	return nil
}
