package syncpipe

import (
	"fmt"
	"os"
	"strconv"
)

// TopicReplication is an explicit deployment choice. Changing it never
// reassigns existing replicas or lowers the durability of a running topic.
// The single-broker demonstration keeps its original 1/1 defaults.
type TopicReplication struct {
	Factor    int16
	MinInSync int
}

func (r TopicReplication) Validate() error {
	if r.Factor < 1 || r.Factor > 64 || r.MinInSync < 1 || r.MinInSync > int(r.Factor) {
		return fmt.Errorf("Kafka replication factor must be 1–64 and min.insync.replicas must be 1–factor")
	}
	return nil
}

func configuredReplication() (TopicReplication, error) {
	r := TopicReplication{Factor: 1, MinInSync: 1}
	if value := os.Getenv("TICKETPULSE_KAFKA_REPLICATION_FACTOR"); value != "" {
		n, err := strconv.ParseInt(value, 10, 16)
		if err != nil {
			return r, fmt.Errorf("invalid TICKETPULSE_KAFKA_REPLICATION_FACTOR")
		}
		r.Factor = int16(n)
	}
	if value := os.Getenv("TICKETPULSE_KAFKA_MIN_INSYNC_REPLICAS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil {
			return r, fmt.Errorf("invalid TICKETPULSE_KAFKA_MIN_INSYNC_REPLICAS")
		}
		r.MinInSync = n
	}
	return r, r.Validate()
}
