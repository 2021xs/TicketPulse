package syncpipe

import (
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaReplicationAndBootstrapContract(t *testing.T) {
	t.Setenv("TICKETPULSE_KAFKA_REPLICATION_FACTOR", "3")
	t.Setenv("TICKETPULSE_KAFKA_MIN_INSYNC_REPLICAS", "2")
	q, err := ConnectQueue("127.0.0.1:19094, 127.0.0.1:19096,127.0.0.1:19098", "isolated")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if q.ReplicationFactor != 3 || q.MinInSyncReplicas != 2 {
		t.Fatal(q.ReplicationFactor, q.MinInSyncReplicas)
	}
	if !reflect.DeepEqual(q.Client.OptValue(kgo.SeedBrokers), []string{"127.0.0.1:19094", "127.0.0.1:19096", "127.0.0.1:19098"}) {
		t.Fatal("bootstrap list not propagated")
	}
	if q.Client.OptValue(kgo.DisableIdempotentWrite) != false || q.Client.OptValue(kgo.RequiredAcks) != kgo.AllISRAcks() {
		t.Fatal("durable producer options changed")
	}
	if q.Client.OptValue(kgo.RecordRetries) != int64(5) || q.Client.OptValue(kgo.RecordDeliveryTimeout) != 10*time.Second {
		t.Fatal("producer retry budget changed")
	}
	consumer, err := q.ConsumerClient("ck")
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if consumer.OptValue(kgo.DisableAutoCommit) != true || consumer.OptValue(kgo.BlockRebalanceOnPoll) != true || consumer.OptValue(kgo.ConsumerGroup) != q.Group("ck") {
		t.Fatal("independent manual commit contract changed")
	}
	if q.Group("ck") == q.Group("es") {
		t.Fatal("projection groups are shared")
	}
}

func TestKafkaReplicationRejectsUnsafeOrInvalidConfiguration(t *testing.T) {
	for _, r := range []TopicReplication{{0, 1}, {3, 0}, {3, 4}, {65, 2}} {
		if r.Validate() == nil {
			t.Fatal("accepted", r)
		}
	}
	for _, tc := range [][2]string{{"garbage", "2"}, {"3", "4"}, {"1", "2"}, {"32768", "2"}} {
		t.Setenv("TICKETPULSE_KAFKA_REPLICATION_FACTOR", tc[0])
		t.Setenv("TICKETPULSE_KAFKA_MIN_INSYNC_REPLICAS", tc[1])
		if q, err := ConnectQueue("127.0.0.1:19094", "test"); err == nil {
			q.Close()
			t.Fatal("accepted", tc)
		}
	}
	t.Setenv("TICKETPULSE_KAFKA_REPLICATION_FACTOR", "")
	t.Setenv("TICKETPULSE_KAFKA_MIN_INSYNC_REPLICAS", "")
	q, err := ConnectQueue("127.0.0.1:19092", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if q.ReplicationFactor != 1 || q.MinInSyncReplicas != 1 {
		t.Fatal("demo defaults changed")
	}
}
