package syncpipe

import "testing"

func TestKafkaConfigurationRejectsInvalidTopicAndPartitionCount(t *testing.T) {
	for _, n := range []int32{0, -1, 65} {
		if q, err := NewQueue([]string{"127.0.0.1:19092"}, "ticketpulse.test", n); err == nil {
			q.Close()
			t.Errorf("accepted partitions=%d", n)
		}
	}
	for _, name := range []string{"", "invalid topic", "x;DROP"} {
		if q, err := NewQueue([]string{"127.0.0.1:19092"}, name, 3); err == nil {
			q.Close()
			t.Error("accepted", name)
		}
	}
	if q, err := NewQueue([]string{"nats://127.0.0.1:14222"}, "ticketpulse.test", 3); err == nil {
		q.Close()
		t.Fatal("legacy MQ endpoint accepted")
	}
}
