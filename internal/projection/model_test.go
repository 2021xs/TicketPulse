package projection

import (
	"encoding/json"
	"testing"
	"time"
)

func exampleVersion(t *testing.T) Version {
	t.Helper()
	c := Contract{SchemaVersion: 1, Source: Source{ServerUUID: "mysql_uuid", Schema: "ticketpulse_test_x"}, Kafka: Kafka{Topic: "ticketpulse.ticketpulse_test_x.tickets.v1", TopicUUID: "topic_uuid", Partitions: []int32{0, 1, 2}}, ES: ES{Endpoint: "http://127.0.0.1:19205", Index: "test_tickets", UUID: "es_uuid"}, CK: CK{Endpoint: "http://127.0.0.1:18128", Database: "test_ck", EventsUUID: "events_uuid", StatesUUID: "states_uuid"}}
	c.ESWriter = Binding{"consumer_group", c.Kafka.Topic + ".es.v1", "__consumer_offsets", "es"}
	c.CKWriter = Binding{"consumer_group", c.Kafka.Topic + ".ck.v1", "__consumer_offsets", "ck"}
	report, _ := json.Marshal(map[string]any{"state": "consistent", "started_at": time.Now().UTC(), "completed_at": time.Now().UTC(), "source_after": map[string]any{}, "difference_count": 0, "es": map[string]string{"target": c.ES.Endpoint + "/" + c.ES.Index}, "ck": map[string]string{"target": c.CK.Endpoint + "/" + c.CK.Database}})
	revision := int64(1)
	v, err := NewCheckedVersion("existing_v1", Active, c, report, &revision)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestContractRejectsUnverifiedPromotionAndIncompleteOwnership(t *testing.T) {
	v := exampleVersion(t)
	for name, change := range map[string]func(*Version){
		"promotion":      func(v *Version) { v.Contract.HandoffVerified = true },
		"phase":          func(v *Version) { v.Phase = "PROMOTED" },
		"missing_freeze": func(v *Version) { v.FreezeRevision = nil },
		"missing_report": func(v *Version) { v.ValidationReport = nil },
		"wrong_status":   func(v *Version) { v.Contract.CKWriter.StatusComponent = "es" },
		"wrong_owner":    func(v *Version) { v.Contract.ESWriter.Owner = "another_group" },
		"partitions":     func(v *Version) { v.Contract.Kafka.Partitions = []int32{0, 2} },
		"credentials":    func(v *Version) { v.Contract.ES.Endpoint = "http://secret@localhost:19205" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := v
			change(&copy)
			if copy.Validate() == nil {
				t.Fatal("invalid contract accepted")
			}
		})
	}
}

func TestCanonicalJSONHashSurvivesMySQLRepresentation(t *testing.T) {
	a, err := HashJSON([]byte(`{"large":9007199254740993,"a":{"z":2,"x":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := HashJSON([]byte(`{ "a": {"x": 1, "z": 2}, "large": 9007199254740993 }`))
	if err != nil || a != b {
		t.Fatalf("canonical hash changed a=%s b=%s err=%v", a, b, err)
	}
}
