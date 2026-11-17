package ticket

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func testPolicy() Routing {
	return Routing{Categories: []string{CategoryNetwork, CategoryAccount, CategoryDevice, CategorySoftware, CategoryOther},
		Priorities: []string{PriorityUrgent, PriorityHigh, PriorityMedium, PriorityLow}, CategoryTeams: map[string]int64{CategoryNetwork: 10}, DefaultTeamID: 90}
}

func TestCreateValidation(t *testing.T) {
	valid := CreateRequest{RequestID: "REQ-001", Title: "办公网络无法连接", Description: "无线网络断开，请检查交换机。", Category: CategoryNetwork, Priority: PriorityUrgent}
	tests := []struct {
		name   string
		change func(*CreateRequest)
	}{
		{"missing_request", func(r *CreateRequest) { r.RequestID = "" }},
		{"overlong_request", func(r *CreateRequest) { r.RequestID = strings.Repeat("r", 65) }},
		{"invalid_request_character", func(r *CreateRequest) { r.RequestID = "bad key" }},
		{"missing_title", func(r *CreateRequest) { r.Title = "" }},
		{"short_title", func(r *CreateRequest) { r.Title = "网络故障" }},
		{"blank_title", func(r *CreateRequest) { r.Title = "     " }},
		{"overlong_unicode_title", func(r *CreateRequest) { r.Title = strings.Repeat("网", 121) }},
		{"missing_description", func(r *CreateRequest) { r.Description = "" }},
		{"blank_description", func(r *CreateRequest) { r.Description = " \n\t" }},
		{"overlong_description", func(r *CreateRequest) { r.Description = strings.Repeat("故", 5001) }},
		{"invalid_category", func(r *CreateRequest) { r.Category = "finance" }},
		{"invalid_priority", func(r *CreateRequest) { r.Priority = "P0" }},
	}
	if err := valid.Validate(testPolicy()); err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := valid
			test.change(&r)
			var business *Error
			if err := r.Validate(testPolicy()); !errors.As(err, &business) || business.Code != "INVALID_ARGUMENT" {
				t.Fatalf("validation = %v", err)
			}
		})
	}
	valid.Title = strings.Repeat("网", 120)
	valid.Description = strings.Repeat("故", 5000)
	if err := valid.Validate(testPolicy()); err != nil {
		t.Fatalf("maximum Unicode length rejected: %v", err)
	}
}

func TestFingerprintContentContract(t *testing.T) {
	var a, b CreateRequest
	if err := json.Unmarshal([]byte(`{"request_id":"one","title":"office network down","description":"need help","category":"network","priority":"urgent"}`), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{ "priority":"urgent", "description":"need help", "request_id":"two", "category":"network", "title":"office network down" }`), &b); err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("JSON order/format or request id changes business fingerprint")
	}
	changes := []func(*CreateRequest){func(r *CreateRequest) { r.Title += " " }, func(r *CreateRequest) { r.Description += " " }, func(r *CreateRequest) { r.Category = CategoryDevice }, func(r *CreateRequest) { r.Priority = PriorityLow }}
	for _, change := range changes {
		b = a
		change(&b)
		if a.Fingerprint() == b.Fingerprint() {
			t.Fatal("different business content has same fingerprint")
		}
	}
}
