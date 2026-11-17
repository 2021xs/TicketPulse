package search

import (
	"encoding/json"
	"errors"
	"testing"
	"ticketpulse/internal/ticket"
	"time"
)

func TestReadRevisionCursorCannotReviveOnSameContract(t *testing.T) {
	es, err := NewElasticsearch("http://127.0.0.1:19208", "read_revision_tickets")
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	a1, _ := NewService(nil, es, nil, key, WithTargetVersion("A:immutable_hash:read_revision=1", nil))
	b2, _ := NewService(nil, es, nil, key, WithTargetVersion("B:immutable_hash_b:read_revision=2", nil))
	a3, _ := NewService(nil, es, nil, key, WithTargetVersion("A:immutable_hash:read_revision=3", nil))
	user := ticket.User{ID: 5, OrganizationID: 1}
	request := Request{Sort: "id", Order: "asc", PageSize: 2}
	c := cursor{UserID: user.ID, OrgID: user.OrganizationID, Query: request.Fingerprint(), Expires: time.Now().Add(time.Hour).Unix(), After: []json.RawMessage{json.RawMessage("2")}, Target: a1.target}
	token, err := a1.encodeCursor(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []*Service{b2, a3} {
		if _, err = target.decodeCursor(token, user, request); !errors.Is(err, ErrCursorTargetChanged) {
			t.Fatalf("revision/target change allowed cursor: %v", err)
		}
	}
	if _, err = a1.decodeCursor(token, user, request); err != nil {
		t.Fatal("same revision failed", err)
	}
	c.Expires = time.Now().Add(-time.Minute).Unix()
	expired, _ := a1.encodeCursor(c)
	if _, err = a3.decodeCursor(expired, user, request); err == nil || errors.Is(err, ErrCursorTargetChanged) {
		t.Fatal("expiry must keep original invalid-cursor error", err)
	}
	wrongKey := make([]byte, 32)
	wrongKey[0] = 1
	wrong, _ := NewService(nil, es, nil, wrongKey, WithTargetVersion(b2.target, nil))
	if _, err = wrong.decodeCursor(token, user, request); err == nil || errors.Is(err, ErrCursorTargetChanged) {
		t.Fatal("cipher failure must retain original invalid-cursor error", err)
	}
}
