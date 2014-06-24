package events

import (
	"testing"
)

func TestUpdateStatus(t *testing.T) {
	r := &Reparent{
		Keyspace: "keyspace1",
		Status:   "status1",
	}

	r.UpdateStatus("status2")

	if r.Keyspace != "keyspace1" {
		t.Errorf("got %v, want keyspace1", r.Keyspace)
	}
	if r.Status != "status2" {
		t.Errorf("got %v, want status2", r.Status)
	}
}
