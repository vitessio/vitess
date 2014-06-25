package events

import (
	"testing"
)

func TestUpdateStatus(t *testing.T) {
	r := &Reparent{
		Status: "status1",
	}

	r.UpdateStatus("status2")

	if r.Status != "status2" {
		t.Errorf("got %v, want status2", r.Status)
	}
}
