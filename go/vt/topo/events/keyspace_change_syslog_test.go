package events

import (
	"log/syslog"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
)

func TestKeyspaceChangeSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123 [keyspace] status"
	kc := &KeyspaceChange{
		KeyspaceInfo: *topo.NewKeyspaceInfo("keyspace-123", nil, -1),
		Status:       "status",
	}
	gotSev, gotMsg := kc.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}
