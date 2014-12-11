package events

import (
	"log/syslog"
	"testing"

	"github.com/henryanand/vitess/go/vt/topo"
)

func TestShardChangeSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123/shard-123 [shard] status"
	sc := &ShardChange{
		ShardInfo: *topo.NewShardInfo("keyspace-123", "shard-123", nil, -1),
		Status:    "status",
	}
	gotSev, gotMsg := sc.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}
