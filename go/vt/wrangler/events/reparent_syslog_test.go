package events

import (
	"log/syslog"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
)

func TestReparentSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123/shard-123 [reparent cell-0000012345 -> cell-0000054321] status"
	tc := &Reparent{
		ShardInfo: *topo.NewShardInfo("keyspace-123", "shard-123", nil),
		OldMaster: topo.Tablet{
			Alias: topo.TabletAlias{
				Cell: "cell",
				Uid:  12345,
			},
		},
		NewMaster: topo.Tablet{
			Alias: topo.TabletAlias{
				Cell: "cell",
				Uid:  54321,
			},
		},
		Status: "status",
	}
	gotSev, gotMsg := tc.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}
