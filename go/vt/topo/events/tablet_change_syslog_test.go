package events

import (
	"log/syslog"
	"testing"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestTabletChangeSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123/shard-123/cell-0000012345 [tablet] status"
	tc := &TabletChange{
		Tablet: topodatapb.Tablet{
			Keyspace: "keyspace-123",
			Shard:    "shard-123",
			Alias: &topodatapb.TabletAlias{
				Cell: "cell",
				Uid:  12345,
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
