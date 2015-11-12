package events

import (
	"log/syslog"
	"testing"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestKeyspaceChangeSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123 [keyspace] status value: sharding_column_name:\"sharded_by_me\" "
	kc := &KeyspaceChange{
		KeyspaceName: "keyspace-123",
		Keyspace: &topodatapb.Keyspace{
			ShardingColumnName: "sharded_by_me",
		},
		Status: "status",
	}
	gotSev, gotMsg := kc.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}
