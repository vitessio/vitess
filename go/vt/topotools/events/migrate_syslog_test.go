package events

import (
	"log/syslog"
	"testing"

	base "vitess.io/vitess/go/vt/events"
	"vitess.io/vitess/go/vt/topo"
)

func TestMigrateServedFromSyslogForward(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-1 [migrate served-from keyspace-2/source-shard -> keyspace-1/dest-shard] status"
	ev := &MigrateServedFrom{
		KeyspaceName:     "keyspace-1",
		SourceShard:      *topo.NewShardInfo("keyspace-2", "source-shard", nil, nil),
		DestinationShard: *topo.NewShardInfo("keyspace-1", "dest-shard", nil, nil),
		Reverse:          false,
		StatusUpdater:    base.StatusUpdater{Status: "status"},
	}
	gotSev, gotMsg := ev.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}

func TestMigrateServedFromSyslogReverse(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-1 [migrate served-from keyspace-2/source-shard <- keyspace-1/dest-shard] status"
	ev := &MigrateServedFrom{
		KeyspaceName:     "keyspace-1",
		SourceShard:      *topo.NewShardInfo("keyspace-2", "source-shard", nil, nil),
		DestinationShard: *topo.NewShardInfo("keyspace-1", "dest-shard", nil, nil),
		Reverse:          true,
		StatusUpdater:    base.StatusUpdater{Status: "status"},
	}
	gotSev, gotMsg := ev.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}

func TestMigrateServedTypesSyslogForward(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-1 [migrate served-types {src1, src2} -> {dst1, dst2}] status"
	ev := &MigrateServedTypes{
		KeyspaceName: "keyspace-1",
		SourceShards: []*topo.ShardInfo{
			topo.NewShardInfo("keyspace-1", "src1", nil, nil),
			topo.NewShardInfo("keyspace-1", "src2", nil, nil),
		},
		DestinationShards: []*topo.ShardInfo{
			topo.NewShardInfo("keyspace-1", "dst1", nil, nil),
			topo.NewShardInfo("keyspace-1", "dst2", nil, nil),
		},
		Reverse:       false,
		StatusUpdater: base.StatusUpdater{Status: "status"},
	}
	gotSev, gotMsg := ev.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}

func TestMigrateServedTypesSyslogForwardWithNil(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-1 [migrate served-types {src1, } -> {, dst2}] status"
	ev := &MigrateServedTypes{
		KeyspaceName: "keyspace-1",
		SourceShards: []*topo.ShardInfo{
			topo.NewShardInfo("keyspace-1", "src1", nil, nil),
			nil,
		},
		DestinationShards: []*topo.ShardInfo{
			nil,
			topo.NewShardInfo("keyspace-1", "dst2", nil, nil),
		},
		Reverse:       false,
		StatusUpdater: base.StatusUpdater{Status: "status"},
	}
	gotSev, gotMsg := ev.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}

func TestMigrateServedTypesSyslogReverse(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-1 [migrate served-types {src1, src2} <- {dst1, dst2}] status"
	ev := &MigrateServedTypes{
		KeyspaceName: "keyspace-1",
		SourceShards: []*topo.ShardInfo{
			topo.NewShardInfo("keyspace-1", "src1", nil, nil),
			topo.NewShardInfo("keyspace-1", "src2", nil, nil),
		},
		DestinationShards: []*topo.ShardInfo{
			topo.NewShardInfo("keyspace-1", "dst1", nil, nil),
			topo.NewShardInfo("keyspace-1", "dst2", nil, nil),
		},
		Reverse:       true,
		StatusUpdater: base.StatusUpdater{Status: "status"},
	}
	gotSev, gotMsg := ev.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}
