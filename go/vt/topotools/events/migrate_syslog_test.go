// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package events

import (
	"log/syslog"
	"testing"

	base "github.com/youtube/vitess/go/vt/events"
	"github.com/youtube/vitess/go/vt/topo"
)

func TestMigrateServedFromSyslogForward(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-1 [migrate served-from keyspace-2/source-shard -> keyspace-1/dest-shard] status"
	ev := &MigrateServedFrom{
		Keyspace:         *topo.NewKeyspaceInfo("keyspace-1", nil),
		SourceShard:      *topo.NewShardInfo("keyspace-2", "source-shard", nil),
		DestinationShard: *topo.NewShardInfo("keyspace-1", "dest-shard", nil),
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
		Keyspace:         *topo.NewKeyspaceInfo("keyspace-1", nil),
		SourceShard:      *topo.NewShardInfo("keyspace-2", "source-shard", nil),
		DestinationShard: *topo.NewShardInfo("keyspace-1", "dest-shard", nil),
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
		Keyspace: *topo.NewKeyspaceInfo("keyspace-1", nil),
		SourceShards: []*topo.ShardInfo{
			topo.NewShardInfo("keyspace-1", "src1", nil),
			topo.NewShardInfo("keyspace-1", "src2", nil),
		},
		DestinationShards: []*topo.ShardInfo{
			topo.NewShardInfo("keyspace-1", "dst1", nil),
			topo.NewShardInfo("keyspace-1", "dst2", nil),
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
		Keyspace: *topo.NewKeyspaceInfo("keyspace-1", nil),
		SourceShards: []*topo.ShardInfo{
			topo.NewShardInfo("keyspace-1", "src1", nil),
			topo.NewShardInfo("keyspace-1", "src2", nil),
		},
		DestinationShards: []*topo.ShardInfo{
			topo.NewShardInfo("keyspace-1", "dst1", nil),
			topo.NewShardInfo("keyspace-1", "dst2", nil),
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
