// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package events

import (
	"log/syslog"
	"testing"

	base "github.com/youtube/vitess/go/vt/events"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestReparentSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123/shard-123 [reparent cell-0000012345 -> cell-0000054321] status (123-456-789)"
	tc := &Reparent{
		ShardInfo: *topo.NewShardInfo("keyspace-123", "shard-123", nil, -1),
		OldMaster: topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "cell",
				Uid:  12345,
			},
		},
		NewMaster: topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "cell",
				Uid:  54321,
			},
		},
		ExternalID:    "123-456-789",
		StatusUpdater: base.StatusUpdater{Status: "status"},
	}
	gotSev, gotMsg := tc.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}
