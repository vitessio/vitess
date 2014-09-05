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

func TestMultiSnapshotSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123/shard-123/cell-0000012345 [multisnapshot] status"
	ev := &MultiSnapshot{
		Tablet: topo.Tablet{
			Keyspace: "keyspace-123",
			Shard:    "shard-123",
			Alias:    topo.TabletAlias{Cell: "cell", Uid: 12345},
		},
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

func TestMultiRestoreSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123/shard-123/cell-0000012345 [multirestore] status"
	ev := &MultiRestore{
		Tablet: topo.Tablet{
			Keyspace: "keyspace-123",
			Shard:    "shard-123",
			Alias:    topo.TabletAlias{Cell: "cell", Uid: 12345},
		},
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
