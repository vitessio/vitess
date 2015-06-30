// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"

	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	. "github.com/youtube/vitess/go/vt/topotools"
)

const (
	testShard    = "0"
	testKeyspace = "test_keyspace"
)

func addTablet(ctx context.Context, t *testing.T, ts topo.Server, uid int, cell string, tabletType topo.TabletType) *topo.TabletInfo {
	tablet := &topo.Tablet{
		Alias:    topo.TabletAlias{Cell: cell, Uid: uint32(uid)},
		Hostname: fmt.Sprintf("%vbsr%v", cell, uid),
		IPAddr:   fmt.Sprintf("212.244.218.%v", uid),
		Portmap: map[string]int{
			"vt":    3333 + 10*uid,
			"mysql": 3334 + 10*uid,
		},
		Keyspace: testKeyspace,
		Type:     tabletType,
		Shard:    testShard,
	}
	if err := topo.CreateTablet(ctx, ts, tablet); err != nil {
		t.Fatalf("CreateTablet: %v", err)
	}

	ti, err := ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet: %v", err)
	}
	return ti
}

func TestRebuildShard(t *testing.T) {
	ctx := context.Background()
	cells := []string{"test_cell"}
	logger := logutil.NewMemoryLogger()

	// Set up topology.
	ts := zktopo.NewTestServer(t, cells)
	si, err := GetOrCreateShard(ctx, ts, testKeyspace, testShard)
	if err != nil {
		t.Fatalf("GetOrCreateShard: %v", err)
	}
	si.Cells = append(si.Cells, cells[0])
	if err := topo.UpdateShard(ctx, ts, si); err != nil {
		t.Fatalf("UpdateShard: %v", err)
	}

	masterInfo := addTablet(ctx, t, ts, 1, cells[0], topo.TYPE_MASTER)
	replicaInfo := addTablet(ctx, t, ts, 2, cells[0], topo.TYPE_REPLICA)

	// Do an initial rebuild.
	if _, err := RebuildShard(ctx, logger, ts, testKeyspace, testShard, cells, time.Minute); err != nil {
		t.Fatalf("RebuildShard: %v", err)
	}

	// Check initial state.
	ep, _, err := ts.GetEndPoints(ctx, cells[0], testKeyspace, testShard, topo.TYPE_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints: %v", err)
	}
	if got, want := len(ep.Entries), 1; got != want {
		t.Fatalf("len(Entries) = %v, want %v", got, want)
	}
	ep, _, err = ts.GetEndPoints(ctx, cells[0], testKeyspace, testShard, topo.TYPE_REPLICA)
	if err != nil {
		t.Fatalf("GetEndPoints: %v", err)
	}
	if got, want := len(ep.Entries), 1; got != want {
		t.Fatalf("len(Entries) = %v, want %v", got, want)
	}

	// Make a change.
	masterInfo.Type = topo.TYPE_SPARE
	if err := topo.UpdateTablet(ctx, ts, masterInfo); err != nil {
		t.Fatalf("UpdateTablet: %v", err)
	}
	if _, err := RebuildShard(ctx, logger, ts, testKeyspace, testShard, cells, time.Minute); err != nil {
		t.Fatalf("RebuildShard: %v", err)
	}

	// Make another change.
	replicaInfo.Type = topo.TYPE_SPARE
	if err := topo.UpdateTablet(ctx, ts, replicaInfo); err != nil {
		t.Fatalf("UpdateTablet: %v", err)
	}
	if _, err := RebuildShard(ctx, logger, ts, testKeyspace, testShard, cells, time.Minute); err != nil {
		t.Fatalf("RebuildShard: %v", err)
	}

	// Check that the rebuild picked up both changes.
	if _, _, err := ts.GetEndPoints(ctx, cells[0], testKeyspace, testShard, topo.TYPE_MASTER); err == nil || !strings.Contains(err.Error(), "node doesn't exist") {
		t.Errorf("first change wasn't picked up by second rebuild")
	}
	if _, _, err := ts.GetEndPoints(ctx, cells[0], testKeyspace, testShard, topo.TYPE_REPLICA); err == nil || !strings.Contains(err.Error(), "node doesn't exist") {
		t.Errorf("second change was overwritten by first rebuild finishing late")
	}
}

func TestUpdateTabletEndpoints(t *testing.T) {
	ctx := context.Background()
	cell := "test_cell"

	// Set up topology.
	ts := zktopo.NewTestServer(t, []string{cell})
	si, err := GetOrCreateShard(ctx, ts, testKeyspace, testShard)
	if err != nil {
		t.Fatalf("GetOrCreateShard: %v", err)
	}
	si.Cells = append(si.Cells, cell)
	if err := topo.UpdateShard(ctx, ts, si); err != nil {
		t.Fatalf("UpdateShard: %v", err)
	}

	tablet1 := addTablet(ctx, t, ts, 1, cell, topo.TYPE_MASTER).Tablet
	tablet2 := addTablet(ctx, t, ts, 2, cell, topo.TYPE_REPLICA).Tablet

	update := func(tablet *topo.Tablet) {
		if err := UpdateTabletEndpoints(ctx, ts, tablet); err != nil {
			t.Fatalf("UpdateTabletEndpoints(%v): %v", tablet, err)
		}
	}
	expect := func(tabletType topo.TabletType, want int) {
		eps, _, err := ts.GetEndPoints(ctx, cell, testKeyspace, testShard, tabletType)
		if err != nil && err != topo.ErrNoNode {
			t.Errorf("GetEndPoints(%v): %v", tabletType, err)
			return
		}
		var got int
		if err == nil {
			got = len(eps.Entries)
			if got == 0 {
				t.Errorf("len(EndPoints) = 0, expected ErrNoNode instead")
			}
		}
		if got != want {
			t.Errorf("len(GetEndPoints(%v)) = %v, want %v. EndPoints = %v", tabletType, len(eps.Entries), want, eps)
		}
	}

	// Update tablets. This should create the serving graph dirs too.
	update(tablet1)
	expect(topo.TYPE_MASTER, 1)
	update(tablet2)
	expect(topo.TYPE_REPLICA, 1)

	// Re-update an identical tablet.
	update(tablet1)
	expect(topo.TYPE_MASTER, 1)

	// Change a tablet, but keep it the same type.
	tablet2.Hostname += "extra"
	update(tablet2)
	expect(topo.TYPE_REPLICA, 1)

	// Move the master to replica.
	tablet1.Type = topo.TYPE_REPLICA
	update(tablet1)
	expect(topo.TYPE_MASTER, 0)
	expect(topo.TYPE_REPLICA, 2)

	// Take a replica out of serving.
	tablet1.Type = topo.TYPE_SPARE
	update(tablet1)
	expect(topo.TYPE_MASTER, 0)
	expect(topo.TYPE_REPLICA, 1)

	// Put it back to serving.
	tablet1.Type = topo.TYPE_REPLICA
	update(tablet1)
	expect(topo.TYPE_MASTER, 0)
	expect(topo.TYPE_REPLICA, 2)

	// Move a replica to master.
	tablet2.Type = topo.TYPE_MASTER
	update(tablet2)
	expect(topo.TYPE_MASTER, 1)
	expect(topo.TYPE_REPLICA, 1)
}
