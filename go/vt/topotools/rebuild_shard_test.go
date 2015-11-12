// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools_test

import (
	"fmt"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"

	. "github.com/youtube/vitess/go/vt/topotools"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	testShard    = "0"
	testKeyspace = "test_keyspace"
)

func addTablet(ctx context.Context, t *testing.T, ts topo.Server, uid int, cell string, tabletType topodatapb.TabletType) *topo.TabletInfo {
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: cell, Uid: uint32(uid)},
		Hostname: fmt.Sprintf("%vbsr%v", cell, uid),
		Ip:       fmt.Sprintf("212.244.218.%v", uid),
		PortMap: map[string]int32{
			"vt":    3333 + 10*int32(uid),
			"mysql": 3334 + 10*int32(uid),
		},
		Keyspace: testKeyspace,
		Type:     tabletType,
		Shard:    testShard,
	}
	if err := ts.CreateTablet(ctx, tablet); err != nil {
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
	if err := ts.UpdateShard(ctx, si); err != nil {
		t.Fatalf("UpdateShard: %v", err)
	}

	masterInfo := addTablet(ctx, t, ts, 1, cells[0], topodatapb.TabletType_MASTER)
	replicaInfo := addTablet(ctx, t, ts, 2, cells[0], topodatapb.TabletType_REPLICA)

	// Do an initial rebuild.
	if _, err := RebuildShard(ctx, logger, ts, testKeyspace, testShard, cells); err != nil {
		t.Fatalf("RebuildShard: %v", err)
	}

	// Check initial state.
	ep, _, err := ts.GetEndPoints(ctx, cells[0], testKeyspace, testShard, topodatapb.TabletType_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints: %v", err)
	}
	if got, want := len(ep.Entries), 1; got != want {
		t.Fatalf("len(Entries) = %v, want %v", got, want)
	}
	ep, _, err = ts.GetEndPoints(ctx, cells[0], testKeyspace, testShard, topodatapb.TabletType_REPLICA)
	if err != nil {
		t.Fatalf("GetEndPoints: %v", err)
	}
	if got, want := len(ep.Entries), 1; got != want {
		t.Fatalf("len(Entries) = %v, want %v", got, want)
	}

	// Make a change.
	masterInfo.Type = topodatapb.TabletType_SPARE
	if err := ts.UpdateTablet(ctx, masterInfo); err != nil {
		t.Fatalf("UpdateTablet: %v", err)
	}
	if _, err := RebuildShard(ctx, logger, ts, testKeyspace, testShard, cells); err != nil {
		t.Fatalf("RebuildShard: %v", err)
	}

	// Make another change.
	replicaInfo.Type = topodatapb.TabletType_SPARE
	if err := ts.UpdateTablet(ctx, replicaInfo); err != nil {
		t.Fatalf("UpdateTablet: %v", err)
	}
	if _, err := RebuildShard(ctx, logger, ts, testKeyspace, testShard, cells); err != nil {
		t.Fatalf("RebuildShard: %v", err)
	}

	// Check that the rebuild picked up both changes.
	if _, _, err := ts.GetEndPoints(ctx, cells[0], testKeyspace, testShard, topodatapb.TabletType_MASTER); err == nil || !strings.Contains(err.Error(), "node doesn't exist") {
		t.Errorf("first change wasn't picked up by second rebuild")
	}
	if _, _, err := ts.GetEndPoints(ctx, cells[0], testKeyspace, testShard, topodatapb.TabletType_REPLICA); err == nil || !strings.Contains(err.Error(), "node doesn't exist") {
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
	if err := ts.UpdateShard(ctx, si); err != nil {
		t.Fatalf("UpdateShard: %v", err)
	}

	tablet1 := addTablet(ctx, t, ts, 1, cell, topodatapb.TabletType_MASTER).Tablet
	tablet2 := addTablet(ctx, t, ts, 2, cell, topodatapb.TabletType_REPLICA).Tablet

	update := func(tablet *topodatapb.Tablet) {
		if err := UpdateTabletEndpoints(ctx, ts, tablet); err != nil {
			t.Fatalf("UpdateTabletEndpoints(%v): %v", tablet, err)
		}
	}
	expect := func(tabletType topodatapb.TabletType, want int) {
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
	expect(topodatapb.TabletType_MASTER, 1)
	update(tablet2)
	expect(topodatapb.TabletType_REPLICA, 1)

	// Re-update an identical tablet.
	update(tablet1)
	expect(topodatapb.TabletType_MASTER, 1)

	// Change a tablet, but keep it the same type.
	tablet2.Hostname += "extra"
	update(tablet2)
	expect(topodatapb.TabletType_REPLICA, 1)

	// Move the master to replica.
	tablet1.Type = topodatapb.TabletType_REPLICA
	update(tablet1)
	expect(topodatapb.TabletType_MASTER, 0)
	expect(topodatapb.TabletType_REPLICA, 2)

	// Take a replica out of serving.
	tablet1.Type = topodatapb.TabletType_SPARE
	update(tablet1)
	expect(topodatapb.TabletType_MASTER, 0)
	expect(topodatapb.TabletType_REPLICA, 1)

	// Put it back to serving.
	tablet1.Type = topodatapb.TabletType_REPLICA
	update(tablet1)
	expect(topodatapb.TabletType_MASTER, 0)
	expect(topodatapb.TabletType_REPLICA, 2)

	// Move a replica to master.
	tablet2.Type = topodatapb.TabletType_MASTER
	update(tablet2)
	expect(topodatapb.TabletType_MASTER, 1)
	expect(topodatapb.TabletType_REPLICA, 1)
}
