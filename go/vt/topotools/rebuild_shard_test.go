// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools_test

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

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
	ts := zktestserver.New(t, cells)
	si, err := GetOrCreateShard(ctx, ts, testKeyspace, testShard)
	if err != nil {
		t.Fatalf("GetOrCreateShard: %v", err)
	}
	si.Cells = append(si.Cells, cells[0])
	si.MasterAlias = &topodatapb.TabletAlias{Cell: cells[0], Uid: 1}
	if err := ts.UpdateShard(ctx, si); err != nil {
		t.Fatalf("UpdateShard: %v", err)
	}

	addTablet(ctx, t, ts, 1, cells[0], topodatapb.TabletType_MASTER)
	addTablet(ctx, t, ts, 2, cells[0], topodatapb.TabletType_REPLICA)

	// Do a rebuild.
	if _, err := RebuildShard(ctx, logger, ts, testKeyspace, testShard, cells); err != nil {
		t.Fatalf("RebuildShard: %v", err)
	}

	srvShard, err := ts.GetSrvShard(ctx, cells[0], testKeyspace, testShard)
	if err != nil {
		t.Fatalf("GetSrvShard: %v", err)
	}
	if srvShard.MasterCell != cells[0] {
		t.Errorf("Invalid cell name, got %v expected %v", srvShard.MasterCell, cells[0])
	}
}
