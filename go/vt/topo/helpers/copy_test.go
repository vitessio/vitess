// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package helpers

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func createSetup(ctx context.Context, t *testing.T) (topo.Impl, topo.Impl) {
	fromTS := memorytopo.New("test_cell")
	toTS := memorytopo.New("test_cell")

	// create a keyspace and a couple tablets
	if err := fromTS.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("cannot create keyspace: %v", err)
	}
	if err := fromTS.CreateShard(ctx, "test_keyspace", "0", &topodatapb.Shard{Cells: []string{"test_cell"}}); err != nil {
		t.Fatalf("cannot create shard: %v", err)
	}
	tts := topo.Server{Impl: fromTS}
	if err := tts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  123,
		},
		Hostname: "masterhost",
		Ip:       "1.2.3.4",
		PortMap: map[string]int32{
			"vt":    8101,
			"gprc":  8102,
			"mysql": 3306,
		},
		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topodatapb.TabletType_MASTER,
		DbNameOverride: "",
		KeyRange:       nil,
	}); err != nil {
		t.Fatalf("cannot create master tablet: %v", err)
	}
	if err := tts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  234,
		},
		Ip: "2.3.4.5",
		PortMap: map[string]int32{
			"vt":    8101,
			"grpc":  8102,
			"mysql": 3306,
		},
		Hostname: "slavehost",

		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topodatapb.TabletType_REPLICA,
		DbNameOverride: "",
		KeyRange:       nil,
	}); err != nil {
		t.Fatalf("cannot create slave tablet: %v", err)
	}

	return fromTS, toTS
}

func TestBasic(t *testing.T) {
	ctx := context.Background()
	fromTS, toTS := createSetup(ctx, t)

	// check keyspace copy
	CopyKeyspaces(ctx, fromTS, toTS)
	keyspaces, err := toTS.GetKeyspaces(ctx)
	if err != nil {
		t.Fatalf("toTS.GetKeyspaces failed: %v", err)
	}
	if len(keyspaces) != 1 || keyspaces[0] != "test_keyspace" {
		t.Fatalf("unexpected keyspaces: %v", keyspaces)
	}
	CopyKeyspaces(ctx, fromTS, toTS)

	// check shard copy
	CopyShards(ctx, fromTS, toTS)
	shards, err := toTS.GetShardNames(ctx, "test_keyspace")
	if err != nil {
		t.Fatalf("toTS.GetShardNames failed: %v", err)
	}
	if len(shards) != 1 || shards[0] != "0" {
		t.Fatalf("unexpected shards: %v", shards)
	}
	CopyShards(ctx, fromTS, toTS)
	s, _, err := toTS.GetShard(ctx, "test_keyspace", "0")
	if err != nil {
		t.Fatalf("cannot read shard: %v", err)
	}
	if len(s.Cells) != 1 || s.Cells[0] != "test_cell" {
		t.Fatalf("bad shard data: %v", *s)
	}

	// check ShardReplication copy
	sr, err := fromTS.GetShardReplication(ctx, "test_cell", "test_keyspace", "0")
	if err != nil {
		t.Fatalf("fromTS.GetShardReplication failed: %v", err)
	}
	CopyShardReplications(ctx, fromTS, toTS)
	sr, err = toTS.GetShardReplication(ctx, "test_cell", "test_keyspace", "0")
	if err != nil {
		t.Fatalf("toTS.GetShardReplication failed: %v", err)
	}
	if len(sr.Nodes) != 2 {
		t.Fatalf("unexpected ShardReplication: %v", sr)
	}

	// check tablet copy
	CopyTablets(ctx, fromTS, toTS)
	tablets, err := toTS.GetTabletsByCell(ctx, "test_cell")
	if err != nil {
		t.Fatalf("toTS.GetTabletsByCell failed: %v", err)
	}
	if len(tablets) != 2 || tablets[0].Uid != 123 || tablets[1].Uid != 234 {
		t.Fatalf("unexpected tablets: %v", tablets)
	}
	CopyTablets(ctx, fromTS, toTS)
}
