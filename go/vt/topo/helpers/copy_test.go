/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package helpers

import (
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func createSetup(ctx context.Context, t *testing.T) (*topo.Server, *topo.Server) {
	// Create a source and destination TS. They will have
	// different generations, so we test using the Version for
	// both works as expected.
	fromTS := memorytopo.NewServer("test_cell")
	toTS := memorytopo.NewServer("test_cell")

	// create a keyspace and a couple tablets
	if err := fromTS.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("cannot create keyspace: %v", err)
	}
	if err := fromTS.CreateShard(ctx, "test_keyspace", "0"); err != nil {
		t.Fatalf("cannot create shard: %v", err)
	}
	tablet1 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  123,
		},
		Hostname:      "masterhost",
		MysqlHostname: "masterhost",
		PortMap: map[string]int32{
			"vt":   8101,
			"gprc": 8102,
		},
		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topodatapb.TabletType_MASTER,
		DbNameOverride: "",
		KeyRange:       nil,
	}
	topoproto.SetMysqlPort(tablet1, 3306)
	if err := fromTS.CreateTablet(ctx, tablet1); err != nil {
		t.Fatalf("cannot create master tablet: %v", err)
	}
	tablet2 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  234,
		},
		PortMap: map[string]int32{
			"vt":   8101,
			"grpc": 8102,
		},
		Hostname:      "slavehost",
		MysqlHostname: "slavehost",

		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topodatapb.TabletType_REPLICA,
		DbNameOverride: "",
		KeyRange:       nil,
	}
	topoproto.SetMysqlPort(tablet2, 3306)
	if err := fromTS.CreateTablet(ctx, tablet2); err != nil {
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

	// check ShardReplication copy
	_, err = fromTS.GetShardReplication(ctx, "test_cell", "test_keyspace", "0")
	if err != nil {
		t.Fatalf("fromTS.GetShardReplication failed: %v", err)
	}
	CopyShardReplications(ctx, fromTS, toTS)
	sr, err := toTS.GetShardReplication(ctx, "test_cell", "test_keyspace", "0")
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
