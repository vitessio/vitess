// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
)

func CheckShard(t *testing.T, ts topo.Server) {
	if err := ts.CreateKeyspace("test_keyspace"); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	if err := topo.CreateShard(ts, "test_keyspace", "B0-C0"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if err := topo.CreateShard(ts, "test_keyspace", "B0-C0"); err != topo.ErrNodeExists {
		t.Errorf("CreateShard called second time, got: %v", err)
	}

	if _, err := ts.GetShard("test_keyspace", "666"); err != topo.ErrNoNode {
		t.Errorf("GetShard(666): %v", err)
	}

	shardInfo, err := ts.GetShard("test_keyspace", "B0-C0")
	if err != nil {
		t.Errorf("GetShard: %v", err)
	}
	if want := newKeyRange("B0-C0"); shardInfo.KeyRange != want {
		t.Errorf("shardInfo.KeyRange: want %v, got %v", want, shardInfo.KeyRange)
	}
	master := topo.TabletAlias{Cell: "ny", Uid: 1}
	shardInfo.MasterAlias = master
	shardInfo.KeyRange = newKeyRange("B0-C0")
	shardInfo.ServedTypes = []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY}
	shardInfo.SourceShards = []topo.SourceShard{
		topo.SourceShard{
			Uid:      1,
			Keyspace: "source_ks",
			Shard:    "B8-C0",
			KeyRange: newKeyRange("B8-C0"),
		},
	}

	if err := ts.UpdateShard(shardInfo); err != nil {
		t.Errorf("UpdateShard: %v", err)
	}

	shardInfo, err = ts.GetShard("test_keyspace", "B0-C0")
	if err != nil {
		t.Errorf("GetShard: %v", err)
	}
	if shardInfo.MasterAlias != master {
		t.Errorf("after UpdateShard: shardInfo.MasterAlias got %v", shardInfo.MasterAlias)
	}
	if shardInfo.KeyRange != newKeyRange("B0-C0") {
		t.Errorf("after UpdateShard: shardInfo.KeyRange got %v", shardInfo.KeyRange)
	}
	if len(shardInfo.ServedTypes) != 3 || shardInfo.ServedTypes[0] != topo.TYPE_MASTER || shardInfo.ServedTypes[1] != topo.TYPE_REPLICA || shardInfo.ServedTypes[2] != topo.TYPE_RDONLY {
		t.Errorf("after UpdateShard: shardInfo.ServedTypes got %v", shardInfo.ServedTypes)
	}
	if len(shardInfo.SourceShards) != 1 || shardInfo.SourceShards[0].Uid != 1 || shardInfo.SourceShards[0].Keyspace != "source_ks" || shardInfo.SourceShards[0].Shard != "B8-C0" || shardInfo.SourceShards[0].KeyRange != newKeyRange("B8-C0") {
		t.Errorf("after UpdateShard: shardInfo.SourceShards got %v", shardInfo.SourceShards)
	}

	shards, err := ts.GetShardNames("test_keyspace")
	if err != nil {
		t.Errorf("GetShardNames: %v", err)
	}
	if len(shards) != 1 || shards[0] != "B0-C0" {
		t.Errorf(`GetShardNames: want [ "B0-C0" ], got %v`, shards)
	}

	if _, err := ts.GetShardNames("test_keyspace666"); err != topo.ErrNoNode {
		t.Errorf("GetShardNames(666): %v", err)
	}

}
