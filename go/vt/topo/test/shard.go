// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"encoding/json"
	"testing"

	"code.google.com/p/go.net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

func shardEqual(left, right *topo.Shard) (bool, error) {
	lj, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rj, err := json.Marshal(right)
	if err != nil {
		return false, err
	}
	return string(lj) == string(rj), nil
}

func CheckShard(t *testing.T, ts topo.Server) {
	if err := ts.CreateKeyspace("test_keyspace", &topo.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	if err := topo.CreateShard(ts, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if err := topo.CreateShard(ts, "test_keyspace", "b0-c0"); err != topo.ErrNodeExists {
		t.Errorf("CreateShard called second time, got: %v", err)
	}

	if _, err := ts.GetShard("test_keyspace", "666"); err != topo.ErrNoNode {
		t.Errorf("GetShard(666): %v", err)
	}

	shardInfo, err := ts.GetShard("test_keyspace", "b0-c0")
	if err != nil {
		t.Errorf("GetShard: %v", err)
	}
	if want := newKeyRange("b0-c0"); shardInfo.KeyRange != want {
		t.Errorf("shardInfo.KeyRange: want %v, got %v", want, shardInfo.KeyRange)
	}
	master := topo.TabletAlias{Cell: "ny", Uid: 1}
	shardInfo.MasterAlias = master
	shardInfo.KeyRange = newKeyRange("b0-c0")
	shardInfo.ServedTypesMap = map[topo.TabletType]*topo.ShardServedType{
		topo.TYPE_MASTER:  &topo.ShardServedType{},
		topo.TYPE_REPLICA: &topo.ShardServedType{Cells: []string{"c1"}},
		topo.TYPE_RDONLY:  &topo.ShardServedType{},
	}
	shardInfo.SourceShards = []topo.SourceShard{
		topo.SourceShard{
			Uid:      1,
			Keyspace: "source_ks",
			Shard:    "b8-c0",
			KeyRange: newKeyRange("b8-c0"),
			Tables:   []string{"table1", "table2"},
		},
	}
	shardInfo.TabletControlMap = map[topo.TabletType]*topo.TabletControl{
		topo.TYPE_MASTER: &topo.TabletControl{
			Cells:             []string{"c1", "c2"},
			BlacklistedTables: []string{"black1", "black2"},
		},
		topo.TYPE_REPLICA: &topo.TabletControl{
			DisableQueryService: true,
		},
	}
	if err := topo.UpdateShard(context.TODO(), ts, shardInfo); err != nil {
		t.Errorf("UpdateShard: %v", err)
	}

	updatedShardInfo, err := ts.GetShard("test_keyspace", "b0-c0")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}

	if eq, err := shardEqual(shardInfo.Shard, updatedShardInfo.Shard); err != nil {
		t.Errorf("cannot compare shards: %v", err)
	} else if !eq {
		t.Errorf("put and got shards are not identical:\n%#v\n%#v", shardInfo.Shard, updatedShardInfo.Shard)
	}

	// test GetShardNames
	shards, err := ts.GetShardNames("test_keyspace")
	if err != nil {
		t.Errorf("GetShardNames: %v", err)
	}
	if len(shards) != 1 || shards[0] != "b0-c0" {
		t.Errorf(`GetShardNames: want [ "b0-c0" ], got %v`, shards)
	}

	if _, err := ts.GetShardNames("test_keyspace666"); err != topo.ErrNoNode {
		t.Errorf("GetShardNames(666): %v", err)
	}

}
