package main

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func testVersionedObjectCache(t *testing.T, voc *VersionedObjectCache, vo VersionedObject, expectedVO VersionedObject) {
	ctx := context.Background()
	result, err := voc.Get(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, vo); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if vo.GetVersion() != 1 {
		t.Fatalf("Got wrong initial version: %v", vo.GetVersion())
	}
	expectedVO.SetVersion(1)
	if !reflect.DeepEqual(vo, expectedVO) {
		t.Fatalf("Got bad result: %#v expected: %#v", vo, expectedVO)
	}

	result2, err := voc.Get(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("Bad content from cache: %v != %v", string(result), string(result2))
	}

	// force a re-get with same content, version shouldn't change
	voc.timestamp = time.Time{}
	result2, err = voc.Get(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("Bad content from cache: %v != %v", string(result), string(result2))
	}

	// force a reget with different content, version should change
	voc.timestamp = time.Time{}
	voc.versionedObject.Reset() // poking inside the object here
	result, err = voc.Get(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, vo); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if vo.GetVersion() != 2 {
		t.Fatalf("Got wrong second version: %v", vo.GetVersion())
	}
	expectedVO.SetVersion(2)
	if !reflect.DeepEqual(vo, expectedVO) {
		t.Fatalf("Got bad result: %#v expected: %#v", vo, expectedVO)
	}

	// force a flush and see the version increase again
	voc.Flush()
	result, err = voc.Get(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, vo); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if vo.GetVersion() != 3 {
		t.Fatalf("Got wrong third version: %v", vo.GetVersion())
	}
	expectedVO.SetVersion(3)
	if !reflect.DeepEqual(vo, expectedVO) {
		t.Fatalf("Got bad result: %#v expected: %#v", vo, expectedVO)
	}
}

func testVersionedObjectCacheMap(t *testing.T, vocm *VersionedObjectCacheMap, key string, vo VersionedObject, expectedVO VersionedObject) {
	ctx := context.Background()
	result, err := vocm.Get(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, vo); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if vo.GetVersion() != 1 {
		t.Fatalf("Got wrong initial version: %v", vo.GetVersion())
	}
	expectedVO.SetVersion(1)
	if !reflect.DeepEqual(vo, expectedVO) {
		t.Fatalf("Got bad result: %+v expected: %+v", vo, expectedVO)
	}

	result2, err := vocm.Get(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("Bad content from cache: %v != %v", string(result), string(result2))
	}

	// force a re-get with same content, version shouldn't change
	vocm.cacheMap[key].timestamp = time.Time{}
	result2, err = vocm.Get(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("Bad content from cache: %v != %v", string(result), string(result2))
	}

	// force a reget with different content, version should change
	vocm.cacheMap[key].timestamp = time.Time{}
	vocm.cacheMap[key].versionedObject.Reset() // poking inside the object here
	result, err = vocm.Get(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, vo); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if vo.GetVersion() != 2 {
		t.Fatalf("Got wrong second version: %v", vo.GetVersion())
	}
	expectedVO.SetVersion(2)
	if !reflect.DeepEqual(vo, expectedVO) {
		t.Fatalf("Got bad result: %#v expected: %#v", vo, expectedVO)
	}

	// force a flush and see the version increase again
	vocm.Flush()
	result, err = vocm.Get(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, vo); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if vo.GetVersion() != 3 {
		t.Fatalf("Got wrong third version: %v", vo.GetVersion())
	}
	expectedVO.SetVersion(3)
	if !reflect.DeepEqual(vo, expectedVO) {
		t.Fatalf("Got bad result: %#v expected: %#v", vo, expectedVO)
	}
}

func TestKnownCellsCache(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	kcc := newKnownCellsCache(ts)
	var kc KnownCells
	expectedKc := KnownCells{
		Cells: []string{"cell1", "cell2"},
	}

	testVersionedObjectCache(t, kcc, &kc, &expectedKc)
}

func TestKeyspacesCache(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	if err := ts.CreateKeyspace(ctx, "ks1", &pb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	if err := ts.CreateKeyspace(ctx, "ks2", &pb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	kc := newKeyspacesCache(ts)
	var k Keyspaces
	expectedK := Keyspaces{
		Keyspaces: []string{"ks1", "ks2"},
	}

	testVersionedObjectCache(t, kc, &k, &expectedK)
}

func TestKeyspaceCache(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	if err := ts.CreateKeyspace(ctx, "ks1", &pb.Keyspace{
		ShardingColumnName: "sharding_key",
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	if err := ts.CreateKeyspace(ctx, "ks2", &pb.Keyspace{
		SplitShardCount: 10,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	kc := newKeyspaceCache(ts)
	var k Keyspace

	expectedK := Keyspace{
		KeyspaceName: "ks1",
		Keyspace: &pb.Keyspace{
			ShardingColumnName: "sharding_key",
		},
	}
	testVersionedObjectCacheMap(t, kc, "ks1", &k, &expectedK)

	k = Keyspace{}
	expectedK = Keyspace{
		KeyspaceName: "ks2",
		Keyspace: &pb.Keyspace{
			SplitShardCount: 10,
		},
	}
	testVersionedObjectCacheMap(t, kc, "ks2", &k, &expectedK)
}

func TestShardNamesCache(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	if err := ts.CreateKeyspace(ctx, "ks1", &pb.Keyspace{
		ShardingColumnName: "sharding_key",
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	if err := ts.CreateShard(ctx, "ks1", "s1", &pb.Shard{
		Cells: []string{"cell1", "cell2"},
	}); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
	if err := ts.CreateShard(ctx, "ks1", "s2", &pb.Shard{
		MasterAlias: &pb.TabletAlias{
			Cell: "cell1",
			Uid:  12,
		},
	}); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
	snc := newShardNamesCache(ts)
	var sn ShardNames

	expectedSN := ShardNames{
		KeyspaceName: "ks1",
		ShardNames:   []string{"s1", "s2"},
	}
	testVersionedObjectCacheMap(t, snc, "ks1", &sn, &expectedSN)
}

func TestShardCache(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	if err := ts.CreateKeyspace(ctx, "ks1", &pb.Keyspace{
		ShardingColumnName: "sharding_key",
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	if err := ts.CreateShard(ctx, "ks1", "s1", &pb.Shard{
		Cells: []string{"cell1", "cell2"},
	}); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
	if err := ts.CreateShard(ctx, "ks1", "s2", &pb.Shard{
		MasterAlias: &pb.TabletAlias{
			Cell: "cell1",
			Uid:  12,
		},
	}); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
	sc := newShardCache(ts)
	var s Shard

	expectedS := Shard{
		KeyspaceName: "ks1",
		ShardName:    "s1",
		Shard: &pb.Shard{
			Cells: []string{"cell1", "cell2"},
		},
	}
	testVersionedObjectCacheMap(t, sc, "ks1/s1", &s, &expectedS)

	s = Shard{}
	expectedS = Shard{
		KeyspaceName: "ks1",
		ShardName:    "s2",
		Shard: &pb.Shard{
			MasterAlias: &pb.TabletAlias{
				Cell: "cell1",
				Uid:  12,
			},
		},
	}
	testVersionedObjectCacheMap(t, sc, "ks1/s2", &s, &expectedS)
}

func TestCellShardTabletsCache(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	if err := ts.UpdateShardReplicationFields(ctx, "cell1", "ks1", "s1", func(sr *pb.ShardReplication) error {
		sr.Nodes = []*pb.ShardReplication_Node{
			&pb.ShardReplication_Node{
				TabletAlias: &pb.TabletAlias{
					Cell: "cell1",
					Uid:  12,
				},
			},
			&pb.ShardReplication_Node{
				TabletAlias: &pb.TabletAlias{
					Cell: "cell1",
					Uid:  13,
				},
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardReplicationFields failed: %v", err)
	}
	cstc := newCellShardTabletsCache(ts)
	var cst CellShardTablets

	expectedCST := CellShardTablets{
		Cell:         "cell1",
		KeyspaceName: "ks1",
		ShardName:    "s1",
		TabletAliases: []topo.TabletAlias{
			topo.TabletAlias{
				Cell: "cell1",
				Uid:  12,
			},
			topo.TabletAlias{
				Cell: "cell1",
				Uid:  13,
			},
		},
	}
	testVersionedObjectCacheMap(t, cstc, "cell1/ks1/s1", &cst, &expectedCST)
}
