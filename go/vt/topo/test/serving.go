// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"testing"

	"github.com/henryanand/vitess/go/vt/key"
	"github.com/henryanand/vitess/go/vt/topo"
)

func CheckServingGraph(t *testing.T, ts topo.Server) {
	cell := getLocalCell(t, ts)

	// test individual cell/keyspace/shard/type entries
	if _, err := ts.GetSrvTabletTypesPerShard(cell, "test_keyspace", "-10"); err != topo.ErrNoNode {
		t.Errorf("GetSrvTabletTypesPerShard(invalid): %v", err)
	}
	if _, err := ts.GetEndPoints(cell, "test_keyspace", "-10", topo.TYPE_MASTER); err != topo.ErrNoNode {
		t.Errorf("GetEndPoints(invalid): %v", err)
	}

	endPoints := topo.EndPoints{
		Entries: []topo.EndPoint{
			topo.EndPoint{
				Uid:          1,
				Host:         "host1",
				NamedPortMap: map[string]int{"vt": 1234, "mysql": 1235, "vts": 1236},
			},
		},
	}

	if err := ts.UpdateEndPoints(cell, "test_keyspace", "-10", topo.TYPE_MASTER, &endPoints); err != nil {
		t.Errorf("UpdateEndPoints(master): %v", err)
	}
	if types, err := ts.GetSrvTabletTypesPerShard(cell, "test_keyspace", "-10"); err != nil || len(types) != 1 || types[0] != topo.TYPE_MASTER {
		t.Errorf("GetSrvTabletTypesPerShard(1): %v %v", err, types)
	}

	addrs, err := ts.GetEndPoints(cell, "test_keyspace", "-10", topo.TYPE_MASTER)
	if err != nil {
		t.Errorf("GetEndPoints: %v", err)
	}
	if len(addrs.Entries) != 1 || addrs.Entries[0].Uid != 1 {
		t.Errorf("GetEndPoints(1): %v", addrs)
	}
	if pm := addrs.Entries[0].NamedPortMap; pm["vt"] != 1234 || pm["mysql"] != 1235 || pm["vts"] != 1236 {
		t.Errorf("GetSrcTabletType(1).NamedPortmap: want %v, got %v", endPoints.Entries[0].NamedPortMap, pm)
	}

	if err := ts.UpdateTabletEndpoint(cell, "test_keyspace", "-10", topo.TYPE_REPLICA, &topo.EndPoint{Uid: 2, Host: "host2"}); err != nil {
		t.Errorf("UpdateTabletEndpoint(invalid): %v", err)
	}
	if err := ts.UpdateTabletEndpoint(cell, "test_keyspace", "-10", topo.TYPE_MASTER, &topo.EndPoint{Uid: 1, Host: "host2"}); err != nil {
		t.Errorf("UpdateTabletEndpoint(master): %v", err)
	}
	if addrs, err := ts.GetEndPoints(cell, "test_keyspace", "-10", topo.TYPE_MASTER); err != nil || len(addrs.Entries) != 1 || addrs.Entries[0].Uid != 1 {
		t.Errorf("GetEndPoints(2): %v %v", err, addrs)
	}
	if err := ts.UpdateTabletEndpoint(cell, "test_keyspace", "-10", topo.TYPE_MASTER, &topo.EndPoint{Uid: 3, Host: "host3"}); err != nil {
		t.Errorf("UpdateTabletEndpoint(master): %v", err)
	}
	if addrs, err := ts.GetEndPoints(cell, "test_keyspace", "-10", topo.TYPE_MASTER); err != nil || len(addrs.Entries) != 2 {
		t.Errorf("GetEndPoints(2): %v %v", err, addrs)
	}

	if err := ts.DeleteEndPoints(cell, "test_keyspace", "-10", topo.TYPE_REPLICA); err != topo.ErrNoNode {
		t.Errorf("DeleteEndPoints(unknown): %v", err)
	}
	if err := ts.DeleteEndPoints(cell, "test_keyspace", "-10", topo.TYPE_MASTER); err != nil {
		t.Errorf("DeleteEndPoints(master): %v", err)
	}

	// test cell/keyspace/shard entries (SrvShard)
	srvShard := topo.SrvShard{
		ServedTypes: []topo.TabletType{topo.TYPE_MASTER},
		TabletTypes: []topo.TabletType{topo.TYPE_REPLICA, topo.TYPE_RDONLY},
	}
	if err := ts.UpdateSrvShard(cell, "test_keyspace", "-10", &srvShard); err != nil {
		t.Errorf("UpdateSrvShard(1): %v", err)
	}
	if _, err := ts.GetSrvShard(cell, "test_keyspace", "666"); err != topo.ErrNoNode {
		t.Errorf("GetSrvShard(invalid): %v", err)
	}
	if s, err := ts.GetSrvShard(cell, "test_keyspace", "-10"); err != nil ||
		len(s.ServedTypes) != 1 ||
		s.ServedTypes[0] != topo.TYPE_MASTER ||
		len(s.TabletTypes) != 2 ||
		s.TabletTypes[0] != topo.TYPE_REPLICA ||
		s.TabletTypes[1] != topo.TYPE_RDONLY {
		t.Errorf("GetSrvShard(valid): %v", err)
	}

	// test cell/keyspace entries (SrvKeyspace)
	srvKeyspace := topo.SrvKeyspace{
		Partitions: map[topo.TabletType]*topo.KeyspacePartition{
			topo.TYPE_MASTER: &topo.KeyspacePartition{
				Shards: []topo.SrvShard{
					topo.SrvShard{
						ServedTypes: []topo.TabletType{topo.TYPE_MASTER},
					},
				},
			},
		},
		TabletTypes:        []topo.TabletType{topo.TYPE_MASTER},
		ShardingColumnName: "video_id",
		ShardingColumnType: key.KIT_UINT64,
		ServedFrom: map[topo.TabletType]string{
			topo.TYPE_REPLICA: "other_keyspace",
		},
	}
	if err := ts.UpdateSrvKeyspace(cell, "test_keyspace", &srvKeyspace); err != nil {
		t.Errorf("UpdateSrvKeyspace(1): %v", err)
	}
	if _, err := ts.GetSrvKeyspace(cell, "test_keyspace666"); err != topo.ErrNoNode {
		t.Errorf("GetSrvKeyspace(invalid): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(cell, "test_keyspace"); err != nil ||
		len(k.TabletTypes) != 1 ||
		k.TabletTypes[0] != topo.TYPE_MASTER ||
		len(k.Partitions) != 1 ||
		len(k.Partitions[topo.TYPE_MASTER].Shards) != 1 ||
		len(k.Partitions[topo.TYPE_MASTER].Shards[0].ServedTypes) != 1 ||
		k.Partitions[topo.TYPE_MASTER].Shards[0].ServedTypes[0] != topo.TYPE_MASTER ||
		k.ShardingColumnName != "video_id" ||
		k.ShardingColumnType != key.KIT_UINT64 ||
		k.ServedFrom[topo.TYPE_REPLICA] != "other_keyspace" {
		t.Errorf("GetSrvKeyspace(valid): %v %v", err, k)
	}
	if k, err := ts.GetSrvKeyspaceNames(cell); err != nil || len(k) != 1 || k[0] != "test_keyspace" {
		t.Errorf("GetSrvKeyspaceNames(): %v", err)
	}

	// check that updating a SrvKeyspace out of the blue works
	if err := ts.UpdateSrvKeyspace(cell, "unknown_keyspace_so_far", &srvKeyspace); err != nil {
		t.Errorf("UpdateSrvKeyspace(2): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(cell, "unknown_keyspace_so_far"); err != nil ||
		len(k.TabletTypes) != 1 ||
		k.TabletTypes[0] != topo.TYPE_MASTER ||
		len(k.Partitions) != 1 ||
		len(k.Partitions[topo.TYPE_MASTER].Shards) != 1 ||
		len(k.Partitions[topo.TYPE_MASTER].Shards[0].ServedTypes) != 1 ||
		k.Partitions[topo.TYPE_MASTER].Shards[0].ServedTypes[0] != topo.TYPE_MASTER ||
		k.ShardingColumnName != "video_id" ||
		k.ShardingColumnType != key.KIT_UINT64 ||
		k.ServedFrom[topo.TYPE_REPLICA] != "other_keyspace" {
		t.Errorf("GetSrvKeyspace(out of the blue): %v %v", err, *k)
	}
}
