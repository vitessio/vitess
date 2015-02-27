// Package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// CheckServingGraph makes sure the serving graph functions work properly.
func CheckServingGraph(ctx context.Context, t *testing.T, ts topo.Server) {
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

	if err := topo.UpdateEndPoints(ctx, ts, cell, "test_keyspace", "-10", topo.TYPE_MASTER, &endPoints); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}
	if types, err := ts.GetSrvTabletTypesPerShard(cell, "test_keyspace", "-10"); err != nil || len(types) != 1 || types[0] != topo.TYPE_MASTER {
		t.Errorf("GetSrvTabletTypesPerShard(1): %v %v", err, types)
	}

	// Delete the SrvShard (need to delete endpoints first).
	if err := ts.DeleteEndPoints(cell, "test_keyspace", "-10", topo.TYPE_MASTER); err != nil {
		t.Errorf("DeleteEndPoints: %v", err)
	}
	if err := ts.DeleteSrvShard(cell, "test_keyspace", "-10"); err != nil {
		t.Errorf("DeleteSrvShard: %v", err)
	}
	if _, err := ts.GetSrvShard(cell, "test_keyspace", "-10"); err != topo.ErrNoNode {
		t.Errorf("GetSrvShard(deleted) got %v, want ErrNoNode", err)
	}

	// Re-add endpoints.
	if err := topo.UpdateEndPoints(ctx, ts, cell, "test_keyspace", "-10", topo.TYPE_MASTER, &endPoints); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
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
		t.Fatalf("UpdateTabletEndpoint(invalid): %v", err)
	}
	if err := ts.UpdateTabletEndpoint(cell, "test_keyspace", "-10", topo.TYPE_MASTER, &topo.EndPoint{Uid: 1, Host: "host2"}); err != nil {
		t.Fatalf("UpdateTabletEndpoint(master): %v", err)
	}
	if addrs, err := ts.GetEndPoints(cell, "test_keyspace", "-10", topo.TYPE_MASTER); err != nil || len(addrs.Entries) != 1 || addrs.Entries[0].Uid != 1 {
		t.Errorf("GetEndPoints(2): %v %v", err, addrs)
	}
	if err := ts.UpdateTabletEndpoint(cell, "test_keyspace", "-10", topo.TYPE_MASTER, &topo.EndPoint{Uid: 3, Host: "host3"}); err != nil {
		t.Fatalf("UpdateTabletEndpoint(master): %v", err)
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
		t.Fatalf("UpdateSrvShard(1): %v", err)
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
				ShardReferences: []topo.ShardReference{
					topo.ShardReference{
						Name:     "-80",
						KeyRange: newKeyRange("-80"),
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
		len(k.Partitions[topo.TYPE_MASTER].ShardReferences) != 1 ||
		k.Partitions[topo.TYPE_MASTER].ShardReferences[0].Name != "-80" ||
		k.Partitions[topo.TYPE_MASTER].ShardReferences[0].KeyRange != newKeyRange("-80") ||
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
		t.Fatalf("UpdateSrvKeyspace(2): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(cell, "unknown_keyspace_so_far"); err != nil ||
		len(k.TabletTypes) != 1 ||
		k.TabletTypes[0] != topo.TYPE_MASTER ||
		len(k.Partitions) != 1 ||
		len(k.Partitions[topo.TYPE_MASTER].Shards) != 1 ||
		len(k.Partitions[topo.TYPE_MASTER].Shards[0].ServedTypes) != 1 ||
		k.Partitions[topo.TYPE_MASTER].Shards[0].ServedTypes[0] != topo.TYPE_MASTER ||
		len(k.Partitions[topo.TYPE_MASTER].ShardReferences) != 1 ||
		k.Partitions[topo.TYPE_MASTER].ShardReferences[0].Name != "-80" ||
		k.Partitions[topo.TYPE_MASTER].ShardReferences[0].KeyRange != newKeyRange("-80") ||
		k.ShardingColumnName != "video_id" ||
		k.ShardingColumnType != key.KIT_UINT64 ||
		k.ServedFrom[topo.TYPE_REPLICA] != "other_keyspace" {
		t.Errorf("GetSrvKeyspace(out of the blue): %v %v", err, *k)
	}
}

// CheckWatchEndPoints makes sure WatchEndPoints works as expected
func CheckWatchEndPoints(ctx context.Context, t *testing.T, ts topo.Server) {
	cell := getLocalCell(t, ts)
	keyspace := "test_keyspace"
	shard := "-10"
	tabletType := topo.TYPE_MASTER

	// start watching, should get nil first
	notifications, stopWatching, err := ts.WatchEndPoints(cell, keyspace, shard, tabletType)
	if err != nil {
		t.Fatalf("WatchEndPoints failed: %v", err)
	}
	ep, ok := <-notifications
	if !ok || ep != nil {
		t.Fatalf("first value is wrong: %v %v", ep, ok)
	}

	// update the endpoints, should get a notification
	endPoints := topo.EndPoints{
		Entries: []topo.EndPoint{
			topo.EndPoint{
				Uid:          1,
				Host:         "host1",
				NamedPortMap: map[string]int{"vt": 1234, "mysql": 1235, "vts": 1236},
			},
		},
	}
	if err := topo.UpdateEndPoints(ctx, ts, cell, keyspace, shard, tabletType, &endPoints); err != nil {
		t.Fatalf("UpdateEndPoints failed: %v", err)
	}
	for {
		ep, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if ep == nil {
			// duplicate notification of the first value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if !reflect.DeepEqual(&endPoints, ep) {
			t.Fatalf("first value is wrong: %v %v", ep, ok)
		}
		break
	}

	// delete the endpoints, should get a notification
	if err := ts.DeleteEndPoints(cell, keyspace, shard, tabletType); err != nil {
		t.Fatalf("DeleteEndPoints failed: %v", err)
	}
	for {
		ep, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if ep == nil {
			break
		}

		// duplicate notification of the first value, that's OK,
		// but value better be good.
		if !reflect.DeepEqual(&endPoints, ep) {
			t.Fatalf("duplicate notification value is bad: %v", ep)
		}
	}

	// re-create the value, a bit different, should get a notification
	endPoints.Entries[0].Uid = 2
	if err := topo.UpdateEndPoints(ctx, ts, cell, keyspace, shard, tabletType, &endPoints); err != nil {
		t.Fatalf("UpdateEndPoints failed: %v", err)
	}
	for {
		ep, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if ep == nil {
			// duplicate notification of the closed value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if !reflect.DeepEqual(&endPoints, ep) {
			t.Fatalf("value after delete / re-create is wrong: %v %v", ep, ok)
		}
		break
	}

	// close the stopWatching channel, should eventually get a closed
	// notifications channel too
	close(stopWatching)
	for {
		ep, ok := <-notifications
		if !ok {
			break
		}
		if !reflect.DeepEqual(&endPoints, ep) {
			t.Fatalf("duplicate notification value is bad: %v", ep)
		}
	}
}
