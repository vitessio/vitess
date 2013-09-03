// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

func newKeyRange(value string) key.KeyRange {
	_, result, err := topo.ValidateShardName(value)
	if err != nil {
		panic(err)
	}
	return result
}

func CheckKeyspace(t *testing.T, ts topo.Server) {
	keyspaces, err := ts.GetKeyspaces()
	if err != nil {
		t.Errorf("GetKeyspaces(empty): %v", err)
	}
	if len(keyspaces) != 0 {
		t.Errorf("len(GetKeyspaces()) != 0: %v", keyspaces)
	}

	if err := ts.CreateKeyspace("test_keyspace"); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	if err := ts.CreateKeyspace("test_keyspace"); err != topo.ErrNodeExists {
		t.Errorf("CreateKeyspace(again) is not ErrNodeExists: %v", err)
	}

	keyspaces, err = ts.GetKeyspaces()
	if err != nil {
		t.Errorf("GetKeyspaces: %v", err)
	}
	if len(keyspaces) != 1 || keyspaces[0] != "test_keyspace" {
		t.Errorf("GetKeyspaces: want %v, got %v", []string{"test_keyspace"}, keyspaces)
	}

	if err := ts.CreateKeyspace("test_keyspace2"); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	keyspaces, err = ts.GetKeyspaces()
	if err != nil {
		t.Errorf("GetKeyspaces: %v", err)
	}
	if len(keyspaces) != 2 || keyspaces[0] != "test_keyspace" || keyspaces[1] != "test_keyspace2" {
		t.Errorf("GetKeyspaces: want %v, got %v", []string{"test_keyspace", "test_keyspace2"}, keyspaces)
	}
}

func tabletEqual(left, right *topo.Tablet) (bool, error) {
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

func CheckTablet(t *testing.T, ts topo.Server) {
	cell := getLocalCell(t, ts)
	tablet := &topo.Tablet{
		Cell:     cell,
		Uid:      1,
		Parent:   topo.TabletAlias{},
		Addr:     "localhost:3333",
		Keyspace: "test_keyspace",
		Type:     topo.TYPE_MASTER,
		State:    topo.STATE_READ_WRITE,
		KeyRange: newKeyRange("-10"),
	}
	if err := ts.CreateTablet(tablet); err != nil {
		t.Errorf("CreateTablet: %v", err)
	}
	if err := ts.CreateTablet(tablet); err != topo.ErrNodeExists {
		t.Errorf("CreateTablet(again): %v", err)
	}

	if _, err := ts.GetTablet(topo.TabletAlias{cell, 666}); err != topo.ErrNoNode {
		t.Errorf("GetTablet(666): %v", err)
	}

	ti, err := ts.GetTablet(tablet.Alias())
	if err != nil {
		t.Errorf("GetTablet %v: %v", tablet.Alias(), err)
	}
	if eq, err := tabletEqual(ti.Tablet, tablet); err != nil {
		t.Errorf("cannot compare tablets: %v", err)
	} else if !eq {
		t.Errorf("put and got tablets are not identical:\n%#v\n%#v", tablet, ti.Tablet)
	}

	if _, err := ts.GetTabletsByCell("666"); err != topo.ErrNoNode {
		t.Errorf("GetTabletsByCell(666): %v", err)
	}

	inCell, err := ts.GetTabletsByCell(cell)
	if err != nil {
		t.Errorf("GetTabletsByCell: %v", err)
	}
	if len(inCell) != 1 || inCell[0] != tablet.Alias() {
		t.Errorf("GetTabletsByCell: want [%v], got %v", tablet.Alias(), inCell)
	}

	ti.State = topo.STATE_READ_ONLY
	if err := topo.UpdateTablet(ts, ti); err != nil {
		t.Errorf("UpdateTablet: %v", err)
	}

	ti, err = ts.GetTablet(tablet.Alias())
	if err != nil {
		t.Errorf("GetTablet %v: %v", tablet.Alias(), err)
	}
	if want := topo.STATE_READ_ONLY; ti.State != want {
		t.Errorf("ti.State: want %v, got %v", want, ti.State)
	}

	if err := ts.UpdateTabletFields(tablet.Alias(), func(t *topo.Tablet) error {
		t.State = topo.STATE_READ_WRITE
		return nil
	}); err != nil {
		t.Errorf("UpdateTabletFields: %v", err)
	}
	ti, err = ts.GetTablet(tablet.Alias())
	if err != nil {
		t.Errorf("GetTablet %v: %v", tablet.Alias(), err)
	}

	if want := topo.STATE_READ_WRITE; ti.State != want {
		t.Errorf("ti.State: want %v, got %v", want, ti.State)
	}

	if err := ts.DeleteTablet(tablet.Alias()); err != nil {
		t.Errorf("DeleteTablet: %v", err)
	}
	if err := ts.DeleteTablet(tablet.Alias()); err != topo.ErrNoNode {
		t.Errorf("DeleteTablet(again): %v", err)
	}

	if _, err := ts.GetTablet(tablet.Alias()); err != topo.ErrNoNode {
		t.Errorf("GetTablet: expected error, tablet was deleted: %v", err)
	}

}

func getLocalCell(t *testing.T, ts topo.Server) string {
	cells, err := ts.GetKnownCells()
	if err != nil {
		t.Fatalf("GetKnownCells: %v", err)
	}
	if len(cells) < 1 {
		t.Fatalf("provided topo.Server doesn't have enough cells (need at least 1): %v", cells)
	}
	return cells[0]
}

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
	replica1 := topo.TabletAlias{Cell: "ny", Uid: 2}
	replica2 := topo.TabletAlias{Cell: "ny", Uid: 3}
	rdonly1 := topo.TabletAlias{Cell: "sj", Uid: 4}
	rdonly2 := topo.TabletAlias{Cell: "sj", Uid: 5}
	shardInfo.MasterAlias = master
	shardInfo.ReplicaAliases = []topo.TabletAlias{replica1, replica2}
	shardInfo.RdonlyAliases = []topo.TabletAlias{rdonly1, rdonly2}
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
	if len(shardInfo.ReplicaAliases) != 2 || shardInfo.ReplicaAliases[0] != replica1 || shardInfo.ReplicaAliases[1] != replica2 {
		t.Errorf("after UpdateShard: shardInfo.ReplicaAliases got %v", shardInfo.ReplicaAliases)
	}
	if len(shardInfo.RdonlyAliases) != 2 || shardInfo.RdonlyAliases[0] != rdonly1 || shardInfo.RdonlyAliases[1] != rdonly2 {
		t.Errorf("after UpdateShard: shardInfo.RdonlyAliases got %v", shardInfo.RdonlyAliases)
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

func CheckReplicationPaths(t *testing.T, ts topo.Server) {
	if _, err := ts.GetReplicationPaths("test_keyspace", "-10", "/"); err != topo.ErrNoNode {
		t.Errorf("GetReplicationPaths(bad shard): %v", err)
	}

	if err := ts.CreateKeyspace("test_keyspace"); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	if err := topo.CreateShard(ts, "test_keyspace", "-10"); err != nil {
		t.Errorf("CreateShard: %v", err)
	}

	if paths, err := ts.GetReplicationPaths("test_keyspace", "-10", "/"); err != nil || len(paths) != 0 {
		t.Errorf("GetReplicationPaths(empty shard): %v, %v", err, paths)
	}
	if _, err := ts.GetReplicationPaths("test_keyspace", "-10", "/666"); err != topo.ErrNoNode {
		t.Errorf("GetReplicationPaths(non-existing path): %v", err)
	}

	if err := ts.CreateReplicationPath("test_keyspace", "-10", "/cell-0000000001"); err != nil {
		t.Errorf("CreateReplicationPath: %v", err)
	}
	if err := ts.CreateReplicationPath("test_keyspace", "-10", "/cell-0000000001"); err != topo.ErrNodeExists {
		t.Errorf("CreateReplicationPath(again): %v", err)
	}

	if paths, err := ts.GetReplicationPaths("test_keyspace", "-10", "/"); err != nil || len(paths) != 1 || paths[0].String() != "cell-0000000001" {
		t.Errorf("GetReplicationPaths(root): %v, %v", err, paths)
	}

	if err := ts.CreateReplicationPath("test_keyspace", "-10", "/cell-0000000001/cell-0000000002"); err != nil {
		t.Errorf("CreateReplicationPath(2): %v", err)
	}
	if err := ts.CreateReplicationPath("test_keyspace", "-10", "/cell-0000000001/cell-0000000003"); err != nil {
		t.Errorf("CreateReplicationPath(3): %v", err)
	}
	if paths, err := ts.GetReplicationPaths("test_keyspace", "-10", "/cell-0000000001"); err != nil || len(paths) != 2 || paths[0].String() != "cell-0000000002" || paths[1].String() != "cell-0000000003" {
		t.Errorf("GetReplicationPaths(master): %v, %v", err, paths)
	}

	if err := ts.DeleteReplicationPath("test_keyspace", "-10", "/cell-0000000001"); err != topo.ErrNotEmpty {
		t.Errorf("DeleteReplicationPath(master with slaves): %v", err)
	}
	if err := ts.DeleteReplicationPath("test_keyspace", "-10", "/cell-0000000001/cell-0000000002"); err != nil {
		t.Errorf("DeleteReplicationPath(slave1): %v", err)
	}
	if paths, err := ts.GetReplicationPaths("test_keyspace", "-10", "/cell-0000000001"); err != nil || len(paths) != 1 || paths[0].String() != "cell-0000000003" {
		t.Errorf("GetReplicationPaths(master): %v, %v", err, paths)
	}
	if err := ts.DeleteReplicationPath("test_keyspace", "-10", "/cell-0000000001/cell-0000000003"); err != nil {
		t.Errorf("DeleteReplicationPath(slave2): %v", err)
	}
	if paths, err := ts.GetReplicationPaths("test_keyspace", "-10", "/cell-0000000001"); err != nil || len(paths) != 0 {
		t.Errorf("GetReplicationPaths(master): %v, %v", err, paths)
	}
	if err := ts.DeleteReplicationPath("test_keyspace", "-10", "/cell-0000000001"); err != nil {
		t.Errorf("DeleteReplicationPath(master): %v", err)
	}
	if paths, err := ts.GetReplicationPaths("test_keyspace", "-10", "/"); err != nil || len(paths) != 0 {
		t.Errorf("GetReplicationPaths(root): %v, %v", err, paths)
	}
}

func CheckServingGraph(t *testing.T, ts topo.Server) {

	// test individual cell/keyspace/shard/type entries
	if _, err := ts.GetSrvTabletTypesPerShard("cell", "test_keyspace", "-10"); err != topo.ErrNoNode {
		t.Errorf("GetSrvTabletTypesPerShard(invalid): %v", err)
	}
	if _, err := ts.GetSrvTabletType("cell", "test_keyspace", "-10", topo.TYPE_MASTER); err != topo.ErrNoNode {
		t.Errorf("GetSrvTabletType(invalid): %v", err)
	}

	vtnsAddrs := topo.VtnsAddrs{
		Entries: []topo.VtnsAddr{
			topo.VtnsAddr{
				Uid:  1,
				Host: "host1",
				Port: 1,
			},
		},
	}

	if err := ts.UpdateSrvTabletType("cell", "test_keyspace", "-10", topo.TYPE_MASTER, &vtnsAddrs); err != nil {
		t.Errorf("UpdateSrvTabletType(master): %v", err)
	}
	if types, err := ts.GetSrvTabletTypesPerShard("cell", "test_keyspace", "-10"); err != nil || len(types) != 1 || types[0] != topo.TYPE_MASTER {
		t.Errorf("GetSrvTabletTypesPerShard(1): %v %v", err, types)
	}
	if addrs, err := ts.GetSrvTabletType("cell", "test_keyspace", "-10", topo.TYPE_MASTER); err != nil || len(addrs.Entries) != 1 || addrs.Entries[0].Uid != 1 {
		t.Errorf("GetSrvTabletType(1): %v %v", err, addrs)
	}

	if err := ts.UpdateTabletEndpoint("cell", "test_keyspace", "-10", topo.TYPE_REPLICA, &topo.VtnsAddr{Uid: 2, Host: "host2", Port: 2}); err != nil {
		t.Errorf("UpdateTabletEndpoint(invalid): %v", err)
	}
	if err := ts.UpdateTabletEndpoint("cell", "test_keyspace", "-10", topo.TYPE_MASTER, &topo.VtnsAddr{Uid: 1, Host: "host2", Port: 2}); err != nil {
		t.Errorf("UpdateTabletEndpoint(master): %v", err)
	}
	if addrs, err := ts.GetSrvTabletType("cell", "test_keyspace", "-10", topo.TYPE_MASTER); err != nil || len(addrs.Entries) != 1 || addrs.Entries[0].Uid != 1 {
		t.Errorf("GetSrvTabletType(2): %v %v", err, addrs)
	}
	if err := ts.UpdateTabletEndpoint("cell", "test_keyspace", "-10", topo.TYPE_MASTER, &topo.VtnsAddr{Uid: 3, Host: "host3", Port: 3}); err != nil {
		t.Errorf("UpdateTabletEndpoint(master): %v", err)
	}
	if addrs, err := ts.GetSrvTabletType("cell", "test_keyspace", "-10", topo.TYPE_MASTER); err != nil || len(addrs.Entries) != 2 {
		t.Errorf("GetSrvTabletType(2): %v %v", err, addrs)
	}

	if err := ts.DeleteSrvTabletType("cell", "test_keyspace", "-10", topo.TYPE_REPLICA); err != topo.ErrNoNode {
		t.Errorf("DeleteSrvTabletType(unknown): %v", err)
	}
	if err := ts.DeleteSrvTabletType("cell", "test_keyspace", "-10", topo.TYPE_MASTER); err != nil {
		t.Errorf("DeleteSrvTabletType(master): %v", err)
	}

	// test cell/keyspace/shard entries (SrvShard)
	srvShard := topo.SrvShard{
		ServedTypes: []topo.TabletType{topo.TYPE_MASTER},
	}
	if err := ts.UpdateSrvShard("cell", "test_keyspace", "-10", &srvShard); err != nil {
		t.Errorf("UpdateSrvShard(1): %v", err)
	}
	if _, err := ts.GetSrvShard("cell", "test_keyspace", "666"); err != topo.ErrNoNode {
		t.Errorf("GetSrvShard(invalid): %v", err)
	}
	if s, err := ts.GetSrvShard("cell", "test_keyspace", "-10"); err != nil || len(s.ServedTypes) != 1 || s.ServedTypes[0] != topo.TYPE_MASTER {
		t.Errorf("GetSrvShard(valid): %v", err)
	}

	// test cell/keyspace entries (SrvKeyspace)
	srvKeyspace := topo.SrvKeyspace{
		TabletTypes: []topo.TabletType{topo.TYPE_MASTER},
	}
	if err := ts.UpdateSrvKeyspace("cell", "test_keyspace", &srvKeyspace); err != nil {
		t.Errorf("UpdateSrvKeyspace(1): %v", err)
	}
	if _, err := ts.GetSrvKeyspace("cell", "test_keyspace666"); err != topo.ErrNoNode {
		t.Errorf("GetSrvKeyspace(invalid): %v", err)
	}
	if s, err := ts.GetSrvKeyspace("cell", "test_keyspace"); err != nil || len(s.TabletTypes) != 1 || s.TabletTypes[0] != topo.TYPE_MASTER {
		t.Errorf("GetSrvKeyspace(valid): %v", err)
	}

}

func CheckKeyspaceLock(t *testing.T, ts topo.Server) {
	if err := ts.CreateKeyspace("test_keyspace"); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	interrupted := make(chan struct{}, 1)
	lockPath, err := ts.LockKeyspaceForAction("test_keyspace", "fake-content", 5*time.Second, interrupted)
	if err != nil {
		t.Fatalf("LockKeyspaceForAction: %v", err)
	}

	// test we can't take the lock again
	if _, err := ts.LockKeyspaceForAction("test_keyspace", "unused-fake-content", time.Second/10, interrupted); err != topo.ErrTimeout {
		t.Errorf("LockKeyspaceForAction(again): %v", err)
	}

	// test we can interrupt taking the lock
	go func() {
		time.Sleep(time.Second / 10)
		close(interrupted)
	}()
	if _, err := ts.LockKeyspaceForAction("test_keyspace", "unused-fake-content", 5*time.Second, interrupted); err != topo.ErrInterrupted {
		t.Errorf("LockKeyspaceForAction(interrupted): %v", err)
	}

	if err := ts.UnlockKeyspaceForAction("test_keyspace", lockPath, "fake-results"); err != nil {
		t.Errorf("UnlockKeyspaceForAction(): %v", err)
	}

	// test we can't unlock again
	if err := ts.UnlockKeyspaceForAction("test_keyspace", lockPath, "fake-results"); err == nil {
		t.Error("UnlockKeyspaceForAction(again) worked")
	}

	// test we can't lock a non-existing keyspace
	interrupted = make(chan struct{}, 1)
	if _, err := ts.LockKeyspaceForAction("test_keyspace_666", "fake-content", 5*time.Second, interrupted); err == nil {
		t.Fatalf("LockKeyspaceForAction(test_keyspace_666) worked for non-existing keyspace")
	}
}

func CheckShardLock(t *testing.T, ts topo.Server) {
	if err := ts.CreateKeyspace("test_keyspace"); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}
	if err := topo.CreateShard(ts, "test_keyspace", "10-20"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	interrupted := make(chan struct{}, 1)
	lockPath, err := ts.LockShardForAction("test_keyspace", "10-20", "fake-content", 5*time.Second, interrupted)
	if err != nil {
		t.Fatalf("LockShardForAction: %v", err)
	}

	// test we can't take the lock again
	if _, err := ts.LockShardForAction("test_keyspace", "10-20", "unused-fake-content", time.Second/2, interrupted); err != topo.ErrTimeout {
		t.Errorf("LockShardForAction(again): %v", err)
	}

	// test we can interrupt taking the lock
	go func() {
		time.Sleep(time.Second / 2)
		close(interrupted)
	}()
	if _, err := ts.LockShardForAction("test_keyspace", "10-20", "unused-fake-content", 5*time.Second, interrupted); err != topo.ErrInterrupted {
		t.Errorf("LockShardForAction(interrupted): %v", err)
	}

	if err := ts.UnlockShardForAction("test_keyspace", "10-20", lockPath, "fake-results"); err != nil {
		t.Errorf("UnlockShardForAction(): %v", err)
	}

	// test we can't unlock again
	if err := ts.UnlockShardForAction("test_keyspace", "10-20", lockPath, "fake-results"); err == nil {
		t.Error("UnlockShardForAction(again) worked")
	}

	// test we can't lock a non-existing shard
	interrupted = make(chan struct{}, 1)
	if _, err := ts.LockShardForAction("test_keyspace", "20-30", "fake-content", 5*time.Second, interrupted); err == nil {
		t.Fatalf("LockShardForAction(test_keyspace/20-30) worked for non-existing shard")
	}
}

func CheckPid(t *testing.T, ts topo.Server) {
	cell := getLocalCell(t, ts)
	tablet := &topo.Tablet{
		Cell:     cell,
		Uid:      1,
		Parent:   topo.TabletAlias{},
		Addr:     "localhost:3333",
		Keyspace: "test_keyspace",
		Type:     topo.TYPE_MASTER,
		State:    topo.STATE_READ_WRITE,
		KeyRange: newKeyRange("-10"),
	}
	if err := ts.CreateTablet(tablet); err != nil {
		t.Fatalf("CreateTablet: %v", err)
	}
	tabletAlias := topo.TabletAlias{cell, 1}

	done := make(chan struct{}, 1)
	if err := ts.CreateTabletPidNode(tabletAlias, done); err != nil {
		t.Errorf("ts.CreateTabletPidNode: %v", err)
	}

	if err := ts.ValidateTabletPidNode(tabletAlias); err != nil {
		t.Errorf("ts.ValidateTabletPidNode: %v", err)
	}

	close(done)
}

func CheckActions(t *testing.T, ts topo.Server) {
	cell := getLocalCell(t, ts)
	tablet := &topo.Tablet{
		Cell:     cell,
		Uid:      1,
		Parent:   topo.TabletAlias{},
		Addr:     "localhost:3333",
		Keyspace: "test_keyspace",
		Type:     topo.TYPE_MASTER,
		State:    topo.STATE_READ_WRITE,
		KeyRange: newKeyRange("-10"),
	}
	if err := ts.CreateTablet(tablet); err != nil {
		t.Fatalf("CreateTablet: %v", err)
	}
	tabletAlias := topo.TabletAlias{cell, 1}

	actionPath, err := ts.WriteTabletAction(tabletAlias, "contents1")
	if err != nil {
		t.Fatalf("WriteTabletAction: %v", err)
	}

	interrupted := make(chan struct{}, 1)
	if _, err := ts.WaitForTabletAction(actionPath, time.Second/100, interrupted); err != topo.ErrTimeout {
		t.Errorf("WaitForTabletAction returned %v", err)
	}
	go func() {
		time.Sleep(time.Second / 10)
		close(interrupted)
	}()
	if _, err := ts.WaitForTabletAction(actionPath, time.Second*5, interrupted); err != topo.ErrInterrupted {
		t.Errorf("WaitForTabletAction returned %v", err)
	}

	wg := sync.WaitGroup{}

	// wait for the result in one thread
	wg.Add(1)
	go func() {
		interrupted := make(chan struct{}, 1)
		result, err := ts.WaitForTabletAction(actionPath, time.Second*10, interrupted)
		if err != nil {
			t.Errorf("WaitForTabletAction returned %v", err)
		}
		if result != "contents3" {
			t.Errorf("WaitForTabletAction returned bad result: %v", result)
		}
		wg.Done()
	}()

	// process the action in another thread
	done := make(chan struct{}, 1)
	wg.Add(1)
	go ts.ActionEventLoop(tabletAlias, func(ap, data string) error {
		if ap != actionPath {
			t.Errorf("Bad action path: %v", ap)
		}
		if data != "contents1" {
			t.Errorf("Bad data: %v", data)
		}
		ta, contents, version, err := ts.ReadTabletActionPath(ap)
		if err != nil {
			t.Errorf("Error from ReadTabletActionPath: %v", err)
		}
		if contents != data {
			t.Errorf("Bad contents: %v", contents)
		}
		if ta != tabletAlias {
			t.Errorf("Bad tablet alias: %v", ta)
		}

		if err := ts.UpdateTabletAction(ap, "contents2", version); err != nil {
			t.Errorf("UpdateTabletAction failed: %v", err)
		}
		if err := ts.StoreTabletActionResponse(ap, "contents3"); err != nil {
			t.Errorf("StoreTabletActionResponse failed: %v", err)
		}
		if err := ts.UnblockTabletAction(ap); err != nil {
			t.Errorf("UnblockTabletAction failed: %v", err)
		}

		wg.Done()
		return nil
	}, done)
	close(done)
	wg.Wait()
}
