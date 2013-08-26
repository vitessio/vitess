// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"encoding/json"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

// PrepareTSFunc is a function that returns a fully functional
// topo.Server, that should contain at least two cells, one of which
// is global.
type PrepareTSFunc func(t *testing.T) topo.Server

// CheckFunc is a function that tests the implementation of a fragment
// of the topo.Server API.
type CheckFunc func(t *testing.T, ts topo.Server)

// CheckKeyspace runs test
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

	krStart, _ := key.HexKeyspaceId("").Unhex()
	krEnd, _ := key.HexKeyspaceId("10").Unhex()

	tablet := &topo.Tablet{
		Cell:     cell,
		Uid:      1,
		Parent:   topo.TabletAlias{},
		Addr:     "localhost:3333",
		Keyspace: "test_keyspace",
		Type:     topo.TYPE_MASTER,
		State:    topo.STATE_READ_WRITE,
		KeyRange: key.KeyRange{Start: krStart, End: krEnd},
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

	if err := topo.CreateShard(ts, "test_keyspace", "-10"); err != nil {
		t.Errorf("CreateShard: %v", err)
	}
	if err := topo.CreateShard(ts, "test_keyspace", "-10"); err != topo.ErrNodeExists {
		t.Errorf("CreateShard called second time, want: %v, got: %v", err, topo.ErrNodeExists)
	}

	if _, err := ts.GetShard("test_keyspace", "666"); err != topo.ErrNoNode {
		t.Errorf("GetShard(666): %v", err)
	}

	shardInfo, err := ts.GetShard("test_keyspace", "-10")
	if err != nil {
		t.Errorf("GetShard: %v", err)
	}
	if want, _ := key.ParseKeyRangeParts("", "10"); shardInfo.KeyRange != want {
		t.Errorf("shardInfo.KeyRange: want %v, got %v", want, shardInfo.KeyRange)
	}
	newMaster := topo.TabletAlias{Cell: "nyc", Uid: 1}
	shardInfo.MasterAlias = newMaster

	if err := ts.UpdateShard(shardInfo); err != nil {
		t.Errorf("UpdateShard: %v", err)
	}

	shardInfo, err = ts.GetShard("test_keyspace", "-10")
	if err != nil {
		t.Errorf("GetShard: %v", err)
	}
	if shardInfo.MasterAlias != newMaster {
		t.Errorf("after UpdateShard: shardInfo.MasterAlias want %v, got %v", newMaster, shardInfo.MasterAlias)
	}

	shards, err := ts.GetShardNames("test_keyspace")
	if err != nil {
		t.Errorf("GetShardNames: %v", err)
	}
	if len(shards) != 1 && shards[0] != "-10" {
		t.Errorf(`GetShardNames: want [ "-10" ], got %v`, shards)
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

// AllChecks is a list of functions that are called by CheckAll.
var AllChecks = []CheckFunc{
	CheckKeyspace,
	CheckTablet,
	CheckReplicationPaths,
}

// CheckAll runs all available checks. For each check, a fresh
// topo.Server is created by calling prepareTS. The server will be
// closed after running the check.
func CheckAll(t *testing.T, prepareTS PrepareTSFunc) {
	for _, checkFunc := range AllChecks {
		func() {
			ts := prepareTS(t)
			defer ts.Close()
			checkFunc(t, ts)
		}()
	}
}
