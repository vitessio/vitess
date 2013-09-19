// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
)

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
