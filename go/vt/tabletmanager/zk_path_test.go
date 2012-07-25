// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"testing"
)

func testExpectedShardPanic(t *testing.T, path string) {
	defer func() {
		recover()
	}()
	MustBeShardPath(path)
	t.Errorf("expected shard panic: %v", path)
}

func testExpectedTabletPanic(t *testing.T, path string) {
	defer func() {
		recover()
	}()
	MustBeTabletPath(path)
	t.Errorf("expected tablet panic: %v", path)
}

func TestInvalidShard(t *testing.T) {
	testExpectedShardPanic(t, "/vt/keyspaces/test/shards/0/123456789")
}

func TestEmptyShard(t *testing.T) {
	testExpectedShardPanic(t, "")
}

func TestValidShard(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Error(x)
		}
	}()
	MustBeShardPath("/zk/global/vt/keyspaces/test/shards/0")
}

func TestValidShardWithTrailingSlash(t *testing.T) {
	// we have to be strict - otherwise things are a mess
	testExpectedShardPanic(t, "/zk/global/vt/keyspaces/test/shards/0/")
}

func TestValidTablet(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Error(x)
		}
	}()
	MustBeTabletPath("/vt/tablets/0")
}

func TestValidTabletWithTrailingSlash(t *testing.T) {
	testExpectedTabletPanic(t, "/vt/tablets/0/")
}

func TestRealPath(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Error(x)
		}
	}()
	MustBeTabletPath("/zk/test/vt/tablets/0000062344")
}

func TestShardInfo(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Error(x)
		}
	}()
	zkPath := "/zk/global/vt/keyspaces/test_keyspace/shards/shard0"
	// intentionally degenerate data to bypass empty data check
	si, err := newShardInfo(zkPath, "{}")
	if err != nil {
		t.Error("newShardErr: %v", err)
	}
	if si.zkVtRoot != "/zk/global/vt" {
		t.Errorf("bad zkVtRoot: %v", si.zkVtRoot)
	}
	if si.keyspace != "test_keyspace" {
		t.Errorf("bad keyspace: %v", si.keyspace)
	}
	if si.shardName != "shard0" {
		t.Errorf("bad shard: %v", si.shardName)
	}
	if zkPath != si.ShardPath() {
		t.Errorf("bad ShardPath: %v", si.ShardPath())
	}
}

func TestVtRootFromShardPath(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Error(x)
		}
	}()
	path := VtRootFromShardPath("/zk/global/vt/keyspaces/test_keyspace/shards/shard0")
	expectedPath := "/zk/global/vt"
	if path != expectedPath {
		t.Errorf("%v not expected path %v", path, expectedPath)
	}
}

func TestVtRootFromTabletPath(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Error(x)
		}
	}()
	path := VtRootFromTabletPath("/zk/test/vt/tablets/0000062344")
	expectedPath := "/zk/test/vt"
	if path != expectedPath {
		t.Errorf("%v not expected path %v", path, expectedPath)
	}
}

func TestTabletPathFromActionPath(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Error(x)
		}
	}()
	path := TabletPathFromActionPath("/zk/test/vt/tablets/0000062344/action/0000000001")
	expectedPath := "/zk/test/vt/tablets/0000062344"
	if path != expectedPath {
		t.Errorf("%v not expected path %v", path, expectedPath)
	}
}
