// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"testing"
)

func testExpectedTabletPanic(t *testing.T, path string) {
	if err := IsTabletPath(path); err == nil {
		t.Errorf("expected tablet error: %v", path)
	}
}

func TestValidTablet(t *testing.T) {
	if err := IsTabletPath("/vt/tablets/0"); err != nil {
		t.Error(err)
	}
}

func TestValidTabletWithTrailingSlash(t *testing.T) {
	testExpectedTabletPanic(t, "/vt/tablets/0/")
}

func TestRealPath(t *testing.T) {
	if err := IsTabletPath("/zk/test/vt/tablets/0000062344"); err != nil {
		t.Error(err)
	}
}

func TestShardInfo(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Error(x)
		}
	}()
	zkPath := "/zk/global/vt/keyspaces/test_keyspace/shards/shard0"
	// intentionally degenerate data to bypass empty data check
	si, err := NewShardInfo("test_keyspace", "shard0", "{}")
	if err != nil {
		t.Error("newShardErr: %v", err)
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

func TestVtRootFromTabletPath(t *testing.T) {
	path, err := VtRootFromTabletPath("/zk/test/vt/tablets/0000062344")
	if err != nil {
		t.Error(err)
	}
	expectedPath := "/zk/test/vt"
	if path != expectedPath {
		t.Errorf("%v not expected path %v", path, expectedPath)
	}
}

func TestTabletPathFromActionPath(t *testing.T) {
	path, alias, err := TabletPathFromActionPath("/zk/test/vt/tablets/0000062344/action/0000000001")
	if err != nil {
		t.Error(err)
	}
	expectedPath := "/zk/test/vt/tablets/0000062344"
	if path != expectedPath {
		t.Errorf("%v not expected path %v", path, expectedPath)
	}
	expectedAlias := "test-0000062344"
	if alias.String() != expectedAlias {
		t.Errorf("%v not expected alias %v", alias, expectedAlias)
	}
}

func TestReplicationPath2(t *testing.T) {
	badReplPath := "/zk/global/vt/keyspaces/test_keyspace/shards/8000000000000000-FFFFFFFFFFFFFFFF"
	if IsTabletReplicationPath(badReplPath) {
		t.Errorf("expected IsTabletReplicationPath to be false: %v", badReplPath)
	}

	badReplPath = "/zk/global/vt/keyspaces/test_keyspace/shards/0000000000000000-8000000000000000"
	if IsTabletReplicationPath(badReplPath) {
		t.Errorf("expected IsTabletReplicationPath to be false: %v", badReplPath)
	}

	goodReplPath := "/zk/global/vt/keyspaces/test_keyspace/shards/8000000000000000-FFFFFFFFFFFFFFFF/xxx-01"
	if !IsTabletReplicationPath(goodReplPath) {
		t.Errorf("expected IsTabletReplicationPath to be true")
	}
}
