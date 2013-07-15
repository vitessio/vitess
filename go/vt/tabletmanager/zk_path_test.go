// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"testing"
)

func TestShardInfo(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Error(x)
		}
	}()
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
