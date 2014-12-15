// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"testing"

	"code.google.com/p/go.net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// TestHookLockSrvShardForAction makes sure that changes to the upstream
// topo.Server interface don't break our hook. For example, if someone changes
// the function name in the interface and all its call sites, but doesn't change
// the name of our override to match.
func TestHookLockSrvShardForAction(t *testing.T) {
	cells := []string{"test_cell"}
	ts := NewTestServer(t, cells)

	triggered := false
	ts.HookLockSrvShardForAction = func() {
		triggered = true
	}

	ctx := context.Background()
	topo.Server(ts).LockSrvShardForAction(ctx, cells[0], "keyspace", "shard", "contents")

	if !triggered {
		t.Errorf("HookLockSrvShardForAction wasn't triggered")
	}
}
