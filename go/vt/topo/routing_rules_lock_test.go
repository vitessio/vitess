/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// TestKeyspaceRoutingRulesLock tests that the lock is acquired and released correctly.
func TestKeyspaceRoutingRulesLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	currentTopoLockTimeout := topo.LockTimeout
	topo.LockTimeout = testLockTimeout
	defer func() {
		topo.LockTimeout = currentTopoLockTimeout
	}()

	err := ts.CreateKeyspaceRoutingRules(ctx, &vschemapb.KeyspaceRoutingRules{})
	require.NoError(t, err)

	lock, err := topo.NewRoutingRulesLock(ctx, ts, "ks1")
	require.NoError(t, err)
	_, unlock, err := lock.Lock(ctx)
	require.NoError(t, err)

	// re-acquiring the lock should fail
	_, _, err = lock.Lock(ctx)
	require.Error(t, err)

	unlock(&err)

	// re-acquiring the lock should succeed
	_, _, err = lock.Lock(ctx)
	require.NoError(t, err)
}
