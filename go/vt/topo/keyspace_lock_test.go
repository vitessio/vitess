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
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestTopoKeyspaceLock tests keyspace lock operations.
func TestTopoKeyspaceLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	currentTopoLockTimeout := topo.LockTimeout
	topo.LockTimeout = testLockTimeout
	defer func() {
		topo.LockTimeout = currentTopoLockTimeout
	}()

	ks1 := "ks1"
	ks2 := "ks2"
	err := ts.CreateKeyspace(ctx, ks1, &topodatapb.Keyspace{})
	require.NoError(t, err)
	err = ts.CreateKeyspace(ctx, ks2, &topodatapb.Keyspace{})
	require.NoError(t, err)

	origCtx := ctx
	ctx, unlock, err := ts.LockKeyspace(origCtx, ks1, "ks1")
	require.NoError(t, err)

	// locking the same key again, without unlocking, should return an error
	_, _, err2 := ts.LockKeyspace(ctx, ks1, "ks1")
	require.ErrorContains(t, err2, "already held")

	// Check that we have the keyspace lock shouldn't return an error
	err = topo.CheckKeyspaceLocked(ctx, ks1)
	require.NoError(t, err)

	// Check that we have the keyspace lock for the other keyspace should return an error
	err = topo.CheckKeyspaceLocked(ctx, ks2)
	require.ErrorContains(t, err, "keyspace ks2 is not locked")

	// Check we can acquire a keyspace lock for the other keyspace
	ctx2, unlock2, err := ts.LockKeyspace(ctx, ks2, "ks2")
	require.NoError(t, err)
	defer unlock2(&err)

	// Unlock the first keyspace
	unlock(&err)

	// Check keyspace locked output for both keyspaces
	err = topo.CheckKeyspaceLocked(ctx2, ks1)
	require.ErrorContains(t, err, "keyspace ks1 is not locked")
	err = topo.CheckKeyspaceLocked(ctx2, ks2)
	require.NoError(t, err)

	// confirm that the lock can be re-acquired after unlocking
	_, unlock, err = ts.LockKeyspace(origCtx, ks1, "ks1")
	require.NoError(t, err)
	defer unlock(&err)
}

// TestTopoKeyspaceLockWithTTL tests keyspace lock with a custom TTL.
func TestTopoKeyspaceLockWithTTL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts, tsf := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	currentTopoLockTimeout := topo.LockTimeout
	topo.LockTimeout = testLockTimeout
	defer func() {
		topo.LockTimeout = currentTopoLockTimeout
	}()

	ks1 := "ks1"
	ttl := time.Second
	err := ts.CreateKeyspace(ctx, ks1, &topodatapb.Keyspace{})
	require.NoError(t, err)

	ctx, unlock, err := ts.LockKeyspace(ctx, ks1, ks1, topo.WithTTL(ttl))
	require.NoError(t, err)
	defer unlock(&err)

	err = topo.CheckKeyspaceLocked(ctx, ks1)
	require.NoError(t, err)

	// Confirm the new stats.
	stats := tsf.GetCallStats()
	require.NotNil(t, stats)
	require.Equal(t, int64(1), stats.Counts()["LockWithTTL"])
}
