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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestTopoNamedLock tests named lock operations.
func TestTopoNamedLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts, tsf := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	currentTopoLockTimeout := topo.LockTimeout
	topo.LockTimeout = testLockTimeout
	defer func() {
		topo.LockTimeout = currentTopoLockTimeout
	}()

	lockName := "testy"
	action := "testing"
	lockNameCalls := int64(0)

	ctx, unlock, err := ts.LockName(ctx, lockName, action)
	require.NoError(t, err)
	lockNameCalls++

	// Locking the same name again, without unlocking, should return an error.
	// This does not attempt the lock within the topo server implementation
	// as we first check the context and see that the lock is held, thus the
	// lockNameCalls should not be increased.
	_, _, err = ts.LockName(ctx, lockName, action)
	require.ErrorContains(t, err, fmt.Sprintf("%s is already held", lockName))

	// Check that we have the named lock.
	err = topo.CheckNameLocked(ctx, lockName)
	require.NoError(t, err)

	// Confirm that we can acquire a different named lock.
	lockName2 := "testy2"
	ctx, unlock2, err := ts.LockName(ctx, lockName2, action)
	require.NoError(t, err)
	defer unlock2(&err)
	lockNameCalls++

	// Unlock the first name.
	unlock(&err)

	// Confirm that we no longer have the first named lock.
	err = topo.CheckNameLocked(ctx, lockName)
	require.ErrorContains(t, err, fmt.Sprintf("%s is not locked", lockName))
	err = topo.CheckNameLocked(ctx, lockName2)
	require.NoError(t, err)

	// Confirm that the first named lock can be re-acquired after unlocking.
	_, unlock, err = ts.LockName(ctx, lockName, action)
	require.NoError(t, err)
	defer unlock(&err)
	lockNameCalls++

	// Confirm the stats.
	stats := tsf.GetCallStats()
	require.NotNil(t, stats)
	require.Equal(t, lockNameCalls, stats.Counts()["LockName"])
}
