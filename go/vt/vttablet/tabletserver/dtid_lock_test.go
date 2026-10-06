/*
Copyright 2026 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// TestDTIDLocks verifies that operations on the same DTID wait for each
// other, that a waiter gives up when its context is done, and that different
// DTIDs do not wait for each other.
func TestDTIDLocks(t *testing.T) {
	locks := newDTIDLocks()
	unlockAA, err := locks.lock(t.Context(), "aa")
	require.NoError(t, err)

	unlockBB, err := locks.lock(t.Context(), "bb")
	require.NoError(t, err, "a different DTID must not wait")
	unlockBB()

	ctx, cancel := context.WithCancel(t.Context())
	cancelled := make(chan error, 1)
	go func() {
		_, err := locks.lock(ctx, "aa")
		cancelled <- err
	}()
	require.Eventually(t, func() bool { return locks.waiting("aa") == 1 }, 30*time.Second, 10*time.Millisecond)
	cancel()
	err = <-cancelled
	require.ErrorContains(t, err, "waiting for another operation on distributed transaction aa")
	assert.Equal(t, vtrpcpb.Code_CANCELED, vterrors.Code(err))
	assert.Equal(t, 0, locks.waiting("aa"))

	acquired := make(chan func(), 1)
	go func() {
		unlock, err := locks.lock(t.Context(), "aa")
		assert.NoError(t, err)
		acquired <- unlock
	}()
	require.Eventually(t, func() bool { return locks.waiting("aa") == 1 }, 30*time.Second, 10*time.Millisecond)
	assert.Empty(t, acquired, "the lock must not be acquired while it is held")
	unlockAA()
	unlock := <-acquired
	unlock()
	assert.Empty(t, locks.held)
}
