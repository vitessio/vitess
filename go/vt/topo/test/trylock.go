/*
Copyright 2022 The Vitess Authors.

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

package test

import (
	"context"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// checkTryLock checks if we can lock / unlock as expected. It's using a keyspace
// as the lock target.
func checkTryLock(t *testing.T, ctx context.Context, ts *topo.Server) {
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		require.Fail(t, "CreateKeyspace fail", err.Error())
	}

	conn, err := ts.ConnForCell(context.Background(), topo.GlobalCell)
	if err != nil {
		require.Fail(t, "ConnForCell(global) failed", err.Error())
	}

	t.Log("===      checkTryLockTimeout")
	checkTryLockTimeout(ctx, t, conn)

	t.Log("===      checkTryLockUnblocks")
	checkTryLockUnblocks(ctx, t, conn)

	t.Log("===      checkTryLockConcurrent")
	checkTryLockConcurrent(ctx, t, conn)

	t.Log("===      checkTryLockWithQueuedLock")
	checkTryLockWithQueuedLock(ctx, t, conn)

	t.Log("===      checkTryLockNoNode")
	checkTryLockNoNode(ctx, t, conn)
}

// checkTryLockConcurrent checks that concurrent TryLock calls on a free lock
// all return without waiting: one gets the lock, the others return NodeExists
// and leave nothing behind that blocks the next TryLock.
func checkTryLockConcurrent(ctx context.Context, t *testing.T, conn topo.Conn) {
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace")
	tryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	const contenders = 20
	start := make(chan struct{})
	locks := make([]topo.LockDescriptor, contenders)
	errs := make([]error, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Go(func() {
			<-start
			locks[i], errs[i] = conn.TryLock(tryCtx, keyspacePath, fmt.Sprintf("contender-%d", i))
		})
	}
	close(start)
	wg.Wait()

	var held topo.LockDescriptor
	for i, err := range errs {
		if err == nil {
			require.Nil(t, held, "more than one TryLock got the lock")
			held = locks[i]
			continue
		}
		require.True(t, topo.IsErrType(err, topo.NodeExists), "TryLock should return NodeExists, got: %v", err)
	}
	require.NotNil(t, held, "no TryLock got the lock")
	require.NoError(t, held.Unlock(ctx))

	lockDescriptor, err := conn.TryLock(tryCtx, keyspacePath, "after")
	require.NoError(t, err)
	require.NoError(t, lockDescriptor.Unlock(ctx))
}

// checkTryLockWithQueuedLock checks that TryLock calls racing with a Lock
// queued behind the holder all return NodeExists, and that their short-lived
// attempts don't hand the lock to the queued caller while the holder has it.
func checkTryLockWithQueuedLock(ctx context.Context, t *testing.T, conn topo.Conn) {
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace")
	lockCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	holder, err := conn.Lock(lockCtx, keyspacePath, "holder")
	require.NoError(t, err)

	type lockResult struct {
		ld  topo.LockDescriptor
		err error
	}
	// The queued Lock joins once the first attempt returns, while the
	// contenders' retries keep rejected attempts appearing ahead of it.
	const contenders, attempts = 20, 10
	firstAttempt := make(chan struct{})
	var firstAttemptOnce sync.Once
	errs := make([]error, contenders*attempts)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Go(func() {
			for j := range attempts {
				_, errs[i*attempts+j] = conn.TryLock(lockCtx, keyspacePath, fmt.Sprintf("contender-%d-%d", i, j))
				firstAttemptOnce.Do(func() { close(firstAttempt) })
			}
		})
	}

	queued := make(chan lockResult, 1)
	go func() {
		<-firstAttempt
		ld, err := conn.Lock(lockCtx, keyspacePath, "queued")
		queued <- lockResult{ld, err}
	}()
	wg.Wait()
	for _, err := range errs {
		require.True(t, topo.IsErrType(err, topo.NodeExists), "TryLock should return NodeExists, got: %v", err)
	}
	require.Never(t, func() bool { return len(queued) > 0 }, 2*time.Second, 50*time.Millisecond, "queued Lock returned while the lock was held")

	require.NoError(t, holder.Unlock(ctx))
	select {
	case res := <-queued:
		require.NoError(t, res.err)
		require.NoError(t, res.ld.Unlock(ctx))
	case <-lockCtx.Done():
		require.Fail(t, "queued Lock did not get the lock after the holder unlocked")
	}
}

// checkTryLockNoNode checks that TryLock returns NoNode for a directory that
// doesn't exist, instead of creating it.
func checkTryLockNoNode(ctx context.Context, t *testing.T, conn topo.Conn) {
	_, err := conn.TryLock(ctx, path.Join(topo.KeyspacesPath, "missing_keyspace"), "")
	require.True(t, topo.IsErrType(err, topo.NoNode), "TryLock should return NoNode, got: %v", err)
}

// checkTryLockTimeout test the fail-fast nature of TryLock
func checkTryLockTimeout(ctx context.Context, t *testing.T, conn topo.Conn) {
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace")
	lockDescriptor, err := conn.TryLock(ctx, keyspacePath, "")
	if err != nil {
		require.Fail(t, "TryLock failed", err.Error())
	}

	// We have the lock, list the keyspace directory.
	// It should not contain anything, except Ephemeral files.
	entries, err := conn.ListDir(ctx, keyspacePath, true /*full*/)
	if err != nil {
		require.Failf(t, "ListDir failed", "%v", err.Error())
	}
	for _, e := range entries {
		if e.Name == "Keyspace" {
			continue
		}
		if e.Ephemeral {
			t.Logf("skipping ephemeral node %v in %v", e, keyspacePath)
			continue
		}
		// Non-ephemeral entries better have only ephemeral children.
		p := path.Join(keyspacePath, e.Name)
		entries, err := conn.ListDir(ctx, p, true /*full*/)
		if err != nil {
			require.Fail(t, "ListDir failed", err.Error())
		}
		for _, e := range entries {
			if e.Ephemeral {
				t.Logf("skipping ephemeral node %v in %v", e, p)
			} else {
				require.Fail(t, "non-ephemeral DirEntry")
			}
		}
	}

	// We should not be able to take the lock again. It should throw `NodeExists` error.
	fastCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if _, err := conn.TryLock(fastCtx, keyspacePath, "again"); !topo.IsErrType(err, topo.NodeExists) {
		require.Fail(t, "TryLock failed", err.Error())
	}
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	finished := make(chan struct{})

	// go routine to cancel the context.
	go func() {
		<-finished
		cancel()
	}()

	waitUntil := time.Now().Add(10 * time.Second)
	firstTime := true
	// after attempting the `TryLock` and getting an error `NodeExists`, we will cancel the context deliberately
	// and expect `context canceled` error in next iteration of `for` loop.
	for {
		if time.Now().After(waitUntil) {
			t.Fatalf("Unlock(test_keyspace) timed out")
		}
		// we expect context to fail with `context canceled` error
		if interruptCtx.Err() != nil {
			require.ErrorContains(t, interruptCtx.Err(), "context canceled")
			break
		}
		if _, err := conn.TryLock(interruptCtx, keyspacePath, "interrupted"); !topo.IsErrType(err, topo.NodeExists) {
			require.Fail(t, "TryLock failed", err.Error())
		}
		if firstTime {
			close(finished)
			firstTime = false
		}
		time.Sleep(1 * time.Second)
	}

	if err := lockDescriptor.Check(ctx); err != nil {
		t.Errorf("Check(): %v", err)
	}

	if err := lockDescriptor.Unlock(ctx); err != nil {
		require.Fail(t, "Unlock failed", err.Error())
	}

	// test we can't unlock again
	if err := lockDescriptor.Unlock(ctx); err == nil {
		require.Fail(t, "Unlock succeeded but should not have")
	}
}

// unlike 'checkLockUnblocks', checkTryLockUnblocks will not block on other client but instead
// keep retrying until it gets the lock.
func checkTryLockUnblocks(ctx context.Context, t *testing.T, conn topo.Conn) {
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace")
	unblock := make(chan struct{})
	finished := make(chan struct{})

	duration := 10 * time.Second
	waitUntil := time.Now().Add(duration)
	// TryLock will keep getting NodeExists until lockDescriptor2 unlock itself.
	// It will not wait but immediately return with NodeExists error.
	go func() {
		<-unblock
		for time.Now().Before(waitUntil) {
			lockDescriptor, err := conn.TryLock(ctx, keyspacePath, "unblocks")
			if err != nil {
				if !topo.IsErrType(err, topo.NodeExists) {
					assert.Fail(t, "expected node exists during trylock", err.Error())
					return
				}
				time.Sleep(1 * time.Second)
			} else {
				if err = lockDescriptor.Unlock(ctx); err != nil {
					assert.Fail(t, "Unlock(test_keyspace) failed", err.Error())
					return
				}
				close(finished)
				break
			}
		}
	}()

	// Lock the keyspace.
	lockDescriptor2, err := conn.TryLock(ctx, keyspacePath, "")
	if err != nil {
		require.Fail(t, "Lock(test_keyspace) failed", err.Error())
	}

	// unblock the go routine so it starts waiting
	close(unblock)

	if err = lockDescriptor2.Unlock(ctx); err != nil {
		require.Fail(t, "Unlock(test_keyspace) failed", err.Error())
	}

	timeout := time.After(2 * duration)
	select {
	case <-finished:
	case <-timeout:
		require.Fail(t, "Unlock(test_keyspace) timed out")
	}
}
