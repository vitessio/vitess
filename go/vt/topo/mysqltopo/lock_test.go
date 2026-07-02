/*
Copyright 2025 The Vitess Authors.

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

package mysqltopo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
)

// TestLockOwnershipNotStolen verifies that a lock descriptor whose row
// expired and was re-acquired by another process cannot validate, extend, or
// delete the new holder's lock. Check/Unlock/heartbeat must match the exact
// row this descriptor inserted (path AND contents), not just the path —
// otherwise a stalled holder resuming after expiry silently breaks the new
// holder's mutual exclusion.
func TestLockOwnershipNotStolen(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	ctx := context.Background()

	// Create the directory node so LockWithTTL's existence pre-check passes.
	_, err := server.Create(ctx, "locktest/somefile", []byte("data"))
	require.NoError(t, err)

	// Holder A acquires the lock.
	lockA, err := server.Lock(ctx, "locktest", "holder-a")
	require.NoError(t, err)
	require.NoError(t, lockA.Check(ctx))

	// Simulate holder A stalling past its TTL: force-expire its row so the
	// next acquisition reaps it (this is what the expiry-based reaping does
	// for a genuinely stalled process, without waiting out the real TTL).
	_, err = server.db.ExecContext(ctx,
		"UPDATE topo_locks SET expires_at = DATE_SUB(NOW(), INTERVAL 60 SECOND) WHERE path = ?",
		server.resolvePath("locktest"))
	require.NoError(t, err)

	// Holder B (a different process in real life) acquires the same path.
	lockB, err := server.TryLock(ctx, "locktest", "holder-b")
	require.NoError(t, err, "the expired lock must be reapable by a new acquirer")
	require.NoError(t, lockB.Check(ctx))

	// A's descriptor must now be invalid: its row is gone. With a
	// path-only match, this Check would wrongly succeed against B's row and
	// both holders would believe they own the lock.
	require.Error(t, lockA.Check(ctx), "stale descriptor must not validate against the new holder's lock")

	// A's Unlock must not release B's lock. With a path-only match, this
	// DELETE would remove B's live row.
	err = lockA.Unlock(ctx)
	require.True(t, topo.IsErrType(err, topo.NoNode), "stale unlock should report the lock as gone, got: %v", err)
	require.NoError(t, lockB.Check(ctx), "the new holder's lock must survive the stale holder's Unlock")

	require.NoError(t, lockB.Unlock(ctx))
}

// TestLockAcquiredAfterWaitIsNotBornExpired verifies that a blocking lock
// acquisition that waited on a contended path longer than its TTL still
// inserts a row with a future expiry. An expiry computed once before the
// retry loop would already be in the past, making the fresh lock instantly
// reapable by any other acquirer — two processes holding the same lock.
func TestLockAcquiredAfterWaitIsNotBornExpired(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	ctx := context.Background()

	_, err := server.Create(ctx, "waitlock/somefile", []byte("data"))
	require.NoError(t, err)

	// Holder A takes the lock and keeps it beyond B's TTL.
	lockA, err := server.Lock(ctx, "waitlock", "holder-a")
	require.NoError(t, err)

	const ttlB = 1 * time.Second

	type result struct {
		ld  topo.LockDescriptor
		err error
	}
	acquired := make(chan result, 1)
	go func() {
		// Blocking acquisition with a TTL shorter than the wait it is
		// about to endure.
		ld, err := server.LockWithTTL(ctx, "waitlock", "holder-b", ttlB)
		acquired <- result{ld, err}
	}()

	// Hold A for clearly longer than B's TTL, then release.
	time.Sleep(ttlB + 500*time.Millisecond)
	require.NoError(t, lockA.Unlock(ctx))

	var res result
	select {
	case res = <-acquired:
	case <-time.After(10 * time.Second):
		t.Fatal("blocking lock acquisition did not complete")
	}
	require.NoError(t, res.err)

	// B's freshly acquired lock must not be expired (or reapable): a
	// concurrent TryLock must fail, and Check must succeed.
	require.NoError(t, res.ld.Check(ctx),
		"a lock acquired after waiting past its TTL must not be born expired")
	_, err = server.TryLock(ctx, "waitlock", "holder-c")
	require.Error(t, err, "the fresh lock must not be reapable by another acquirer")
	require.True(t, topo.IsErrType(err, topo.NodeExists), "expected NodeExists, got: %v", err)

	require.NoError(t, res.ld.Unlock(ctx))
}

// TestUnconditionalUpdateVersionsAreDistinct verifies that concurrent
// unconditional (version=nil) Updates each produce a distinct version. A
// SELECT-then-UPDATE implementation lets two writers read the same version
// and both write version+1; the notification system dedups change events by
// version, so the second change would never be delivered to watchers.
func TestUnconditionalUpdateVersionsAreDistinct(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	ctx := context.Background()

	_, err := server.Create(ctx, "vfile", []byte("v0"))
	require.NoError(t, err)

	const writers = 8
	versions := make(chan int64, writers)
	errs := make(chan error, writers)
	start := make(chan struct{})
	for i := range writers {
		go func(i int) {
			<-start
			v, err := server.Update(ctx, "vfile", []byte{byte('a' + i)}, nil)
			if err != nil {
				errs <- err
				return
			}
			versions <- int64(v.(MySQLVersion))
		}(i)
	}
	close(start)

	seen := make(map[int64]bool)
	for range writers {
		select {
		case err := <-errs:
			t.Fatalf("unconditional update failed: %v", err)
		case v := <-versions:
			require.False(t, seen[v], "two unconditional updates produced the same version %d", v)
			seen[v] = true
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for concurrent updates")
		}
	}
}
