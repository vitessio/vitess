// Package test contains utilities to test topo.Impl
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// timeUntilLockIsTaken is the time to wait until a lock is taken.
// We haven't found a better simpler way to guarantee a routine is stuck
// waiting for a topo lock than sleeping that amount.
var timeUntilLockIsTaken = 10 * time.Millisecond

// CheckKeyspaceLock checks we can take a keyspace lock as expected.
func CheckKeyspaceLock(ctx context.Context, t *testing.T, ts topo.Impl) {
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	checkKeyspaceLockTimeout(ctx, t, ts)
	checkKeyspaceLockMissing(ctx, t, ts)
	checkKeyspaceLockUnblocks(ctx, t, ts)
}

func checkKeyspaceLockTimeout(ctx context.Context, t *testing.T, ts topo.Impl) {
	lockPath, err := ts.LockKeyspaceForAction(ctx, "test_keyspace", "fake-content")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction: %v", err)
	}

	// test we can't take the lock again
	fastCtx, cancel := context.WithTimeout(ctx, timeUntilLockIsTaken)
	if _, err := ts.LockKeyspaceForAction(fastCtx, "test_keyspace", "unused-fake-content"); err != topo.ErrTimeout {
		t.Fatalf("LockKeyspaceForAction(again): %v", err)
	}
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(timeUntilLockIsTaken)
		cancel()
	}()
	if _, err := ts.LockKeyspaceForAction(interruptCtx, "test_keyspace", "unused-fake-content"); err != topo.ErrInterrupted {
		t.Fatalf("LockKeyspaceForAction(interrupted): %v", err)
	}

	if err := ts.UnlockKeyspaceForAction(ctx, "test_keyspace", lockPath, "fake-results"); err != nil {
		t.Fatalf("UnlockKeyspaceForAction(): %v", err)
	}

	// test we can't unlock again
	if err := ts.UnlockKeyspaceForAction(ctx, "test_keyspace", lockPath, "fake-results"); err == nil {
		t.Fatalf("UnlockKeyspaceForAction(again) worked")
	}
}

// checkKeyspaceLockMissing makes sure we can't lock a non-existing keyspace
func checkKeyspaceLockMissing(ctx context.Context, t *testing.T, ts topo.Impl) {
	if _, err := ts.LockKeyspaceForAction(ctx, "test_keyspace_666", "fake-content"); err == nil {
		t.Fatalf("LockKeyspaceForAction(test_keyspace_666) worked for non-existing keyspace")
	}
}

// checkKeyspaceLockUnblocks makes sure that a routine waiting on a lock
// is unblocked when another routine frees the lock
func checkKeyspaceLockUnblocks(ctx context.Context, t *testing.T, ts topo.Impl) {
	unblock := make(chan struct{})
	finished := make(chan struct{})

	// as soon as we're unblocked, we try to lock the keyspace
	go func() {
		<-unblock
		lockPath, err := ts.LockKeyspaceForAction(ctx, "test_keyspace", "fake-content")
		if err != nil {
			t.Fatalf("LockKeyspaceForAction(test_keyspace) failed: %v", err)
		}
		if err = ts.UnlockKeyspaceForAction(ctx, "test_keyspace", lockPath, "fake-results"); err != nil {
			t.Fatalf("UnlockKeyspaceForAction(test_keyspace): %v", err)
		}
		close(finished)
	}()

	// lock the keyspace
	lockPath2, err := ts.LockKeyspaceForAction(ctx, "test_keyspace", "fake-content")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction(test_keyspace) failed: %v", err)
	}

	// unblock the go routine so it starts waiting
	close(unblock)

	// sleep for a while so we're sure the go routine is blocking
	time.Sleep(timeUntilLockIsTaken)

	if err = ts.UnlockKeyspaceForAction(ctx, "test_keyspace", lockPath2, "fake-results"); err != nil {
		t.Fatalf("UnlockKeyspaceForAction(test_keyspace): %v", err)
	}

	timeout := time.After(10 * time.Second)
	select {
	case <-finished:
	case <-timeout:
		t.Fatalf("unlocking timed out")
	}
}

// CheckShardLock checks we can take a shard lock
func CheckShardLock(ctx context.Context, t *testing.T, ts topo.Impl) {
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "10-20", &topodatapb.Shard{
		KeyRange: newKeyRange("10-20"),
	}); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	checkShardLockTimeout(ctx, t, ts)
	checkShardLockMissing(ctx, t, ts)
	checkShardLockUnblocks(ctx, t, ts)
}

func checkShardLockTimeout(ctx context.Context, t *testing.T, ts topo.Impl) {
	lockPath, err := ts.LockShardForAction(ctx, "test_keyspace", "10-20", "fake-content")
	if err != nil {
		t.Fatalf("LockShardForAction: %v", err)
	}

	// test we can't take the lock again
	fastCtx, cancel := context.WithTimeout(ctx, timeUntilLockIsTaken)
	if _, err := ts.LockShardForAction(fastCtx, "test_keyspace", "10-20", "unused-fake-content"); err != topo.ErrTimeout {
		t.Fatalf("LockShardForAction(again): %v", err)
	}
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(timeUntilLockIsTaken)
		cancel()
	}()
	if _, err := ts.LockShardForAction(interruptCtx, "test_keyspace", "10-20", "unused-fake-content"); err != topo.ErrInterrupted {
		t.Fatalf("LockShardForAction(interrupted): %v", err)
	}

	if err := ts.UnlockShardForAction(ctx, "test_keyspace", "10-20", lockPath, "fake-results"); err != nil {
		t.Fatalf("UnlockShardForAction(): %v", err)
	}

	// test we can't unlock again
	if err := ts.UnlockShardForAction(ctx, "test_keyspace", "10-20", lockPath, "fake-results"); err == nil {
		t.Error("UnlockShardForAction(again) worked")
	}
}

func checkShardLockMissing(ctx context.Context, t *testing.T, ts topo.Impl) {
	// test we can't lock a non-existing shard
	if _, err := ts.LockShardForAction(ctx, "test_keyspace", "20-30", "fake-content"); err == nil {
		t.Fatalf("LockShardForAction(test_keyspace/20-30) worked for non-existing shard")
	}
}

// checkShardLockUnblocks makes sure that a routine waiting on a lock
// is unblocked when another routine frees the lock
func checkShardLockUnblocks(ctx context.Context, t *testing.T, ts topo.Impl) {
	unblock := make(chan struct{})
	finished := make(chan struct{})

	// as soon as we're unblocked, we try to lock the shard
	go func() {
		<-unblock
		lockPath, err := ts.LockShardForAction(ctx, "test_keyspace", "10-20", "fake-content")
		if err != nil {
			t.Fatalf("LockShardForAction(test_keyspace, 10-20) failed: %v", err)
		}
		if err = ts.UnlockShardForAction(ctx, "test_keyspace", "10-20", lockPath, "fake-results"); err != nil {
			t.Fatalf("UnlockShardForAction(test_keyspace, 10-20): %v", err)
		}
		close(finished)
	}()

	// lock the shard
	lockPath2, err := ts.LockShardForAction(ctx, "test_keyspace", "10-20", "fake-content")
	if err != nil {
		t.Fatalf("LockShardForAction(test_keyspace, 10-20) failed: %v", err)
	}

	// unblock the go routine so it starts waiting
	close(unblock)

	// sleep for a while so we're sure the go routine is blocking
	time.Sleep(timeUntilLockIsTaken)

	if err = ts.UnlockShardForAction(ctx, "test_keyspace", "10-20", lockPath2, "fake-results"); err != nil {
		t.Fatalf("UnlockShardForAction(test_keyspace, 10-20): %v", err)
	}

	timeout := time.After(10 * time.Second)
	select {
	case <-finished:
	case <-timeout:
		t.Fatalf("unlocking timed out")
	}
}
