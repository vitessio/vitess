// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

func CheckKeyspaceLock(t *testing.T, ts topo.Server) {
	if err := ts.CreateKeyspace("test_keyspace", &topo.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	lockPath, err := ts.LockKeyspaceForAction(ctx, "test_keyspace", "fake-content")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction: %v", err)
	}

	// test we can't take the lock again
	fastCtx, cancel := context.WithTimeout(ctx, time.Second/10)
	if _, err := ts.LockKeyspaceForAction(fastCtx, "test_keyspace", "unused-fake-content"); err != topo.ErrTimeout {
		t.Errorf("LockKeyspaceForAction(again): %v", err)
	}
	cancel()

	// test we can interrupt taking the lock
	go func() {
		time.Sleep(time.Second / 10)
		ctxCancel()
	}()
	if _, err := ts.LockKeyspaceForAction(ctx, "test_keyspace", "unused-fake-content"); err != topo.ErrInterrupted {
		t.Errorf("LockKeyspaceForAction(interrupted): %v", err)
	}

	if err := ts.UnlockKeyspaceForAction("test_keyspace", lockPath, "fake-results"); err != nil {
		t.Errorf("UnlockKeyspaceForAction(): %v", err)
	}

	// test we can't unlock again
	if err := ts.UnlockKeyspaceForAction("test_keyspace", lockPath, "fake-results"); err == nil {
		t.Error("UnlockKeyspaceForAction(again) worked")
	}

	// test we can't lock a non-existing keyspace
	ctx = context.Background()
	if _, err := ts.LockKeyspaceForAction(ctx, "test_keyspace_666", "fake-content"); err == nil {
		t.Errorf("LockKeyspaceForAction(test_keyspace_666) worked for non-existing keyspace")
	}
}

func CheckShardLock(t *testing.T, ts topo.Server) {
	if err := ts.CreateKeyspace("test_keyspace", &topo.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}
	if err := topo.CreateShard(ts, "test_keyspace", "10-20"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	lockPath, err := ts.LockShardForAction(ctx, "test_keyspace", "10-20", "fake-content")
	if err != nil {
		t.Fatalf("LockShardForAction: %v", err)
	}

	// test we can't take the lock again
	fastCtx, cancel := context.WithTimeout(ctx, time.Second/10)
	if _, err := ts.LockShardForAction(fastCtx, "test_keyspace", "10-20", "unused-fake-content"); err != topo.ErrTimeout {
		t.Errorf("LockShardForAction(again): %v", err)
	}
	cancel()

	// test we can interrupt taking the lock
	go func() {
		time.Sleep(time.Second / 2)
		ctxCancel()
	}()
	if _, err := ts.LockShardForAction(ctx, "test_keyspace", "10-20", "unused-fake-content"); err != topo.ErrInterrupted {
		t.Errorf("LockShardForAction(interrupted): %v", err)
	}

	if err := ts.UnlockShardForAction("test_keyspace", "10-20", lockPath, "fake-results"); err != nil {
		t.Errorf("UnlockShardForAction(): %v", err)
	}

	// test we can't unlock again
	if err := ts.UnlockShardForAction("test_keyspace", "10-20", lockPath, "fake-results"); err == nil {
		t.Error("UnlockShardForAction(again) worked")
	}

	// test we can't lock a non-existing shard
	ctx = context.Background()
	if _, err := ts.LockShardForAction(ctx, "test_keyspace", "20-30", "fake-content"); err == nil {
		t.Errorf("LockShardForAction(test_keyspace/20-30) worked for non-existing shard")
	}
}

func CheckSrvShardLock(t *testing.T, ts topo.Server) {
	// make sure we can create the lock even if no directory exists
	ctx, ctxCancel := context.WithCancel(context.Background())
	lockPath, err := ts.LockSrvShardForAction(ctx, "test", "test_keyspace", "10-20", "fake-content")
	if err != nil {
		t.Fatalf("LockSrvShardForAction: %v", err)
	}

	if err := ts.UnlockSrvShardForAction("test", "test_keyspace", "10-20", lockPath, "fake-results"); err != nil {
		t.Errorf("UnlockShardForAction(): %v", err)
	}

	// now take the lock again after the root exists
	lockPath, err = ts.LockSrvShardForAction(ctx, "test", "test_keyspace", "10-20", "fake-content")
	if err != nil {
		t.Fatalf("LockSrvShardForAction: %v", err)
	}

	// test we can't take the lock again
	fastCtx, cancel := context.WithTimeout(ctx, time.Second/10)
	if _, err := ts.LockSrvShardForAction(fastCtx, "test", "test_keyspace", "10-20", "unused-fake-content"); err != topo.ErrTimeout {
		t.Errorf("LockSrvShardForAction(again): %v", err)
	}
	cancel()

	// test we can interrupt taking the lock
	go func() {
		time.Sleep(time.Second / 2)
		ctxCancel()
	}()
	if _, err := ts.LockSrvShardForAction(ctx, "test", "test_keyspace", "10-20", "unused-fake-content"); err != topo.ErrInterrupted {
		t.Errorf("LockSrvShardForAction(interrupted): %v", err)
	}

	// unlock now
	if err := ts.UnlockSrvShardForAction("test", "test_keyspace", "10-20", lockPath, "fake-results"); err != nil {
		t.Errorf("UnlockSrvShardForAction(): %v", err)
	}

	// test we can't unlock again
	if err := ts.UnlockSrvShardForAction("test", "test_keyspace", "10-20", lockPath, "fake-results"); err == nil {
		t.Error("UnlockSrvShardForAction(again) worked")
	}

}
