/*
Copyright 2019 The Vitess Authors.

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
	"path"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// checkLock checks we can lock / unlock as expected. It's using a keyspace
// as the lock target.
func checkTryLock(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	conn, err := ts.ConnForCell(context.Background(), topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell(global) failed: %v", err)
	}

	t.Log("===      checkLockTimeout")
	checkTryLockTimeout(ctx, t, conn)

	t.Log("===      checkLockMissing")
	checkTryLockMissing(ctx, t, conn)

	t.Log("===      checkLockUnblocks")
	checkTryLockUnblocks(ctx, t, conn)
}

func checkTryLockTimeout(ctx context.Context, t *testing.T, conn topo.Conn) {
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace")
	lockDescriptor, err := conn.TryLock(ctx, keyspacePath, "")
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	// We have the lock, list the keyspace directory.
	// It should not contain anything, except Ephemeral files.
	entries, err := conn.ListDir(ctx, keyspacePath, true /*full*/)
	if err != nil {
		t.Fatalf("Listdir(%v) failed: %v", keyspacePath, err)
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
			t.Fatalf("Listdir(%v) failed: %v", p, err)
		}
		for _, e := range entries {
			if e.Ephemeral {
				t.Logf("skipping ephemeral node %v in %v", e, p)
			} else {
				t.Errorf("Entry in %v has non-ephemeral DirEntry: %v", p, e)
			}
		}
	}

	// test we can't take the lock again
	fastCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if _, err := conn.TryLock(fastCtx, keyspacePath, "again"); !topo.IsErrType(err, topo.NodeExists) {
		t.Fatalf("TryLock(again): %v", err)
	}
	cancel()

	// test we can interrupt taking the lock
	interruptCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(1 * time.Second)
		cancel()
	}()
	if _, err := conn.TryLock(interruptCtx, keyspacePath, "interrupted"); !topo.IsErrType(err, topo.NodeExists) {
		t.Fatalf("TryLock(interrupted): %v", err)
	}

	if err := lockDescriptor.Check(ctx); err != nil {
		t.Errorf("Check(): %v", err)
	}

	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock(): %v", err)
	}

	// test we can't unlock again
	if err := lockDescriptor.Unlock(ctx); err == nil {
		t.Fatalf("Unlock(again) worked")
	}
}

// checkLockMissing makes sure we can't lock a non-existing directory.
func checkTryLockMissing(ctx context.Context, t *testing.T, conn topo.Conn) {
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace_666")
	if _, err := conn.TryLock(ctx, keyspacePath, "missing"); err == nil {
		t.Fatalf("TryLock(test_keyspace_666) worked for non-existing keyspace")
	}
}

// checkLockUnblocks makes sure that a routine waiting on a lock
// is unblocked when another routine frees the lock
func checkTryLockUnblocks(ctx context.Context, t *testing.T, conn topo.Conn) {
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace")
	unblock := make(chan struct{})
	finished := make(chan struct{})

	// As soon as we're unblocked, we try to lock the keyspace.
	go func() {
		<-unblock
		waitUntil := time.Now().Add(10 * time.Second)
		for time.Now().Before(waitUntil) {
			lockDescriptor, err := conn.TryLock(ctx, keyspacePath, "unblocks")
			if err != nil {
				if !topo.IsErrType(err, topo.NodeExists) {
					t.Errorf("expected nonode exists during trylock")
				}
				time.Sleep(1 * time.Second)
			} else {
				if err = lockDescriptor.Unlock(ctx); err != nil {
					t.Errorf("Unlock(test_keyspace): %v", err)
				}
				close(finished)
				break
			}
		}
	}()

	// Lock the keyspace.
	lockDescriptor2, err := conn.TryLock(ctx, keyspacePath, "")
	if err != nil {
		t.Fatalf("Lock(test_keyspace) failed: %v", err)
	}

	// unblock the go routine so it starts waiting
	close(unblock)

	// sleep for a while so we're sure the go routine is blocking
	time.Sleep(5 * time.Second)

	if err = lockDescriptor2.Unlock(ctx); err != nil {
		t.Fatalf("Unlock(test_keyspace): %v", err)
	}

	timeout := time.After(10 * time.Second)
	select {
	case <-finished:
	case <-timeout:
		t.Fatalf("unlocking timed out")
	}
}
