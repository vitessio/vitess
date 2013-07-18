// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakezk

import (
	"sort"
	"testing"
	"time"

	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// Make sure Stat implements the interface.
var _ zk.Stat = stat{}

func TestBasic(t *testing.T) {
	conn := NewConn()
	defer conn.Close()

	// Make sure Conn implements the interface.
	var _ zk.Conn = conn
	if _, err := conn.Create("/zk", "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}

	if _, err := conn.Create("/zk/foo", "foo", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}
	data, _, err := conn.Get("/zk/foo")
	if err != nil {
		t.Fatalf("conn.Get: %v", err)
	}
	if data != "foo" {
		t.Errorf("got %q, wanted %q", data, "foo")
	}

	if _, err := conn.Set("/zk/foo", "bar", -1); err != nil {
		t.Fatalf("conn.Set: %v", err)
	}

	data, _, err = conn.Get("/zk/foo")
	if err != nil {
		t.Fatalf("conn.Get: %v", err)
	}
	if data != "bar" {
		t.Errorf("got %q, wanted %q", data, "bar")
	}

	// Try Set with the wrong version.
	if _, err := conn.Set("/zk/foo", "bar", 0); err == nil {
		t.Error("conn.Set with a wrong version: expected error")
	}

	// Try Get with a node that doesn't exist.
	if _, _, err := conn.Get("/zk/rabarbar"); err == nil {
		t.Error("conn.Get with a node that doesn't exist: expected error")
	}

	// Try Set with a node that doesn't exist.
	if _, err := conn.Set("/zk/barbarbar", "bar", -1); err == nil {
		t.Error("conn.Get with a node that doesn't exist: expected error")
	}

	// Try Create with a node that exists.
	if _, err := conn.Create("/zk/foo", "foo", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err == nil {
		t.Errorf("conn.Create with a node that exists: expected error")
	}
	// Try Create with a node whose parents don't exist.
	if _, err := conn.Create("/a/b/c", "foo", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err == nil {
		t.Errorf("conn.Create with a node whose parents don't exist: expected error")
	}

	if err := conn.Delete("/zk/foo", -1); err != nil {
		t.Errorf("conn.Delete: %v", err)
	}
	stat, err := conn.Exists("/zk/foo")
	if err != nil {
		t.Errorf("conn.Exists: %v", err)
	}
	if stat != nil {
		t.Errorf("/zk/foo should be deleted, got: %v", stat)
	}

}

func TestChildren(t *testing.T) {
	conn := NewConn()
	defer conn.Close()
	nodes := []string{"/zk", "/zk/foo", "/zk/bar"}
	wantChildren := []string{"bar", "foo"}
	for _, path := range nodes {
		if _, err := conn.Create(path, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
			t.Fatalf("conn.Create: %v", err)
		}
	}
	children, _, err := conn.Children("/zk")
	if err != nil {
		t.Fatalf(`conn.Children("/zk"): %v`, err)
	}
	sort.Strings(children)
	if length := len(children); length != 2 {
		t.Errorf("children: got %v, wanted %v", children, wantChildren)
	}

	for i, path := range children {
		if wantChildren[i] != path {
			t.Errorf("children: got %v, wanted %v", children, wantChildren)
			break
		}
	}

}

func TestWatches(t *testing.T) {
	conn := NewConn()
	defer conn.Close()

	// Creating sends an event to ExistsW.

	stat, watch, err := conn.ExistsW("/zk")
	if err != nil {
		t.Errorf("conn.ExistsW: %v", err)
	}
	if stat != nil {
		t.Errorf("stat is not nil: %v", stat)
	}

	if _, err := conn.Create("/zk", "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}

	fireWatch(t, watch)

	// Creating a child sends an event to ChildrenW.
	_, _, watch, err = conn.ChildrenW("/zk")
	if err != nil {
		t.Errorf(`conn.ChildrenW("/zk"): %v`, err)
	}
	if _, err := conn.Create("/zk/foo", "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}

	fireWatch(t, watch)
	// Updating sends an event to GetW.

	_, _, watch, err = conn.GetW("/zk")
	if err != nil {
		t.Errorf(`conn.GetW("/zk"): %v`, err)
	}

	if _, err := conn.Set("/zk", "foo", -1); err != nil {
		t.Errorf("conn.Set /zk: %v", err)
	}
	fireWatch(t, watch)

	// Deleting sends an event to ExistsW and to ChildrenW of the
	// parent.
	_, watch, err = conn.ExistsW("/zk/foo")
	if err != nil {
		t.Errorf("conn.ExistsW: %v", err)
	}

	_, _, parentWatch, err := conn.ChildrenW("/zk")
	if err != nil {
		t.Errorf(`conn.ChildrenW("/zk"): %v`, err)
	}

	if err := conn.Delete("/zk/foo", -1); err != nil {
		t.Errorf("conn.Delete: %v", err)
	}

	fireWatch(t, watch)
	fireWatch(t, parentWatch)

}

func fireWatch(t *testing.T, watch <-chan zookeeper.Event) zookeeper.Event {
	timer := time.NewTimer(50 * time.Millisecond)
	select {
	case event := <-watch:
		// TODO(szopa): Figure out what's the exact type of
		// event.
		return event
	case <-timer.C:
		t.Errorf("watch didn't get event")
	}
	panic("unreachable")
}

func TestSequence(t *testing.T) {
	conn := NewConn()
	defer conn.Close()
	if _, err := conn.Create("/zk", "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}

	newPath, err := conn.Create("/zk/", "", zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		t.Errorf("conn.Create: %v", err)
	}
	if wanted := "/zk/0000000001"; newPath != wanted {
		t.Errorf("new path: got %q, wanted %q", newPath, wanted)
	}

	newPath, err = conn.Create("/zk/", "", zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		t.Errorf("conn.Create: %v", err)
	}

	if wanted := "/zk/0000000002"; newPath != wanted {
		t.Errorf("new path: got %q, wanted %q", newPath, wanted)
	}

	if err := conn.Delete("/zk/0000000002", -1); err != nil {
		t.Fatalf("conn.Delete: %v", err)
	}

	newPath, err = conn.Create("/zk/", "", zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		t.Errorf("conn.Create: %v", err)
	}

	if wanted := "/zk/0000000003"; newPath != wanted {
		t.Errorf("new path: got %q, wanted %q", newPath, wanted)
	}

}
