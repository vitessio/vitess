// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/pools"
)

func testFactory() (pools.Resource, error) {
	return &sandboxConn{}, nil
}

func TestInfiniPool(t *testing.T) {
	ifp := NewInfiniPool(testFactory, 1, 1*time.Second)
	if a := ifp.connections.Available(); a != 1 {
		t.Errorf("want 1, got %v", a)
	}
	c1, _ := ifp.Get()
	if a := ifp.connections.Available(); a != 0 {
		t.Errorf("want 0, got %v", a)
	}
	c2, _ := ifp.Get()
	c2.Recycle()
	if a := ifp.connections.Available(); a != 0 {
		t.Errorf("want 0, got %v", a)
	}
	c2, _ = ifp.Get()
	c2.Close()
	if a := ifp.connections.Available(); a != 0 {
		t.Errorf("want 0, got %v", a)
	}
	c1.Recycle()
	if a := ifp.connections.Available(); a != 1 {
		t.Errorf("want 1, got %v", a)
	}
	c3, _ := ifp.Get()
	if c1.TabletConn != c3.TabletConn {
		t.Errorf("%#v != %#v", c1, c3)
	}
	c3.Close()
	if a := ifp.connections.Available(); a != 1 {
		t.Errorf("want 1, got %v", a)
	}
	c4, _ := ifp.Get()
	if c1.TabletConn == c4.TabletConn {
		t.Errorf("%#v == %#v", c1, c4)
	}
}
