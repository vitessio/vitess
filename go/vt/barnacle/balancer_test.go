// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"fmt"
	"testing"
	"time"
)

var counter = 0

func endPoints3() ([]*VTAddress, error) {
	counter++
	addrs := make([]*VTAddress, 3)
	addrs[0] = &VTAddress{Id: 0, Address: "localhost:1000"}
	addrs[1] = &VTAddress{Id: 1, Address: "localhost:1001"}
	addrs[2] = &VTAddress{Id: 2, Address: "localhost:1002"}
	return addrs, nil
}

func TestNew(t *testing.T) {
	for i := 0; i < 100; i++ {
		b := NewBalancer(endPoints3)
		if b.index >= 3 || b.index < 0 {
			t.Errorf("want <3, got %d", b.index)
		}
	}
}

func TestAddrs(t *testing.T) {
	addrs, _ := endPoints3()
	goti := findAddress(addrs, 1)
	if goti != 1 {
		t.Errorf("want 1, got %d", goti)
	}
	addrs = delAddress(addrs, 1)
	if len(addrs) != 2 {
		t.Errorf("want 2, got %d", len(addrs))
	}
	if addrs[1].Id != 2 {
		t.Errorf("want 2, got %d", addrs[1].Id)
	}
}

func endPointsError() ([]*VTAddress, error) {
	return nil, fmt.Errorf("expected error")
}

func TestFail(t *testing.T) {
	b := NewBalancer(endPointsError)
	addr := b.Get()
	if addr != nil {
		t.Errorf("want nil, got %v", addr)
	}
}

func TestGetSimple(t *testing.T) {
	b := NewBalancer(endPoints3)
	addrs := make([]*VTAddress, 0, 4)
	for i := 0; i < 4; i++ {
		addrs = append(addrs, b.Get())
	}
	if addrs[0].Id == addrs[1].Id {
		t.Errorf("ids are equal: %d", addrs[0].Id)
	}
	if addrs[0].Id != addrs[3].Id {
		t.Errorf("ids are not equal: %d, %d", addrs[0].Id, addrs[3].Id)
	}
}

func TestMarkDown(t *testing.T) {
	start := counter
	b := NewBalancer(endPoints3)
	if start+1 != counter {
		t.Errorf("want %v, got %v", start+1, counter)
	}
	for i := 0; i < 2; i++ {
		b.Get().MarkDown()
	}
	addr1 := b.Get()
	addr2 := b.Get()
	if addr1.Id != addr2.Id {
		t.Errorf("ids are not equal: %d, %d", addr1.Id, addr2.Id)
	}
	b.Get().MarkDown()
	addr := b.Get()
	if addr != nil {
		t.Errorf("want nil, got %v", addr)
	}
	time.Sleep(1100 * time.Millisecond)
	addr = b.Get()
	if addr == nil {
		t.Errorf("want non-nil, got nil")
	}
	if start+2 != counter {
		t.Errorf("want %v, got %v", start+2, counter)
	}
}

func TestRefresh(t *testing.T) {
	start := counter
	b := NewBalancer(endPoints3)
	if start+1 != counter {
		t.Errorf("want %v, got %v", start+1, counter)
	}
	for i := 0; i < 3; i++ {
		b.Get().MarkDown()
	}
	b.Refresh()
	addr := b.Get()
	if addr == nil {
		t.Errorf("want non-nil, got nil")
	}
	if start+2 != counter {
		t.Errorf("want %v, got %v", start+2, counter)
	}
}
