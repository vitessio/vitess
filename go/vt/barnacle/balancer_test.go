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

func endPoints3() ([]string, error) {
	counter++
	return []string{"0", "1", "2"}, nil
}

func TestRandomness(t *testing.T) {
	for i := 0; i < 100; i++ {
		b := NewBalancer(endPoints3)
		addr := b.Get()
		if addr == "0" {
			continue
		}
		return
	}
	t.Errorf("end points are not shuffled")
}

func TestFindAddress(t *testing.T) {
	addrs, _ := endPoints3()
	goti := findAddress(addrs, "1")
	if goti != 1 {
		t.Errorf("want 1, got %d", goti)
	}
}

func TestFindDeleteAddrNode(t *testing.T) {
	addrNodes := []*addressStatus{
		{Address: "0"},
		{Address: "1"},
		{Address: "2"},
	}
	goti := findAddrNode(addrNodes, "1")
	if goti != 1 {
		t.Errorf("want 1, got %d", goti)
	}
	addrNodes = delAddrNode(addrNodes, 1)
	if len(addrNodes) != 2 {
		t.Errorf("want 2, got %d", len(addrNodes))
	}
	if addrNodes[1].Address != "2" {
		t.Errorf("want 2, got %v", addrNodes[1].Address)
	}
}

func endPointsError() ([]string, error) {
	return nil, fmt.Errorf("expected error")
}

func TestGetAddressesFail(t *testing.T) {
	b := NewBalancer(endPointsError)
	addr := b.Get()
	if addr != "" {
		t.Errorf("want empty, got %v", addr)
	}
}

func TestGetSimple(t *testing.T) {
	b := NewBalancer(endPoints3)
	addrs := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		addrs = append(addrs, b.Get())
	}
	if addrs[0] == addrs[1] {
		t.Errorf("ids are equal: %d", addrs[0])
	}
	if addrs[0] != addrs[3] {
		t.Errorf("ids are not equal: %d, %d", addrs[0], addrs[3])
	}
}

func TestMarkDown(t *testing.T) {
	start := counter
	b := NewBalancer(endPoints3)
	if start+1 != counter {
		t.Errorf("want %v, got %v", start+1, counter)
	}
	for i := 0; i < 2; i++ {
		b.MarkDown(b.Get())
	}
	addr1 := b.Get()
	addr2 := b.Get()
	if addr1 != addr2 {
		t.Errorf("ids are not equal: %v, %v", addr1, addr2)
	}
	b.MarkDown(b.Get())
	addr := b.Get()
	if addr != "" {
		t.Errorf("want empty, got %v", addr)
	}
	time.Sleep(1100 * time.Millisecond)
	addr = b.Get()
	if addr == "" {
		t.Errorf("want non-empty, got nil")
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
		b.MarkDown(b.Get())
	}
	b.Refresh()
	addr := b.Get()
	if addr == "" {
		t.Errorf("want non-empty, got nil")
	}
	if start+2 != counter {
		t.Errorf("want %v, got %v", start+2, counter)
	}
}

var addrNum = 10

func endPointsMorph() ([]string, error) {
	addrNum++
	return []string{fmt.Sprintf("%d", addrNum), "1", "2"}, nil
}

func TestMorph(t *testing.T) {
	b := NewBalancer(endPointsMorph)
	index := findAddrNode(b.addressNodes, "11")
	if index == -1 {
		t.Errorf("got other than -1: %v", index)
	}
	b.MarkDown("1")
	b.refresh()
	index = findAddrNode(b.addressNodes, "11")
	if index != -1 {
		t.Errorf("want -1, got %v", index)
	}
	index = findAddrNode(b.addressNodes, "12")
	if index == -1 {
		t.Errorf("got other than -1: %v", index)
	}
	index = findAddrNode(b.addressNodes, "1")
	if b.addressNodes[index].timeDown.IsZero() {
		t.Errorf("want non-zero, got 0")
	}
	b.Refresh()
	if !b.addressNodes[index].timeDown.IsZero() {
		t.Errorf("want zero, got %v", b.addressNodes[index].timeDown)
	}
}
