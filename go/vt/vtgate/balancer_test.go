// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"testing"
	"time"
)

var (
	RETRY_DELAY = time.Duration(1 * time.Second)
)

var counter = 0

func endPoints3() ([]string, error) {
	counter++
	return []string{"0", "1", "2"}, nil
}

func TestRandomness(t *testing.T) {
	for i := 0; i < 100; i++ {
		b := NewBalancer(endPoints3, RETRY_DELAY)
		addr, _ := b.Get()
		// Ensure that you don't always get the first element
		// in the balancer.
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
	// The middle node "1" was deleted.
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

func endPointsNone() ([]string, error) {
	return nil, nil
}

func TestGetAddressesFail(t *testing.T) {
	b := NewBalancer(endPointsError, RETRY_DELAY)
	_, err := b.Get()
	// Ensure that end point errors are returned correctly.
	want := "expected error"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}

	b.getAddresses = endPointsNone
	_, err = b.Get()
	// Ensure no available addresses is treated as error
	want = "no available addresses"
	if err == nil || err.Error() != want {
		t.Errorf("want nil, got %v", err)
	}

	b.getAddresses = endPoints3
	_, err = b.Get()
	// Ensure no error is returned if end point doesn't fail.
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestGetSimple(t *testing.T) {
	b := NewBalancer(endPoints3, RETRY_DELAY)
	addrs := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		addr, _ := b.Get()
		addrs = append(addrs, addr)
	}
	// Ensure that the same address is not always returned.
	if addrs[0] == addrs[1] {
		t.Errorf("ids are equal: %v", addrs[0])
	}
	// Ensure that the 4th addres is the first one (round-robin).
	if addrs[0] != addrs[3] {
		t.Errorf("ids are not equal: %d, %d", addrs[0], addrs[3])
	}
}

func TestMarkDown(t *testing.T) {
	start := counter
	b := NewBalancer(endPoints3, 10*time.Millisecond)
	addr, _ := b.Get()
	b.MarkDown(addr)
	addr, _ = b.Get()
	b.MarkDown(addr)
	addr1, _ := b.Get()
	addr2, _ := b.Get()
	// Two addresses are marked down. Only one address is avaiilable.
	if addr1 != addr2 {
		t.Errorf("ids are not equal: %v, %v", addr1, addr2)
	}
	addr, _ = b.Get()
	b.MarkDown(addr)
	startTime := time.Now()
	addr, _ = b.Get()
	// All were marked down. Get should return only after the retry delay.
	if time.Now().Sub(startTime) < (10 * time.Millisecond) {
		t.Errorf("want >10ms, got %v", time.Now().Sub(startTime))
	}
	if addr == "" {
		t.Errorf("want non-empty")
	}
	// Ensure end points were refreshed, counter should have gone up.
	if start == counter {
		t.Errorf("want %v < %v", start, counter)
	}
}

var addrNum = 10

func endPointsMorph() ([]string, error) {
	addrNum++
	return []string{fmt.Sprintf("%d", addrNum), "1", "2"}, nil
}

func TestRefresh(t *testing.T) {
	b := NewBalancer(endPointsMorph, RETRY_DELAY)
	b.refresh()
	index := findAddrNode(b.addressNodes, "11")
	// "11" should be found in the list.
	if index == -1 {
		t.Errorf("want other than -1: %v", index)
	}
	b.MarkDown("1")
	b.refresh()
	// "11" should not be found. It should be "12" now.
	index = findAddrNode(b.addressNodes, "11")
	if index != -1 {
		t.Errorf("want -1, got %v", index)
	}
	index = findAddrNode(b.addressNodes, "12")
	if index == -1 {
		t.Errorf("got other than -1: %v", index)
	}
	index = findAddrNode(b.addressNodes, "1")
	// "1" should be marked down (non-zero timeRetry)
	if b.addressNodes[index].timeRetry.IsZero() {
		t.Errorf("want non-zero, got 0")
	}
}
