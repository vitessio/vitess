// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
)

var (
	RETRY_DELAY = time.Duration(1 * time.Second)
)

var counter = 0

func endPoints3() (*topo.EndPoints, error) {
	counter++
	return &topo.EndPoints{
		Entries: []topo.EndPoint{
			topo.EndPoint{
				Uid:  0,
				Host: "0",
				NamedPortMap: map[string]int{
					"vt": 1,
				},
			},
			topo.EndPoint{
				Uid:  1,
				Host: "1",
				NamedPortMap: map[string]int{
					"vt": 2,
				},
			},
			topo.EndPoint{
				Uid:  2,
				Host: "2",
				NamedPortMap: map[string]int{
					"vt": 3,
				},
			},
		},
	}, nil
}

func TestRandomness(t *testing.T) {
	for i := 0; i < 100; i++ {
		b := NewBalancer(endPoints3, RETRY_DELAY)
		endPoint, _ := b.Get()
		// Ensure that you don't always get the first element
		// in the balancer.
		if endPoint.Uid == 0 {
			continue
		}
		return
	}
	t.Errorf("end points are not shuffled")
}

func TestFindAddress(t *testing.T) {
	addrs, _ := endPoints3()
	goti := findAddress(addrs, 1)
	if goti != 1 {
		t.Errorf("want 1, got %d", goti)
	}
}

func TestFindDeleteAddrNode(t *testing.T) {
	addrNodes := []*addressStatus{
		{endPoint: topo.EndPoint{Uid: 0}},
		{endPoint: topo.EndPoint{Uid: 1}},
		{endPoint: topo.EndPoint{Uid: 2}},
	}
	goti := findAddrNode(addrNodes, 1)
	if goti != 1 {
		t.Errorf("want 1, got %d", goti)
	}
	addrNodes = delAddrNode(addrNodes, 1)
	// The middle node "1" was deleted.
	if len(addrNodes) != 2 {
		t.Errorf("want 2, got %d", len(addrNodes))
	}
	if addrNodes[1].endPoint.Uid != 2 {
		t.Errorf("want 2, got %v", addrNodes[1].endPoint.Uid)
	}
}

func endPointsError() (*topo.EndPoints, error) {
	return nil, fmt.Errorf("expected error")
}

func endPointsNone() (*topo.EndPoints, error) {
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

	b.getEndPoints = endPointsNone
	_, err = b.Get()
	// Ensure no available addresses is treated as error
	want = "no available addresses"
	if err == nil || err.Error() != want {
		t.Errorf("want nil, got %v", err)
	}

	b.getEndPoints = endPoints3
	_, err = b.Get()
	// Ensure no error is returned if end point doesn't fail.
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestGetSimple(t *testing.T) {
	b := NewBalancer(endPoints3, RETRY_DELAY)
	firstEndPoint, _ := b.Get()
	for i := 0; i < 100; i++ {
		endPoint, _ := b.Get()
		if endPoint.Uid == firstEndPoint.Uid {
			continue
		}
		return
	}
	// Ensure that the same address is not always returned.
	t.Errorf("ids are equal: %v", firstEndPoint)
}

func TestMarkDown(t *testing.T) {
	start := counter
	retryDelay := 100 * time.Millisecond
	b := NewBalancer(endPoints3, retryDelay)
	addr, _ := b.Get()
	startTime := time.Now()
	b.MarkDown(addr.Uid, "")
	addr, _ = b.Get()
	b.MarkDown(addr.Uid, "")
	addr1, _ := b.Get()
	addr2, _ := b.Get()
	// Two addresses are marked down. Only one address is avaiilable.
	if addr1.Uid != addr2.Uid {
		t.Errorf("ids are not equal: %v, %v", addr1, addr2)
	}
	addr, _ = b.Get()
	b.MarkDown(addr.Uid, "")
	// All were marked down. Get should return only after the retry delay since first markdown.
	done := make(chan struct{})
	go func() {
		addr, _ = b.Get()
		if got := time.Now().Sub(startTime); got < retryDelay {
			t.Errorf("Get() returned too soon, want >= %v, got %v", retryDelay, got)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Errorf("Get() is stuck in Sleep()")
	}
	if addr.Host == "" {
		t.Errorf("want non-empty")
	}
	// Ensure end points were refreshed, counter should have gone up.
	if start == counter {
		t.Errorf("want %v < %v", start, counter)
	}
}

var addrNum uint32 = 10

func endPointsMorph() (*topo.EndPoints, error) {
	addrNum++
	return &topo.EndPoints{
		Entries: []topo.EndPoint{
			topo.EndPoint{
				Uid:  addrNum,
				Host: fmt.Sprintf("%d", addrNum),
				NamedPortMap: map[string]int{
					"vt": 1,
				},
			},
			topo.EndPoint{
				Uid:  1,
				Host: "1",
				NamedPortMap: map[string]int{
					"vt": int(addrNum),
				},
			},
			topo.EndPoint{
				Uid:  2,
				Host: "2",
				NamedPortMap: map[string]int{
					"vt": 3,
				},
			},
		},
	}, nil
}

func TestRefresh(t *testing.T) {
	b := NewBalancer(endPointsMorph, RETRY_DELAY)
	b.refresh()
	index := findAddrNode(b.addressNodes, 11)
	// "11" should be found in the list.
	if index == -1 {
		t.Errorf("want other than -1: %v", index)
	}
	// "1" should be in the list with port 11
	port_start := b.addressNodes[findAddrNode(b.addressNodes, 1)].endPoint.NamedPortMap["vt"]
	if port_start != 11 {
		t.Errorf("want 11, got %v", port_start)
	}
	b.MarkDown(1, "")
	b.refresh()
	// "11" should not be found. It should be "12" now.
	index = findAddrNode(b.addressNodes, 11)
	if index != -1 {
		t.Errorf("want -1, got %v", index)
	}
	index = findAddrNode(b.addressNodes, 12)
	if index == -1 {
		t.Errorf("got other than -1: %v", index)
	}
	index = findAddrNode(b.addressNodes, 1)
	// "1" should be marked down (non-zero timeRetry)
	if b.addressNodes[index].timeRetry.IsZero() {
		t.Errorf("want non-zero, got 0")
	}
	// "1" should have the updated port 12
	port_new := b.addressNodes[index].endPoint.NamedPortMap["vt"]
	if port_new != 12 {
		t.Errorf("want 12, got %v", port_new)
	}
}
