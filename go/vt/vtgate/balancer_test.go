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
	endPoints := make([]topo.EndPoint, 0, 4)
	for i := 0; i < 4; i++ {
		endPoint, _ := b.Get()
		endPoints = append(endPoints, endPoint)
	}
	// Ensure that the same address is not always returned.
	if endPoints[0].Uid == endPoints[1].Uid {
		t.Errorf("ids are equal: %v", endPoints[0])
	}
	// Ensure that the 4th addres is the first one (round-robin).
	if endPoints[0].Uid != endPoints[3].Uid {
		t.Errorf("ids are not equal: %v, %v", endPoints[0], endPoints[3])
	}
}

func TestMarkDown(t *testing.T) {
	start := counter
	b := NewBalancer(endPoints3, 10*time.Millisecond)
	addr, _ := b.Get()
	b.MarkDown(addr.Uid)
	addr, _ = b.Get()
	b.MarkDown(addr.Uid)
	addr1, _ := b.Get()
	addr2, _ := b.Get()
	// Two addresses are marked down. Only one address is avaiilable.
	if addr1.Uid != addr2.Uid {
		t.Errorf("ids are not equal: %v, %v", addr1, addr2)
	}
	addr, _ = b.Get()
	b.MarkDown(addr.Uid)
	startTime := time.Now()
	addr, _ = b.Get()
	// All were marked down. Get should return only after the retry delay.
	if time.Now().Sub(startTime) < (10 * time.Millisecond) {
		t.Errorf("want >10ms, got %v", time.Now().Sub(startTime))
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
			},
			topo.EndPoint{
				Uid:  1,
				Host: "1",
			},
			topo.EndPoint{
				Uid:  2,
				Host: "2",
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
	b.MarkDown(1)
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
}
