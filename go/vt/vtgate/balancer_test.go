// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"testing"
	"time"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	RetryDelay = time.Duration(1 * time.Second)
)

var counter = 0

func endPoints3() (*topodatapb.EndPoints, error) {
	counter++
	return &topodatapb.EndPoints{
		Entries: []*topodatapb.EndPoint{
			{
				Uid:  0,
				Host: "0",
				PortMap: map[string]int32{
					"vt": 1,
				},
			},
			{
				Uid:  1,
				Host: "1",
				PortMap: map[string]int32{
					"vt": 2,
				},
			},
			{
				Uid:  2,
				Host: "2",
				PortMap: map[string]int32{
					"vt": 3,
				},
			},
		},
	}, nil
}

func TestRandomness(t *testing.T) {
	for i := 0; i < 100; i++ {
		b := NewBalancer(endPoints3, RetryDelay)
		endPoints, _ := b.Get()
		// Ensure that you don't always get the first element at front
		// in the balancer.
		if endPoints[0].Uid == 0 {
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
		{endPoint: &topodatapb.EndPoint{Uid: 0}},
		{endPoint: &topodatapb.EndPoint{Uid: 1}},
		{endPoint: &topodatapb.EndPoint{Uid: 2}},
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

func endPointsError() (*topodatapb.EndPoints, error) {
	return nil, fmt.Errorf("expected error")
}

func endPointsNone() (*topodatapb.EndPoints, error) {
	return nil, nil
}

func TestGetAddressesFail(t *testing.T) {
	b := NewBalancer(endPointsError, RetryDelay)
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
	b := NewBalancer(endPoints3, RetryDelay)
	firstEndPoints, _ := b.Get()
	for i := 0; i < 100; i++ {
		endPoints, _ := b.Get()
		if endPoints[0].Uid == firstEndPoints[0].Uid {
			continue
		}
		return
	}
	// Ensure that the same address is not always returned.
	t.Errorf("ids are equal: %v", firstEndPoints[0])
}

func TestMarkDown(t *testing.T) {
	start := counter
	retryDelay := 100 * time.Millisecond
	b := NewBalancer(endPoints3, retryDelay)
	addrs, _ := b.Get()
	b.MarkDown(addrs[0].Uid, "")
	addrs, _ = b.Get()
	b.MarkDown(addrs[0].Uid, "")
	addrs1, _ := b.Get()
	addrs2, _ := b.Get()
	// Two addresses are marked down. Only one address is avaiilable.
	if addrs1[0].Uid != addrs2[0].Uid {
		t.Errorf("ids are not equal: %v, %v", addrs1[0], addrs2[0])
	}
	addrs, _ = b.Get()
	b.MarkDown(addrs[0].Uid, "")
	// All were marked down. Get should return immediately with empty endpoints.
	done := make(chan struct{})
	go func() {
		addrs, _ = b.Get()
		if len(addrs) != 0 {
			t.Errorf("Get() returned endpoints, want 0, got %v", len(addrs))
		}
		time.Sleep(retryDelay)
		addrs, _ = b.Get()
		if len(addrs) == 0 {
			t.Errorf("Get() returned no endpoints, want >0, got %v", len(addrs))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Errorf("Get() is stuck in Sleep()")
	}
	if addrs[0].Host == "" {
		t.Errorf("want non-empty")
	}
	// Ensure end points were refreshed, counter should have gone up.
	if start == counter {
		t.Errorf("want %v < %v", start, counter)
	}
}

var addrNum uint32 = 10

func endPointsMorph() (*topodatapb.EndPoints, error) {
	addrNum++
	return &topodatapb.EndPoints{
		Entries: []*topodatapb.EndPoint{
			{
				Uid:  addrNum,
				Host: fmt.Sprintf("%d", addrNum),
				PortMap: map[string]int32{
					"vt": 1,
				},
			},
			{
				Uid:  1,
				Host: "1",
				PortMap: map[string]int32{
					"vt": int32(addrNum),
				},
			},
			{
				Uid:  2,
				Host: "2",
				PortMap: map[string]int32{
					"vt": 3,
				},
			},
		},
	}, nil
}

func TestRefresh(t *testing.T) {
	b := NewBalancer(endPointsMorph, RetryDelay)
	b.refresh()
	index := findAddrNode(b.addressNodes, 11)
	// "11" should be found in the list.
	if index == -1 {
		t.Errorf("want other than -1: %v", index)
	}
	// "1" should be in the list with port 11
	portStart := b.addressNodes[findAddrNode(b.addressNodes, 1)].endPoint.PortMap["vt"]
	if portStart != 11 {
		t.Errorf("want 11, got %v", portStart)
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
	portNew := b.addressNodes[index].endPoint.PortMap["vt"]
	if portNew != 12 {
		t.Errorf("want 12, got %v", portNew)
	}
}
