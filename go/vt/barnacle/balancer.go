// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"math/rand"
	"sync"
	"time"

	log "github.com/golang/glog"
)

var (
	RETRY_DELAY = time.Duration(1 * time.Second)
)

type GetEndPointsFunc func() ([]*VTAddress, error)

type Balancer struct {
	mu           sync.Mutex
	addresses    []*VTAddress
	index        int
	getEndPoints GetEndPointsFunc
}

type VTAddress struct {
	Id       int64
	Address  string
	timeDown time.Time
	balancer *Balancer
}

func NewBalancer(getEndPoints GetEndPointsFunc) *Balancer {
	blc := new(Balancer)
	blc.getEndPoints = getEndPoints
	blc.refresh()
	if len(blc.addresses) != 0 {
		blc.index = int(rand.Int63()) % len(blc.addresses)
	}
	return blc
}

func (blc *Balancer) Get() *VTAddress {
	blc.mu.Lock()
	defer blc.mu.Unlock()
	if len(blc.addresses) == 0 {
		return nil
	}
	for _ = range blc.addresses {
		blc.index = (blc.index + 1) % len(blc.addresses)
		addr := blc.addresses[blc.index]
		if addr.timeDown.IsZero() {
			return addr
		}
		if time.Now().Sub(addr.timeDown) > RETRY_DELAY {
			addr.timeDown = time.Time{}
			blc.refresh()
			return addr
		}
	}
	return nil
}

func (blc *Balancer) Refresh() {
	blc.mu.Lock()
	defer blc.mu.Unlock()
	blc.refresh()
	for _, addr := range blc.addresses {
		addr.timeDown = time.Time{}
	}
}

func (blc *Balancer) refresh() {
	addresses, err := blc.getEndPoints()
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	// Add new addresses
	for _, addr := range addresses {
		if index := findAddress(blc.addresses, addr.Id); index == -1 {
			addr.balancer = blc
			blc.addresses = append(blc.addresses, addr)
		} else if blc.addresses[index].Address != addr.Address {
			blc.addresses[index].Address = addr.Address
			blc.addresses[index].timeDown = time.Time{}
		}
	}
	// Remove those that went away
	i := 0
	for i < len(blc.addresses) {
		if index := findAddress(addresses, blc.addresses[i].Id); index == -1 {
			blc.addresses = delAddress(blc.addresses, i)
			continue
		}
		i++
	}
}

func findAddress(addresses []*VTAddress, id int64) (index int) {
	for i, addr := range addresses {
		if id == addr.Id {
			return i
		}
	}
	return -1
}

func delAddress(addresses []*VTAddress, index int) []*VTAddress {
	copy(addresses[index:len(addresses)-1], addresses[index+1:])
	return addresses[:len(addresses)-1]
}

func (vta *VTAddress) MarkDown() {
	// We use the balancer's mutex for this
	vta.balancer.mu.Lock()
	defer vta.balancer.mu.Unlock()
	vta.timeDown = time.Now()
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
