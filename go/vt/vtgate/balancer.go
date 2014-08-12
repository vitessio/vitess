// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

type GetEndPointsFunc func() (*topo.EndPoints, error)

// Balancer is a simple round-robin load balancer.
// It allows you to temporarily mark down nodes that
// are non-functional.
type Balancer struct {
	mu           sync.Mutex
	addressNodes []*addressStatus
	index        int
	getEndPoints GetEndPointsFunc
	retryDelay   time.Duration
}

type addressStatus struct {
	endPoint  topo.EndPoint
	timeRetry time.Time
	balancer  *Balancer
}

// NewBalancer creates a Balancer. getAddreses is the function
// it will use to refresh the list of addresses if one of the
// nodes has been marked down. The list of addresses is shuffled.
// retryDelay specifies the minimum time a node will be marked down
// before it will be cleared for a retry.
func NewBalancer(getEndPoints GetEndPointsFunc, retryDelay time.Duration) *Balancer {
	blc := new(Balancer)
	blc.getEndPoints = getEndPoints
	blc.retryDelay = retryDelay
	return blc
}

// Get returns a single endpoint that was not recently marked down.
// If it finds an address that was down for longer than retryDelay,
// it refreshes the list of addresses and returns the next available
// node. If all addresses are marked down, it waits and retries.
// If a refresh fails, it returns an error.
func (blc *Balancer) Get() (endPoint topo.EndPoint, err error) {
	blc.mu.Lock()
	defer blc.mu.Unlock()

	if len(blc.addressNodes) == 0 {
		err = blc.refresh()
		if err != nil {
			return topo.EndPoint{}, err
		}
	}

outer:
	for {
		for i := range blc.addressNodes {
			index := (blc.index + i + 1) % len(blc.addressNodes)
			addrNode := blc.addressNodes[index]
			if addrNode.timeRetry.IsZero() {
				blc.index = index
				return addrNode.endPoint, nil
			}
			if time.Now().Sub(addrNode.timeRetry) > 0 {
				addrNode.timeRetry = time.Time{}
				err = blc.refresh()
				if err != nil {
					return topo.EndPoint{}, err
				}
				continue outer
			}
		}
		// Allow mark downs to happen while sleeping.
		blc.mu.Unlock()
		time.Sleep(blc.retryDelay + (1 * time.Millisecond))
		blc.mu.Lock()
	}
}

// MarkDown marks the specified address down. Such addresses
// will not be used by Balancer for the duration of retryDelay.
func (blc *Balancer) MarkDown(uid uint32, reason string) {
	blc.mu.Lock()
	defer blc.mu.Unlock()
	if index := findAddrNode(blc.addressNodes, uid); index != -1 {
		log.Infof("Marking down %v at %+v (%v)", uid, blc.addressNodes[index].endPoint, reason)
		blc.addressNodes[index].timeRetry = time.Now().Add(blc.retryDelay)
	}
}

func (blc *Balancer) refresh() error {
	endPoints, err := blc.getEndPoints()
	if err != nil {
		return err
	}
	// Add new addressNodes
	if endPoints != nil {
		for _, endPoint := range endPoints.Entries {
			if index := findAddrNode(blc.addressNodes, endPoint.Uid); index == -1 {
				addrNode := &addressStatus{
					endPoint: endPoint,
					balancer: blc,
				}
				blc.addressNodes = append(blc.addressNodes, addrNode)
			} else {
				blc.addressNodes[index].endPoint = endPoint
			}
		}
	}
	// Remove those that went away
	i := 0
	for i < len(blc.addressNodes) {
		if index := findAddress(endPoints, blc.addressNodes[i].endPoint.Uid); index == -1 {
			blc.addressNodes = delAddrNode(blc.addressNodes, i)
			continue
		}
		i++
	}
	if len(blc.addressNodes) == 0 {
		return fmt.Errorf("no available addresses")
	}
	shuffle(blc.addressNodes)
	return nil
}

func findAddrNode(addressNodes []*addressStatus, uid uint32) (index int) {
	for i, addrNode := range addressNodes {
		if uid == addrNode.endPoint.Uid {
			return i
		}
	}
	return -1
}

func findAddress(endPoints *topo.EndPoints, uid uint32) (index int) {
	if endPoints == nil {
		return -1
	}
	for i, endPoint := range endPoints.Entries {
		if uid == endPoint.Uid {
			return i
		}
	}
	return -1
}

func delAddrNode(addressNodes []*addressStatus, index int) []*addressStatus {
	copy(addressNodes[index:len(addressNodes)-1], addressNodes[index+1:])
	return addressNodes[:len(addressNodes)-1]
}

// shuffle uses the Fisher-Yates algorithm.
func shuffle(addressNodes []*addressStatus) {
	index := 0
	for i := len(addressNodes) - 1; i > 0; i-- {
		index = int(rand.Int63()) % (i + 1)
		addressNodes[i], addressNodes[index] = addressNodes[index], addressNodes[i]
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
	idGen.Set(rand.Int63())
}
