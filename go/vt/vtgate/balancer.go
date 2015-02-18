// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

var resetDownConnDelay = flag.Duration("reset-down-conn-delay", 10*time.Minute, "delay to reset a marked down tabletconn")

// GetEndPointsFunc defines the callback to topo server.
type GetEndPointsFunc func() (*topo.EndPoints, error)

// Balancer is a simple round-robin load balancer.
// It allows you to temporarily mark down nodes that
// are non-functional.
type Balancer struct {
	mu                 sync.Mutex
	addressNodes       []*addressStatus
	index              int
	getEndPoints       GetEndPointsFunc
	retryDelay         time.Duration
	resetDownConnDelay time.Duration
}

type addressStatus struct {
	endPoint  topo.EndPoint
	timeRetry time.Time
	balancer  *Balancer
}

// NewBalancer creates a Balancer. getAddresses is the function
// it will use to refresh the list of addresses if one of the
// nodes has been marked down. The list of addresses is shuffled.
// retryDelay specifies the minimum time a node will be marked down
// before it will be cleared for a retry.
func NewBalancer(getEndPoints GetEndPointsFunc, retryDelay time.Duration) *Balancer {
	blc := new(Balancer)
	blc.getEndPoints = getEndPoints
	blc.retryDelay = retryDelay
	blc.resetDownConnDelay = *resetDownConnDelay
	return blc
}

// Get returns a single endpoint that was not recently marked down.
// If it finds an address that was down for longer than retryDelay,
// it refreshes the list of addresses and returns the next available
// node. If all addresses are marked down, it waits and retries.
// If a refresh fails, it returns an error.
func (blc *Balancer) Get() (endPoints []topo.EndPoint, err error) {
	blc.mu.Lock()
	defer blc.mu.Unlock()

	// Clear timeRetry if it has been a long time
	for _, addrNode := range blc.addressNodes {
		if time.Now().Sub(addrNode.timeRetry) > blc.resetDownConnDelay {
			addrNode.timeRetry = time.Time{}
		}
	}

	// Get the latest endpoints
	err = blc.refresh()
	if err != nil {
		return []topo.EndPoint{}, err
	}

	// Return all endpoints without markdown and timeRetry < now(),
	// so endpoints just marked down (within retryDelay) are ignored.
	validEndPoints := make([]topo.EndPoint, 0, 1)
	for _, addrNode := range blc.addressNodes {
		if addrNode.timeRetry.IsZero() || addrNode.timeRetry.Before(time.Now()) {
			validEndPoints = append(validEndPoints, addrNode.endPoint)
			continue
		}
		break
	}

	return validEndPoints, nil
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
	// Sort endpoints by timeRetry (from ZERO to largest)
	sort.Sort(AddressList(blc.addressNodes))
	// Randomize endpoints with ZERO timeRetry
	shuffle(blc.addressNodes, findFirstAddrNodeNonZeroTimeRetry(blc.addressNodes))
	return nil
}

// AddressList is the slice of addressStatus.
type AddressList []*addressStatus

func (al AddressList) Len() int {
	return len(al)
}

func (al AddressList) Swap(i, j int) {
	al[i], al[j] = al[j], al[i]
}

func (al AddressList) Less(i, j int) bool {
	return al[i].timeRetry.Before(al[j].timeRetry)
}

func findFirstAddrNodeNonZeroTimeRetry(addressNodes []*addressStatus) (index int) {
	for i, addrNode := range addressNodes {
		if !addrNode.timeRetry.IsZero() {
			return i
		}
	}
	return len(addressNodes)
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
func shuffle(addressNodes []*addressStatus, length int) {
	index := 0
	for i := length - 1; i > 0; i-- {
		index = rand.Intn(i + 1)
		addressNodes[i], addressNodes[index] = addressNodes[index], addressNodes[i]
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
	idGen.Set(rand.Int63())
}
