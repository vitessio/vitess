// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type GetAddressesFunc func() ([]string, error)

// Balancer is a simple round-robin load balancer.
// It allows you to temporarily mark down nodes that
// are non-functional.
type Balancer struct {
	mu           sync.Mutex
	addressNodes []*addressStatus
	index        int
	getAddresses GetAddressesFunc
	retryDelay   time.Duration
}

type addressStatus struct {
	Address   string
	timeRetry time.Time
	balancer  *Balancer
}

// NewBalancer creates a Balancer. getAddreses is the function
// it will use to refresh the list of addresses if one of the
// nodes has been marked down. The list of addresses is shuffled.
// retryDelay specifies the minimum time a node will be marked down
// before it will be cleared for a retry.
func NewBalancer(getAddresses GetAddressesFunc, retryDelay time.Duration) *Balancer {
	blc := new(Balancer)
	blc.getAddresses = getAddresses
	blc.retryDelay = retryDelay
	return blc
}

// Get returns a single address that was not recently marked down.
// If it finds an address that was down for longer than retryDelay,
// it refreshes the list of addresses and returns the next available
// node. If all addresses are marked down, it waits and retries.
// If a refresh fails, it returns an error.
func (blc *Balancer) Get() (address string, err error) {
	blc.mu.Lock()
	defer blc.mu.Unlock()

	if len(blc.addressNodes) == 0 {
		err = blc.refresh()
		if err != nil {
			return "", err
		}
	}

outer:
	for {
		for i := range blc.addressNodes {
			index := (blc.index + i + 1) % len(blc.addressNodes)
			addrNode := blc.addressNodes[index]
			if addrNode.timeRetry.IsZero() {
				blc.index = index
				return addrNode.Address, nil
			}
			if time.Now().Sub(addrNode.timeRetry) > 0 {
				addrNode.timeRetry = time.Time{}
				err = blc.refresh()
				if err != nil {
					return "", err
				}
				continue outer
			}
		}
		// Allow mark downs to hapen while sleeping.
		blc.mu.Unlock()
		time.Sleep(blc.retryDelay + (1 * time.Millisecond))
		blc.mu.Lock()
	}
	panic("unreachable")
}

// MarkDown marks the specified address down. Such addresses
// will not be used by Balancer for the duration of retryDelay.
func (blc *Balancer) MarkDown(address string) {
	blc.mu.Lock()
	defer blc.mu.Unlock()
	if index := findAddrNode(blc.addressNodes, address); index != -1 {
		blc.addressNodes[index].timeRetry = time.Now().Add(blc.retryDelay)
	}
}

func (blc *Balancer) refresh() error {
	addresses, err := blc.getAddresses()
	if err != nil {
		return err
	}
	// Add new addressNodes
	for _, address := range addresses {
		if index := findAddrNode(blc.addressNodes, address); index == -1 {
			addrNode := &addressStatus{Address: address, balancer: blc}
			blc.addressNodes = append(blc.addressNodes, addrNode)
		}
	}
	// Remove those that went away
	i := 0
	for i < len(blc.addressNodes) {
		if index := findAddress(addresses, blc.addressNodes[i].Address); index == -1 {
			blc.addressNodes = delAddrNode(blc.addressNodes, i)
			continue
		}
		i++
	}
	if len(blc.addressNodes) == 0 {
		return fmt.Errorf("No available addresses")
	}
	shuffle(blc.addressNodes)
	return nil
}

func findAddrNode(addressNodes []*addressStatus, address string) (index int) {
	for i, addrNode := range addressNodes {
		if address == addrNode.Address {
			return i
		}
	}
	return -1
}

func findAddress(addresses []string, address string) (index int) {
	for i, addr := range addresses {
		if address == addr {
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
}
