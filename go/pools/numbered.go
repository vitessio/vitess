// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pools

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Numbered allows you to manage resources by tracking them with numbers.
// There are no interface restrictions on what you can track.
type Numbered struct {
	mu        sync.Mutex
	empty     *sync.Cond // Broadcast when pool becomes empty
	resources map[int64]*numberedWrapper
}

type numberedWrapper struct {
	val         interface{}
	inUse       bool
	timeCreated time.Time
}

func NewNumbered() *Numbered {
	n := &Numbered{resources: make(map[int64]*numberedWrapper)}
	n.empty = sync.NewCond(&n.mu)
	return n
}

// Register starts tracking a resource by the supplied id.
// It does not lock the object.
// It returns an error if the id already exists.
func (nu *Numbered) Register(id int64, val interface{}) error {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	if _, ok := nu.resources[id]; ok {
		return errors.New("already present")
	}
	nu.resources[id] = &numberedWrapper{val, false, time.Now()}
	return nil
}

// Unregiester forgets the specified resource.
// If the resource is not present, it's ignored.
func (nu *Numbered) Unregister(id int64) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	delete(nu.resources, id)
	if len(nu.resources) == 0 {
		nu.empty.Broadcast()
	}
}

// Get locks the resource for use. If it cannot be found or
// is already in use, it returns an error.
func (nu *Numbered) Get(id int64) (val interface{}, err error) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	nw, ok := nu.resources[id]
	if !ok {
		return nil, errors.New("not found")
	}
	if nw.inUse {
		return nil, errors.New("in use")
	}
	nw.inUse = true
	return nw.val, nil
}

// Put unlocks a resource for someone else to use.
func (nu *Numbered) Put(id int64) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	if nw, ok := nu.resources[id]; ok {
		nw.inUse = false
	}
}

// GetTimedout returns a list of timedout resources, and locks them.
// It does not return any resources that are already locked.
func (nu *Numbered) GetTimedout(timeout time.Duration) (vals []interface{}) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	now := time.Now()
	for _, nw := range nu.resources {
		if nw.inUse {
			continue
		}
		if nw.timeCreated.Add(timeout).Sub(now) <= 0 {
			nw.inUse = true
			vals = append(vals, nw.val)
		}
	}
	return vals
}

// WaitForEmpty returns as soon as the pool becomes empty
func (nu *Numbered) WaitForEmpty() {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	for len(nu.resources) != 0 {
		nu.empty.Wait()
	}
}

func (nu *Numbered) StatsJSON() string {
	s := nu.Stats()
	return fmt.Sprintf("{\"Size\": %v}", s)
}

func (nu *Numbered) Stats() (size int) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	return len(nu.resources)
}
