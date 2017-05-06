/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pools

import (
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
	purpose     string
	timeCreated time.Time
	timeUsed    time.Time
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
		return fmt.Errorf("already present")
	}
	now := time.Now()
	nu.resources[id] = &numberedWrapper{
		val:         val,
		timeCreated: now,
		timeUsed:    now,
	}
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

// Get locks the resource for use. It accepts a purpose as a string.
// If it cannot be found, it returns a "not found" error. If in use,
// it returns a "in use: purpose" error.
func (nu *Numbered) Get(id int64, purpose string) (val interface{}, err error) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	nw, ok := nu.resources[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	if nw.inUse {
		return nil, fmt.Errorf("in use: %s", nw.purpose)
	}
	nw.inUse = true
	nw.purpose = purpose
	return nw.val, nil
}

// Put unlocks a resource for someone else to use.
func (nu *Numbered) Put(id int64) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	if nw, ok := nu.resources[id]; ok {
		nw.inUse = false
		nw.purpose = ""
		nw.timeUsed = time.Now()
	}
}

// GetAll returns the list of all resources in the pool.
func (nu *Numbered) GetAll() (vals []interface{}) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	vals = make([]interface{}, 0, len(nu.resources))
	for _, nw := range nu.resources {
		vals = append(vals, nw.val)
	}
	return vals
}

// GetOutdated returns a list of resources that are older than age, and locks them.
// It does not return any resources that are already locked.
func (nu *Numbered) GetOutdated(age time.Duration, purpose string) (vals []interface{}) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	now := time.Now()
	for _, nw := range nu.resources {
		if nw.inUse {
			continue
		}
		if nw.timeCreated.Add(age).Sub(now) <= 0 {
			nw.inUse = true
			nw.purpose = purpose
			vals = append(vals, nw.val)
		}
	}
	return vals
}

// GetIdle returns a list of resurces that have been idle for longer
// than timeout, and locks them. It does not return any resources that
// are already locked.
func (nu *Numbered) GetIdle(timeout time.Duration, purpose string) (vals []interface{}) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	now := time.Now()
	for _, nw := range nu.resources {
		if nw.inUse {
			continue
		}
		if nw.timeUsed.Add(timeout).Sub(now) <= 0 {
			nw.inUse = true
			nw.purpose = purpose
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
	return fmt.Sprintf("{\"Size\": %v}", nu.Size())
}

func (nu *Numbered) Size() (size int64) {
	nu.mu.Lock()
	defer nu.mu.Unlock()
	return int64(len(nu.resources))
}
