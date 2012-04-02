/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
func (self *Numbered) Register(id int64, val interface{}) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if _, ok := self.resources[id]; ok {
		return errors.New("already present")
	}
	self.resources[id] = &numberedWrapper{val, false, time.Now()}
	return nil
}

// Unregiester forgets the specified resource.
// If the resource is not present, it's ignored.
func (self *Numbered) Unregister(id int64) {
	self.mu.Lock()
	defer self.mu.Unlock()
	delete(self.resources, id)
	if len(self.resources) == 0 {
		self.empty.Broadcast()
	}
}

// Get locks the resource for use. If it cannot be found or
// is already in use, it returns an error.
func (self *Numbered) Get(id int64) (val interface{}, err error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	nw, ok := self.resources[id]
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
func (self *Numbered) Put(id int64) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if nw, ok := self.resources[id]; ok {
		nw.inUse = false
	}
}

// GetTimedout returns a list of timedout resources, and locks them.
// It does not return any resources that are already locked.
func (self *Numbered) GetTimedout(timeout time.Duration) (vals []interface{}) {
	self.mu.Lock()
	defer self.mu.Unlock()
	now := time.Now()
	for _, nw := range self.resources {
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
func (self *Numbered) WaitForEmpty() {
	self.mu.Lock()
	defer self.mu.Unlock()
	for len(self.resources) != 0 {
		self.empty.Wait()
	}
}

func (self *Numbered) StatsJSON() string {
	s := self.Stats()
	return fmt.Sprintf("{\"Size\": %v}", s)
}

func (self *Numbered) Stats() (size int) {
	self.mu.Lock()
	defer self.mu.Unlock()
	return len(self.resources)
}
