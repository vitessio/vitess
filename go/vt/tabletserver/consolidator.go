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

package tabletserver

import (
	"code.google.com/p/vitess/go/cache"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Consolidator struct {
	mu             sync.Mutex
	queries        map[string]*Result
	consolidations *cache.LRUCache
}

func NewConsolidator() *Consolidator {
	self := &Consolidator{queries: make(map[string]*Result), consolidations: cache.NewLRUCache(1000)}
	http.Handle("/debug/consolidations", self)
	return self
}

type Result struct {
	executing    sync.RWMutex
	consolidator *Consolidator
	sql          string
	Result       *QueryResult
	Err          error
}

func (self *Consolidator) Create(sql string) (r *Result, created bool) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if r, ok := self.queries[sql]; ok {
		return r, false
	}
	r = &Result{consolidator: self, sql: sql}
	r.executing.Lock()
	self.queries[sql] = r
	return r, true
}

func (self *Consolidator) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	items := self.consolidations.Items()
	response.Header().Set("Content-Type", "text/plain")
	if items == nil {
		response.Write([]byte("empty\n"))
		return
	}
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(items))))
	for _, v := range items {
		response.Write([]byte(fmt.Sprintf("%v: %s\n", *(v.Value.(*ccount)), v.Key)))
	}
}

func (self *Consolidator) record(sql string) {
	if v, ok := self.consolidations.Get(sql); ok {
		atomic.AddInt64((*int64)(v.(*ccount)), 1)
	} else {
		c := ccount(1)
		self.consolidations.Set(sql, &c)
	}
}

func (self *Result) Broadcast() {
	self.consolidator.mu.Lock()
	defer self.consolidator.mu.Unlock()
	delete(self.consolidator.queries, self.sql)
	self.executing.Unlock()
}

func (self *Result) Wait() {
	self.consolidator.record(self.sql)
	defer waitStats.Record("Consolidations", time.Now())
	self.executing.RLock()
}

type ccount int64

func (self *ccount) Size() int {
	return 1
}
