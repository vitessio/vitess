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
	"testing"
	"time"
)

var lastId int64

type TestResource struct {
	num    int64
	closed bool
}

func (self *TestResource) Close() {
	self.closed = true
}

func (self *TestResource) IsClosed() bool {
	return self.closed
}

func PoolFactory() (Resource, error) {
	lastId++
	return &TestResource{lastId, false}, nil
}

func FailFactory() (Resource, error) {
	return nil, errors.New("Failed")
}

func TestPool(t *testing.T) {
	lastId = 0
	p := NewRoundRobin(5, time.Duration(10e9))
	p.Open(PoolFactory)
	defer p.Close()

	for i := 0; i < 2; i++ {
		r, err := p.TryGet()
		if err != nil {
			t.Errorf("TryGet failed: %v", err)
		}
		if r.(*TestResource).num != 1 {
			t.Errorf("Expecting 1, received %d", r.(*TestResource).num)
		}
		p.Put(r)
	}
	// p = [1]

	all := make([]Resource, 5)
	for i := 0; i < 5; i++ {
		if all[i], _ = p.TryGet(); all[i] == nil {
			t.Errorf("TryGet failed with nil")
		}
	}
	// all = [1-5], p is empty
	if none, _ := p.TryGet(); none != nil {
		t.Errorf("TryGet failed with non-nil")
	}

	ch := make(chan bool)
	go ResourceWait(p, t, ch)
	time.Sleep(1e8)
	for i := 0; i < 5; i++ {
		p.Put(all[i])
	}
	// p = [1-5]
	<-ch
	// p = [1-5]
	if p.waitCount != 1 {
		t.Errorf("Expecting 1, received %d", p.waitCount)
	}

	for i := 0; i < 5; i++ {
		all[i], _ = p.Get()
	}
	// all = [1-5], p is empty
	all[0].(*TestResource).Close()
	for i := 0; i < 5; i++ {
		p.Put(all[i])
	}
	// p = [2-5]

	for i := 0; i < 4; i++ {
		r, _ := p.Get()
		if r.(*TestResource).num != int64(i+2) {
			t.Errorf("Expecting %d, received %d", i+2, r.(*TestResource).num)
		}
		p.Put(r)
	}

	p.SetCapacity(3)
	// p = [2-4]
	if p.size != 3 {
		t.Errorf("Expecting 3, received %d", p.size)
	}

	p.SetIdleTimeout(time.Duration(1e8))
	time.Sleep(2e8)
	r, _ := p.Get()
	if r.(*TestResource).num != 6 {
		t.Errorf("Expecting 6, received %d", r.(*TestResource).num)
	}
	p.Put(r)
	// p = [6]

	r, _ = p.Get()
	// p is empty
	p.Open(FailFactory)
	if _, err := p.Get(); err.Error() != "Failed" {
		t.Errorf("Expecting Failed, received %c", err)
	}
	p.Put(r)
	// p = [6]
}

func ResourceWait(p *RoundRobin, t *testing.T, ch chan bool) {
	for i := 0; i < 5; i++ {
		if r, err := p.Get(); err != nil {
			t.Errorf("TryGet failed: %v", err)
		} else if r.(*TestResource).num != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, r.(*TestResource).num)
		} else {
			p.Put(r)
		}
	}
	ch <- true
}
