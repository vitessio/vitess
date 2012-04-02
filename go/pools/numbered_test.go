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
	"testing"
	"time"
)

func TestNumbered(t *testing.T) {
	id := int64(0)
	p := NewNumbered()

	var err error
	if err = p.Register(id, id); err != nil {
		t.Errorf("Error %v", err)
	}
	if err = p.Register(id, id); err.Error() != "already present" {
		t.Errorf("Expecting 'already present', received '%v'", err)
	}
	var v interface{}
	if v, err = p.Get(id); err != nil {
		t.Errorf("Error %v", err)
	}
	if v.(int64) != id {
		t.Errorf("Expecting %v, received %v", id, v.(int64))
	}
	if v, err = p.Get(id); err.Error() != "in use" {
		t.Errorf("Expecting 'in use', received '%v'", err)
	}
	p.Put(id)
	if v, err = p.Get(1); err.Error() != "not found" {
		t.Errorf("Expecting 'not found', received '%v'", err)
	}
	p.Unregister(1) // Should not fail
	p.Unregister(0)

	p.Register(id, id)
	id++
	p.Register(id, id)
	time.Sleep(3e8)
	id++
	p.Register(id, id)
	time.Sleep(1e8)
	vals := p.GetTimedout(time.Duration(2e8))
	if len(vals) != 2 {
		t.Errorf("Expecting 2, received %v", len(vals))
	}
	for _, v := range vals {
		p.Unregister(v.(int64))
	}

	if p.Stats() != 1 {
		t.Errorf("Expecting 1, received %v", p.Stats())
	}
	go func() {
		p.Unregister(2)
	}()
	p.WaitForEmpty()
}
