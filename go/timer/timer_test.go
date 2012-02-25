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

package timer

import (
	"testing"
	"time"
)

const (
	one     = time.Duration(1e9)
	half    = time.Duration(500e6)
	quarter = time.Duration(250e6)
	tenth   = time.Duration(100e6)
)

func TestWait(t *testing.T) {
	start := time.Now()
	timer := NewTimer(quarter)
	result := timer.Next()
	if !result {
		t.Errorf("Want true, got false")
	}
	if start.Add(quarter).After(time.Now()) {
		t.Error("Next returned too soon")
	}
}

func TestReset(t *testing.T) {
	start := time.Now()
	timer := NewTimer(quarter)
	ch := next(timer)
	timer.SetInterval(tenth)
	result := <-ch
	if !result {
		t.Errorf("Want true, got false")
	}
	if start.Add(tenth).After(time.Now()) {
		t.Error("Next returned too soon")
	}
	if start.Add(quarter).Before(time.Now()) {
		t.Error("Next returned too late")
	}
}

func TestIndefinite(t *testing.T) {
	start := time.Now()
	timer := NewTimer(0)
	ch := next(timer)
	timer.TriggerAfter(quarter)
	result := <-ch
	if !result {
		t.Errorf("Want true, got false")
	}
	if start.Add(quarter).After(time.Now()) {
		t.Error("Next returned too soon")
	}
}

func TestClose(t *testing.T) {
	start := time.Now()
	timer := NewTimer(0)
	ch := next(timer)
	timer.Close()
	result := <-ch
	if result {
		t.Errorf("Want false, got true")
	}
	if start.Add(tenth).Before(time.Now()) {
		t.Error("Next returned too late")
	}
}

func next(timer *Timer) chan bool {
	ch := make(chan bool)
	go func() {
		ch <- timer.Next()
	}()
	return ch
}
