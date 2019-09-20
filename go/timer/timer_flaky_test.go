/*
Copyright 2019 The Vitess Authors.

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

package timer

import (
	"sync/atomic"
	"testing"
	"time"
)

const (
	half    = time.Duration(500e5)
	quarter = time.Duration(250e5)
	tenth   = time.Duration(100e5)
)

var numcalls int32

func f() {
	atomic.AddInt32(&numcalls, 1)
}

func TestWait(t *testing.T) {
	atomic.StoreInt32(&numcalls, 0)
	timer := NewTimer(quarter)
	timer.Start(f)
	defer timer.Stop()
	time.Sleep(tenth)
	if atomic.LoadInt32(&numcalls) != 0 {
		t.Errorf("want 0, received %v", numcalls)
	}
	time.Sleep(quarter)
	if atomic.LoadInt32(&numcalls) != 1 {
		t.Errorf("want 1, received %v", numcalls)
	}
	time.Sleep(quarter)
	if atomic.LoadInt32(&numcalls) != 2 {
		t.Errorf("want 1, received %v", numcalls)
	}
}

func TestReset(t *testing.T) {
	atomic.StoreInt32(&numcalls, 0)
	timer := NewTimer(half)
	timer.Start(f)
	defer timer.Stop()
	timer.SetInterval(quarter)
	time.Sleep(tenth)
	if atomic.LoadInt32(&numcalls) != 0 {
		t.Errorf("want 0, received %v", numcalls)
	}
	time.Sleep(quarter)
	if atomic.LoadInt32(&numcalls) != 1 {
		t.Errorf("want 1, received %v", numcalls)
	}
}

func TestIndefinite(t *testing.T) {
	atomic.StoreInt32(&numcalls, 0)
	timer := NewTimer(0)
	timer.Start(f)
	defer timer.Stop()
	timer.TriggerAfter(quarter)
	time.Sleep(tenth)
	if atomic.LoadInt32(&numcalls) != 0 {
		t.Errorf("want 0, received %v", numcalls)
	}
	time.Sleep(quarter)
	if atomic.LoadInt32(&numcalls) != 1 {
		t.Errorf("want 1, received %v", numcalls)
	}
}
