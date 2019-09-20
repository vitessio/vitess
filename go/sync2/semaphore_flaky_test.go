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

package sync2

import (
	"testing"
	"time"
)

func TestSemaNoTimeout(t *testing.T) {
	s := NewSemaphore(1, 0)
	s.Acquire()
	released := false
	go func() {
		time.Sleep(10 * time.Millisecond)
		released = true
		s.Release()
	}()
	s.Acquire()
	if !released {
		t.Errorf("release: false, want true")
	}
}

func TestSemaTimeout(t *testing.T) {
	s := NewSemaphore(1, 5*time.Millisecond)
	s.Acquire()
	go func() {
		time.Sleep(10 * time.Millisecond)
		s.Release()
	}()
	if s.Acquire() {
		t.Errorf("Acquire: true, want false")
	}
	time.Sleep(10 * time.Millisecond)
	if !s.Acquire() {
		t.Errorf("Acquire: false, want true")
	}
}

func TestSemaTryAcquire(t *testing.T) {
	s := NewSemaphore(1, 0)
	if !s.TryAcquire() {
		t.Errorf("TryAcquire: false, want true")
	}
	if s.TryAcquire() {
		t.Errorf("TryAcquire: true, want false")
	}
	s.Release()
	if !s.TryAcquire() {
		t.Errorf("TryAcquire: false, want true")
	}
}
