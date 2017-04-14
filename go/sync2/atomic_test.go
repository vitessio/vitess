// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync2

import (
	"testing"
)

func TestAtomicString(t *testing.T) {
	var s AtomicString
	if s.Get() != "" {
		t.Errorf("want empty, got %s", s.Get())
	}
	s.Set("a")
	if s.Get() != "a" {
		t.Errorf("want a, got %s", s.Get())
	}
	if s.CompareAndSwap("b", "c") {
		t.Errorf("want false, got true")
	}
	if s.Get() != "a" {
		t.Errorf("want a, got %s", s.Get())
	}
	if !s.CompareAndSwap("a", "c") {
		t.Errorf("want true, got false")
	}
	if s.Get() != "c" {
		t.Errorf("want c, got %s", s.Get())
	}
}

func TestAtomicBool(t *testing.T) {
	b := NewAtomicBool(true)
	if !b.Get() {
		t.Error("b.Get: false, want true")
	}
	b.Set(false)
	if b.Get() {
		t.Error("b.Get: true, want false")
	}
	b.Set(true)
	if !b.Get() {
		t.Error("b.Get: false, want true")
	}
}
