// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ratelimiter

import (
	"testing"
	"time"
)

func TestLimiter1(t *testing.T) {
	rl := NewRateLimiter(1, 10*time.Millisecond)
	var result bool
	result = rl.Allow()
	if !result {
		t.Error("Allow: false, want true")
	}
	result = rl.Allow()
	if result {
		t.Error("Allow: true, want false")
	}

	time.Sleep(11 * time.Millisecond)
	result = rl.Allow()
	if !result {
		t.Error("Allow: false, want true")
	}
	result = rl.Allow()
	if result {
		t.Error("Allow: true, want false")
	}
}

func TestLimiter2(t *testing.T) {
	rl := NewRateLimiter(2, 10*time.Millisecond)
	var result bool
	for i := 0; i < 2; i++ {
		result = rl.Allow()
		if !result {
			t.Errorf("Allow(%d): false, want true", i)
		}
	}
	result = rl.Allow()
	if result {
		t.Error("Allow: true, want false")
	}

	time.Sleep(11 * time.Millisecond)
	for i := 0; i < 2; i++ {
		result = rl.Allow()
		if !result {
			t.Errorf("Allow(%d): false, want true", i)
		}
	}
	result = rl.Allow()
	if result {
		t.Error("Allow: true, want false")
	}
}
