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

package ratelimiter

import (
	"testing"
	"time"
)

func TestLimiter1(t *testing.T) {
	rl := NewRateLimiter(1, 10*time.Millisecond)
	result := rl.Allow()
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
