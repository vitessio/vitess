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

package txlimiter

import (
	"testing"

	"vitess.io/vitess/go/vt/callerid"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func resetVariables() {
	rejections.ResetAll()
	rejectionsDryRun.ResetAll()
}

func createCallers(username, principal, component, subcomponent string) (*querypb.VTGateCallerID, *vtrpcpb.CallerID) {
	im := callerid.NewImmediateCallerID(username)
	ef := callerid.NewEffectiveCallerID(principal, component, subcomponent)
	return im, ef
}

func TestTxLimiter_DisabledAllowsAll(t *testing.T) {
	limiter := New(10, 0.1, false, false, false, false, false, false)
	im, ef := createCallers("", "", "", "")
	for i := 0; i < 5; i++ {
		if got, want := limiter.Get(im, ef), true; got != want {
			t.Errorf("Transaction number %d, Get(): got %v, want %v", i, got, want)
		}
	}

}

func TestTxLimiter_LimitsOnlyOffendingUser(t *testing.T) {
	resetVariables()

	// This should allow 3 slots to all users
	newlimiter := New(10, 0.3, true, false, true, false, false, false)
	limiter, ok := newlimiter.(*Impl)
	if !ok {
		t.Fatalf("New returned limiter of unexpected type: got %T, want %T", newlimiter, limiter)
	}
	im1, ef1 := createCallers("user1", "", "", "")
	im2, ef2 := createCallers("user2", "", "", "")

	// user1 uses 3 slots
	for i := 0; i < 3; i++ {
		if got, want := limiter.Get(im1, ef1), true; got != want {
			t.Errorf("Transaction number %d, Get(im1, ef1): got %v, want %v", i, got, want)
		}
	}

	// user1 not allowed to use 4th slot, which increases counter
	if got, want := limiter.Get(im1, ef1), false; got != want {
		t.Errorf("Get(im1, ef1) after using up all allowed attempts: got %v, want %v", got, want)
	}

	key1 := limiter.extractKey(im1, ef1)
	if got, want := rejections.Counts()[key1], int64(1); got != want {
		t.Errorf("Rejections count for %s: got %d, want %d", key1, got, want)
	}

	// user2 uses 3 slots
	for i := 0; i < 3; i++ {
		if got, want := limiter.Get(im2, ef2), true; got != want {
			t.Errorf("Transaction number %d, Get(im2, ef2): got %v, want %v", i, got, want)
		}
	}

	// user2 not allowed to use 4th slot, which increases counter
	if got, want := limiter.Get(im2, ef2), false; got != want {
		t.Errorf("Get(im2, ef2) after using up all allowed attempts: got %v, want %v", got, want)
	}
	key2 := limiter.extractKey(im2, ef2)
	if got, want := rejections.Counts()[key2], int64(1); got != want {
		t.Errorf("Rejections count for %s: got %d, want %d", key2, got, want)
	}

	// user1 releases a slot, which allows to get another
	limiter.Release(im1, ef1)
	if got, want := limiter.Get(im1, ef1), true; got != want {
		t.Errorf("Get(im1, ef1) after releasing: got %v, want %v", got, want)
	}

	// Rejection coutner for user 1 should still be 1.
	if got, want := rejections.Counts()[key1], int64(1); got != want {
		t.Errorf("Rejections count for %s: got %d, want %d", key1, got, want)
	}
}

func TestTxLimiterDryRun(t *testing.T) {
	resetVariables()

	// This should allow 3 slots to all users
	newlimiter := New(10, 0.3, true, true, true, false, false, false)
	limiter, ok := newlimiter.(*Impl)
	if !ok {
		t.Fatalf("New returned limiter of unexpected type: got %T, want %T", newlimiter, limiter)
	}
	im, ef := createCallers("user", "", "", "")
	key := limiter.extractKey(im, ef)

	// uses 3 slots
	for i := 0; i < 3; i++ {
		if got, want := limiter.Get(im, ef), true; got != want {
			t.Errorf("Transaction number %d, Get(im, ef): got %v, want %v", i, got, want)
		}
	}

	// allowed to use 4th slot, but dry run rejection counter increased
	if got, want := limiter.Get(im, ef), true; got != want {
		t.Errorf("Get(im, ef) after using up all allowed attempts: got %v, want %v", got, want)
	}

	if got, want := rejections.Counts()[key], int64(0); got != want {
		t.Errorf("Rejections count for %s: got %d, want %d", key, got, want)
	}
	if got, want := rejectionsDryRun.Counts()[key], int64(1); got != want {
		t.Errorf("RejectionsDryRun count for %s: got %d, want %d", key, got, want)
	}
}
