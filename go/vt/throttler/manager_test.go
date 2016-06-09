// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"reflect"
	"testing"
	"time"
)

func TestManager_Registration(t *testing.T) {
	m := newManager()
	t1, err := newThrottler(m, "t1", "TPS", 1 /* threadCount */, MaxRateModuleDisabled, ReplicationLagModuleDisabled, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.registerThrottler("t1", t1); err == nil {
		t.Fatalf("manager should not accept a duplicate registration of a throttler: %v", err)
	}
	t1.Close()

	// Unregistering an unregistered throttler should log an error.
	m.unregisterThrottler("t1")
}

func TestManager_SetMaxRate(t *testing.T) {
	// Create throttlers.
	m := newManager()
	t1, err := newThrottler(m, "t1", "TPS", 1 /* threadCount */, MaxRateModuleDisabled, ReplicationLagModuleDisabled, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer t1.Close()
	t2, err := newThrottler(m, "t2", "TPS", 1 /* threadCount */, MaxRateModuleDisabled, ReplicationLagModuleDisabled, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer t2.Close()

	// Test SetMaxRate().
	want := []string{"t1", "t2"}
	if got := m.SetMaxRate(23); !reflect.DeepEqual(got, want) {
		t.Errorf("manager did not set the rate on all throttlers. got = %v, want = %v", got, want)
	}
}
