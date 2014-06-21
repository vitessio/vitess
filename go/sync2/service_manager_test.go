// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync2

import (
	"testing"
	"time"
)

func TestServiceManager(t *testing.T) {
	activated := AtomicInt64(0)
	service := func(svm *ServiceManager) {
		if !activated.CompareAndSwap(0, 1) {
			t.Fatalf("service called more than once")
		}
		for svm.State() == SERVICE_RUNNING {
			time.Sleep(10 * time.Millisecond)
		}
		if !activated.CompareAndSwap(1, 0) {
			t.Fatalf("service ended more than once")
		}
	}
	var sm ServiceManager
	if sm.StateName() != "Stopped" {
		t.Errorf("want Stopped, got %s", sm.StateName())
	}
	result := sm.Go(service)
	if !result {
		t.Errorf("want true, got false")
	}
	if sm.StateName() != "Running" {
		t.Errorf("want Running, got %s", sm.StateName())
	}
	time.Sleep(5 * time.Millisecond)
	if val := activated.Get(); val != 1 {
		t.Errorf("want 1, got %d", val)
	}
	result = sm.Go(service)
	if result {
		t.Errorf("want false, got true")
	}
	result = sm.Stop()
	if !result {
		t.Errorf("want true, got false")
	}
	if val := activated.Get(); val != 0 {
		t.Errorf("want 0, got %d", val)
	}
	result = sm.Stop()
	if result {
		t.Errorf("want false, got true")
	}
	sm.state.Set(SERVICE_SHUTTING_DOWN)
	if sm.StateName() != "ShuttingDown" {
		t.Errorf("want ShuttingDown, got %s", sm.StateName())
	}
}
