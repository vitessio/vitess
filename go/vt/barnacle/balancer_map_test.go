// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"testing"
	"time"
)

// This file uses the sandbox_test framework.

func TestSimple(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	blc := blm.Balancer("test_keyspace", "0", "master", 1*time.Second)
	blc2 := blm.Balancer("test_keyspace", "0", "master", 1*time.Second)
	if blc != blc2 {
		t.Errorf("Balancers don't match, map is %v", blm.balancers)
	}
	blc3 := blm.Balancer("other_keyspace", "0", "master", 1*time.Second)
	if blc == blc3 {
		t.Errorf("Balancers match, map is %v", blm.balancers)
	}
	for i := 0; i < 3; i++ {
		addr, _ := blc.Get()
		if addr == "0:1" {
			return
		}
	}
	t.Errorf("address 0:1 not found")
}

func TestPortError(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "noport")
	blc := blm.Balancer("test_keyspace", "0", "master", 1*time.Second)
	got, err := blc.Get()
	if got != "" {
		t.Errorf("want empty, got %s", got)
	}
	if err.Error() != "named port noport not found in map[vt:1]" {
		t.Errorf("want named port noport not found in map[vt:1], got %v", err)
	}
}

func TestTopoError(t *testing.T) {
	resetSandbox()
	endPointMustFail = 1
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	blc := blm.Balancer("test_keyspace", "", "master", 1*time.Second)
	got, err := blc.Get()
	if got != "" {
		t.Errorf("want empty, got %s", got)
	}
	if err == nil || err.Error() != "topo error" {
		t.Errorf("want topo error, got %v", err)
	}
}
