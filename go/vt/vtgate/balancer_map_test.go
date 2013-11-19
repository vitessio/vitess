// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"testing"
	"time"
)

// This file uses the sandbox_test framework.

func TestSimple(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa")
	blc := blm.Balancer("test_keyspace", "0", "master", 1*time.Second)
	blc2 := blm.Balancer("test_keyspace", "0", "master", 1*time.Second)
	// You should get the same balancer every time for a give set of input keys.
	if blc != blc2 {
		t.Errorf("Balancers don't match, map is %v", blm.balancers)
	}
	blc3 := blm.Balancer("other_keyspace", "0", "master", 1*time.Second)
	// You should get a different balancer when input keys are different.
	if blc == blc3 {
		t.Errorf("Balancers match, map is %v", blm.balancers)
	}
	// At least one of the values has to be "0:1"
	for i := 0; i < 3; i++ {
		endPoint, err := blc.Get()
		if err == nil && endPoint.Uid == 0 {
			return
		}
	}
	t.Errorf("address 0:1 not found")
}

func TestTopoError(t *testing.T) {
	resetSandbox()
	endPointMustFail = 1
	blm := NewBalancerMap(new(sandboxTopo), "aa")
	blc := blm.Balancer("test_keyspace", "", "master", 1*time.Second)
	got, err := blc.Get()
	// If topo serv is down, you should get an error.
	if got.Host != "" {
		t.Errorf("want empty, got %s", got)
	}
	want := "endpoints fetch error: topo error"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}
