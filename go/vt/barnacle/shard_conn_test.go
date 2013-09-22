// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"testing"
	"time"
)

// This file uses the sandbox_test framework.

func TestShardConnExecute(t *testing.T) {
	// Test topo failure
	resetSandbox()
	endPointMustFail = 1
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sdc := NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
	_, err := sdc.Execute("query", nil)
	if err.Error() != "topo error" {
		t.Errorf("want topo error, got %v", err)
	}
	if endPointCounter != 1 {
		t.Errorf("want 1, got %v", endPointCounter)
	}

	// Test Connect failure
	resetSandbox()
	dialMustFail = 3
	sdc = NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
	_, err = sdc.Execute("query", nil)
	if err.Error() != "conn error" {
		t.Errorf("want conn error, got %v", err)
	}
	if dialCounter != 3 {
		t.Errorf("want 3, got %v", dialCounter)
	}
}
