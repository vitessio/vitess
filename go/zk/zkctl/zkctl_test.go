// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkctl

import (
	"testing"
)

// This test depend on starting and stopping a ZK instance,
// but may leave files/processes behind if they don't succeed,
// so some manual cleanup may be required.

func TestLifeCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	config := "255@voltron:2888:3888:2181"
	myID := 255

	zkConf := MakeZkConfigFromString(config, uint32(myID))
	zkd := NewZkd(zkConf)
	if err := zkd.Init(); err != nil {
		t.Fatalf("Init() err: %v", err)
	}

	if err := zkd.Shutdown(); err != nil {
		t.Fatalf("Shutdown() err: %v", err)
	}

	if err := zkd.Start(); err != nil {
		t.Fatalf("Start() err: %v", err)
	}

	if err := zkd.Teardown(); err != nil {
		t.Fatalf("Teardown() err: %v", err)
	}
}
