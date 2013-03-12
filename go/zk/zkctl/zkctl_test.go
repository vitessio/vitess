// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkctl

import "testing"

// these test depend on starting and stopping ZK instances,
// but may leave files/processes behind if they don't succeed,
// so some manual cleanup may be required.

func TestLifeCycle(t *testing.T) {
	testLifeCycle(t, "255@voltron:2889:3889:2182", 255)
}

func TestLifeCycleGlobal(t *testing.T) {
	testLifeCycle(t, "1255@voltron:2890:3890:2183", 1255)
}

func testLifeCycle(t *testing.T, config string, myId uint32) {
	zkConf := MakeZkConfigFromString(config, myId)
	zkd := NewZkd(zkConf)
	var err error

	err = zkd.Init()
	if err != nil {
		t.Fatalf("Init() err: %v", err)
	}

	err = zkd.Shutdown()
	if err != nil {
		t.Fatalf("Shutdown() err: %v", err)
	}

	err = zkd.Start()
	if err != nil {
		t.Fatalf("Start() err: %v", err)
	}

	err = zkd.Teardown()
	if err != nil {
		t.Fatalf("Teardown() err: %v", err)
	}
}

func testInit(t *testing.T) {
	zkConf := MakeZkConfigFromString("255@voltron:2889:3889:2182", 0)
	zkd := NewZkd(zkConf)
	var err error

	err = zkd.Init()
	if err != nil {
		t.Fatalf("Init() err: %v", err)
	}
}
