// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkctl

import "testing"

func TestLifeCycle(t *testing.T) {
	testLifeCycle(t, "255@voltron:2889:3889:2182")
}

func TestLifeCycleGlobal(t *testing.T) {
	testLifeCycle(t, "1255@voltron:2890:3890:2183")
}

func testLifeCycle(t *testing.T, config string) {
	zkConf := MakeZkConfigFromString(config)
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
	zkConf := MakeZkConfigFromString("255@voltron:2889:3889:2182")
	zkd := NewZkd(zkConf)
	var err error

	err = zkd.Init()
	if err != nil {
		t.Fatalf("Init() err: %v", err)
	}
}
