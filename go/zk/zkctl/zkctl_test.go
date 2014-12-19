// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkctl

import (
	"fmt"
	"os"
	"path"
	"testing"
)

func getUUID(t *testing.T) string {
	f, err := os.Open("/dev/urandom")
	if err != nil {
		t.Fatalf("os.Open(/dev/urandom): %v", err)
	}
	b := make([]byte, 16)
	f.Read(b)
	f.Close()
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// these test depend on starting and stopping ZK instances,
// but may leave files/processes behind if they don't succeed,
// so some manual cleanup may be required.

func TestLifeCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	testLifeCycle(t, "255@voltron:2888:3888:2181", 255)
}

func TestLifeCycleGlobal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	testLifeCycle(t, "1255@voltron:2890:3890:2183", 1255)
}

func testLifeCycle(t *testing.T, config string, myID uint32) {
	currentVtDataRoot := os.Getenv("VTDATAROOT")
	vtDataRoot := path.Join(os.TempDir(), fmt.Sprintf("VTDATAROOT_%v", getUUID(t)))
	if err := os.Setenv("VTDATAROOT", vtDataRoot); err != nil {
		t.Fatalf("cannot set VTDATAROOT: %v", err)
	}
	defer os.Setenv("VTDATAROOT", currentVtDataRoot)
	if err := os.Mkdir(vtDataRoot, 0755); err != nil {
		t.Fatalf("cannot create VTDATAROOT directory")
	}
	defer func() {
		if err := os.RemoveAll(vtDataRoot); err != nil {
			t.Errorf("cannot remove test VTDATAROOT directory: %v", err)
		}
	}()
	zkConf := MakeZkConfigFromString(config, myID)
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
