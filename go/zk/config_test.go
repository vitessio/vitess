// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
)

func TestZkConfig(t *testing.T) {
	configPath := fmt.Sprintf("./.zk-test-conf-%v", time.Now().UnixNano())
	defer func() {
		os.Remove(configPath)
	}()

	if err := os.Setenv("ZK_CLIENT_CONFIG", configPath); err != nil {
		t.Errorf("setenv ZK_CLIENT_CONFIG failed: %v", err)
	}
	if err := os.Setenv("ZK_CLIENT_LOCAL_CELL", ""); err != nil {
		t.Errorf("setenv ZK_CLIENT_LOCAL_CELL failed: %v", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		t.Errorf("hostname failed: %v")
	}

	fakeCell := hostname[:2]
	fakeAddr := "localhost:2181"
	configMap := map[string]string{
		fakeCell:              fakeAddr,
		fakeCell + "z:_zkocc": "localhost:2182",
		fakeCell + "-global":  "localhost:2183",
	}

	file, err := os.Create(configPath)
	if err != nil {
		t.Errorf("create failed: %v")
	}
	err = json.NewEncoder(file).Encode(configMap)
	if err != nil {
		t.Errorf("encode failed: %v")
	}
	file.Close()

	// test ZkPathToZkAddr
	for _, path := range []string{"/zk/" + fakeCell, "/zk/" + fakeCell + "/", "/zk/local", "/zk/local/"} {
		zkAddr, err := ZkPathToZkAddr(path, false)
		if err != nil {
			t.Errorf("got error: %v", err.Error())
		}
		if zkAddr != fakeAddr {
			t.Errorf("addr mismatch for path %v %v != %v", path, zkAddr, fakeAddr)
		}
	}

	// test ZkKnownCells
	knownCells := ZkKnownCells(false)
	expectedKnownCells := []string{fakeCell, "global"}
	sort.Strings(expectedKnownCells)
	if len(knownCells) != 2 || knownCells[0] != expectedKnownCells[0] || knownCells[1] != expectedKnownCells[1] {
		t.Errorf("ZkKnownCells(false) failed, expected %v got %v", expectedKnownCells, knownCells)
	}
	knownCells = ZkKnownCells(true)
	if len(knownCells) != 1 || knownCells[0] != fakeCell+"z" {
		t.Errorf("ZkKnownCells(true) failed, expected %v got %v", []string{fakeCell}, knownCells)
	}
}
