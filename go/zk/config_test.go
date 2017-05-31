/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	configPath := fmt.Sprintf("%v/.zk-test-conf-%v", os.TempDir(), time.Now().UnixNano())

	if err := os.Setenv("ZK_CLIENT_CONFIG", configPath); err != nil {
		t.Errorf("setenv ZK_CLIENT_CONFIG failed: %v", err)
	}
	if err := os.Setenv("ZK_CLIENT_LOCAL_CELL", ""); err != nil {
		t.Errorf("setenv ZK_CLIENT_LOCAL_CELL failed: %v", err)
	}
	fakeCell := GuessLocalCell()
	t.Logf("fakeCell: %v", fakeCell)

	fakeAddr := "localhost:2181"
	t.Logf("fakeAddr: %v", fakeAddr)

	configMap := map[string]string{
		fakeCell:             fakeAddr,
		fakeCell + "-global": "localhost:2183",
	}
	t.Logf("configMap: %+v", configMap)

	file, err := os.Create(configPath)
	if err != nil {
		t.Errorf("create failed: %v", err)
	}

	defer func() {
		os.Remove(configPath)
	}()

	err = json.NewEncoder(file).Encode(configMap)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	file.Close()

	// test ZkPathToZkAddr
	for _, path := range []string{"/zk/" + fakeCell, "/zk/" + fakeCell + "/", "/zk/local", "/zk/local/"} {
		zkAddr, err := ZkPathToZkAddr(path)
		if err != nil {
			t.Errorf("ZkPathToZkAddr(%v, false): %v", path, err.Error())
		}
		if zkAddr != fakeAddr {
			t.Errorf("addr mismatch for path %v %v != %v", path, zkAddr, fakeAddr)
		}
	}

	// test ZkKnownCells
	knownCells, err := ZkKnownCells()
	if err != nil {
		t.Errorf("unexpected error from ZkKnownCells(): %v", err)
	}
	expectedKnownCells := []string{fakeCell, "global"}
	sort.Strings(expectedKnownCells)
	if len(knownCells) != 2 || knownCells[0] != expectedKnownCells[0] || knownCells[1] != expectedKnownCells[1] {
		t.Errorf("ZkKnownCells(false) failed, expected %v got %v", expectedKnownCells, knownCells)
	}
}

func TestZkCellFromZkPathInvalid(t *testing.T) {
	// The following paths should be rejected so the invalid cell name doesn't
	// cause problems down the line.
	inputs := []string{
		"/zk",
		"bad/zk/path",
		"/wrongprefix/cell",
		"/zk//emptycellname",
		"/zk/bad-cell-name/",
	}
	for _, input := range inputs {
		if _, err := ZkCellFromZkPath(input); err == nil {
			t.Errorf("expected error for ZkCellFromZkPath(%q), got none", input)
		}
	}
}
