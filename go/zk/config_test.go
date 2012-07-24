// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestZkPathToZkAddr(t *testing.T) {
	configPath := fmt.Sprintf("./.zk-test-conf-%v", time.Now().UnixNano())
	defer func() {
		os.Remove(configPath)
	}()

	if err := os.Setenv("ZK_CLIENT_CONFIG", configPath); err != nil {
		t.Errorf("setenv failed: %v", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		t.Errorf("hostname failed: %v")
	}

	fakeCell := hostname[:2]
	fakeAddr := "localhost:2181"
	configMap := map[string]string{fakeCell: fakeAddr}

	file, err := os.Create(configPath)
	if err != nil {
		t.Errorf("create failed: %v")
	}
	err = json.NewEncoder(file).Encode(configMap)
	if err != nil {
		t.Errorf("encode failed: %v")
	}
	file.Close()

	for _, path := range []string{"/zk/" + fakeCell, "/zk/" + fakeCell + "/", "/zk/local", "/zk/local/"} {
		zkAddr := ZkPathToZkAddr(path)
		if zkAddr != fakeAddr {
			t.Errorf("addr mismatch for path %v %v != %v", path, zkAddr, fakeAddr)
		}
	}
}
