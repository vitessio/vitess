/*
Copyright 2019 The Vitess Authors.

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

// Package test contains utilities to test topo.Conn
// implementations. If you are testing your implementation, you will
// want to call TopoServerTestSuite in your test method. For an
// example, look at the tests in
// vitess.io/vitess/go/vt/topo/memorytopo.
package test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"vitess.io/vitess/go/testfiles"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// LocalCellName is the cell name used by this test suite.
const LocalCellName = "test"

func newKeyRange(value string) *topodatapb.KeyRange {
	_, result, err := topo.ValidateShardName(value)
	if err != nil {
		panic(err)
	}
	return result
}

// TopoServerTestSuite runs the full topo.Server/Conn test suite.
// The factory method should return a topo.Server that has a single cell
// called LocalCellName.
func TopoServerTestSuite(t *testing.T, factory func() *topo.Server) {
	var ts *topo.Server

	t.Log("=== checkKeyspace")
	ts = factory()
	checkKeyspace(t, ts)
	ts.Close()

	t.Log("=== checkShard")
	ts = factory()
	checkShard(t, ts)
	ts.Close()

	t.Log("=== checkTablet")
	ts = factory()
	checkTablet(t, ts)
	ts.Close()

	t.Log("=== checkShardReplication")
	ts = factory()
	checkShardReplication(t, ts)
	ts.Close()

	t.Log("=== checkSrvKeyspace")
	ts = factory()
	checkSrvKeyspace(t, ts)
	ts.Close()

	t.Log("=== checkSrvVSchema")
	ts = factory()
	checkSrvVSchema(t, ts)
	ts.Close()

	t.Log("=== checkLock")
	ts = factory()
	checkLock(t, ts)
	ts.Close()

	t.Log("=== checkVSchema")
	ts = factory()
	checkVSchema(t, ts)
	ts.Close()

	t.Log("=== checkRoutingRules")
	ts = factory()
	checkRoutingRules(t, ts)
	ts.Close()

	t.Log("=== checkElection")
	ts = factory()
	checkElection(t, ts)
	ts.Close()

	t.Log("=== checkDirectory")
	ts = factory()
	checkDirectory(t, ts)
	ts.Close()

	t.Log("=== checkFile")
	ts = factory()
	checkFile(t, ts)
	ts.Close()

	t.Log("=== checkWatch")
	ts = factory()
	checkWatch(t, ts)
	checkWatchInterrupt(t, ts)
	ts.Close()
}

// StartConsul starts a consul subprocess, and waits for it to be ready.
// Returns the exec.Cmd forked, the config file to remove after the test,
// and the server address to RPC-connect to.
func StartConsul(t *testing.T, authToken string) (*exec.Cmd, string, string) {
	// Create a temporary config file, as ports cannot all be set
	// via command line. The file name has to end with '.json' so
	// we're not using TempFile.
	configDir, err := os.MkdirTemp("", "consul")
	if err != nil {
		t.Fatalf("cannot create temp dir: %v", err)
	}
	defer os.RemoveAll(configDir)

	configFilename := path.Join(configDir, "consul.json")
	configFile, err := os.OpenFile(configFilename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("cannot create tempfile: %v", err)
	}

	// Create the JSON config, save it.
	port := testfiles.GoVtTopoConsultopoPort
	config := map[string]interface{}{
		"ports": map[string]int{
			"dns":      port,
			"http":     port + 1,
			"serf_lan": port + 2,
			"serf_wan": port + 3,
		},
	}

	if authToken != "" {
		config["datacenter"] = "vitess"
		config["acl_datacenter"] = "vitess"
		config["acl_master_token"] = authToken
		config["acl_default_policy"] = "deny"
		config["acl_down_policy"] = "extend-cache"
	}

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("cannot json-encode config: %v", err)
	}
	if _, err := configFile.Write(data); err != nil {
		t.Fatalf("cannot write config: %v", err)
	}
	if err := configFile.Close(); err != nil {
		t.Fatalf("cannot close config: %v", err)
	}

	cmd := exec.Command("consul",
		"agent",
		"-dev",
		"-config-file", configFilename)
	err = cmd.Start()
	if err != nil {
		t.Fatalf("failed to start consul: %v", err)
	}

	// Create a client to connect to the created consul.
	serverAddr := fmt.Sprintf("localhost:%v", port+1)
	cfg := api.DefaultConfig()
	cfg.Address = serverAddr
	if authToken != "" {
		cfg.Token = authToken
	}
	c, err := api.NewClient(cfg)
	if err != nil {
		t.Fatalf("api.NewClient(%v) failed: %v", serverAddr, err)
	}

	// Wait until we can list "/", or timeout.
	start := time.Now()
	kv := c.KV()
	for {
		_, _, err := kv.List("/", nil)
		if err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("Failed to start consul daemon in time. Consul is returning error: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	return cmd, configFilename, serverAddr
}
