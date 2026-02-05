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

package consultopo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"
)

// startConsul starts a consul subprocess, and waits for it to be ready.
// Returns the exec.Cmd forked, the config file to remove after the test,
// and the server address to RPC-connect to.
func startConsul(t *testing.T, authToken string) (*exec.Cmd, string, string) {
	// Create a temporary config file, as ports cannot all be set
	// via command line. The file name has to end with '.json' so
	// we're not using TempFile.
	configDir := t.TempDir()

	configFilename := path.Join(configDir, "consul.json")
	configFile, err := os.OpenFile(configFilename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("cannot create tempfile: %v", err)
	}

	// Create the JSON config, save it.
	port := testfiles.GoVtTopoConsultopoPort
	config := map[string]any{
		"ports": map[string]int{
			"dns":      port,
			"http":     port + 1,
			"serf_lan": port + 2,
			"serf_wan": port + 3,
		},
	}

	// TODO(deepthi): this is the legacy ACL format. We run v1.4.0 by default in which this has been deprecated.
	// We should start using the new format
	// https://learn.hashicorp.com/tutorials/consul/access-control-replication-multiple-datacenters?in=consul/security-operations
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

func TestConsulTopo(t *testing.T) {
	originalWatchPollDuration := watchPollDuration
	defer func() {
		watchPollDuration = originalWatchPollDuration
	}()

	// One test is going to wait that full period, so make it shorter.
	watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := startConsul(t, "")
	defer func() {
		// Alerts command did not run successful
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd process kill has an error: %v", err)
		}
		// Alerts command did not run successful
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd wait has an error: %v", err)
		}

		os.Remove(configFilename)
	}()

	// Run the TopoServerTestSuite tests.
	testIndex := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("consul", serverAddr, path.Join(testRoot, topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Create the CellInfo.
		if err := ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	}, []string{})
}

func TestConsulTopoWithChecks(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	originalWatchPollDuration := watchPollDuration
	originalConsulLockSessionChecks := consulLockSessionChecks
	originalConsulLockSessionTTL := consulLockSessionTTL

	defer func() {
		watchPollDuration = originalWatchPollDuration
		consulLockSessionTTL = originalConsulLockSessionTTL
		consulLockSessionChecks = originalConsulLockSessionChecks
	}()

	watchPollDuration = 100 * time.Millisecond
	consulLockSessionChecks = "serfHealth"
	consulLockSessionTTL = "15s"

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := startConsul(t, "")
	defer func() {
		// Alerts command did not run successful
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd process kill has an error: %v", err)
		}
		// Alerts command did not run successful
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd wait has an error: %v", err)
		}

		os.Remove(configFilename)
	}()

	// Run the TopoServerTestSuite tests.
	testIndex := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("consul", serverAddr, path.Join(testRoot, topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Create the CellInfo.
		if err := ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	}, []string{})
}

func TestConsulTopoWithAuth(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := startConsul(t, "123456")
	defer func() {
		// Alerts command did not run successful
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd process kill has an error: %v", err)
		}
		// Alerts command did not run successful
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd process wait has an error: %v", err)
		}
		os.Remove(configFilename)
	}()

	// Run the TopoServerTestSuite tests.
	testIndex := 0
	tmpFile, err := os.CreateTemp("", "consul_auth_client_static_file.json")

	if err != nil {
		t.Fatalf("couldn't create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	originalConsulAuthClientStaticFile := consulAuthClientStaticFile
	defer func() {
		consulAuthClientStaticFile = originalConsulAuthClientStaticFile
	}()

	consulAuthClientStaticFile = tmpFile.Name()

	jsonConfig := "{\"global\":{\"acl_token\":\"123456\"}, \"test\":{\"acl_token\":\"123456\"}}"
	if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't write temp file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("consul", serverAddr, path.Join(testRoot, topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Create the CellInfo.
		if err := ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	}, []string{})
}

func TestConsulTopoWithAuthFailure(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := startConsul(t, "123456")
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.Remove(configFilename)
	}()

	tmpFile, err := os.CreateTemp("", "consul_auth_client_static_file.json")

	if err != nil {
		t.Fatalf("couldn't create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	originalConsulAuthClientStaticFile := consulAuthClientStaticFile
	defer func() {
		consulAuthClientStaticFile = originalConsulAuthClientStaticFile
	}()

	consulAuthClientStaticFile = tmpFile.Name()

	// check valid, empty json causes error
	{
		jsonConfig := "{}"
		if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
			t.Fatalf("couldn't write temp file: %v", err)
		}

		// Create the server on the new root.
		_, err := topo.OpenServer("consul", serverAddr, path.Join("globalRoot", topo.GlobalCell))
		if err == nil {
			t.Fatal("Expected OpenServer() to return an error due to bad config, got nil")
		}
	}

	// check bad token causes error
	{
		jsonConfig := "{\"global\":{\"acl_token\":\"badtoken\"}}"
		if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
			t.Fatalf("couldn't write temp file: %v", err)
		}

		// Create the server on the new root.
		ts, err := topo.OpenServer("consul", serverAddr, path.Join("globalRoot", topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Attempt to Create the CellInfo.
		err = ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          path.Join("globalRoot", test.LocalCellName),
		})

		want := "Failed request: ACL not found"
		if err == nil || err.Error() != want {
			t.Errorf("Expected CreateCellInfo to fail: got  %v, want %s", err, want)
		}
	}
}

// TestConsulWatcherStormPrevention tests that resilient watchers don't storm subscribers during Consul outages.
// This test validates the fix for the specific Consul storm scenario reported by the team.
func TestConsulWatcherStormPrevention(t *testing.T) {
	// Save original values and restore them after the test
	originalWatchPollDuration := watchPollDuration
	originalConsulAuthClientStaticFile := consulAuthClientStaticFile
	defer func() {
		watchPollDuration = originalWatchPollDuration
		consulAuthClientStaticFile = originalConsulAuthClientStaticFile
	}()

	// Configure test settings - using direct assignment since flag parsing in tests is complex
	watchPollDuration = 100 * time.Millisecond // Faster polling for test
	consulAuthClientStaticFile = ""            // Clear auth file to avoid conflicts

	// Start Consul server
	cmd, configFilename, serverAddr := startConsul(t, "")
	defer func() {
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd process kill has an error: %v", err)
		}
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd wait has an error: %v", err)
		}
		os.Remove(configFilename)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testRoot := "storm-test"

	// Create the topo server
	ts, err := topo.OpenServer("consul", serverAddr, path.Join(testRoot, topo.GlobalCell))
	require.NoError(t, err, "OpenServer() failed")

	// Create the CellInfo
	cellName := "test_cell"
	err = ts.CreateCellInfo(ctx, cellName, &topodatapb.CellInfo{
		ServerAddress: serverAddr,
		Root:          path.Join(testRoot, cellName),
	})
	require.NoError(t, err, "CreateCellInfo() failed")

	// Create resilient server
	counts := stats.NewCountersWithSingleLabel("", "Consul storm test", "type")
	rs := srvtopo.NewResilientServer(ctx, ts, counts)

	// Set initial VSchema
	initialVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"test_keyspace": {Sharded: false},
		},
	}
	err = ts.UpdateSrvVSchema(ctx, cellName, initialVSchema)
	require.NoError(t, err, "UpdateSrvVSchema() failed")

	// Set up watcher with call counter
	var watcherCallCount atomic.Int32
	var lastWatcherError error

	rs.WatchSrvVSchema(ctx, cellName, func(v *vschemapb.SrvVSchema, e error) bool {
		count := watcherCallCount.Add(1)
		lastWatcherError = e
		if e != nil {
			t.Logf("Watcher callback #%d - error: %v", count, e)
		} else {
			t.Logf("Watcher callback #%d - success", count)
		}
		return true
	})

	// Wait for initial callback
	assert.Eventually(t, func() bool {
		return watcherCallCount.Load() >= 1
	}, 10*time.Second, 10*time.Millisecond)

	initialWatcherCalls := watcherCallCount.Load()
	require.GreaterOrEqual(t, initialWatcherCalls, int32(1), "Expected at least 1 initial watcher call")
	require.NoError(t, lastWatcherError, "Initial watcher call should not have error")

	// Verify Get operations work normally
	vschema, err := rs.GetSrvVSchema(ctx, cellName)
	require.NoError(t, err, "GetSrvVSchema() failed")
	require.NotNil(t, vschema, "GetSrvVSchema() returned nil")

	t.Logf("Setup complete. Initial watcher calls: %d", initialWatcherCalls)

	// Simulate Consul outage by killing the Consul process
	// This will cause watch errors which previously triggered storms
	err = cmd.Process.Kill()
	require.NoError(t, err, "Failed to kill consul process")

	// Get should still work from cache during outage
	vschema, err = rs.GetSrvVSchema(ctx, cellName)
	assert.NoError(t, err, "GetSrvVSchema() should work from cache during outage")
	assert.NotNil(t, vschema, "GetSrvVSchema() should return cached value during outage")

	// Wait during outage period - this is when storms would occur without our fix
	outageDuration := 2 * time.Second
	t.Logf("Waiting %v during Consul outage to check for watcher storms...", outageDuration)
	time.Sleep(outageDuration)

	// Check watcher calls during outage - key assertion for storm prevention
	watcherCallsDuringOutage := watcherCallCount.Load() - initialWatcherCalls
	t.Logf("Watcher calls during outage: %d", watcherCallsDuringOutage)

	// With our fix, watchers should remain silent during outage when cached data is available
	// This is the core validation: no storm of subscriber calls during Consul outages
	assert.Equal(t, int32(0), watcherCallsDuringOutage, "Watchers should remain completely silent during Consul outage")

	// Get operations should continue working from cache
	vschema, err = rs.GetSrvVSchema(ctx, cellName)
	assert.NoError(t, err, "GetSrvVSchema() should continue working from cache")
	assert.NotNil(t, vschema, "GetSrvVSchema() should continue returning cached value")

	t.Log("Consul storm prevention test completed - watchers remained quiet during outage")
}
