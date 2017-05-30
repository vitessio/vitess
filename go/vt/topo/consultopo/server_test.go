/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consultopo

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/consul/api"
	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// startConsul starts a consul subprocess, and waits for it to be ready.
// Returns the exec.Cmd forked, the config file to remove after the test,
// and the server address to RPC-connect to.
func startConsul(t *testing.T) (*exec.Cmd, string, string) {
	// Create a temporary config file, as ports cannot all be set via
	// command line.
	configFile, err := ioutil.TempFile("", "consul")
	if err != nil {
		t.Fatalf("cannot create tempfile: %v", err)
	}
	configFilename := configFile.Name()

	// Create the JSON config, save it.
	port := testfiles.GoVtTopoConsultopoPort
	config := map[string]interface{}{
		"ports": map[string]int{
			"dns":      port,
			"http":     port + 1,
			"rpc":      port + 2,
			"serf_lan": port + 3,
			"serf_wan": port + 4,
		},
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
	c, err := api.NewClient(cfg)
	if err != nil {
		t.Fatalf("api.NewClient(%v) failed: %v", serverAddr, err)
	}

	// Wait until we can list "/", or timeout.
	start := time.Now()
	kv := c.KV()
	for {
		if _, _, err := kv.List("/", nil); err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("Failed to start consul daemon in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	return cmd, configFilename, serverAddr
}

func TestConsulTopo(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	*watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := startConsul(t)
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.Remove(configFilename)
	}()

	// This function will create a toplevel directory for a new test.
	testIndex := 0
	newServer := func() *Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		s, err := NewServer(serverAddr, path.Join(testRoot, "global"))
		if err != nil {
			t.Fatalf("NewServer() failed: %v", err)
		}

		// Create the CellInfo.
		cell := "test"
		ci := &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          path.Join(testRoot, cell),
		}
		data, err := proto.Marshal(ci)
		if err != nil {
			t.Fatalf("cannot proto.Marshal CellInfo: %v", err)
		}
		nodePath := path.Join(s.global.root, cellsPath, cell, topo.CellInfoFile)
		if _, err := s.global.kv.Put(&api.KVPair{
			Key:   nodePath,
			Value: data,
		}, nil); err != nil {
			t.Fatalf("s.global.kv.Put(%v) failed: %v", nodePath, err)
		}

		return s
	}

	// Run the TopoServerTestSuite tests.
	test.TopoServerTestSuite(t, func() topo.Impl {
		return newServer()
	})
}
