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
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestConsulTopo(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	*watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := test.StartConsul(t, "")
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
	test.TopoServerTestSuite(t, func() *topo.Server {
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
	})
}

func TestConsulTopoWithChecks(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	*watchPollDuration = 100 * time.Millisecond
	*consulLockSessionChecks = "serfHealth"
	*consulLockSessionTTL = "15s"

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := test.StartConsul(t, "")
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
	test.TopoServerTestSuite(t, func() *topo.Server {
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
	})
}

func TestConsulTopoWithAuth(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	*watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := test.StartConsul(t, "123456")
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

	*consulAuthClientStaticFile = tmpFile.Name()

	jsonConfig := "{\"global\":{\"acl_token\":\"123456\"}, \"test\":{\"acl_token\":\"123456\"}}"
	if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't write temp file: %v", err)
	}

	test.TopoServerTestSuite(t, func() *topo.Server {
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
	})
}

func TestConsulTopoWithAuthFailure(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	*watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := test.StartConsul(t, "123456")
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

	*consulAuthClientStaticFile = tmpFile.Name()

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
