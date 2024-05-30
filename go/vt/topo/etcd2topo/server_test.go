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

package etcd2topo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// startEtcd starts an etcd subprocess, and waits for it to be ready.
func startEtcd(t *testing.T, port int) (string, *exec.Cmd) {
	// Create a temporary directory.
	dataDir := t.TempDir()

	// Get our two ports to listen to.
	if port == 0 {
		port = testfiles.GoVtTopoEtcd2topoPort
	}
	name := "vitess_unit_test"
	clientAddr := fmt.Sprintf("http://localhost:%v", port)
	peerAddr := fmt.Sprintf("http://localhost:%v", port+1)
	initialCluster := fmt.Sprintf("%v=%v", name, peerAddr)

	cmd := exec.Command("etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-initial-cluster", initialCluster,
		"-data-dir", dataDir)
	err := cmd.Start()
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}

	// Create a client to connect to the created etcd.
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("newCellClient(%v) failed: %v", clientAddr, err)
	}
	defer cli.Close()

	// Wait until we can list "/", or timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	for {
		if _, err := cli.Get(ctx, "/"); err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("Failed to start etcd daemon in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() {
		// log error
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd.Process.Kill() failed : %v", err)
		}
		// log error
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd.wait() failed : %v", err)
		}
	})

	return clientAddr, cmd
}

// startEtcdWithTLS starts an etcd subprocess with TLS setup, and waits for it to be ready.
func startEtcdWithTLS(t *testing.T) (string, *tlstest.ClientServerKeyPairs) {
	// Create a temporary directory.
	dataDir := t.TempDir()

	// Get our two ports to listen to.
	port := testfiles.GoVtTopoEtcd2topoPort
	name := "vitess_unit_test"
	clientAddr := fmt.Sprintf("https://localhost:%v", port+2)
	peerAddr := fmt.Sprintf("https://localhost:%v", port+3)
	initialCluster := fmt.Sprintf("%v=%v", name, peerAddr)

	certs := tlstest.CreateClientServerCertPairs(dataDir)

	cmd := exec.Command("etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-initial-cluster", initialCluster,
		"-cert-file", certs.ServerCert,
		"-key-file", certs.ServerKey,
		"-trusted-ca-file", certs.ClientCA,
		"-peer-trusted-ca-file", certs.ClientCA,
		"-peer-cert-file", certs.ServerCert,
		"-peer-key-file", certs.ServerKey,
		"-client-cert-auth",
		"-data-dir", dataDir)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Start()
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}

	tlsConfig, err := newTLSConfig(certs.ClientCert, certs.ClientKey, certs.ServerCA)
	if err != nil {
		t.Fatalf("failed to get tls.Config: %v", err)
	}

	var cli *clientv3.Client
	// Create client
	start := time.Now()
	for {
		// Create a client to connect to the created etcd.
		cli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{clientAddr},
			TLS:         tlsConfig,
			DialTimeout: 5 * time.Second,
		})
		if err == nil {
			break
		}
		t.Logf("error establishing client for etcd tls test: %v", err)
		if time.Since(start) > 60*time.Second {
			t.Fatalf("failed to start client for etcd tls test in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer cli.Close()

	// Wait until we can list "/", or timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start = time.Now()
	for {
		if _, err := cli.Get(ctx, "/"); err == nil {
			break
		}
		if time.Since(start) > 60*time.Second {
			t.Fatalf("failed to start etcd daemon in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Cleanup(func() {
		// log error
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd.Process.Kill() failed : %v", err)
		}
		// log error
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd.wait() failed : %v", err)
		}
	})

	return clientAddr, &certs
}

func TestEtcd2TLS(t *testing.T) {
	// Start a single etcd in the background.
	clientAddr, certs := startEtcdWithTLS(t)

	testIndex := 0
	testRoot := fmt.Sprintf("/test-%v", testIndex)

	// Create the server on the new root.
	server, err := NewServerWithOpts(clientAddr, testRoot, certs.ClientCert, certs.ClientKey, certs.ServerCA)
	if err != nil {
		t.Fatalf("NewServerWithOpts failed: %v", err)
	}
	defer server.Close()

	testCtx := context.Background()
	testKey := "testkey"
	testVal := "testval"
	_, err = server.Create(testCtx, testKey, []byte(testVal))
	if err != nil {
		t.Fatalf("Failed to set key value pair: %v", err)
	}
	val, _, err := server.Get(testCtx, testKey)
	if err != nil {
		t.Fatalf("Failed to retrieve value at key we just set: %v", err)
	}
	if string(val) != testVal {
		t.Fatalf("Value returned doesn't match %s, err: %v", testVal, err)
	}
}

func TestEtcd2Topo(t *testing.T) {
	// Start a single etcd in the background.
	clientAddr, _ := startEtcd(t, 0)

	testIndex := 0
	newServer := func() *topo.Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("etcd2", clientAddr, path.Join(testRoot, topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Create the CellInfo.
		if err := ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: clientAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	}

	// Run the TopoServerTestSuite tests.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		return newServer()
	}, []string{})

	// Run etcd-specific tests.
	ts := newServer()
	testKeyspaceLock(t, ts)
	ts.Close()
}

// TestEtcd2TopoGetTabletsPartialResults confirms that GetTablets handles partial results
// correctly when etcd2 is used along with the normal vtctldclient <-> vtctld client/server
// path.
func TestEtcd2TopoGetTabletsPartialResults(t *testing.T) {
	ctx := context.Background()
	cells := []string{"cell1", "cell2"}
	root := "/vitess"
	// Start three etcd instances in the background. One will serve the global topo data
	// while the other two will serve the cell topo data.
	globalClientAddr, _ := startEtcd(t, 0)
	cellClientAddrs := make([]string, len(cells))
	cellClientCmds := make([]*exec.Cmd, len(cells))
	cellTSs := make([]*topo.Server, len(cells))
	for i := 0; i < len(cells); i++ {
		addr, cmd := startEtcd(t, testfiles.GoVtTopoEtcd2topoPort+(i+100*i))
		cellClientAddrs[i] = addr
		cellClientCmds[i] = cmd
	}
	require.Equal(t, len(cells), len(cellTSs))

	// Setup the global topo server.
	globalTS, err := topo.OpenServer("etcd2", globalClientAddr, path.Join(root, topo.GlobalCell))
	require.NoError(t, err, "OpenServer() failed for global topo server: %v", err)

	// Setup the cell topo servers.
	for i, cell := range cells {
		cellTSs[i], err = topo.OpenServer("etcd2", cellClientAddrs[i], path.Join(root, topo.GlobalCell))
		require.NoError(t, err, "OpenServer() failed for cell %s topo server: %v", cell, err)
	}

	// Create the CellInfo and Tablet records/keys.
	for i, cell := range cells {
		err = globalTS.CreateCellInfo(ctx, cell, &topodatapb.CellInfo{
			ServerAddress: cellClientAddrs[i],
			Root:          path.Join(root, cell),
		})
		require.NoError(t, err, "CreateCellInfo() failed in global cell for cell %s: %v", cell, err)
		ta := &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  uint32(100 + i),
		}
		err = globalTS.CreateTablet(ctx, &topodatapb.Tablet{Alias: ta})
		require.NoError(t, err, "CreateTablet() failed in cell %s: %v", cell, err)
	}

	// This returns stdout and stderr lines as a slice of strings along with the command error.
	getTablets := func(strict bool) ([]string, []string, error) {
		cmd := exec.Command("vtctldclient", "--server", "internal", "--topo-implementation", "etcd2", "--topo-global-server-address", globalClientAddr, "GetTablets", fmt.Sprintf("--strict=%t", strict))
		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		// Trim any leading and trailing newlines so we don't have an empty string at
		// either end of the slices which throws off the logical number of lines produced.
		var stdoutLines, stderrLines []string
		if stdout.Len() > 0 { // Otherwise we'll have a 1 element slice with an empty string
			stdoutLines = strings.Split(strings.Trim(stdout.String(), "\n"), "\n")
		}
		if stderr.Len() > 0 { // Otherwise we'll have a 1 element slice with an empty string
			stderrLines = strings.Split(strings.Trim(stderr.String(), "\n"), "\n")
		}
		return stdoutLines, stderrLines, err
	}

	// Execute the vtctldclient command.
	stdout, stderr, err := getTablets(false)
	require.NoError(t, err, "Unexpected error: %v, output: %s", err, strings.Join(stdout, "\n"))
	// We get each of the single tablets in each cell.
	require.Len(t, stdout, len(cells))
	// And no error message.
	require.Len(t, stderr, 0, "Unexpected error message: %s", strings.Join(stderr, "\n"))

	// Stop the last cell topo server.
	cmd := cellClientCmds[len(cells)-1]
	require.NotNil(t, cmd)
	err = cmd.Process.Kill()
	require.NoError(t, err)
	_ = cmd.Wait()

	// Execute the vtctldclient command to get partial results.
	stdout, stderr, err = getTablets(false)
	require.NoError(t, err, "Unexpected error: %v, output: %s", err, strings.Join(stdout, "\n"))
	// We get partial results, missing the tablet from the last cell.
	require.Len(t, stdout, len(cells)-1, "Unexpected output: %s", strings.Join(stdout, "\n"))
	// We get an error message for the cell that was unreachable.
	require.Greater(t, len(stderr), 0, "Unexpected error message: %s", strings.Join(stderr, "\n"))

	// Execute the vtctldclient command with strict enabled.
	_, stderr, err = getTablets(true)
	require.Error(t, err) // We get an error
	// We still get an error message printed to the console for the cell that was unreachable.
	require.Greater(t, len(stderr), 0, "Unexpected error message: %s", strings.Join(stderr, "\n"))

	globalTS.Close()
	for _, cellTS := range cellTSs {
		cellTS.Close()
	}
}

// testKeyspaceLock tests etcd-specific heartbeat (TTL).
// Note TTL granularity is in seconds, even though the API uses time.Duration.
// So we have to wait a long time in these tests.
func testKeyspaceLock(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace")
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell failed: %v", err)
	}

	// Long TTL, unlock before lease runs out.
	leaseTTL = 1000
	lockDescriptor, err := conn.Lock(ctx, keyspacePath, "ttl")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Short TTL, make sure it doesn't expire.
	leaseTTL = 1
	lockDescriptor, err = conn.Lock(ctx, keyspacePath, "short ttl")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	time.Sleep(2 * time.Second)
	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}
