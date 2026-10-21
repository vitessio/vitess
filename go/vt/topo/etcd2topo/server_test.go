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
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// startEtcd starts an etcd subprocess, and waits for it to be ready.
func startEtcd(t *testing.T, clientPort, peerPort int) (string, *exec.Cmd) {
	// Create a temporary directory.
	dataDir := t.TempDir()

	name := "vitess_unit_test"
	clientAddr := fmt.Sprintf("http://localhost:%v", clientPort)
	peerAddr := fmt.Sprintf("http://localhost:%v", peerPort)
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
	require.NoError(t, err)

	// Create a client to connect to the created etcd.
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoErrorf(t, err, "newCellClient(%v) failed", clientAddr)
	defer cli.Close()

	// Wait until we can list "/", or timeout.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := time.Now()
	for {
		if _, err := cli.Get(ctx, "/"); err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			require.FailNow(t, "Failed to start etcd daemon in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() {
		// log error
		if err := cmd.Process.Kill(); err != nil {
			log.Error(fmt.Sprintf("cmd.Process.Kill() failed : %v", err))
		}
		// log error
		if err := cmd.Wait(); err != nil {
			log.Error(fmt.Sprintf("cmd.wait() failed : %v", err))
		}
	})

	return clientAddr, cmd
}

// startEtcdWithTLS starts an etcd subprocess with TLS setup, and waits for it to be ready.
func startEtcdWithTLS(t *testing.T) (string, *tlstest.ClientServerKeyPairs) {
	// Create a temporary directory.
	dataDir := t.TempDir()

	name := "vitess_unit_test"
	clientAddr := fmt.Sprintf("https://localhost:%v", testfiles.GoVtTopoEtcd2topoTLSPort)
	peerAddr := fmt.Sprintf("https://localhost:%v", testfiles.GoVtTopoEtcd2topoTLSPeerPort)
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
	require.NoError(t, err)

	tlsConfig, err := newTLSConfig(certs.ClientCert, certs.ClientKey, certs.ServerCA)
	require.NoError(t, err)

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
			require.FailNow(t, "failed to start client for etcd tls test in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer cli.Close()

	// Wait until we can list "/", or timeout.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start = time.Now()
	for {
		if _, err := cli.Get(ctx, "/"); err == nil {
			break
		}
		if time.Since(start) > 60*time.Second {
			require.FailNow(t, "failed to start etcd daemon in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Cleanup(func() {
		// log error
		if err := cmd.Process.Kill(); err != nil {
			log.Error(fmt.Sprintf("cmd.Process.Kill() failed : %v", err))
		}
		// log error
		if err := cmd.Wait(); err != nil {
			log.Error(fmt.Sprintf("cmd.wait() failed : %v", err))
		}
	})

	return clientAddr, &certs
}

// TestNewServerAgainstUnreachableEtcd ensures a topo server cannot be
// constructed against an etcd address nobody is listening on: the error must
// surface at construction time, so a misconfigured or unreachable topo
// address is reported at startup rather than as timeouts on later topo
// operations.
func TestNewServerAgainstUnreachableEtcd(t *testing.T) {
	// Reserve a port and close the listener again, so nothing is listening
	// at the address.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	s, err := NewServer("http://"+addr, "/vitess")
	if s != nil {
		s.Close()
	}
	require.Error(t, err, "NewServer should fail when no etcd is reachable at the given address")
}

func TestEtcd2TLS(t *testing.T) {
	// Start a single etcd in the background.
	clientAddr, certs := startEtcdWithTLS(t)

	testIndex := 0
	testRoot := fmt.Sprintf("/test-%v", testIndex)

	// Create the server on the new root.
	server, err := NewServerWithOpts(clientAddr, testRoot, certs.ClientCert, certs.ClientKey, certs.ServerCA)
	require.NoError(t, err)
	defer server.Close()

	testCtx := t.Context()
	testKey := "testkey"
	testVal := "testval"
	_, err = server.Create(testCtx, testKey, []byte(testVal))
	require.NoError(t, err)
	val, _, err := server.Get(testCtx, testKey)
	require.NoError(t, err)
	require.Equalf(t, testVal, string(val), "Value returned doesn't match %s, err: %v", testVal, err)
}

func TestEtcd2Topo(t *testing.T) {
	// Start a single etcd in the background.
	clientAddr, _ := startEtcd(t, testfiles.GoVtTopoEtcd2topoPort, testfiles.GoVtTopoEtcd2topoPeerPort)

	testIndex := 0
	newServer := func() *topo.Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("etcd2", clientAddr, path.Join(testRoot, topo.GlobalCell))
		require.NoError(t, err)

		// Create the CellInfo.
		if err := ts.CreateCellInfo(t.Context(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: clientAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		}); err != nil {
			require.NoError(t, err)
		}

		return ts
	}

	// Run the TopoServerTestSuite tests.
	ctx := t.Context()
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
	ctx := t.Context()
	cells := []string{"cell1", "cell2"}
	root := "/vitess"
	// Start three etcd instances in the background. One will serve the global topo data
	// while the other two will serve the cell topo data.
	globalClientAddr, _ := startEtcd(t, testfiles.GoVtTopoEtcd2topoPort, testfiles.GoVtTopoEtcd2topoPeerPort)
	cellPorts := [...]struct {
		client, peer int
	}{
		{testfiles.GoVtTopoEtcd2topoCell1Port, testfiles.GoVtTopoEtcd2topoCell1PeerPort},
		{testfiles.GoVtTopoEtcd2topoCell2Port, testfiles.GoVtTopoEtcd2topoCell2PeerPort},
	}
	require.Len(t, cellPorts, len(cells))
	cellClientAddrs := make([]string, len(cells))
	cellClientCmds := make([]*exec.Cmd, len(cells))
	cellTSs := make([]*topo.Server, len(cells))
	for i := range cells {
		addr, cmd := startEtcd(t, cellPorts[i].client, cellPorts[i].peer)
		cellClientAddrs[i] = addr
		cellClientCmds[i] = cmd
	}

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
	// Filter out gRPC transport warnings emitted by the etcd v3.6 client
	// during connection establishment.
	stderr = slices.DeleteFunc(stderr, func(line string) bool {
		return strings.Contains(line, "grpc: addrConn.createTransport failed to connect")
	})

	// And no error message.
	require.Empty(t, stderr, "Unexpected error message: %s", strings.Join(stderr, "\n"))

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
	require.NotEmpty(t, stderr, "Unexpected error message: %s", strings.Join(stderr, "\n"))

	// Execute the vtctldclient command with strict enabled.
	_, stderr, err = getTablets(true)
	require.Error(t, err) // We get an error
	// We still get an error message printed to the console for the cell that was unreachable.
	require.NotEmpty(t, stderr, "Unexpected error message: %s", strings.Join(stderr, "\n"))

	globalTS.Close()
	for _, cellTS := range cellTSs {
		cellTS.Close()
	}
}

// TestEtcd2TopoServerClosed tests that operations on a closed server return
// appropriate errors instead of panicking due to nil pointer dereference.
func TestEtcd2TopoServerClosed(t *testing.T) {
	// Start a single etcd in the background.
	clientAddr, _ := startEtcd(t, testfiles.GoVtTopoEtcd2topoPort, testfiles.GoVtTopoEtcd2topoPeerPort)

	testRoot := "/test-closed"

	// Create the server on the new root.
	ts, err := topo.OpenServer("etcd2", clientAddr, path.Join(testRoot, topo.GlobalCell))
	require.NoError(t, err, "OpenServer() failed: %v", err)

	// Create the CellInfo first.
	ctx := t.Context()
	err = ts.CreateCellInfo(ctx, "test_cell", &topodatapb.CellInfo{
		ServerAddress: clientAddr,
		Root:          path.Join(testRoot, "test_cell"),
	})
	require.NoError(t, err, "CreateCellInfo() failed: %v", err)

	// Get the connection for the cell
	conn, err := ts.ConnForCell(ctx, "test_cell")
	require.NoError(t, err, "ConnForCell() failed: %v", err)

	// Test that operations work before closing
	testPath := "test_key"
	testContents := []byte("test_value")

	_, err = conn.Create(ctx, testPath, testContents)
	require.NoError(t, err, "Create() before close should succeed")

	// Close the connection
	ts.Close()

	// Test that operations return appropriate errors after closing
	_, err = conn.Create(ctx, "another_key", testContents)
	require.Error(t, err, "Create() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, _, err = conn.Get(ctx, testPath)
	require.Error(t, err, "Get() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, err = conn.GetVersion(ctx, testPath, 1)
	require.Error(t, err, "GetVersion() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	err = conn.Delete(ctx, testPath, nil)
	require.Error(t, err, "Delete() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, err = conn.List(ctx, "/")
	require.Error(t, err, "List() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, err = conn.Update(ctx, testPath, testContents, nil)
	require.Error(t, err, "Update() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	// Test watch operations after close
	_, _, err = conn.Watch(ctx, testPath)
	require.Error(t, err, "Watch() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, _, err = conn.WatchRecursive(ctx, "/")
	require.Error(t, err, "WatchRecursive() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)
}

// testKeyspaceLock tests etcd-specific heartbeat (TTL).
// Note TTL granularity is in seconds, even though the API uses time.Duration.
// So we have to wait a long time in these tests.
func testKeyspaceLock(t *testing.T, ts *topo.Server) {
	ctx := t.Context()
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace")
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		require.NoError(t, err)
	}

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err)

	// Long TTL, unlock before lease runs out.
	leaseTTL = 1000
	lockDescriptor, err := conn.Lock(ctx, keyspacePath, "ttl")
	require.NoError(t, err)
	if err := lockDescriptor.Unlock(ctx); err != nil {
		require.NoError(t, err)
	}

	// Short TTL, make sure it doesn't expire.
	leaseTTL = 1
	lockDescriptor, err = conn.Lock(ctx, keyspacePath, "short ttl")
	require.NoError(t, err)
	time.Sleep(2 * time.Second)
	if err := lockDescriptor.Unlock(ctx); err != nil {
		require.NoError(t, err)
	}

	// tryLock reports whether TryLock took the lock, and any error other than
	// the lock being held.
	tryLock := func() (bool, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ld, err := conn.TryLock(ctx, keyspacePath, "probe")
		if err == nil {
			return true, ld.Unlock(ctx)
		}
		if topo.IsErrType(err, topo.NodeExists) {
			return false, nil
		}
		t.Logf("unexpected TryLock error: %v", err)
		return false, err
	}
	takenOrFailed := func() bool {
		taken, err := tryLock()
		return taken || err != nil
	}
	taken := func() bool {
		taken, err := tryLock()
		return taken && err == nil
	}

	// Short TTL through topo.Server, which ends the acquisition context once the lock is
	// acquired: the lease must still be kept alive until the unlock.
	_, unlock, err := ts.LockKeyspace(ctx, "test_keyspace", "short ttl, acquisition context ended")
	require.NoError(t, err)
	assert.Never(t, takenOrFailed, 5*time.Second, 250*time.Millisecond, "the lock expired while still held")
	var unlockErr error
	unlock(&unlockErr)
	require.NoError(t, unlockErr)

	// An explicit TTL still releases the lock after it, without an unlock.
	_, unlock, err = ts.LockKeyspace(ctx, "test_keyspace", "explicit ttl", topo.WithTTL(time.Second))
	require.NoError(t, err)
	assert.Eventually(t, taken, 30*time.Second, 250*time.Millisecond, "the lock with an explicit TTL did not expire")
	unlock(&unlockErr)
}

// startRestartableEtcd starts etcd on free ports, and returns its client
// address and functions that stop it and start it again on the same data.
func startRestartableEtcd(t *testing.T) (addr string, stop, start func()) {
	freeAddr := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		return "http://" + listener.Addr().String()
	}
	addr, peer := freeAddr(), freeAddr()
	dataDir := t.TempDir()
	var cmd *exec.Cmd
	start = func() {
		cmd = exec.Command("etcd",
			"-name", "vitess_unit_test",
			"-advertise-client-urls", addr,
			"-listen-client-urls", addr,
			"-initial-advertise-peer-urls", peer,
			"-listen-peer-urls", peer,
			"-initial-cluster", "vitess_unit_test="+peer,
			"-data-dir", dataDir)
		require.NoError(t, cmd.Start())
		cli, err := clientv3.New(clientv3.Config{Endpoints: []string{addr}, DialTimeout: 5 * time.Second})
		require.NoError(t, err)
		defer cli.Close()
		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, err := cli.Get(ctx, "/")
			return err == nil
		}, 30*time.Second, 50*time.Millisecond, "etcd did not start")
	}
	stop = func() {
		if cmd != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			cmd = nil
		}
	}
	t.Cleanup(stop)
	start()
	return addr, stop, start
}

// countLeaseExpiredWarnings counts keepAlive's warnings that a lock's lease
// expired before the lock was released.
func countLeaseExpiredWarnings(t *testing.T) *atomic.Int32 {
	var count atomic.Int32
	original := log.Warn
	t.Cleanup(func() { log.Warn = original })
	log.Warn = func(msg string, attrs ...slog.Attr) {
		if strings.Contains(msg, "expired before the lock was released") {
			count.Add(1)
		}
		original(msg, attrs...)
	}
	return &count
}

// TestLockSurvivesEtcdRestart checks that a lock keeps its lease through an
// etcd outage longer than the lease's TTL. The etcd client gives up on the
// lease's KeepAlive during the outage, and the restarted etcd gives the lease a
// fresh TTL, which must then be renewed.
func TestLockSurvivesEtcdRestart(t *testing.T) {
	addr, stop, start := startRestartableEtcd(t)
	s, err := NewServer(addr, "/test")
	require.NoError(t, err)
	t.Cleanup(s.Close)
	warnings := countLeaseExpiredWarnings(t)

	const ttl = 6
	ld, err := s.lock(t.Context(), "keyspaces/ks", "holder", ttl, true)
	require.NoError(t, err)
	t.Cleanup(ld.(*etcdLockDescriptor).stopKeepAlive)

	stop()
	// The outage is a duration, not a condition to wait for. The etcd client
	// gives up on the KeepAlive after a TTL without a response, or, before the
	// first response, after NewServer's 5s DialTimeout plus 1s; the outage must
	// outlast both. It is kept short because the client's reconnection backoff
	// grows with the outage, and must stay well within the restarted lease's TTL.
	time.Sleep((ttl + 2) * time.Second)
	start()

	lockKeys := path.Join("/test", "keyspaces/ks", locksPath) + "/"
	assert.Never(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		resp, err := s.cli.Get(ctx, lockKeys, clientv3.WithPrefix())
		return err == nil && len(resp.Kvs) == 0
	}, 2*ttl*time.Second, 250*time.Millisecond, "the lock's lease expired after etcd restarted")
	require.NoError(t, ld.Unlock(t.Context()))
	assert.Zero(t, warnings.Load())
}

// TestKeepAliveStops checks that keepAlive returns, once the etcd client has
// given up on the lease's KeepAlive, when the lease is gone, when the lock is
// released and when the client is closed, and that it warns only about the
// first.
func TestKeepAliveStops(t *testing.T) {
	addr, stop, start := startRestartableEtcd(t)
	warnings := countLeaseExpiredWarnings(t)

	// run starts keepAlive with a KeepAlive the etcd client gave up on.
	run := func(ctx context.Context, s *Server, cli *clientv3.Client, leaseID clientv3.LeaseID) chan struct{} {
		gaveUp := make(chan *clientv3.LeaseKeepAliveResponse)
		close(gaveUp)
		returned := make(chan struct{})
		go func() {
			s.keepAlive(ctx, cli, leaseID, "/test/locks", gaveUp)
			close(returned)
		}()
		return returned
	}
	requireReturns := func(t *testing.T, returned chan struct{}) {
		select {
		case <-returned:
		case <-time.After(30 * time.Second):
			require.FailNow(t, "keepAlive did not return")
		}
	}
	newServer := func(t *testing.T) (*Server, clientv3.LeaseID) {
		s, err := NewServer(addr, "/test")
		require.NoError(t, err)
		lease, err := s.cli.Grant(t.Context(), 30)
		require.NoError(t, err)
		return s, lease.ID
	}

	t.Run("lease gone", func(t *testing.T) {
		warnings.Store(0)
		s, leaseID := newServer(t)
		t.Cleanup(s.Close)
		_, err := s.cli.Revoke(t.Context(), leaseID)
		require.NoError(t, err)

		requireReturns(t, run(t.Context(), s, s.cli, leaseID))
		assert.EqualValues(t, 1, warnings.Load())
	})

	t.Run("client closed", func(t *testing.T) {
		warnings.Store(0)
		s, leaseID := newServer(t)
		cli := s.cli
		s.Close()

		requireReturns(t, run(t.Context(), s, cli, leaseID))
		assert.Zero(t, warnings.Load())
	})

	t.Run("lock released during a renewal", func(t *testing.T) {
		warnings.Store(0)
		s, leaseID := newServer(t)
		t.Cleanup(s.Close)
		lease := &leaseNotFoundOnCancel{Lease: s.cli.Lease, called: make(chan struct{})}
		s.cli.Lease = lease

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		returned := run(ctx, s, s.cli, leaseID)
		select {
		case <-lease.called:
		case <-time.After(30 * time.Second):
			require.FailNow(t, "keepAlive did not renew the lease")
		}
		cancel()
		requireReturns(t, returned)
		assert.Zero(t, warnings.Load())
	})

	t.Run("lock released while etcd is down", func(t *testing.T) {
		warnings.Store(0)
		s, leaseID := newServer(t)
		t.Cleanup(s.Close)
		stop()
		t.Cleanup(start)

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		returned := run(ctx, s, s.cli, leaseID)
		cancel()
		requireReturns(t, returned)
		assert.Zero(t, warnings.Load())
	})
}

// leaseNotFoundOnCancel's KeepAliveOnce waits for ctx to end, then reports the
// lease gone, as etcd can when its answer races the lock's release.
type leaseNotFoundOnCancel struct {
	clientv3.Lease
	called chan struct{}
}

func (l *leaseNotFoundOnCancel) KeepAliveOnce(ctx context.Context, _ clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	close(l.called)
	<-ctx.Done()
	return nil, rpctypes.ErrLeaseNotFound
}
