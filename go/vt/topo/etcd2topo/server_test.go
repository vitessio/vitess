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
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/tlstest"

	"github.com/coreos/etcd/pkg/transport"

	"context"

	"github.com/coreos/etcd/clientv3"

	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// startEtcd starts an etcd subprocess, and waits for it to be ready.
func startEtcd(t *testing.T) (*exec.Cmd, string, string) {
	// Create a temporary directory.
	dataDir, err := ioutil.TempDir("", "etcd")
	if err != nil {
		t.Fatalf("cannot create tempdir: %v", err)
	}

	// Get our two ports to listen to.
	port := testfiles.GoVtTopoEtcd2topoPort
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
	err = cmd.Start()
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

	return cmd, dataDir, clientAddr
}

// startEtcdWithTLS starts an etcd subprocess with TLS setup, and waits for it to be ready.
func startEtcdWithTLS(t *testing.T) (string, *tlstest.ClientServerKeyPairs, func()) {
	// Create a temporary directory.
	dataDir, err := ioutil.TempDir("", "etcd")
	if err != nil {
		t.Fatalf("cannot create tempdir: %v", err)
	}

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
	err = cmd.Start()
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}

	// Safe now to build up TLS info.
	tlsInfo := transport.TLSInfo{
		CertFile:      certs.ClientCert,
		KeyFile:       certs.ClientKey,
		TrustedCAFile: certs.ServerCA,
	}

	tlsConfig, err := tlsInfo.ClientConfig()
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

	stopEtcd := func() {
		// log error
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd.Process.Kill() failed : %v", err)
		}
		// log error
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd.wait() failed : %v", err)
		}
		os.RemoveAll(dataDir)
	}

	return clientAddr, &certs, stopEtcd
}

func TestEtcd2TLS(t *testing.T) {
	// Start a single etcd in the background.
	clientAddr, certs, stopEtcd := startEtcdWithTLS(t)
	defer stopEtcd()

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
	cmd, dataDir, clientAddr := startEtcd(t)
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll(dataDir)
	}()

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
	test.TopoServerTestSuite(t, func() *topo.Server {
		return newServer()
	})

	// Run etcd-specific tests.
	ts := newServer()
	testKeyspaceLock(t, ts)
	ts.Close()
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
	*leaseTTL = 1000
	lockDescriptor, err := conn.Lock(ctx, keyspacePath, "ttl")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Short TTL, make sure it doesn't expire.
	*leaseTTL = 1
	lockDescriptor, err = conn.Lock(ctx, keyspacePath, "short ttl")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	time.Sleep(2 * time.Second)
	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}
