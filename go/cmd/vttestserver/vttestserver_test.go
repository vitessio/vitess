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

package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"
	"vitess.io/vitess/go/vt/vttest"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

type columnVindex struct {
	keyspace   string
	table      string
	vindex     string
	vindexType string
	column     string
}

func TestRunsVschemaMigrations(t *testing.T) {
	args := os.Args
	conf := config
	defer resetFlags(args, conf)

	cluster, err := startCluster()
	defer cluster.TearDown()

	assert.NoError(t, err)
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "app_customer", table: "customers", vindex: "hash", vindexType: "hash", column: "id"})

	// Add Hash vindex via vtgate execution on table
	err = addColumnVindex(cluster, "test_keyspace", "alter vschema on test_table1 add vindex my_vdx (id)")
	assert.NoError(t, err)
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table1", vindex: "my_vdx", vindexType: "hash", column: "id"})
}

func TestPersistentMode(t *testing.T) {
	args := os.Args
	conf := config
	defer resetFlags(args, conf)

	dir := t.TempDir()

	cluster, err := startPersistentCluster(dir)
	assert.NoError(t, err)

	// basic sanity checks similar to TestRunsVschemaMigrations
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "app_customer", table: "customers", vindex: "hash", vindexType: "hash", column: "id"})

	// insert some data to ensure persistence across teardowns
	err = execOnCluster(cluster, "app_customer", func(conn *mysql.Conn) error {
		_, err := conn.ExecuteFetch("insert into customers (id, name) values (1, 'gopherson')", 1, false)
		return err
	})
	assert.NoError(t, err)

	expectedRows := [][]sqltypes.Value{
		{sqltypes.NewInt64(1), sqltypes.NewVarChar("gopherson"), sqltypes.NULL},
	}

	// ensure data was actually inserted
	var res *sqltypes.Result
	err = execOnCluster(cluster, "app_customer", func(conn *mysql.Conn) (err error) {
		res, err = conn.ExecuteFetch("SELECT * FROM customers", 1, false)
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, expectedRows, res.Rows)

	// reboot the persistent cluster
	cluster.TearDown()
	cluster, err = startPersistentCluster(dir)
	defer cluster.TearDown()
	assert.NoError(t, err)

	// rerun our sanity checks to make sure vschema migrations are run during every startup
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "app_customer", table: "customers", vindex: "hash", vindexType: "hash", column: "id"})

	// ensure previous data was successfully persisted
	err = execOnCluster(cluster, "app_customer", func(conn *mysql.Conn) (err error) {
		res, err = conn.ExecuteFetch("SELECT * FROM customers", 1, false)
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, expectedRows, res.Rows)
}

func TestForeignKeysAndDDLModes(t *testing.T) {
	args := os.Args
	conf := config
	defer resetFlags(args, conf)

	cluster, err := startCluster("--foreign_key_mode=allow", "--enable_online_ddl=true", "--enable_direct_ddl=true")
	assert.NoError(t, err)
	defer cluster.TearDown()

	err = execOnCluster(cluster, "test_keyspace", func(conn *mysql.Conn) error {
		_, err := conn.ExecuteFetch(`CREATE TABLE test_table_2 (
			id BIGINT,
			test_table_id BIGINT,
			FOREIGN KEY (test_table_id) REFERENCES test_table(id)
		)`, 1, false)
		assert.NoError(t, err)
		_, err = conn.ExecuteFetch("SET @@ddl_strategy='online'", 1, false)
		assert.NoError(t, err)
		_, err = conn.ExecuteFetch("ALTER TABLE test_table ADD COLUMN something_else VARCHAR(255) NOT NULL DEFAULT ''", 1, false)
		assert.NoError(t, err)
		_, err = conn.ExecuteFetch("SET @@ddl_strategy='direct'", 1, false)
		assert.NoError(t, err)
		_, err = conn.ExecuteFetch("ALTER TABLE test_table ADD COLUMN something_else_2 VARCHAR(255) NOT NULL DEFAULT ''", 1, false)
		assert.NoError(t, err)
		_, err = conn.ExecuteFetch("SELECT something_else_2 FROM test_table", 1, false)
		assert.NoError(t, err)
		return nil
	})
	assert.NoError(t, err)

	cluster.TearDown()
	cluster, err = startCluster("--foreign_key_mode=disallow", "--enable_online_ddl=false", "--enable_direct_ddl=false")
	assert.NoError(t, err)
	defer cluster.TearDown()

	err = execOnCluster(cluster, "test_keyspace", func(conn *mysql.Conn) error {
		_, err := conn.ExecuteFetch(`CREATE TABLE test_table_2 (
			id BIGINT,
			test_table_id BIGINT,
			FOREIGN KEY (test_table_id) REFERENCES test_table(id)
		)`, 1, false)
		assert.Error(t, err)
		_, err = conn.ExecuteFetch("SET @@ddl_strategy='online'", 1, false)
		assert.NoError(t, err)
		_, err = conn.ExecuteFetch("ALTER TABLE test_table ADD COLUMN something_else VARCHAR(255) NOT NULL DEFAULT ''", 1, false)
		assert.Error(t, err)
		_, err = conn.ExecuteFetch("SET @@ddl_strategy='direct'", 1, false)
		assert.NoError(t, err)
		_, err = conn.ExecuteFetch("ALTER TABLE test_table ADD COLUMN something_else VARCHAR(255) NOT NULL DEFAULT ''", 1, false)
		assert.Error(t, err)
		return nil
	})
	assert.NoError(t, err)
}

func TestCanGetKeyspaces(t *testing.T) {
	args := os.Args
	conf := config
	defer resetFlags(args, conf)

	cluster, err := startCluster()
	assert.NoError(t, err)
	defer cluster.TearDown()

	assertGetKeyspaces(t, cluster)
}

func TestExternalTopoServerConsul(t *testing.T) {
	args := os.Args
	conf := config
	defer resetFlags(args, conf)

	// Start a single consul in the background.
	cmd, serverAddr := startConsul(t)
	defer func() {
		// Alerts command did not run successful
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd process kill has an error: %v", err)
		}
		// Alerts command did not run successful
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd process wait has an error: %v", err)
		}
	}()

	cluster, err := startCluster("--external_topo_implementation=consul",
		fmt.Sprintf("--external_topo_global_server_address=%s", serverAddr), "--external_topo_global_root=consul_test/global")
	assert.NoError(t, err)
	defer cluster.TearDown()

	assertGetKeyspaces(t, cluster)
}

func TestMtlsAuth(t *testing.T) {
	args := os.Args
	conf := config
	defer resetFlags(args, conf)

	// Our test root.
	root := t.TempDir()

	// Create the certs and configs.
	tlstest.CreateCA(root)
	caCert := path.Join(root, "ca-cert.pem")

	tlstest.CreateSignedCert(root, tlstest.CA, "01", "vtctld", "vtctld.example.com")
	cert := path.Join(root, "vtctld-cert.pem")
	key := path.Join(root, "vtctld-key.pem")

	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "ClientApp")
	clientCert := path.Join(root, "client-cert.pem")
	clientKey := path.Join(root, "client-key.pem")

	// When cluster starts it will apply SQL and VSchema migrations in the configured schema_dir folder
	// With mtls authorization enabled, the authorized CN must match the certificate's CN
	cluster, err := startCluster(
		"--grpc_auth_mode=mtls",
		fmt.Sprintf("--grpc_key=%s", key),
		fmt.Sprintf("--grpc_cert=%s", cert),
		fmt.Sprintf("--grpc_ca=%s", caCert),
		fmt.Sprintf("--vtctld_grpc_key=%s", clientKey),
		fmt.Sprintf("--vtctld_grpc_cert=%s", clientCert),
		fmt.Sprintf("--vtctld_grpc_ca=%s", caCert),
		fmt.Sprintf("--grpc_auth_mtls_allowed_substrings=%s", "CN=ClientApp"))
	assert.NoError(t, err)
	defer cluster.TearDown()

	// startCluster will apply vschema migrations using vtctl grpc and the clientCert.
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "app_customer", table: "customers", vindex: "hash", vindexType: "hash", column: "id"})
}

func TestMtlsAuthUnauthorizedFails(t *testing.T) {
	args := os.Args
	conf := config
	defer resetFlags(args, conf)

	// Our test root.
	root := t.TempDir()

	// Create the certs and configs.
	tlstest.CreateCA(root)
	caCert := path.Join(root, "ca-cert.pem")

	tlstest.CreateSignedCert(root, tlstest.CA, "01", "vtctld", "vtctld.example.com")
	cert := path.Join(root, "vtctld-cert.pem")
	key := path.Join(root, "vtctld-key.pem")

	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "AnotherApp")
	clientCert := path.Join(root, "client-cert.pem")
	clientKey := path.Join(root, "client-key.pem")

	// When cluster starts it will apply SQL and VSchema migrations in the configured schema_dir folder
	// For mtls authorization failure by providing a client certificate with different CN thant the
	// authorized in the configuration
	cluster, err := startCluster(
		"--grpc_auth_mode=mtls",
		fmt.Sprintf("--grpc_key=%s", key),
		fmt.Sprintf("--grpc_cert=%s", cert),
		fmt.Sprintf("--grpc_ca=%s", caCert),
		fmt.Sprintf("--vtctld_grpc_key=%s", clientKey),
		fmt.Sprintf("--vtctld_grpc_cert=%s", clientCert),
		fmt.Sprintf("--vtctld_grpc_ca=%s", caCert),
		fmt.Sprintf("--grpc_auth_mtls_allowed_substrings=%s", "CN=ClientApp"))
	defer cluster.TearDown()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "code = Unauthenticated desc = client certificate not authorized")
}

func startPersistentCluster(dir string, flags ...string) (vttest.LocalCluster, error) {
	flags = append(flags, []string{
		"--persistent_mode",
		// FIXME: if port is not provided, data_dir is not respected
		fmt.Sprintf("--port=%d", randomPort()),
		fmt.Sprintf("--data_dir=%s", dir),
	}...)
	return startCluster(flags...)
}

var clusterKeyspaces = []string{
	"test_keyspace",
	"app_customer",
}

func startCluster(flags ...string) (vttest.LocalCluster, error) {
	schemaDirArg := "--schema_dir=data/schema"
	tabletHostname := "--tablet_hostname=localhost"
	keyspaceArg := "--keyspaces=" + strings.Join(clusterKeyspaces, ",")
	numShardsArg := "--num_shards=2,2"
	vschemaDDLAuthorizedUsers := "--vschema_ddl_authorized_users=%"
	os.Args = append(os.Args, []string{schemaDirArg, keyspaceArg, numShardsArg, tabletHostname, vschemaDDLAuthorizedUsers}...)
	os.Args = append(os.Args, flags...)
	return runCluster()
}

func addColumnVindex(cluster vttest.LocalCluster, keyspace string, vschemaMigration string) error {
	return execOnCluster(cluster, keyspace, func(conn *mysql.Conn) error {
		_, err := conn.ExecuteFetch(vschemaMigration, 1, false)
		return err
	})
}

func execOnCluster(cluster vttest.LocalCluster, keyspace string, f func(*mysql.Conn) error) error {
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:   "localhost",
		DbName: keyspace,
		Port:   cluster.Env.PortForProtocol("vtcombo_mysql_port", ""),
	}

	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		return err
	}
	defer conn.Close()
	return f(conn)
}

func assertColumnVindex(t *testing.T, cluster vttest.LocalCluster, expected columnVindex) {
	server := fmt.Sprintf("localhost:%v", cluster.GrpcPort())
	args := []string{"GetVSchema", expected.keyspace}
	ctx := context.Background()

	err := vtctlclient.RunCommandAndWait(ctx, server, args, func(e *logutilpb.Event) {
		var keyspace vschemapb.Keyspace
		if err := protojson.Unmarshal([]byte(e.Value), &keyspace); err != nil {
			t.Error(err)
		}

		columnVindex := keyspace.Tables[expected.table].ColumnVindexes[0]
		actualVindex := keyspace.Vindexes[expected.vindex]
		assertEqual(t, actualVindex.Type, expected.vindexType, "Actual vindex type different from expected")
		assertEqual(t, columnVindex.Name, expected.vindex, "Actual vindex name different from expected")
		assertEqual(t, columnVindex.Columns[0], expected.column, "Actual vindex column different from expected")
	})
	require.NoError(t, err)
}

func assertEqual(t *testing.T, actual string, expected string, message string) {
	if actual != expected {
		t.Errorf("%s: actual %s, expected %s", message, actual, expected)
	}
}

func resetFlags(args []string, conf vttest.Config) {
	os.Args = args
	config = conf
}

func randomPort() int {
	v := rand.Int31n(20000)
	return int(v + 10000)
}

func assertGetKeyspaces(t *testing.T, cluster vttest.LocalCluster) {
	client, err := vtctlclient.New(fmt.Sprintf("localhost:%v", cluster.GrpcPort()))
	assert.NoError(t, err)
	defer client.Close()
	stream, err := client.ExecuteVtctlCommand(
		context.Background(),
		[]string{
			"GetKeyspaces",
			"--server",
			fmt.Sprintf("localhost:%v", cluster.GrpcPort()),
		},
		30*time.Second,
	)
	assert.NoError(t, err)

	resp, err := consumeEventStream(stream)
	require.NoError(t, err)

	keyspaces := strings.Split(resp, "\n")
	if keyspaces[len(keyspaces)-1] == "" { // trailing newlines make Split annoying
		keyspaces = keyspaces[:len(keyspaces)-1]
	}

	assert.ElementsMatch(t, clusterKeyspaces, keyspaces)
}

func consumeEventStream(stream logutil.EventStream) (string, error) {
	var buf strings.Builder
	for {
		switch e, err := stream.Recv(); err {
		case nil:
			buf.WriteString(e.Value)
		case io.EOF:
			return buf.String(), nil
		default:
			return "", err
		}
	}
}

// startConsul starts a consul subprocess, and waits for it to be ready.
// Returns the exec.Cmd forked, and the server address to RPC-connect to.
func startConsul(t *testing.T) (*exec.Cmd, string) {
	// pick a random port to make sure things work with non-default port
	port := randomPort()

	cmd := exec.Command("consul",
		"agent",
		"-dev",
		"-http-port", fmt.Sprintf("%d", port))
	err := cmd.Start()
	if err != nil {
		t.Fatalf("failed to start consul: %v", err)
	}

	// Create a client to connect to the created consul.
	serverAddr := fmt.Sprintf("localhost:%v", port)
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
		_, _, err := kv.List("/", nil)
		if err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("Failed to start consul daemon in time. Consul is returning error: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	return cmd, serverAddr
}
