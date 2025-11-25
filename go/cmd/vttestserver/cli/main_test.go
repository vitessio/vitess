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

package cli

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
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
	conf := config
	defer resetConfig(conf)

	cluster, err := startCluster()
	defer cluster.TearDown()

	require.NoError(t, err)
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "app_customer", table: "customers", vindex: "hash", vindexType: "hash", column: "id"})

	// Add Hash vindex via vtgate execution on table
	err = addColumnVindex(cluster, "test_keyspace", "alter vschema on test_table1 add vindex my_vdx (id)")
	assert.NoError(t, err)
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table1", vindex: "my_vdx", vindexType: "hash", column: "id"})
}

func TestPersistentMode(t *testing.T) {
	conf := config
	defer resetConfig(conf)

	dir := t.TempDir()

	cluster, err := startPersistentCluster(dir)
	require.NoError(t, err)

	// Add a new "ad-hoc" vindex via vtgate once the cluster is up, to later make sure it is persisted across teardowns
	err = addColumnVindex(cluster, "test_keyspace", "alter vschema on persistence_test add vindex my_vdx(id)")
	assert.NoError(t, err)

	// Basic sanity checks similar to TestRunsVschemaMigrations
	// See go/cmd/vttestserver/data/schema/app_customer/* and go/cmd/vttestserver/data/schema/test_keyspace/*
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "persistence_test", vindex: "my_vdx", vindexType: "hash", column: "id"})
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
	defer func() {
		cluster.PersistentMode = false // Cleanup the tmpdir as we're done
		cluster.TearDown()
	}()
	require.NoError(t, err)

	// rerun our sanity checks to make sure vschema is persisted correctly
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "persistence_test", vindex: "my_vdx", vindexType: "hash", column: "id"})
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
	conf := config
	defer resetConfig(conf)

	cluster, err := startCluster("--foreign-key-mode=allow", "--enable-online-ddl=true", "--enable-direct-ddl=true")
	require.NoError(t, err)
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
	cluster, err = startCluster("--foreign-key-mode=disallow", "--enable-online-ddl=false", "--enable-direct-ddl=false")
	require.NoError(t, err)
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

func TestNoScatter(t *testing.T) {
	conf := config
	defer resetConfig(conf)

	cluster, err := startCluster("--no-scatter")
	require.NoError(t, err)
	defer cluster.TearDown()

	_ = execOnCluster(cluster, "app_customer", func(conn *mysql.Conn) error {
		_, err = conn.ExecuteFetch("SELECT * FROM customers", 100, false)
		require.ErrorContains(t, err, "plan includes scatter, which is disallowed")
		return nil
	})
}

// TestCreateDbaTCPUser tests that the vt_dba_tcp user is created and can connect through TCP/IP connection
// when --initialize-with-vt-dba-tcp is set to true.
func TestCreateDbaTCPUser(t *testing.T) {
	conf := config
	defer resetConfig(conf)

	clusterInstance, err := startCluster("--initialize-with-vt-dba-tcp=true")
	require.NoError(t, err)
	defer clusterInstance.TearDown()

	defer func() {
		if t.Failed() {
			cluster.PrintFiles(t, clusterInstance.Env.Directory(), "init_db_with_vt_dba_tcp.sql")
		}
	}()

	// Ensure that the vt_dba_tcp user was created and can connect through TCP/IP connection.
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:  "127.0.0.1",
		Uname: "vt_dba_tcp",
		Port:  clusterInstance.Env.PortForProtocol("mysql", ""),
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	assert.NoError(t, err)
	defer conn.Close()

	// Ensure that the existing vt_dba user remains unaffected, meaning it cannot connect through TCP/IP connection.
	vtParams.Uname = "vt_dba"
	_, err = mysql.Connect(ctx, &vtParams)
	assert.Error(t, err)
}

func TestCanGetKeyspaces(t *testing.T) {
	conf := config
	defer resetConfig(conf)

	clusterInstance, err := startCluster()
	require.NoError(t, err)
	defer clusterInstance.TearDown()

	defer func() {
		if t.Failed() {
			cluster.PrintFiles(t, clusterInstance.Env.Directory(), "vtcombo.INFO", "error.log")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assertGetKeyspaces(ctx, t, clusterInstance)
}

func TestGatewayInitialTabletTimeout(t *testing.T) {
	conf := config
	defer resetConfig(conf)

	// Start cluster with custom gateway tablet timeout and verify it starts successfully
	cluster, err := startCluster("--gateway-initial-tablet-timeout=1s")
	require.NoError(t, err)
	defer cluster.TearDown()

	// Verify the cluster is functional by getting keyspaces
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assertGetKeyspaces(ctx, t, cluster)
}

func TestExternalTopoServerConsul(t *testing.T) {
	conf := config
	defer resetConfig(conf)

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

	cluster, err := startCluster("--external-topo-implementation=consul",
		"--external-topo-global-server-address="+serverAddr, "--external-topo-global-root=consul_test/global")
	require.NoError(t, err)
	defer cluster.TearDown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assertGetKeyspaces(ctx, t, cluster)
}

func TestMtlsAuth(t *testing.T) {
	conf := config
	defer resetConfig(conf)

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

	// When cluster starts it will apply SQL and VSchema migrations in the configured schema-dir folder
	// With mtls authorization enabled, the authorized CN must match the certificate's CN
	cluster, err := startCluster(
		"--grpc-auth-mode=mtls",
		fmt.Sprintf("%s=%s", "--grpc-key", key),
		fmt.Sprintf("%s=%s", "--grpc-cert", cert),
		fmt.Sprintf("%s=%s", "--grpc-ca", caCert),
		fmt.Sprintf("%s=%s", "--vtctld-grpc-key", clientKey),
		fmt.Sprintf("%s=%s", "--vtctld-grpc-cert", clientCert),
		fmt.Sprintf("%s=%s", "--vtctld-grpc-ca", caCert),
		fmt.Sprintf("%s=%s", "--grpc-auth-mtls-allowed-substrings", "CN=ClientApp"))
	require.NoError(t, err)
	defer func() {
		cluster.PersistentMode = false // Cleanup the tmpdir as we're done
		cluster.TearDown()
	}()

	// startCluster will apply vschema migrations using vtctl grpc and the clientCert.
	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "app_customer", table: "customers", vindex: "hash", vindexType: "hash", column: "id"})
}

func TestMtlsAuthUnauthorizedFails(t *testing.T) {
	conf := config
	defer resetConfig(conf)

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

	// When cluster starts it will apply SQL and VSchema migrations in the configured schema-dir folder
	// For mtls authorization failure by providing a client certificate with different CN thant the
	// authorized in the configuration
	cluster, err := startCluster(
		"--grpc-auth-mode=mtls",
		fmt.Sprintf("%s=%s", "--grpc-key", key),
		fmt.Sprintf("%s=%s", "--grpc-cert", cert),
		fmt.Sprintf("%s=%s", "--grpc-ca", caCert),
		fmt.Sprintf("%s=%s", "--vtctld-grpc-key", clientKey),
		fmt.Sprintf("%s=%s", "--vtctld-grpc-cert", clientCert),
		fmt.Sprintf("%s=%s", "--vtctld-grpc-ca", caCert),
		"--grpc-auth-mtls-allowed-substrings="+"CN=ClientApp")
	defer cluster.TearDown()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "code = Unauthenticated desc = client certificate not authorized")
}

func startPersistentCluster(dir string, flags ...string) (vttest.LocalCluster, error) {
	flags = append(flags, []string{
		"--persistent-mode",
		// FIXME: if port is not provided, data_dir is not respected
		fmt.Sprintf("--port=%d", randomPort()),
		"--data-dir=" + dir,
	}...)
	return startCluster(flags...)
}

var clusterKeyspaces = []string{
	"test_keyspace",
	"app_customer",
}

func startCluster(flags ...string) (cluster vttest.LocalCluster, err error) {
	args := []string{"vttestserver"}
	schemaDirArg := "--schema-dir=data/schema"
	tabletHostname := "--tablet-hostname" + "=localhost"
	keyspaceArg := "--keyspaces=" + strings.Join(clusterKeyspaces, ",")
	numShardsArg := "--num-shards=2,2"
	vschemaDDLAuthorizedUsers := "--vschema-ddl-authorized-users=%"
	alsoLogToStderr := "--alsologtostderr" // better debugging
	args = append(args, []string{schemaDirArg, keyspaceArg, numShardsArg, tabletHostname, vschemaDDLAuthorizedUsers, alsoLogToStderr}...)
	args = append(args, flags...)

	if err = New().ParseFlags(args); err != nil {
		return
	}

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

func resetConfig(conf vttest.Config) {
	config = conf
}

func randomPort() int {
	v := rand.Int32N(20000)
	return int(v + 10000)
}

func assertGetKeyspaces(ctx context.Context, t *testing.T, cluster vttest.LocalCluster) {
	client, err := vtctlclient.New(ctx, fmt.Sprintf("localhost:%v", cluster.GrpcPort()))
	assert.NoError(t, err)
	defer client.Close()
	stream, err := client.ExecuteVtctlCommand(
		ctx,
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
		"-http-port", strconv.Itoa(port))
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
