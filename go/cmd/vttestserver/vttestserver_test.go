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
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/tlstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/vttest"

	"github.com/golang/protobuf/jsonpb"

	"vitess.io/vitess/go/vt/proto/logutil"
	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"
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

	dir, err := ioutil.TempDir("/tmp", "vttestserver_persistent_mode_")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

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

func TestCanVtGateExecute(t *testing.T) {
	args := os.Args
	conf := config
	defer resetFlags(args, conf)

	cluster, err := startCluster()
	assert.NoError(t, err)
	defer cluster.TearDown()

	client, err := vtctlclient.New(fmt.Sprintf("localhost:%v", cluster.GrpcPort()))
	assert.NoError(t, err)
	defer client.Close()
	stream, err := client.ExecuteVtctlCommand(
		context.Background(),
		[]string{
			"VtGateExecute",
			"-server",
			fmt.Sprintf("localhost:%v", cluster.GrpcPort()),
			"select 'success';",
		},
		30*time.Second,
	)
	assert.NoError(t, err)

	var b strings.Builder
	b.Grow(1024)

Out:
	for {
		e, err := stream.Recv()
		switch err {
		case nil:
			b.WriteString(e.Value)
		case io.EOF:
			break Out
		default:
			assert.FailNow(t, err.Error())
		}
	}

	assert.Contains(t, b.String(), "success")
}

func TestMtlsAuth(t *testing.T) {
	args := os.Args
	conf := config
	defer resetFlags(args, conf)

	// Our test root.
	root, err := ioutil.TempDir("", "tlstest")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)

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
		"-grpc_auth_mode=mtls",
		fmt.Sprintf("-grpc_key=%s", key),
		fmt.Sprintf("-grpc_cert=%s", cert),
		fmt.Sprintf("-grpc_ca=%s", caCert),
		fmt.Sprintf("-vtctld_grpc_key=%s", clientKey),
		fmt.Sprintf("-vtctld_grpc_cert=%s", clientCert),
		fmt.Sprintf("-vtctld_grpc_ca=%s", caCert),
		fmt.Sprintf("-grpc_auth_mtls_allowed_substrings=%s", "CN=ClientApp"))
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
	root, err := ioutil.TempDir("", "tlstest")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)

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
		"-grpc_auth_mode=mtls",
		fmt.Sprintf("-grpc_key=%s", key),
		fmt.Sprintf("-grpc_cert=%s", cert),
		fmt.Sprintf("-grpc_ca=%s", caCert),
		fmt.Sprintf("-vtctld_grpc_key=%s", clientKey),
		fmt.Sprintf("-vtctld_grpc_cert=%s", clientCert),
		fmt.Sprintf("-vtctld_grpc_ca=%s", caCert),
		fmt.Sprintf("-grpc_auth_mtls_allowed_substrings=%s", "CN=ClientApp"))
	defer cluster.TearDown()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "code = Unauthenticated desc = client certificate not authorized")
}

func startPersistentCluster(dir string, flags ...string) (vttest.LocalCluster, error) {
	flags = append(flags, []string{
		"-persistent_mode",
		// FIXME: if port is not provided, data_dir is not respected
		fmt.Sprintf("-port=%d", randomPort()),
		fmt.Sprintf("-data_dir=%s", dir),
	}...)
	return startCluster(flags...)
}

func startCluster(flags ...string) (vttest.LocalCluster, error) {
	schemaDirArg := "-schema_dir=data/schema"
	tabletHostname := "-tablet_hostname=localhost"
	keyspaceArg := "-keyspaces=test_keyspace,app_customer"
	numShardsArg := "-num_shards=2,2"
	vschemaDDLAuthorizedUsers := "-vschema_ddl_authorized_users=%"
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

	err := vtctlclient.RunCommandAndWait(ctx, server, args, func(e *logutil.Event) {
		var keyspace vschema.Keyspace
		if err := jsonpb.UnmarshalString(e.Value, &keyspace); err != nil {
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
