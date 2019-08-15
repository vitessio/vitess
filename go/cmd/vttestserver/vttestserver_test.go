package main

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"vitess.io/vitess/go/vt/vttest"

	"github.com/golang/protobuf/jsonpb"
	"vitess.io/vitess/go/vt/proto/logutil"
	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/tlstest"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
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
	cluster, err := startCluster()
	assert.NoError(t, err)

	defer cluster.TearDown()

	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "app_customer", table: "customers", vindex: "hash", vindexType: "hash", column: "id"})
}

func TestMtlsAuth(t *testing.T) {
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

func TestMtlsAuthUnaothorizedFails(t *testing.T) {
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

func startCluster(flags ...string) (*vttest.LocalCluster, error) {
	schemaDirArg := "-schema_dir=data/schema"
	webDirArg := "-web_dir=web/vtctld/app"
	webDir2Arg := "-web_dir2=web/vtctld2/app"
	tabletHostname := "-tablet_hostname=localhost"
	keyspaceArg := "-keyspaces=test_keyspace,app_customer"
	numShardsArg := "-num_shards=2,2"
	os.Args = append(os.Args, []string{schemaDirArg, keyspaceArg, numShardsArg, webDirArg, webDir2Arg, tabletHostname}...)
	os.Args = append(os.Args, flags...)
	return runCluster()
}

func assertColumnVindex(t *testing.T, cluster *vttest.LocalCluster, expected columnVindex) {
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
	if err != nil {
		t.Error(err)
	}
}

func assertEqual(t *testing.T, actual string, expected string, message string) {
	if actual != expected {
		t.Errorf("%s: actual %s, expected %s", message, actual, expected)
	}
}
