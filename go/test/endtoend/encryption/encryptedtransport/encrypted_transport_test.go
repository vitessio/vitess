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

/* This test makes sure encrypted transport over gRPC works.

The security chains are setup the following way:

* root CA
* vttablet server CA
* vttablet server instance cert/key
* vttablet client CA
* vttablet client 1 cert/key
* vtgate server CA
* vtgate server instance cert/key (common name is 'localhost')
* vtgate client CA
* vtgate client 1 cert/key
* vtgate client 2 cert/key

The following table shows all the checks we perform:
process:            will check its peer is signed by:  for link:

vttablet             vttablet client CA                vtgate -> vttablet
vtgate               vttablet server CA                vtgate -> vttablet

vtgate               vtgate client CA                  client -> vtgate
client               vtgate server CA                  client -> vtgate

Additionally, we have the following constraints:
- the client certificate common name is used as immediate
caller ID by vtgate, and forwarded to vttablet. This allows us to use
table ACLs on the vttablet side.
- the vtgate server certificate common name is set to 'localhost' so it matches
the hostname dialed by the vtgate clients. This is not a requirement for the
go client, that can set its expected server name. However, the python gRPC
client doesn't have the ability to set the server name, so they must match.
- the python client needs to have the full chain for the server validation
(that is 'vtgate server CA' + 'root CA'). A go client doesn't. So we read both
below when using the python client, but we only pass the intermediate cert
to the go clients (for vtgate -> vttablet link). */

package encryptedtransport

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/grpcclient"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	clusterInstance    *vitesst.Cluster
	createVtInsertTest = `create table vt_insert_test (
								id bigint auto_increment,
								msg varchar(64),
								keyspace_id bigint(20) unsigned NOT NULL,
								primary key (id)
								) Engine = InnoDB`
	keyspace      = "test_keyspace"
	hostname      = "localhost"
	shardName     = "0"
	certDirectory string
	grpcCert      = ""
	grpcKey       = ""
	grpcCa        = ""
	grpcName      = ""
)

// containerCertDir is where the generated certificates and the table ACL
// config are placed inside the Vitess containers.
const containerCertDir = "/vt/files"

func TestSecureTransport(t *testing.T) {
	ctx := t.Context()

	// create all certs
	certDirectory = t.TempDir()

	err := createCA()
	require.NoError(t, err)

	err = createIntermediateCA("ca", "01", "vttablet-server", "vttablet server CA")
	require.NoError(t, err)

	err = createIntermediateCA("ca", "02", "vttablet-client", "vttablet client CA")
	require.NoError(t, err)

	err = createIntermediateCA("ca", "03", "vtgate-server", "vtgate server CA")
	require.NoError(t, err)

	err = createIntermediateCA("ca", "04", "vtgate-client", "vtgate client CA")
	require.NoError(t, err)

	err = createSignedCert("vttablet-server", "01", "vttablet-server-instance", "vttablet server instance")
	require.NoError(t, err)

	err = createSignedCert("vttablet-client", "01", "vttablet-client-1", "vttablet client 1")
	require.NoError(t, err)

	err = createSignedCert("vtgate-server", "01", "vtgate-server-instance", "localhost")
	require.NoError(t, err)

	err = createSignedCert("vtgate-client", "01", "vtgate-client-1", "vtgate client 1")
	require.NoError(t, err)

	err = createSignedCert("vtgate-client", "02", "vtgate-client-2", "vtgate client 2")
	require.NoError(t, err)

	// table ACL config keyed off the client certificate common name.
	tableACLConfigJSON := `{
	"table_groups": [
	{
		"table_names_or_prefixes": ["vt_insert_test"],
		"readers": ["vtgate client 1"],
		"writers": ["vtgate client 1"],
		"admins": ["vtgate client 1"]
	}
  ]
}`
	tableACLContainerPath := containerCertDir + "/table_acl_config.json"

	certFiles := shipCertFiles(t)

	// vttablet serves with its server certificate and requires client
	// certificates signed by the vttablet client CA, and enforces table ACLs.
	// It also carries client certificates so tablets can reach each other's
	// tabletmanager during reparents.
	tabletArgs := []string{"--table-acl-config", tableACLContainerPath, "--queryserver-config-strict-table-acl"}
	tabletArgs = append(tabletArgs, serverExtraArguments(containerCertDir, "vttablet-server-instance", "vttablet-client")...)
	tabletArgs = append(tabletArgs, tmclientExtraArgs(containerCertDir, "vttablet-client-1")...)

	clusterInstance, err = vitesst.NewCluster(t,
		vitesst.WithoutVTGate(),
		vitesst.WithVTOrc(),
		vitesst.WithVTTabletArgs(tabletArgs...),
		vitesst.WithTabletFiles(append(certFiles, vitesst.ContainerFile{
			Content:       []byte(tableACLConfigJSON),
			ContainerPath: tableACLContainerPath,
		})...),
		vitesst.WithVTCtldArgs(tmclientExtraArgs(containerCertDir, "vttablet-client-1")...),
		vitesst.WithVTCtldFiles(certFiles...),
		vitesst.WithVTGateFiles(certFiles...),
		vitesst.WithKeyspace(keyspace).
			WithShardNames(shardName).
			WithReplicas(1).
			WithSchema(createVtInsertTest),
	)
	require.NoError(t, err)

	cleanup, err := clusterInstance.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cleanup(context.WithoutCancel(ctx)); err != nil {
			fmt.Fprintln(os.Stderr, "cluster teardown:", err)
		}
	})

	shard := clusterInstance.Keyspace(keyspace).Shard(shardName)
	primaryTablet := shard.Primary()
	replicaTablet := shard.Replicas()[0]

	for _, tablet := range []*vitesst.Tablet{primaryTablet, replicaTablet} {
		err = clusterInstance.Vtctld().ExecuteCommand(ctx, "RunHealthCheck", tablet.Alias())
		require.NoError(t, err)
	}

	// Start vtgate, dialing tablets with client certificates and serving
	// clients with its own server certificate.
	vtgateArgs := tabletConnExtraArgs(containerCertDir, "vttablet-client-1")
	vtgateArgs = append(vtgateArgs, serverExtraArguments(containerCertDir, "vtgate-server-instance", "vtgate-client")...)
	_, err = clusterInstance.AddVTGate(t, ctx, vtgateArgs...)
	require.NoError(t, err)

	grpcAddress := vtgateGRPCAddress(ctx, t)

	// 'vtgate client 1' is authorized to access vt_insert_test
	setCreds(t, "vtgate-client-1", "vtgate-server")
	request := getRequest("select * from vt_insert_test")
	vc, err := getVitessClient(ctx, grpcAddress)
	require.NoError(t, err)

	qr, err := vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.NoError(t, err)

	// 'vtgate client 2' is not authorized to access vt_insert_test
	setCreds(t, "vtgate-client-2", "vtgate-server")
	request = getRequest("select * from vt_insert_test")
	vc, err = getVitessClient(ctx, grpcAddress)
	require.NoError(t, err)
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")

	useEffectiveCallerID(ctx, t)
	useEffectiveGroups(ctx, t)
}

func useEffectiveCallerID(ctx context.Context, t *testing.T) {
	// now restart vtgate in the mode where we don't use SSL
	// for client connections, but we copy effective caller id
	// into immediate caller id.
	vtgateArgs := []string{"--grpc-use-effective-callerid"}
	vtgateArgs = append(vtgateArgs, tabletConnExtraArgs(containerCertDir, "vttablet-client-1")...)
	err := clusterInstance.VTGate().Restart(t, ctx, vtgateArgs...)
	require.NoError(t, err)

	grpcAddress := vtgateGRPCAddress(ctx, t)

	setSSLInfoEmpty()

	// get vitess client
	vc, err := getVitessClient(ctx, grpcAddress)
	require.NoError(t, err)

	// test with empty effective caller Id
	request := getRequest("select * from vt_insert_test")
	qr, err := vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")

	// 'vtgate client 1' is authorized to access vt_insert_test
	callerID := &vtrpc.CallerID{
		Principal: "vtgate client 1",
	}
	request = getRequestWithCallerID(callerID, "select * from vt_insert_test")
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.NoError(t, err)

	// 'vtgate client 2' is not authorized to access vt_insert_test
	callerID = &vtrpc.CallerID{
		Principal: "vtgate client 2",
	}
	request = getRequestWithCallerID(callerID, "select * from vt_insert_test")
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")
}

func useEffectiveGroups(ctx context.Context, t *testing.T) {
	// now restart vtgate in the mode where we don't use SSL
	// for client connections, but we copy effective caller's groups
	// into immediate caller id.
	vtgateArgs := []string{"--grpc-use-effective-callerid", "--grpc-use-effective-groups"}
	vtgateArgs = append(vtgateArgs, tabletConnExtraArgs(containerCertDir, "vttablet-client-1")...)
	err := clusterInstance.VTGate().Restart(t, ctx, vtgateArgs...)
	require.NoError(t, err)

	grpcAddress := vtgateGRPCAddress(ctx, t)

	setSSLInfoEmpty()

	// get vitess client
	vc, err := getVitessClient(ctx, grpcAddress)
	require.NoError(t, err)

	// test with empty effective caller Id
	request := getRequest("select * from vt_insert_test")
	qr, err := vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")

	// 'vtgate client 1' is authorized to access vt_insert_test
	callerID := &vtrpc.CallerID{
		Principal: "my-caller",
		Groups:    []string{"vtgate client 1"},
	}
	request = getRequestWithCallerID(callerID, "select * from vt_insert_test")
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.NoError(t, err)

	// 'vtgate client 2' is not authorized to access vt_insert_test
	callerID = &vtrpc.CallerID{
		Principal: "my-caller",
		Groups:    []string{"vtgate client 2"},
	}
	request = getRequestWithCallerID(callerID, "select * from vt_insert_test")
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")
}

// shipCertFiles turns every generated certificate and key into a ContainerFile
// placed under containerCertDir inside the Vitess containers.
func shipCertFiles(t *testing.T) []vitesst.ContainerFile {
	entries, err := os.ReadDir(certDirectory)
	require.NoError(t, err)

	var files []vitesst.ContainerFile
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".pem") {
			continue
		}
		files = append(files, vitesst.ContainerFile{
			HostPath:      path.Join(certDirectory, entry.Name()),
			ContainerPath: containerCertDir + "/" + entry.Name(),
		})
	}
	return files
}

// vtgateGRPCAddress returns the host-reachable gRPC address of the cluster's
// vtgate as "localhost:port", so the vtgate server certificate common name
// 'localhost' matches the dialed hostname.
func vtgateGRPCAddress(ctx context.Context, t *testing.T) string {
	t.Helper()
	addr, err := clusterInstance.VTGate().GRPCAddr(ctx)
	require.NoError(t, err)
	_, port, ok := strings.Cut(addr, ":")
	require.True(t, ok, "malformed vtgate grpc address %q", addr)
	return net.JoinHostPort(hostname, port)
}

func createCA() error {
	tlstest.CreateCA(certDirectory)
	return nil
}

func createIntermediateCA(ca string, serial string, name string, commonName string) error {
	tlstest.CreateIntermediateCA(certDirectory, ca, serial, name, commonName)
	return nil
}

func createSignedCert(ca string, serial string, name string, commonName string) error {
	tlstest.CreateSignedCert(certDirectory, ca, serial, name, commonName)
	return nil
}

func serverExtraArguments(dir string, name string, ca string) []string {
	args := []string{
		"--grpc-cert", dir + "/" + name + "-cert.pem",
		"--grpc-key", dir + "/" + name + "-key.pem",
		"--grpc-ca", dir + "/" + ca + "-cert.pem",
	}
	return args
}

func tmclientExtraArgs(dir string, name string) []string {
	ca := "vttablet-server"
	args := []string{
		"--tablet-manager-grpc-cert", dir + "/" + name + "-cert.pem",
		"--tablet-manager-grpc-key", dir + "/" + name + "-key.pem",
		"--tablet-manager-grpc-ca", dir + "/" + ca + "-cert.pem",
		"--tablet-manager-grpc-server-name", "vttablet server instance",
	}
	return args
}

func tabletConnExtraArgs(dir string, name string) []string {
	ca := "vttablet-server"
	args := []string{
		"--tablet-grpc-cert", dir + "/" + name + "-cert.pem",
		"--tablet-grpc-key", dir + "/" + name + "-key.pem",
		"--tablet-grpc-ca", dir + "/" + ca + "-cert.pem",
		"--tablet-grpc-server-name", "vttablet server instance",
	}
	return args
}

func getVitessClient(ctx context.Context, addr string) (vtgateservicepb.VitessClient, error) {
	opt, err := grpcclient.SecureDialOption(grpcCert, grpcKey, grpcCa, "", grpcName)
	if err != nil {
		return nil, err
	}
	cc, err := grpcclient.DialContext(ctx, addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, err
	}
	c := vtgateservicepb.NewVitessClient(cc)
	return c, nil
}

func setCreds(t *testing.T, name string, ca string) {
	f1, err := os.Open(path.Join(certDirectory, "ca-cert.pem"))
	require.NoError(t, err)
	b1, err := io.ReadAll(f1)
	require.NoError(t, err)

	f2, err := os.Open(path.Join(certDirectory, ca+"-cert.pem"))
	require.NoError(t, err)
	b2, err := io.ReadAll(f2)
	require.NoError(t, err)

	caContent := append(b1, b2...)
	fileName := "ca-" + name + ".pem"
	caVtgateClient := path.Join(certDirectory, fileName)
	f, err := os.Create(caVtgateClient)
	require.NoError(t, err)
	_, err = f.Write(caContent)
	require.NoError(t, err)

	grpcCa = caVtgateClient
	grpcKey = path.Join(certDirectory, name+"-key.pem")
	grpcCert = path.Join(certDirectory, name+"-cert.pem")

	err = f.Close()
	require.NoError(t, err)
	err = f2.Close()
	require.NoError(t, err)
	err = f1.Close()
	require.NoError(t, err)
}

func setSSLInfoEmpty() {
	grpcCa = ""
	grpcCert = ""
	grpcKey = ""
	grpcName = ""
}

func getSession() *vtgatepb.Session {
	return &vtgatepb.Session{
		TargetString: "test_keyspace:0@primary",
	}
}

func getRequestWithCallerID(callerID *vtrpc.CallerID, sql string) *vtgatepb.ExecuteRequest {
	session := getSession()
	return &vtgatepb.ExecuteRequest{
		CallerId: callerID,
		Session:  session,
		Query: &querypb.BoundQuery{
			Sql: sql,
		},
	}
}

func getRequest(sql string) *vtgatepb.ExecuteRequest {
	session := getSession()
	return &vtgatepb.ExecuteRequest{
		Session: session,
		Query: &querypb.BoundQuery{
			Sql: sql,
		},
	}
}
