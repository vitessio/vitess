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

package encryptedreplication

import (
	"context"
	"flag"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/tlstest"
)

var (
	keyspace  = "test_keyspace"
	shardName = "0"
)

// This test makes sure that we can use SSL replication with Vitess
func TestSecure(t *testing.T) {
	testReplicationBase(t, true)
	testReplicationBase(t, false)
}

// This test makes sure that we can use SSL replication with Vitess.
func testReplicationBase(t *testing.T, isClientCertPassed bool) {
	flag.Parse()

	ctx := t.Context()

	certDirectory := t.TempDir()

	tlstest.CreateCA(certDirectory)
	tlstest.CreateSignedCert(certDirectory, "ca", "01", "server", "Mysql Server")
	tlstest.CreateSignedCert(certDirectory, "ca", "02", "client", "Mysql Client")

	secureCnf := "require_secure_transport=true\n" +
		"ssl-ca=/vt/files/ca-cert.pem\n" +
		"ssl-cert=/vt/files/server-cert.pem\n" +
		"ssl-key=/vt/files/server-key.pem\n"

	tabletFiles := []vitesst.ContainerFile{
		{Content: []byte(secureCnf), ContainerPath: "/vt/files/secure.cnf"},
	}
	for _, name := range []string{"ca-cert.pem", "server-cert.pem", "server-key.pem", "client-cert.pem", "client-key.pem"} {
		tabletFiles = append(tabletFiles, vitesst.ContainerFile{
			HostPath:      filepath.Join(certDirectory, name),
			ContainerPath: "/vt/files/" + name,
		})
	}

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithoutVTGate(),
		vitesst.WithTabletFiles(tabletFiles...),
		vitesst.WithTabletEnv(map[string]string{"EXTRA_MY_CNF": "/vt/files/secure.cnf"}),
		vitesst.WithKeyspace(keyspace).
			WithShardNames(shardName).
			WithReplicas(1).
			WithoutPrimaryElection().
			WithTabletSpec(func(spec *vitesst.TabletSpec) {
				if isClientCertPassed && spec.Type == "replica" {
					spec.ExtraArgs = append(
						spec.ExtraArgs,
						"--db-flags", "2048",
						"--db-ssl-ca", "/vt/files/ca-cert.pem",
						"--db-ssl-cert", "/vt/files/client-cert.pem",
						"--db-ssl-key", "/vt/files/client-key.pem",
					)
				}
			}),
	)
	require.NoError(t, err, "setup failed")

	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err, "setup failed")
	defer func() {
		cctx := context.WithoutCancel(ctx)
		if err := cleanup(cctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	}()

	shard := cluster.Keyspace(keyspace).Shard(shardName)
	primaryTablet := shard.Replicas()[0]

	// Reparent using SSL (this will also check replication works)
	err = cluster.Vtctld().ExecuteCommand(
		ctx,
		"PlannedReparentShard",
		keyspace+"/"+shardName,
		"--wait-replicas-timeout", "31s",
		"--new-primary", primaryTablet.Alias(),
	)
	if isClientCertPassed {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
	}

	_, err = cluster.AddVTOrc(t, ctx, "")
	require.NoError(t, err)
}
