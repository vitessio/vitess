/*
Copyright 2020 The Vitess Authors.

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

package vault

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/network"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/tlstest"
)

var (
	createTable = `create table product (id bigint(20) primary key, name char(10), created bigint(20));`
	insertTable = `insert into product (id, name, created) values(%d, '%s', unix_timestamp());`
)

var (
	keyspaceName   = "ks"
	vtgateUser     = "vtgate_user"
	vtgatePassword = "password123"

	commonTabletArg = []string{
		"--vreplication-retry-delay", "1s",
		"--degraded-threshold", "5s",
		"--lock-tables-timeout", "5s",
		// Frequently reload schema, generating some tablet traffic,
		//   so we can speed up token refresh
		"--queryserver-config-schema-reload-time", "5s",
		"--serving-state-grace-period", "1s",
	}
	vaultTabletArg = []string{
		"--db-credentials-server", "vault",
		"--db-credentials-vault-timeout", "3s",
		"--db-credentials-vault-path", "kv/prod/dbcreds",
		// This is overriden by our env VAULT_ADDR
		"--db-credentials-vault-addr", "https://127.0.0.1:8200",
		// This is overriden by our env VAULT_CACERT
		"--db-credentials-vault-tls-ca", "/path/to/ca.pem",
		// This is provided by our env VAULT_ROLEID
		//"--db-credentials-vault-roleid", "34644576-9ffc-8bb5-d046-4a0e41194e15",
		// Contents of this file provided by our env VAULT_SECRETID
		//"--db-credentials-vault-secretidfile", "/path/to/file/containing/secret_id",
		// Make this small, so we can get a renewal
		"--db-credentials-vault-ttl", "21s",
	}
	vaultVTGateArg = []string{
		"--mysql-auth-server-impl", "vault",
		"--mysql-auth-vault-timeout", "3s",
		"--mysql-auth-vault-path", "kv/prod/vtgatecreds",
		// This is overriden by our env VAULT_ADDR
		"--mysql-auth-vault-addr", "https://127.0.0.1:8200",
		// This is overriden by our env VAULT_CACERT
		"--mysql-auth-vault-tls-ca", "/path/to/ca.pem",
		// This is provided by our env VAULT_ROLEID
		//"--mysql-auth-vault-roleid", "34644576-9ffc-8bb5-d046-4a0e41194e15",
		// Contents of this file provided by our env VAULT_SECRETID
		//"--mysql-auth-vault-role-secretidfile", "/path/to/file/containing/secret_id",
		// Make this small, so we can get a renewal
		"--mysql-auth-vault-ttl", "21s",
	}
	// The Vault secret sets a password for each MySQL user; the tablets fetch
	// those passwords from Vault to reach their mysqld.
	dbPasswordSQL = `
					SET PASSWORD FOR 'root'@'localhost' = 'RootPass';
					SET PASSWORD FOR 'vt_dba'@'localhost' = 'VtDbaPass';
					SET PASSWORD FOR 'vt_app'@'localhost' = 'VtAppPass';
					SET PASSWORD FOR 'vt_allprivs'@'localhost' = 'VtAllprivsPass';
					SET PASSWORD FOR 'vt_repl'@'%' = 'VtReplPass';
					SET PASSWORD FOR 'vt_filtered'@'localhost' = 'VtFilteredPass';
					SET PASSWORD FOR 'vt_appdebug'@'localhost' = 'VtDebugPass';
					`
	tokenRenewalString = "Vault client status: token renewed"
)

func TestVaultAuth(t *testing.T) {
	ctx := t.Context()

	// The tablets and vtgate must reach Vault at a stable network address the
	// moment they start, so they share a caller-owned network with it.
	nw, err := network.New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := nw.Remove(context.WithoutCancel(ctx)); err != nil {
			t.Logf("removing vault network: %v", err)
		}
	})

	// The tablets and vtgate verify the Vault server certificate against this CA,
	// so the certificate is minted for the network alias they connect to.
	certDir := t.TempDir()
	tlstest.CreateCA(certDir)
	tlstest.CreateSignedCert(certDir, tlstest.CA, "01", vaultNetworkAlias, vaultNetworkAlias)

	// Start Vault and mint AppRole credentials for Vitess.
	vs, err := startVaultServer(ctx, nw, certDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := vs.stop(context.WithoutCancel(ctx)); err != nil {
			t.Logf("stopping vault server: %v", err)
		}
	})
	require.NotEmpty(t, vs.roleID)
	require.NotEmpty(t, vs.secretID)

	// Passing via environment, easier than trying to modify
	// vtgate/vttablet flags within our test machinery
	vaultEnv := map[string]string{
		"VAULT_ADDR":     "https://" + vaultNetworkAlias + ":8200",
		"VAULT_CACERT":   "/vt/files/vault-ca.pem",
		"VAULT_ROLEID":   vs.roleID,
		"VAULT_SECRETID": vs.secretID,
	}
	caFile := vitesst.ContainerFile{HostPath: filepath.Join(certDir, caCertFile), ContainerPath: "/vt/files/vault-ca.pem"}

	clusterInstance, err := vitesst.NewCluster(t,
		vitesst.WithNetwork(nw),
		vitesst.WithVTTabletArgs(commonTabletArg...),
		vitesst.WithVTTabletArgs(vaultTabletArg...),
		vitesst.WithVTGateArgs(vaultVTGateArg...),
		vitesst.WithTabletEnv(vaultEnv),
		vitesst.WithVTGateEnv(vaultEnv),
		vitesst.WithTabletFiles(caFile),
		vitesst.WithVTGateFiles(caFile),
		vitesst.WithInitDBSQLExtra(dbPasswordSQL),
		vitesst.WithVTOrc(),
		// We don't really need the replica to test this feature
		//   but keeping it in to excercise the vt_repl user/password path
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(1).
			WithDurabilityPolicy("semi_sync").
			WithSchema(createTable),
	)
	require.NoError(t, err)

	cleanup, err := clusterInstance.Start(t, ctx)
	t.Cleanup(func() {
		cctx := context.WithoutCancel(ctx)
		if err := cleanup(cctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	primary := shard.Primary()
	replica := shard.Replicas()[0]

	// This tests the vtgate Vault auth & indirectly vttablet Vault auth too
	insertRow(t, clusterInstance, 1, "prd-1")
	insertRow(t, clusterInstance, 2, "prd-2")

	verifyRowsInTabletForTable(t, replica, 2, "product")

	// Check the primary tablet log for the Vault token renewal message.
	//   If we don't see it, that is a test failure.
	require.Eventually(t, func() bool {
		logs, err := primary.Logs(ctx)
		if err != nil {
			return false
		}
		return strings.Contains(logs, tokenRenewalString)
	}, 90*time.Second, time.Second, "expected %q in primary tablet logs", tokenRenewalString)
}

// verifyRowsInTabletForTable polls a tablet's mysqld until the table holds the
// expected number of rows, so replication has time to catch up.
func verifyRowsInTabletForTable(t *testing.T, tablet *vitesst.Tablet, expectedRows int, tableName string) {
	ctx := t.Context()
	lastNumRowsFound := 0
	require.Eventuallyf(t, func() bool {
		qr, err := tablet.QueryTablet(ctx, "select * from "+tableName)
		if err != nil || qr == nil {
			return false
		}
		lastNumRowsFound = len(qr.Rows)
		return lastNumRowsFound == expectedRows
	}, time.Minute, 300*time.Millisecond,
		"unexpected number of rows in %s (%s), last found %d", tableName, tablet.Alias(), lastNumRowsFound)
}

func insertRow(t *testing.T, clusterInstance *vitesst.Cluster, id int, productName string) {
	ctx := t.Context()
	vtParams := clusterInstance.VTParams(ctx, "")
	vtParams.Uname = vtgateUser
	vtParams.Pass = vtgatePassword
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	insertSmt := fmt.Sprintf(insertTable, id, productName)
	_, err = conn.ExecuteFetch(insertSmt, 1000, true)
	require.NoError(t, err)
}
