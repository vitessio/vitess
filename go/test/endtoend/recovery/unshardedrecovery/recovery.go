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

package unshardedrecovery

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/recovery"
	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

var (
	primary         *vitesst.Tablet
	replica1        *vitesst.Tablet
	replica2        *vitesst.Tablet
	replica3        *vitesst.Tablet
	localCluster    *vitesst.Cluster
	cell            = "zone1"
	keyspaceName    = "ks"
	dbName          = "vt_" + keyspaceName
	shardName       = "0"
	commonTabletArg = []string{
		"--vreplication-retry-delay", "1s",
		"--degraded-threshold", "5s",
		"--lock-tables-timeout", "5s",
		"--serving-state-grace-period", "1s",
	}
	recoveryKS1 = "recovery_ks1"
	recoveryKS2 = "recovery_ks2"

	// dbCredentialFile is where the tablet containers carry the passwords the
	// mysqld users are created with.
	dbCredentialFile = "/vt/files/db_credentials.json"
	dbCredentials    = `{
        "vt_dba": ["VtDbaPass"],
        "vt_app": ["VtAppPass"],
        "vt_allprivs": ["VtAllprivsPass"],
        "vt_repl": ["VtReplPass"],
        "vt_filtered": ["VtFilteredPass"]
	}`
	passwordUpdateSQL = `
					# Set real passwords for all users.
					SET PASSWORD FOR 'root'@'localhost' = 'RootPass';
					SET PASSWORD FOR 'vt_dba'@'localhost' = 'VtDbaPass';
					SET PASSWORD FOR 'vt_app'@'localhost' = 'VtAppPass';
					SET PASSWORD FOR 'vt_allprivs'@'localhost' = 'VtAllprivsPass';
					SET PASSWORD FOR 'vt_repl'@'%' = 'VtReplPass';
					SET PASSWORD FOR 'vt_filtered'@'localhost' = 'VtFilteredPass';
					SET PASSWORD FOR 'vt_appdebug'@'localhost' = 'VtDebugPass';
					`
	oldAlterTableMode = `SET GLOBAL old_alter_table = ON;`

	vtInsertTest = `create table vt_insert_test (
					  id bigint auto_increment,
					  msg varchar(64),
					  primary key (id)
					  ) Engine=InnoDB`
	vSchema = `{
    "tables": {
        "vt_insert_test": {}
    }
}`
)

// TestMainImpl creates cluster for unsharded recovery testing.
func TestMainImpl(m *testing.M) {
	ctx := context.Background()

	exitCode, err := func() (int, error) {
		// The mysqld users are created with passwords, so every vttablet needs
		// the credentials file to reach its mysqld.
		commonTabletArg = append(commonTabletArg, "--db-credentials-file", dbCredentialFile)
		if recovery.UseXb {
			commonTabletArg = append(commonTabletArg, recovery.XbArgs...)
		}

		keyspace := vitesst.WithKeyspace(keyspaceName).
			WithShardNames(shardName).
			WithReplicas(3).
			WithDurabilityPolicy("semi_sync")

		cluster, err := vitesst.NewCluster(
			vitesst.WithCells(cell),
			vitesst.WithBackupStorage(),
			vitesst.WithoutVTGate(),
			vitesst.WithVTOrc("--clusters-to-watch", keyspaceName),
			vitesst.WithVTTabletArgs(commonTabletArg...),
			vitesst.WithTabletFiles(vitesst.ContainerFile{
				Content:       []byte(dbCredentials),
				ContainerPath: dbCredentialFile,
			}),
			vitesst.WithInitDBSQLExtra(passwordUpdateSQL+oldAlterTableMode),
			keyspace,
		)
		if err != nil {
			return 1, err
		}

		cleanup, err := cluster.Start(ctx)
		defer func() {
			if err := cleanup(ctx); err != nil {
				log.Error(err.Error())
			}
		}()
		if err != nil {
			return 1, err
		}
		localCluster = cluster

		shard := cluster.Keyspace(keyspaceName).Shard(shardName)
		primary = shard.Primary()
		replicas := shard.Replicas()
		replica1, replica2, replica3 = replicas[0], replicas[1], replicas[2]

		for _, tablet := range []*vitesst.Tablet{replica2, replica3} {
			if err := detachTablet(ctx, tablet); err != nil {
				return 1, err
			}
		}
		return m.Run(), nil
	}()

	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	} else {
		os.Exit(exitCode)
	}
}

// detachTablet leaves a tablet with a running, empty mysqld and no tablet
// record, so that a test can later start its vttablet in a recovery keyspace
// and have it bootstrap itself from a backup.
func detachTablet(ctx context.Context, tablet *vitesst.Tablet) error {
	if err := tablet.StopVttablet(ctx); err != nil {
		return err
	}
	if err := localCluster.Vtctld().ExecuteCommand(ctx, "DeleteTablets", tablet.Alias()); err != nil {
		return err
	}

	queries := []string{
		"set global super_read_only = off",
		"set global read_only = off",
		"stop replica",
		"reset replica all",
		"drop database if exists " + dbName,
		"drop database if exists _vt",
		"reset binary logs and gtids",
	}
	for _, query := range queries {
		if _, err := tablet.QueryTabletWithDB(ctx, query, ""); err != nil {
			return fmt.Errorf("query %s on %s: %w", query, tablet.Alias(), err)
		}
	}
	return nil
}

// TestRecoveryImpl does following
// 1. create a shard with primary and replica1 only
//   - run InitShardPrimary
//   - insert some data
//
// 2. take a backup
// 3.create a recovery keyspace after first backup
//   - bring up tablet_replica2 in the new keyspace
//   - check that new tablet has data from backup1
//
// 4. insert more data on the primary
//
// 5. take another backup
// 6. create a recovery keyspace after second backup
//   - bring up tablet_replica3 in the new keyspace
//   - check that new tablet has data from backup2
//
// 7. check that vtgate queries work correctly
func TestRecoveryImpl(t *testing.T) {
	ctx := t.Context()
	verifyInitialReplication(t)

	// take first backup of value = test1
	err := localCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
	assert.NoError(t, err)

	backups := listBackups(t)
	require.Equal(t, len(backups), 1)
	assert.Contains(t, backups[0], backupAlias(replica1))

	err = localCluster.Vtctld().ExecuteCommand(ctx, "ApplyVSchema", "--vschema", vSchema, keyspaceName)
	assert.NoError(t, err)

	output, err := localCluster.Vtctld().ExecuteCommandWithOutput(ctx, "GetVSchema", keyspaceName)
	assert.NoError(t, err)
	assert.Contains(t, output, "vt_insert_test")

	// restore with latest backup
	restoreTime := time.Now().UTC()
	restoreTablet(t, replica2, recoveryKS1, restoreTime)

	output, err = localCluster.Vtctld().ExecuteCommandWithOutput(ctx, "GetSrvVSchema", cell)
	assert.NoError(t, err)
	assert.Contains(t, output, keyspaceName)
	assert.Contains(t, output, recoveryKS1)

	output, err = localCluster.Vtctld().ExecuteCommandWithOutput(ctx, "GetVSchema", recoveryKS1)
	assert.NoError(t, err)
	assert.Contains(t, output, "vt_insert_test")

	verifyRowsInTablet(t, replica2, 1)

	// verify that restored replica has value = test1
	qr, err := replica2.QueryTablet(ctx, "select msg from vt_insert_test where id = 1")
	assert.NoError(t, err)
	assert.Equal(t, "test1", qr.Rows[0][0].ToString())

	// insert new row on primary
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test2')")
	assert.NoError(t, err)
	verifyRowsInTablet(t, replica1, 2)

	// update the original row in primary
	_, err = primary.QueryTablet(ctx, "update vt_insert_test set msg = 'msgx1' where id = 1")
	assert.NoError(t, err)

	// verify that primary has new value
	qr, err = primary.QueryTablet(ctx, "select msg from vt_insert_test where id = 1")
	assert.NoError(t, err)
	assert.Equal(t, "msgx1", qr.Rows[0][0].ToString())

	// check that replica1, used for the backup, has the new value
	assert.Eventually(t, func() bool {
		qr, err := replica1.QueryTablet(ctx, "select msg from vt_insert_test where id = 1")
		if err != nil || len(qr.Rows) == 0 {
			return false
		}
		return qr.Rows[0][0].ToString() == "msgx1"
	}, 30*time.Second, time.Second, "timeout waiting for new value to be replicated on replica 1")

	// take second backup of value = msgx1
	err = localCluster.Vtctld().ExecuteCommand(ctx, "Backup", replica1.Alias())
	assert.NoError(t, err)

	// restore to first backup
	restoreTablet(t, replica3, recoveryKS2, restoreTime)

	output, err = localCluster.Vtctld().ExecuteCommandWithOutput(ctx, "GetVSchema", recoveryKS2)
	assert.NoError(t, err)
	assert.Contains(t, output, "vt_insert_test")

	// only one row from first backup
	verifyRowsInTablet(t, replica3, 1)

	// verify that restored replica has value = test1
	qr, err = replica3.QueryTablet(ctx, "select msg from vt_insert_test where id = 1")
	assert.NoError(t, err)
	assert.Equal(t, "test1", qr.Rows[0][0].ToString())

	vtgateInstance, err := localCluster.AddVTGate(ctx, "--tablet-types-to-wait", "REPLICA")
	assert.NoError(t, err)
	assert.NoError(t, waitForTabletsInVTGate(t, vtgateInstance, fmt.Sprintf("%s.%s.primary", keyspaceName, shardName), 1, 30*time.Second))
	assert.NoError(t, waitForTabletsInVTGate(t, vtgateInstance, fmt.Sprintf("%s.%s.replica", keyspaceName, shardName), 1, 30*time.Second))
	assert.NoError(t, waitForTabletsInVTGate(t, vtgateInstance, fmt.Sprintf("%s.%s.replica", recoveryKS1, shardName), 1, 30*time.Second))
	assert.NoError(t, waitForTabletsInVTGate(t, vtgateInstance, fmt.Sprintf("%s.%s.replica", recoveryKS2, shardName), 1, 30*time.Second))

	// Build vtgate grpc connection
	vtgateConn, err := vtgateInstance.DialVTGate(ctx)
	assert.NoError(t, err)
	defer vtgateConn.Close()
	session := vtgateConn.Session("@replica", nil)

	// check that vtgate doesn't route queries to new tablet
	verifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(2)")
	verifyQueriesUsingVtgate(t, session, "select msg from vt_insert_test where id = 1", `VARCHAR("msgx1")`)
	verifyQueriesUsingVtgate(t, session, fmt.Sprintf("select count(*) from %s.vt_insert_test", recoveryKS1), "INT64(1)")
	verifyQueriesUsingVtgate(t, session, fmt.Sprintf("select msg from %s.vt_insert_test where id = 1", recoveryKS1), `VARCHAR("test1")`)
	verifyQueriesUsingVtgate(t, session, fmt.Sprintf("select count(*) from %s.vt_insert_test", recoveryKS2), "INT64(1)")
	verifyQueriesUsingVtgate(t, session, fmt.Sprintf("select msg from %s.vt_insert_test where id = 1", recoveryKS2), `VARCHAR("test1")`)

	// check that new keyspace is accessible with 'use ks'
	executeQueriesUsingVtgate(t, session, "use "+recoveryKS1+"@replica")
	verifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")

	executeQueriesUsingVtgate(t, session, "use "+recoveryKS2+"@replica")
	verifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")

	// check that new tablet is accessible with use `ks:shard`
	executeQueriesUsingVtgate(t, session, "use `"+recoveryKS1+":0@replica`")
	verifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")

	executeQueriesUsingVtgate(t, session, "use `"+recoveryKS2+":0@replica`")
	verifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(1)")
}

// restoreTablet performs a PITR restore: it creates the recovery keyspace as a
// snapshot of the base keyspace at the given time and starts the tablet's
// vttablet in it, so that the tablet bootstraps itself from the backup taken
// before that time.
func restoreTablet(t *testing.T, tablet *vitesst.Tablet, restoreKSName string, restoreTime time.Time) {
	ctx := t.Context()

	_, err := localCluster.Vtctld().ExecuteCommandWithOutput(ctx, "GetKeyspace", restoreKSName)

	if restoreTime.IsZero() {
		restoreTime = time.Now().UTC()
	}

	if err != nil {
		err := localCluster.Vtctld().ExecuteCommand(ctx, "CreateKeyspace", restoreKSName,
			"--type=SNAPSHOT", "--base-keyspace="+keyspaceName,
			"--snapshot-timestamp", restoreTime.Format(time.RFC3339))
		require.NoError(t, err)
	}

	replicaTabletArgs := slices.Clone(commonTabletArg)
	replicaTabletArgs = append(replicaTabletArgs,
		"--enable-replication-reporter=false",
		"--init-tablet-type", "replica",
		"--init-keyspace", restoreKSName,
		"--init-shard", shardName,
		"--init-db-name-override", dbName,
	)

	require.NoError(t, tablet.StartVttablet(ctx, replicaTabletArgs...))
	require.NoError(t, tablet.WaitForTabletStatus(ctx, time.Minute, "SERVING"))
}

// verifyInitialReplication will create schema in primary, insert some data to primary and verify the same data in replica.
func verifyInitialReplication(t *testing.T) {
	ctx := t.Context()
	_, err := primary.QueryTablet(ctx, vtInsertTest)
	assert.NoError(t, err)
	_, err = primary.QueryTablet(ctx, "insert into vt_insert_test (msg) values ('test1')")
	assert.NoError(t, err)
	verifyRowsInTablet(t, replica1, 1)
}

// verifyRowsInTablet waits for the tablet to hold the expected number of rows.
func verifyRowsInTablet(t *testing.T, tablet *vitesst.Tablet, expectedRows int) {
	lastNumRowsFound := 0
	require.Eventuallyf(t, func() bool {
		qr, err := tablet.QueryTablet(t.Context(), "select * from vt_insert_test")
		if err != nil {
			return false
		}
		lastNumRowsFound = len(qr.Rows)
		return lastNumRowsFound == expectedRows
	}, time.Minute, 300*time.Millisecond,
		"unexpected number of rows in %s (%s.vt_insert_test): found %d", tablet.Alias(), keyspaceName, lastNumRowsFound)
}

// waitForTabletsInVTGate waits for vtgate to have the expected number of
// healthy tablets for the given target, e.g. "ks.0.replica".
func waitForTabletsInVTGate(t *testing.T, vtgate *vitesst.VTGate, target string, count int, timeout time.Duration) error {
	var lastCount float64
	if !assert.Eventually(t, func() bool {
		vars, err := vtgate.GetVars(t.Context())
		if err != nil {
			return false
		}
		connections, ok := vars["HealthcheckConnections"].(map[string]any)
		if !ok {
			return false
		}
		lastCount, _ = connections[target].(float64)
		return lastCount == float64(count)
	}, timeout, 300*time.Millisecond) {
		return fmt.Errorf("wait for %s failed, found %v healthy tablets", target, lastCount)
	}
	return nil
}

// verifyQueriesUsingVtgate verifies queries using vtgate.
func verifyQueriesUsingVtgate(t *testing.T, session *vtgateconn.VTGateSession, query string, value string) {
	qr, err := session.Execute(t.Context(), query, nil, false)
	require.NoError(t, err)
	assert.Equal(t, value, fmt.Sprintf("%v", qr.Rows[0][0]))
}

// executeQueriesUsingVtgate executes queries using vtgate.
func executeQueriesUsingVtgate(t *testing.T, session *vtgateconn.VTGateSession, query string) {
	_, err := session.Execute(t.Context(), query, nil, false)
	require.NoError(t, err)
}

// backupAlias returns the tablet alias in the zero-padded form a backup name
// carries, e.g. "zone1-0000000101".
func backupAlias(tablet *vitesst.Tablet) string {
	return fmt.Sprintf("%s-%010d", tablet.Cell, tablet.UID)
}

func listBackups(t *testing.T) []string {
	output, err := localCluster.ListBackups(t.Context(), keyspaceName, shardName)
	assert.NoError(t, err)
	return output
}
