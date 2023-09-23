/*
Copyright 2023 The Vitess Authors.

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

package foreignkey

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

// getTestName prepends whether the test is for a sharded keyspace or not to the test name.
func getTestName(testName string, testSharded bool) string {
	if testSharded {
		return "Sharded - " + testName
	}
	return "Unsharded - " + testName
}

// isMultiColFkTable tells if the table is a multicol table or not.
func isMultiColFkTable(tableName string) bool {
	return strings.Contains(tableName, "multicol")
}

// waitForSchemaTrackingForFkTables waits for schema tracking to have run and seen the tables used
// for foreign key tests.
func waitForSchemaTrackingForFkTables(t *testing.T) {
	err := utils.WaitForColumn(t, clusterInstance.VtgateProcess, shardedKs, "fk_t1", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, shardedKs, "fk_t18", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, shardedKs, "fk_t11", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedKs, "fk_t1", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedKs, "fk_t18", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedKs, "fk_t11", "col")
	require.NoError(t, err)
}

// getReplicaTablets gets all the replica tablets.
func getReplicaTablets(keyspace string) []*cluster.Vttablet {
	var replicaTablets []*cluster.Vttablet
	for _, ks := range clusterInstance.Keyspaces {
		if ks.Name != keyspace {
			continue
		}
		for _, shard := range ks.Shards {
			for _, vttablet := range shard.Vttablets {
				if vttablet.Type != "primary" {
					replicaTablets = append(replicaTablets, vttablet)
				}
			}
		}
	}
	return replicaTablets
}

// removeAllForeignKeyConstraints removes all the foreign key constraints from the given tablet.
func removeAllForeignKeyConstraints(t *testing.T, vttablet *cluster.Vttablet, keyspace string) {
	getAllFksQuery := `SELECT RefCons.table_name, RefCons.constraint_name FROM information_schema.referential_constraints RefCons;`
	res, err := utils.RunSQL(t, getAllFksQuery, vttablet, "")
	require.NoError(t, err)
	var queries []string
	queries = append(queries, "set global super_read_only=0")
	for _, row := range res.Rows {
		tableName := row[0].ToString()
		constraintName := row[1].ToString()
		removeFkQuery := fmt.Sprintf("ALTER TABLE %v DROP CONSTRAINT %v", tableName, constraintName)
		queries = append(queries, removeFkQuery)
	}
	queries = append(queries, "set global super_read_only=1")
	err = utils.RunSQLs(t, queries, vttablet, fmt.Sprintf("vt_%v", keyspace))
	require.NoError(t, err)
}

// checkReplicationHealthy verifies that the replication on the given vttablet is working as expected.
func checkReplicationHealthy(t *testing.T, vttablet *cluster.Vttablet) {
	rs, err := utils.RunSQL(t, "show replica status", vttablet, "")
	require.NoError(t, err)
	var ioThreadRunning, sqlThreadRunning string
	for idx, value := range rs.Rows[0] {
		fieldName := rs.Fields[idx].Name
		if fieldName == "Replica_IO_Running" {
			ioThreadRunning = value.ToString()
		}
		if fieldName == "Replica_SQL_Running" {
			sqlThreadRunning = value.ToString()
		}
	}
	require.Equal(t, "Yes", sqlThreadRunning, "SQL Thread isn't happy on %v, Replica status - %v", vttablet.Alias, rs.Rows)
	require.Equal(t, "Yes", ioThreadRunning, "IO Thread isn't happy on %v, Replica status - %v", vttablet.Alias, rs.Rows)
}

// compareVitessAndMySQLResults compares Vitess and MySQL results and reports if they don't report the same number of rows affected.
func compareVitessAndMySQLResults(t *testing.T, vtRes sql.Result, mysqlRes sql.Result) {
	if vtRes == nil && mysqlRes == nil {
		return
	}
	if vtRes == nil {
		t.Error("Vitess result is 'nil' while MySQL's is not.")
		return
	}
	if mysqlRes == nil {
		t.Error("MySQL result is 'nil' while Vitess' is not.")
		return
	}
	vtRa, err := vtRes.RowsAffected()
	require.NoError(t, err)
	mysqlRa, err := mysqlRes.RowsAffected()
	require.NoError(t, err)
	if mysqlRa != vtRa {
		t.Errorf("Vitess and MySQL don't agree on the rows affected. Vitess rows affected - %v, MySQL rows affected - %v", vtRa, mysqlRa)
	}
}

// compareVitessAndMySQLErrors compares Vitess and MySQL errors and reports if one errors and the other doesn't.
func compareVitessAndMySQLErrors(t *testing.T, vtErr, mysqlErr error) {
	if vtErr != nil && mysqlErr != nil || vtErr == nil && mysqlErr == nil {
		return
	}
	out := fmt.Sprintf("Vitess and MySQL are not erroring the same way.\nVitess error: %v\nMySQL error: %v", vtErr, mysqlErr)
	t.Error(out)
}
