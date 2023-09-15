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
