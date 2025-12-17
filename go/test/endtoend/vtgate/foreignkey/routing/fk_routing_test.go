/*
Copyright 2025 The Vitess Authors.

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

package routing

import (
	_ "embed"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	sourceKs        = "sks" // Source keyspace
	targetKs        = "tks" // Target keyspace
	Cell            = "test"

	//go:embed source_schema.sql
	sourceSchema string

	//go:embed target_schema.sql
	targetSchema string

	//go:embed routing_rules.json
	routingRules string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start source keyspace
		sKs := &cluster.Keyspace{
			Name:      sourceKs,
			SchemaSQL: sourceSchema,
		}

		err = clusterInstance.StartUnshardedKeyspace(*sKs, 0, false, clusterInstance.Cell)
		if err != nil {
			return 1
		}

		// Start target keyspace
		tKs := &cluster.Keyspace{
			Name:      targetKs,
			SchemaSQL: targetSchema,
		}

		err = clusterInstance.StartUnshardedKeyspace(*tKs, 0, false, clusterInstance.Cell)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctldClientProcess.ApplyRoutingRules(routingRules)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctldClientProcess.ExecuteCommand("RebuildVSchemaGraph")
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

// TestForeignKeyRoutingRules validates that foreign key routing rules work correctly:
//
// Setup: Source keyspace (sks) has t1->t2 and t3->t4 FK relationships.
//
//	Target keyspace (tks) has only t1->t2 FK relationship (t3,t4 exist but no FK).
//	Routing rules route all sks.* tables to tks.*
//
// Expected behavior:
//   - Source keyspace tables should have NO FK relationships (routed elsewhere)
//   - Target keyspace should preserve its schema-defined FK relationships:
//   - t1->t2 FK relationship exists within tks
//   - t3,t4 have no FK relationship (as per target schema)
//
// This validates the core routing rules behavior: FKs are only established
// where tables actually reside, preventing cross-keyspace relationships.
func TestForeignKeyRoutingRules(t *testing.T) {
	// Wait for schema tracking to complete
	utils.WaitForVschemaCondition(t, clusterInstance.VtgateProcess, targetKs, func(t *testing.T, keyspace map[string]interface{}) bool {
		tables := keyspace["tables"].(map[string]interface{})
		tbl := tables["t1"].(map[string]interface{})
		return tbl["child_foreign_keys"] != nil
	}, "tks.t1 should have child foreign key")

	vschema := getVSchemaFromVtgate(t)
	keyspaces := convertToMap(vschema["keyspaces"])

	// Core test 1: Source keyspace should have NO FK relationships (all tables routed)
	sourceKeyspace := convertToMap(keyspaces[sourceKs])
	for _, table := range []string{"t1", "t2", "t3", "t4"} {
		assertNoForeignKeys(t, sourceKeyspace, table, "sks."+table+" should have no FKs (routed to tks)")
	}

	// Core test 2: Target keyspace preserves schema-defined FK relationships
	targetKeyspace := convertToMap(keyspaces[targetKs])

	// Verify t1<->t2 FK relationship exists in target (bidirectional)
	// t2 has parent FK to t1 (t2.t1_id references t1.id)
	// t1 has child FK from t2 (automatically created by Vitess)
	assertFKRelationship(t, targetKeyspace, "t2", "t1", targetKs, "parent") // t2 references t1
	assertFKRelationship(t, targetKeyspace, "t1", "t2", targetKs, "child")  // t1 is referenced by t2

	// Verify t3,t4 have NO FK relationship in target (as per target schema)
	assertNoForeignKeys(t, targetKeyspace, "t3", "tks.t3 should have no FKs (target schema has no t3->t4 FK)")
	assertNoForeignKeys(t, targetKeyspace, "t4", "tks.t4 should have no FKs (target schema has no t3->t4 FK)")
}

// getVSchemaFromVtgate fetches the vschema from vtgate using the same pattern as other tests
func getVSchemaFromVtgate(t *testing.T) map[string]interface{} {
	vs, err := clusterInstance.VtgateProcess.ReadVSchema()
	require.NoError(t, err, "failed to read vschema from vtgate")
	return convertToMap(*vs)
}

// convertToMap converts interface{} to map[string]interface{} (from utils)
func convertToMap(input interface{}) map[string]interface{} {
	output, ok := input.(map[string]interface{})
	if !ok {
		return make(map[string]interface{})
	}
	return output
}

// assertFKRelationship validates that a specific FK relationship exists between tables
func assertFKRelationship(t *testing.T, keyspace map[string]interface{}, tableName, expectedRefTable, expectedKeyspace, fkType string) {
	tables := convertToMap(keyspace["tables"])
	table := convertToMap(tables[tableName])

	var fks interface{}
	var tableKey string
	if fkType == "child" {
		fks = table["child_foreign_keys"]
		tableKey = "child_table"
	} else {
		fks = table["parent_foreign_keys"]
		tableKey = "parent_table"
	}

	require.NotNil(t, fks, "%s.%s should have %s foreign keys", expectedKeyspace, tableName, fkType)
	fkSlice, ok := fks.([]interface{})
	require.True(t, ok, "FK should be a slice")
	require.NotEmpty(t, fkSlice, "%s.%s should have at least one %s FK", expectedKeyspace, tableName, fkType)

	// Verify the FK points to the expected table in the expected keyspace
	fk := convertToMap(fkSlice[0])
	refTableName, ok := fk[tableKey].(string)
	require.True(t, ok, "FK referenced table should be a string")
	require.NotEmpty(t, refTableName, "FK referenced table name should not be empty")

	// Parse the table name format "keyspace.table"
	expectedFullName := expectedKeyspace + "." + expectedRefTable
	assert.Equal(t, expectedFullName, refTableName, "%s.%s %s FK should reference table %s", expectedKeyspace, tableName, fkType, expectedFullName)
}

// assertNoForeignKeys checks that a table has no foreign key relationships
func assertNoForeignKeys(t *testing.T, keyspace map[string]interface{}, tableName, message string) {
	tables := convertToMap(keyspace["tables"])
	table := convertToMap(tables[tableName])

	childFKs := table["child_foreign_keys"]
	parentFKs := table["parent_foreign_keys"]

	// Check if FKs are nil or empty
	if childFKs != nil {
		childSlice, ok := childFKs.([]interface{})
		if ok {
			assert.Empty(t, childSlice, message+" (child FKs)")
		}
	}
	if parentFKs != nil {
		parentSlice, ok := parentFKs.([]interface{})
		if ok {
			assert.Empty(t, parentSlice, message+" (parent FKs)")
		}
	}
}
