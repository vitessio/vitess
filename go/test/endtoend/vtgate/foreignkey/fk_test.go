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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestInsertWithFK tests that insertions work as expected when foreign key management is enabled in Vitess.
func TestInsertWithFK(t *testing.T) {
	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()

	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)

	// Verify that inserting data into a table that has shard scoped foreign keys works.
	utils.Exec(t, conn, `insert into t2(id, col) values (100, 125), (1, 132)`)

	// Verify that insertion fails if the data doesn't follow the fk constraint.
	_, err := utils.ExecAllowError(t, conn, `insert into t2(id, col) values (1310, 125)`)
	assert.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")

	// Verify that insertion fails if the table has cross-shard foreign keys (even if the data follows the constraints).
	_, err = utils.ExecAllowError(t, conn, `insert into t3(id, col) values (100, 100)`)
	assert.ErrorContains(t, err, "VT12002: unsupported: cross-shard foreign keys")

	// insert some data in a table with multicol vindex.
	utils.Exec(t, conn, `insert into multicol_tbl1(cola, colb, colc, msg) values (100, 'a', 'b', 'msg'), (101, 'c', 'd', 'msg2')`)

	// Verify that inserting data into a table that has shard scoped multi-column foreign keys works.
	utils.Exec(t, conn, `insert into multicol_tbl2(cola, colb, colc, msg) values (100, 'a', 'b', 'msg3')`)

	// Verify that insertion fails if the data doesn't follow the fk constraint.
	_, err = utils.ExecAllowError(t, conn, `insert into multicol_tbl2(cola, colb, colc, msg) values (103, 'c', 'd', 'msg2')`)
	assert.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")
}

// TestDeleteWithFK tests that deletions work as expected when foreign key management is enabled in Vitess.
func TestDeleteWithFK(t *testing.T) {
	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()

	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)
	utils.Exec(t, conn, `insert into t2(id, col) values (100, 125), (1, 132)`)
	utils.Exec(t, conn, `insert into t4(id, col) values (1, 321)`)
	utils.Exec(t, conn, `insert into multicol_tbl1(cola, colb, colc, msg) values (100, 'a', 'b', 'msg'), (101, 'c', 'd', 'msg2')`)
	utils.Exec(t, conn, `insert into multicol_tbl2(cola, colb, colc, msg) values (100, 'a', 'b', 'msg3')`)

	// child foreign key is shard scoped. Query will fail at mysql due to On Delete Restrict.
	_, err := utils.ExecAllowError(t, conn, `delete from t2 where col = 132`)
	assert.ErrorContains(t, err, "Cannot delete or update a parent row: a foreign key constraint fails")

	// child row does not exist so query will succeed.
	qr := utils.Exec(t, conn, `delete from t2 where col = 125`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// table's child foreign key has cross shard fk, so query will fail at vtgate.
	_, err = utils.ExecAllowError(t, conn, `delete from t1 where id = 42`)
	assert.ErrorContains(t, err, "VT12002: unsupported: foreign keys management at vitess (errno 1235) (sqlstate 42000)")

	// child foreign key is cascade, so query will fail at vtgate.
	_, err = utils.ExecAllowError(t, conn, `delete from multicol_tbl1 where cola = 100`)
	assert.ErrorContains(t, err, "VT12002: unsupported: foreign keys management at vitess (errno 1235) (sqlstate 42000)")
}

// TestUpdations tests that update work as expected when foreign key management is enabled in Vitess.
func TestUpdateWithFK(t *testing.T) {
	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()

	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)
	utils.Exec(t, conn, `insert into t2(id, col, mycol) values (100, 125, 'foo'), (1, 132, 'bar')`)
	utils.Exec(t, conn, `insert into t4(id, col, t2_mycol) values (1, 321, 'bar')`)
	utils.Exec(t, conn, `insert into t5(pk, sk, col1) values (1, 1, 1),(2, 1, 1),(3, 1, 10),(4, 1, 20),(5, 1, 30),(6, 1, 40)`)
	utils.Exec(t, conn, `insert into t6(pk, sk, col1) values (10, 1, 1), (20, 1, 20)`)

	// parent foreign key is shard scoped and value is not updated. Query will succeed.
	_ = utils.Exec(t, conn, `update t4 set t2_mycol = 'bar' where id = 1`)

	// parent foreign key is shard scoped and value does not exists in parent table. Query will fail at mysql due to On Update Restrict.
	_, err := utils.ExecAllowError(t, conn, `update t4 set t2_mycol = 'foo' where id = 1`)
	assert.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")

	// updating column which does not have foreign key constraint, so query will succeed.
	qr := utils.Exec(t, conn, `update t4 set col = 20 where id = 1`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// child table have cascade which is cross shard. Query will fail at vtgate.
	_, err = utils.ExecAllowError(t, conn, `update t2 set col = 125 where id = 100`)
	assert.ErrorContains(t, err, "VT12002: unsupported: foreign keys management at vitess (errno 1235) (sqlstate 42000)")

	// updating column which does not have foreign key constraint, so query will succeed.
	_ = utils.Exec(t, conn, `update t2 set mycol = 'baz' where id = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// child table have restrict in shard scoped and value exists in parent table.
	_ = utils.Exec(t, conn, `update t6 set col1 = 40 where pk = 20`)
	assert.EqualValues(t, 1, qr.RowsAffected)
}

// TestFkScenarios tests the various foreign key scenarios with different constraints
// and makes sure that Vitess works with them as expected.
// The test has 3 independent Schemas that are used for testing -
/*
 *                    fk_t1
 *                        │
 *                        │ On Delete Restrict
 *                        │ On Update Restrict
 *                        ▼
 *   ┌────────────────fk_t2────────────────┐
 *   │                                     │
 *   │On Delete Set Null                   │ On Delete Set Null
 *   │On Update Set Null                   │ On Update Set Null
 *   ▼                                     ▼
 * fk_t7                                fk_t3───────────────────┐
 *                                         │                    │
 *                                         │                    │ On Delete Set Null
 *                      On Delete Set Null │                    │ On Update Set Null
 *                      On Update Set Null │                    │
 *                                         ▼                    ▼
 *                                      fk_t4                fk_t6
 *                                         │
 *                                         │
 *                      On Delete Restrict │
 *                      On Update Restrict │
 *                                         │
 *                                         ▼
 *                                      fk_t5
 */
/*
 *                fk_t10
 *                   │
 * On Delete Cascade │
 * On Update Cascade │
 *                   │
 *                   ▼
 *                fk_t11──────────────────┐
 *                   │                    │
 *                   │                    │ On Delete Restrict
 * On Delete Cascade │                    │ On Update Restrict
 * On Update Cascade │                    │
 *                   │                    │
 *                   ▼                    ▼
 *                fk_t12               fk_t13
 */
/*
 *                 fk_t15
 *                    │
 *                    │
 *  On Delete Cascade │
 *  On Update Cascade │
 *                    │
 *                    ▼
 *                 fk_t16
 *                    │
 * On Delete Set Null │
 * On Update Set Null │
 *                    │
 *                    ▼
 *                 fk_t17──────────────────┐
 *                    │                    │
 *                    │                    │ On Delete Set Null
 *  On Delete Cascade │                    │ On Update Set Null
 *  On Update Cascade │                    │
 *                    │                    │
 *                    ▼                    ▼
 *                 fk_t18               fk_t19
 */
func TestFkScenarios(t *testing.T) {
	// Wait for schema-tracking to be complete.
	err := utils.WaitForColumn(t, clusterInstance.VtgateProcess, "ks", "fk_t1", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, "ks", "fk_t18", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, "ks", "fk_t11", "col")
	require.NoError(t, err)

	testcases := []struct {
		name             string
		dataQueries      []string
		dmlQuery         string
		assertionQueries []string
	}{
		{
			name: "Insert failure due to parent key not existing",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 5)",
			},
			dmlQuery: "insert into t2(id, col) values (1, 7)",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Insert success",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7)",
			},
			dmlQuery: "insert into fk_t2(id, col) values (1, 7)",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			mcmp, closer := start(t)
			defer closer()

			// Insert all the data required for running the test.
			for _, query := range tt.dataQueries {
				mcmp.Exec(query)
			}

			// Run the DML query that needs to be tested and verify output with MySQL.
			_, _ = mcmp.ExecAllowAndCompareError(tt.dmlQuery)

			// Run the assertion queries and verify we get the expected outputs.
			for _, query := range tt.assertionQueries {
				mcmp.Exec(query)
			}
		})
	}
}
