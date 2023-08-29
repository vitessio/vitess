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
	assert.ErrorContains(t, err, "VT12002: unsupported: cross-shard foreign keys (errno 1235) (sqlstate 42000)")

	// child foreign key is cascade, so this should work as expected.
	qr = utils.Exec(t, conn, `delete from multicol_tbl1 where cola = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)
	// we also verify that the rows in the child table were deleted.
	qr = utils.Exec(t, conn, `select * from multicol_tbl2 where cola = 100`)
	assert.Zero(t, qr.Rows)

	// Unsharded keyspace tests
	utils.Exec(t, conn, `use uks`)
	// insert some data.
	utils.Exec(t, conn, `insert into u_t1(id, col1) values (100, 123), (10, 12), (1, 13), (1000, 1234)`)
	utils.Exec(t, conn, `insert into u_t2(id, col2) values (342, 123), (19, 1234)`)

	// Delete from u_t1 which has a foreign key constraint to t2 with SET NULL type.
	qr = utils.Exec(t, conn, `delete from u_t1 where id = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)
	// Verify the result in u_t2 as well
	utils.AssertMatches(t, conn, `select * from u_t2`, `[[INT64(342) NULL] [INT64(19) INT64(1234)]]`)
}

// TestUpdations tests that update work as expected when foreign key management is enabled in Vitess.
func TestUpdateWithFK(t *testing.T) {
	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()

	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)
	utils.Exec(t, conn, `insert into t2(id, col, mycol) values (100, 125, 'foo'), (1, 132, 'bar')`)
	utils.Exec(t, conn, `insert into t4(id, col, t2_col, t2_mycol) values (1, 321, 132, 'bar')`)
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

	// updating column which does not have foreign key constraint, so query will succeed.
	_ = utils.Exec(t, conn, `update t2 set mycol = 'baz' where id = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// child table have restrict in shard scoped and value exists in parent table.
	_ = utils.Exec(t, conn, `update t6 set col1 = 40 where pk = 20`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// Unsharded keyspace tests
	utils.Exec(t, conn, `use uks`)
	// insert some data.
	utils.Exec(t, conn, `insert into u_t1(id, col1) values (100, 123), (10, 12), (1, 13), (1000, 1234)`)
	utils.Exec(t, conn, `insert into u_t2(id, col2) values (342, 123), (19, 1234)`)
	utils.Exec(t, conn, `insert into u_t3(id, col3) values (32, 123), (1, 12)`)

	t.Run("Cascade update with a new value", func(t *testing.T) {
		t.Skip("This doesn't work right now. We are able to only cascade updates for which the data already exists in the parent table")
		_ = utils.Exec(t, conn, `update u_t1 set col1 = 2 where id = 100`)
	})

	// Update u_t1 which has a foreign key constraint to u_t2 with SET NULL type, and to u_t3 with CASCADE type.
	qr = utils.Exec(t, conn, `update u_t1 set col1 = 13 where id = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)
	// Verify the result in u_t2 and u_t3 as well.
	utils.AssertMatches(t, conn, `select * from u_t2 order by id`, `[[INT64(19) INT64(1234)] [INT64(342) NULL]]`)
	utils.AssertMatches(t, conn, `select * from u_t3 order by id`, `[[INT64(1) INT64(12)] [INT64(32) INT64(13)]]`)

	// Update u_t1 which has a foreign key constraint to u_t2 with SET NULL type, and to u_t3 with CASCADE type.
	// This update however doesn't change the table.
	qr = utils.Exec(t, conn, `update u_t1 set col1 = 1234 where id = 1000`)
	assert.EqualValues(t, 0, qr.RowsAffected)
	// Verify the result in u_t2 and u_t3 as well.
	utils.AssertMatches(t, conn, `select * from u_t2 order by id`, `[[INT64(19) INT64(1234)] [INT64(342) NULL]]`)
	utils.AssertMatches(t, conn, `select * from u_t3 order by id`, `[[INT64(1) INT64(12)] [INT64(32) INT64(13)]]`)
}

// TestFkScenarios tests the various foreign key scenarios with different constraints
// and makes sure that Vitess works with them as expected. All the tables are present in both sharded and unsharded keyspace
// and all the foreign key constraints are cross-shard ones for the sharded keyspace.
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
		}, {
			name: "Update failure with restrict foreign keys",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7)",
				"insert into fk_t2(id, col) values (1, 7)",
			},
			dmlQuery: "update fk_t1 set col = 5 where id = 1",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Update success with restrict foreign keys",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7)",
			},
			dmlQuery: "update fk_t1 set col = 5 where id = 2",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Delete failure with restrict foreign keys",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7)",
				"insert into fk_t2(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t1 where id = 1",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Delete success with restrict foreign keys",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t1 where id = 2",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Update success with set null foreign key",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t6(id, col) values (1, 7)",
			},
			dmlQuery: "update fk_t3 set col = 9 where id = 1",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t6 order by id",
			},
		}, {
			name: "Update failure with set null foreign key with child having a restrict foreign key",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t4(id, col) values (1, 7)",
				"insert into fk_t5(id, col) values (1, 7)",
			},
			dmlQuery: "update fk_t3 set col = 9 where id = 1",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t4 order by id",
				"select * from fk_t5 order by id",
			},
		}, {
			name: "Update success with cascaded set nulls",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t4(id, col) values (1, 7), (2, 9)",
				"insert into fk_t6(id, col) values (1, 7), (2, 9)",
			},
			dmlQuery: "update fk_t2 set col = 9 where id = 1",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t4 order by id",
				"select * from fk_t6 order by id",
			},
		}, {
			name: "Delete success with set null foreign key",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t6(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t3 where id = 1",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t6 order by id",
			},
		}, {
			name: "Delete failure with set null foreign key with child having a restrict foreign key",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t4(id, col) values (1, 7)",
				"insert into fk_t5(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t3 where id = 1",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t4 order by id",
				"select * from fk_t5 order by id",
			},
		}, {
			name: "Delete success with cascaded set nulls",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t4(id, col) values (1, 7), (2, 9)",
				"insert into fk_t6(id, col) values (1, 7), (2, 9)",
			},
			dmlQuery: "delete from fk_t2 where id = 1",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t4 order by id",
				"select * from fk_t6 order by id",
			},
		}, {
			name: "Update success with cascade foreign key",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
			},
			dmlQuery: "update fk_t10 set col = 5 where id = 1",
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
			},
		}, {
			name: "Update failure with cascade foreign key with child having a restrict foreign key",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
				"insert into fk_t13(id, col) values (1, 7)",
			},
			dmlQuery: "update fk_t10 set col = 5 where id = 1",
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
				"select * from fk_t13 order by id",
			},
		}, {
			name: "Update success with cascaded cascade foreign keys",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
				"insert into fk_t12(id, col) values (1, 7)",
			},
			dmlQuery: "update fk_t10 set col = 5 where id = 1",
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
				"select * from fk_t12 order by id",
			},
		}, {
			name: "Delete success with cascade foreign key",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t10 where id = 1",
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
			},
		}, {
			name: "Delete failure with cascade foreign key with child having a restrict foreign key",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
				"insert into fk_t13(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t10 where id = 1",
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
				"select * from fk_t13 order by id",
			},
		}, {
			name: "Delete success with cascaded cascade foreign keys",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
				"insert into fk_t12(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t10 where id = 1",
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
				"select * from fk_t12 order by id",
			},
		}, {
			name: "Delete success with set null to an update cascade foreign key",
			dataQueries: []string{
				"insert into fk_t15(id, col) values (1, 7), (2, 9)",
				"insert into fk_t16(id, col) values (1, 7), (2, 9)",
				"insert into fk_t17(id, col) values (1, 7)",
				"insert into fk_t18(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t16 where id = 1",
			assertionQueries: []string{
				"select * from fk_t15 order by id",
				"select * from fk_t16 order by id",
				"select * from fk_t17 order by id",
				"select * from fk_t18 order by id",
			},
		}, {
			name: "Delete success with cascade to delete with set null to an update set null foreign key",
			dataQueries: []string{
				"insert into fk_t15(id, col) values (1, 7), (2, 9)",
				"insert into fk_t16(id, col) values (1, 7), (2, 9)",
				"insert into fk_t17(id, col) values (1, 7)",
				"insert into fk_t19(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t15 where id = 1",
			assertionQueries: []string{
				"select * from fk_t15 order by id",
				"select * from fk_t16 order by id",
				"select * from fk_t17 order by id",
				"select * from fk_t19 order by id",
			},
		}, {
			name: "Update success with cascade to an update set null to an update cascade foreign key",
			dataQueries: []string{
				"insert into fk_t15(id, col) values (1, 7), (2, 9)",
				"insert into fk_t16(id, col) values (1, 7), (2, 9)",
				"insert into fk_t17(id, col) values (1, 7)",
				"insert into fk_t18(id, col) values (1, 7)",
			},
			dmlQuery: "update fk_t15 set col = 3 where id = 1",
			assertionQueries: []string{
				"select * from fk_t15 order by id",
				"select * from fk_t16 order by id",
				"select * from fk_t17 order by id",
				"select * from fk_t18 order by id",
			},
		},
	}

	for _, tt := range testcases {
		for _, testSharded := range []bool{false, true} {
			t.Run(getTestName(tt.name, testSharded), func(t *testing.T) {
				mcmp, closer := start(t)
				defer closer()
				// To test unsharded case in vtgate, we just change the keyspace we use for the test.
				if !testSharded {
					_ = utils.Exec(t, mcmp.VtConn, "use `uks`")
				}

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

	for _, testSharded := range []bool{false, true} {
		t.Run(getTestName("Transactions with intermediate failure", testSharded), func(t *testing.T) {
			mcmp, closer := start(t)
			defer closer()
			// To test unsharded case in vtgate, we just change the keyspace we use for the test.
			if !testSharded {
				_ = utils.Exec(t, mcmp.VtConn, "use `uks`")
			}

			// Insert some rows
			mcmp.Exec("INSERT INTO fk_t10(id, col) VALUES (1, 7), (2, 9), (3, 5)")
			mcmp.Exec("INSERT INTO fk_t11(id, col) VALUES (1, 7), (2, 9), (3, 5)")
			mcmp.Exec("INSERT INTO fk_t12(id, col) VALUES (1, 7), (2, 9), (3, 5)")

			// Start a transaction
			mcmp.Exec("BEGIN")

			// Insert another row.
			mcmp.Exec("INSERT INTO fk_t13(id, col) VALUES (1, 7)")

			// Delete success for cascaded (2, 9)
			mcmp.Exec("DELETE FROM fk_t10 WHERE id = 2")

			// Verify the results
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")

			// Update that fails
			_, err = mcmp.ExecAllowAndCompareError("UPDATE fk_t10 SET col = 15 WHERE id = 1")
			require.Error(t, err)

			// Verify the results
			// Since we are in a transaction, we still expect the transaction to be ongoing, with no change to the tables
			// since the update should fail.
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")

			// Update that is a success
			mcmp.Exec("UPDATE fk_t10 SET col = 15 where id = 3")

			// Verify the results
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")

			// Insert a new row
			mcmp.Exec("INSERT INTO fk_t13(id, col) VALUES (3, 15)")

			// Verify the results
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")

			// Rollback the transaction.
			mcmp.Exec("ROLLBACK")

			// Verify the results
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")
		})
	}
}

// getTestName prepends whether the test is for a sharded keyspace or not to the test name.
func getTestName(testName string, testSharded bool) string {
	if testSharded {
		return "Sharded - " + testName
	}
	return "Unsharded - " + testName
}
