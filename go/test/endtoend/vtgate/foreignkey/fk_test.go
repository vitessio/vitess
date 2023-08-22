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

	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestInsertWithFK tests that insertions work as expected when foreign key management is enabled in Vitess.
func TestInsertWithFK(t *testing.T) {
	conn, closer := start(t)
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
	conn, closer := start(t)
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
	conn, closer := start(t)
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

	// child table have cascade which is cross shard.
	qr = utils.Exec(t, conn, `update t2 set col = 126 where id = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// updating column which does not have foreign key constraint, so query will succeed.
	_ = utils.Exec(t, conn, `update t2 set mycol = 'baz' where id = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// child table have restrict in shard scoped and value exists in parent table.
	_ = utils.Exec(t, conn, `update t6 set col1 = 40 where pk = 20`)
	assert.EqualValues(t, 1, qr.RowsAffected)
}
