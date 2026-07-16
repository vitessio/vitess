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

package vtgate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

func TestUnownedLookupInsertNull(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t9(id, parent_id) VALUES (1, 1)")
	vitesst.Exec(t, conn, "insert into t9(id, parent_id) VALUES (2, 2)")

	vitesst.Exec(t, conn, "insert into t8(id, parent_id, t9_id) VALUES (1, 1, NULL)")
	vitesst.Exec(t, conn, "insert into t8(id, parent_id, t9_id) VALUES (2, 1, 1)")
	vitesst.Exec(t, conn, "insert into t8(id, parent_id, t9_id) VALUES (3, 2, 2)")
}

func TestLookupUniqueWithAutocommit(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	// conn2 is to check entries in the lookup table
	conn2, err := mysql.Connect(t.Context(), &vtParams)
	require.Nil(t, err)
	defer conn2.Close()

	// Test that all vindex writes are autocommitted outside of any ongoing transactions.
	//
	// Also test that autocommited vindex entries are visible inside transactions, as lookups
	// should also use the autocommit connection.

	vitesst.Exec(t, conn, "insert into t10(id, sharding_key) VALUES (1, 1)")

	vitesst.AssertMatches(t, conn2, "select id from t10_id_to_keyspace_id_idx order by id asc", "[[INT64(1)]]")
	vitesst.AssertMatches(t, conn, "select id from t10 where id = 1", "[[INT64(1)]]")

	vitesst.Exec(t, conn, "begin")

	vitesst.Exec(t, conn, "insert into t10(id, sharding_key) VALUES (2, 1)")

	vitesst.AssertMatches(t, conn2, "select id from t10_id_to_keyspace_id_idx order by id asc", "[[INT64(1)] [INT64(2)]]")
	vitesst.AssertMatches(t, conn, "select id from t10 where id = 2", "[[INT64(2)]]")

	vitesst.Exec(t, conn, "insert into t10(id, sharding_key) VALUES (3, 1)")

	vitesst.AssertMatches(t, conn2, "select id from t10_id_to_keyspace_id_idx order by id asc", "[[INT64(1)] [INT64(2)] [INT64(3)]]")
	vitesst.AssertMatches(t, conn, "select id from t10 where id = 3", "[[INT64(3)]]")

	vitesst.Exec(t, conn, "savepoint sp_foobar")

	vitesst.Exec(t, conn, "insert into t10(id, sharding_key) VALUES (4, 1)")

	vitesst.AssertMatches(t, conn2, "select id from t10_id_to_keyspace_id_idx order by id asc", "[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]")
	vitesst.AssertMatches(t, conn, "select id from t10 where id = 4", "[[INT64(4)]]")
}

func TestUnownedLookupInsertChecksKeyspaceIdsAreMatching(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t9(id, parent_id) VALUES (1, 1)")

	// This fails because the keyspace id for `parent_id` does not match the one for `t9_id`
	_, err := vitesst.ExecAllowError(t, conn, "insert into t8(id, parent_id, t9_id) VALUES (4, 2, 1)")
	require.EqualError(t, err, "values [[INT64(1)]] for column [t9_id] does not map to keyspace ids (errno 1105) (sqlstate HY000) during query: insert into t8(id, parent_id, t9_id) VALUES (4, 2, 1)")

	// This fails because the `t9_id` value can't be mapped to a keyspace id
	_, err = vitesst.ExecAllowError(t, conn, "insert into t8(id, parent_id, t9_id) VALUES (4, 2, 2)")
	require.EqualError(t, err, "values [[INT64(2)]] for column [t9_id] does not map to keyspace ids (errno 1105) (sqlstate HY000) during query: insert into t8(id, parent_id, t9_id) VALUES (4, 2, 2)")
}

func TestUnownedLookupSelectNull(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "select * from t8 WHERE t9_id IS NULL")
}

func TestConsistentLookup(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(t.Context(), &vtParams)
	require.Nil(t, err)
	defer conn2.Close()

	// Simple insert.
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	// check that the lookup query happens in the right connection
	vitesst.AssertMatches(t, conn, "select * from t1 where id2 = 4", "[[INT64(1) INT64(4)]]")
	vitesst.Exec(t, conn, "commit")
	vitesst.AssertMatches(t, conn, "select * from t1", "[[INT64(1) INT64(4)]]")
	qr := vitesst.Exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	// Inserting again should fail.
	vitesst.Exec(t, conn, "begin")
	_, err = conn.ExecuteFetch("insert into t1(id1, id2) values(1, 4)", 1000, false)
	vitesst.Exec(t, conn, "rollback")
	require.Error(t, err)
	mysqlErr := err.(*sqlerror.SQLError)
	assert.Equal(t, sqlerror.ERDupEntry, mysqlErr.Num)
	assert.Equal(t, "23000", mysqlErr.State)
	assert.ErrorContains(t, mysqlErr, "lookup.Create: target: ks.80-.primary: vttablet: (errno 1062) (sqlstate 23000)")

	// Simple delete.
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "delete from t1 where id1=1")
	vitesst.AssertMatches(t, conn, "select * from t1 where id2 = 4", "[]")
	vitesst.Exec(t, conn, "commit")
	qr = vitesst.Exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	qr = vitesst.Exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	// Autocommit insert.
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	qr = vitesst.Exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	qr = vitesst.Exec(t, conn, "select id2 from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	// Autocommit delete.
	vitesst.Exec(t, conn, "delete from t1 where id1=1")

	// Dangling row pointing to existing keyspace id.
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	// Delete the main row only.
	vitesst.Exec(t, conn2, "use `ks:-80`")
	vitesst.Exec(t, conn2, "delete from t1 where id1=1")
	// Verify the lookup row is still there.
	qr = vitesst.Exec(t, conn, "select id2 from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	// Insert should still succeed.
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	vitesst.Exec(t, conn, "commit")
	qr = vitesst.Exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	// Lookup row should be unchanged.
	qr = vitesst.Exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	// Dangling row not pointing to existing keyspace id.
	vitesst.Exec(t, conn2, "use `ks:-80`")
	vitesst.Exec(t, conn2, "delete from t1 where id1=1")
	// Update the lookup row with bogus keyspace id.
	vitesst.Exec(t, conn, "update t1_id2_idx set keyspace_id='aaa' where id2=4")
	qr = vitesst.Exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"aaa\")]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	// Insert should still succeed.
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	vitesst.Exec(t, conn, "commit")
	qr = vitesst.Exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	// lookup row must be updated.
	qr = vitesst.Exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	// Update, but don't change anything. This should not deadlock.
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "update t1 set id2=4 where id1=1")
	vitesst.Exec(t, conn, "commit")
	qr = vitesst.Exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	qr = vitesst.Exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	// Update, and change the lookup value. This should change main and lookup rows.
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "update t1 set id2=5 where id1=1")
	vitesst.Exec(t, conn, "commit")
	qr = vitesst.Exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(5)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	qr = vitesst.Exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(5) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
}

func TestDMLScatter(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	/* Simple insert. after this dml, the tables will contain the following:
	t3 (id5, id6, id7):
	1 2 3
	2 2 3
	3 4 3
	4 5 4

	t3_id7_idx (id7, keyspace_id:id6):
	3 2
	3 2
	3 4
	4 5
	*/
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t3(id5, id6, id7) values(1, 2, 3), (2, 2, 3), (3, 4, 3), (4, 5, 4)")
	vitesst.Exec(t, conn, "commit")
	qr := vitesst.Exec(t, conn, "select id5, id6, id7 from t3 order by id5")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(2) INT64(3)] [INT64(2) INT64(2) INT64(3)] [INT64(3) INT64(4) INT64(3)] [INT64(4) INT64(5) INT64(4)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	/* Updating a non lookup column. after this dml, the tables will contain the following:
	t3 (id5, id6, id7):
	42 2 3
	2 2 3
	3 4 3
	4 5 4

	t3_id7_idx (id7, keyspace_id:id6):
	3 2
	3 2
	3 4
	4 5
	*/
	vitesst.Exec(t, conn, "update `ks[-]`.t3 set id5 = 42 where id5 = 1")
	qr = vitesst.Exec(t, conn, "select id5, id6, id7 from t3 order by id5")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) INT64(2) INT64(3)] [INT64(3) INT64(4) INT64(3)] [INT64(4) INT64(5) INT64(4)] [INT64(42) INT64(2) INT64(3)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	/* Updating a lookup column. after this dml, the tables will contain the following:
	t3 (id5, id6, id7):
	42 2 42
	2 2 42
	3 4 3
	4 5 4

	t3_id7_idx (id7, keyspace_id:id6):
	42 2
	42 2
	3 4
	4 5
	*/
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "update t3 set id7 = 42 where id6 = 2")
	vitesst.Exec(t, conn, "commit")
	qr = vitesst.Exec(t, conn, "select id5, id6, id7 from t3 order by id5")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) INT64(2) INT64(42)] [INT64(3) INT64(4) INT64(3)] [INT64(4) INT64(5) INT64(4)] [INT64(42) INT64(2) INT64(42)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	/* delete one specific keyspace id. after this dml, the tables will contain the following:
	t3 (id5, id6, id7):
	3 4 3
	4 5 4

	t3_id7_idx (id7, keyspace_id:id6):
	3 4
	4 5
	*/
	vitesst.Exec(t, conn, "delete from t3 where id6 = 2")
	qr = vitesst.Exec(t, conn, "select * from t3 where id6 = 2")
	require.Empty(t, qr.Rows)
	qr = vitesst.Exec(t, conn, "select * from t3_id7_idx where id6 = 2")
	require.Empty(t, qr.Rows)

	// delete all the rows.
	vitesst.Exec(t, conn, "delete from `ks[-]`.t3")
	qr = vitesst.Exec(t, conn, "select * from t3")
	require.Empty(t, qr.Rows)
	qr = vitesst.Exec(t, conn, "select * from t3_id7_idx")
	require.Empty(t, qr.Rows)
}

func TestDMLIn(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	/* Simple insert. after this dml, the tables will contain the following:
	t3 (id5, id6, id7):
	1 2 3
	2 2 3
	3 4 3
	4 5 4

	t3_id7_idx (id7, keyspace_id:id6):
	3 2
	3 2
	3 4
	4 5
	*/
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t3(id5, id6, id7) values(1, 2, 3), (2, 2, 3), (3, 4, 3), (4, 5, 4)")
	vitesst.Exec(t, conn, "commit")
	qr := vitesst.Exec(t, conn, "select id5, id6, id7 from t3 order by id5, id6")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(2) INT64(3)] [INT64(2) INT64(2) INT64(3)] [INT64(3) INT64(4) INT64(3)] [INT64(4) INT64(5) INT64(4)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	/* Updating a non lookup column. after this dml, the tables will contain the following:
	t3 (id5, id6, id7):
	1 2 3
	2 2 3
	42 4 3
	42 5 4

	t3_id7_idx (id7, keyspace_id:id6):
	3 2
	3 2
	3 4
	4 5
	*/
	vitesst.Exec(t, conn, "update t3 set id5 = 42 where id6 in (4, 5)")
	qr = vitesst.Exec(t, conn, "select id5, id6, id7 from t3 order by id5, id6")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(2) INT64(3)] [INT64(2) INT64(2) INT64(3)] [INT64(42) INT64(4) INT64(3)] [INT64(42) INT64(5) INT64(4)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	/* Updating a non lookup column. after this dml, the tables will contain the following:
	t3 (id5, id6, id7):
	1 2 42
	2 2 42
	42 4 3
	42 5 4

	t3_id7_idx (id7, keyspace_id:id6):
	42 2
	42 2
	3 4
	42 5
	*/
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "update t3 set id7 = 42 where id6 in (2, 5)")
	vitesst.Exec(t, conn, "commit")
	qr = vitesst.Exec(t, conn, "select id5, id6, id7 from t3 order by id5, id6")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(2) INT64(42)] [INT64(2) INT64(2) INT64(42)] [INT64(42) INT64(4) INT64(3)] [INT64(42) INT64(5) INT64(42)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	/* Updating a non lookup column. after this dml, the tables will contain the following:
	t3 (id5, id6, id7):
	42 4 3
	42 5 4

	t3_id7_idx (id7, keyspace_id:id6):
	3 4
	42 5
	*/
	vitesst.Exec(t, conn, "delete from t3 where id6 in (2)")
	qr = vitesst.Exec(t, conn, "select * from t3 where id6 = 2")
	require.Empty(t, qr.Rows)
	qr = vitesst.Exec(t, conn, "select * from t3_id7_idx where id6 = 2")
	require.Empty(t, qr.Rows)

	// delete all the rows.
	vitesst.Exec(t, conn, "delete from t3 where id6 in (4, 5)")
	qr = vitesst.Exec(t, conn, "select * from t3")
	require.Empty(t, qr.Rows)
	qr = vitesst.Exec(t, conn, "select * from t3_id7_idx")
	require.Empty(t, qr.Rows)
}

func TestConsistentLookupMultiInsert(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(t.Context(), &vtParams)
	require.Nil(t, err)
	defer conn2.Close()

	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1,4), (2,5)")
	vitesst.Exec(t, conn, "commit")
	qr := vitesst.Exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)] [INT64(2) INT64(5)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	qr = vitesst.Exec(t, conn, "select count(*) from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	// Delete one row but leave its lookup dangling.
	vitesst.Exec(t, conn2, "use `ks:-80`")
	vitesst.Exec(t, conn2, "delete from t1 where id1=1")
	// Insert a bogus lookup row.
	vitesst.Exec(t, conn, "insert into t1_id2_idx(id2, keyspace_id) values(6, 'aaa')")
	// Insert 3 rows:
	// first row will insert without changing lookup.
	// second will insert and change lookup.
	// third will be a fresh insert for main and lookup.
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1,2), (3,6), (4,7)")
	vitesst.Exec(t, conn, "commit")
	qr = vitesst.Exec(t, conn, "select id1, id2 from t1 order by id1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(2)] [INT64(2) INT64(5)] [INT64(3) INT64(6)] [INT64(4) INT64(7)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	qr = vitesst.Exec(t, conn, "select * from t1_id2_idx where id2=6")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(6) VARBINARY(\"N\\xb1\\x90ɢ\\xfa\\x16\\x9c\")]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
	qr = vitesst.Exec(t, conn, "select count(*) from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(5)]]"; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}
}

func TestHashLookupMultiInsertIgnore(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(t.Context(), &vtParams)
	require.Nil(t, err)
	defer conn2.Close()

	vitesst.Exec(t, conn, "delete from t2")
	vitesst.Exec(t, conn, "delete from t2_id4_idx")
	defer func() {
		vitesst.Exec(t, conn, "delete from t2")
		vitesst.Exec(t, conn, "delete from t2_id4_idx")
	}()

	// DB should start out clean
	vitesst.AssertMatches(t, conn, "select count(*) from t2_id4_idx", "[[INT64(0)]]")
	vitesst.AssertMatches(t, conn, "select count(*) from t2", "[[INT64(0)]]")

	// Try inserting a bunch of ids at once
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert ignore into t2(id3, id4) values(50,60), (30,40), (10,20)")
	vitesst.Exec(t, conn, "commit")

	// Verify
	vitesst.AssertMatches(t, conn, "select id3, id4 from t2 order by id3", "[[INT64(10) INT64(20)] [INT64(30) INT64(40)] [INT64(50) INT64(60)]]")
	vitesst.AssertMatches(t, conn, "select id3, id4 from t2_id4_idx order by id3", "[[INT64(10) INT64(20)] [INT64(30) INT64(40)] [INT64(50) INT64(60)]]")
}

func TestConsistentLookupUpdate(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	/* Simple insert. after this dml, the tables will contain the following:
	t4 (id1, id2):
	1 2
	2 2
	3 3
	4 3

	t4_id2_idx (id2, id1, keyspace_id:id1):
	2 1 1
	2 2 2
	3 3 3
	3 4 4
	*/
	vitesst.Exec(t, conn, "insert into t4(id1, id2) values(1, '2'), (2, '2'), (3, '3'), (4, '3')")
	qr := vitesst.Exec(t, conn, "select id1, id2 from t4 order by id1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("2")] [INT64(2) VARCHAR("2")] [INT64(3) VARCHAR("3")] [INT64(4) VARCHAR("3")]]`; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	/* Updating a lookup column. after this dml, the tables will contain the following:
	t4 (id1, id2):
	1 42
	2 2
	3 3
	4 3

	t4_id2_idx (id2, id1, keyspace_id:id1):
	42 1 1
	2 2 2
	3 3 3
	3 4 4
	*/
	vitesst.Exec(t, conn, "update t4 a set a.id2 = '42' where a.id1 = 1")
	qr = vitesst.Exec(t, conn, "select id1, id2 from t4 order by id1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("42")] [INT64(2) VARCHAR("2")] [INT64(3) VARCHAR("3")] [INT64(4) VARCHAR("3")]]`; got != want {
		assert.Failf(t, "select mismatch", "select:\n%v want\n%v", got, want)
	}

	/* delete one specific keyspace id. after this dml, the tables will contain the following:
	t4 (id1, id2):
	2 2
	3 3
	4 3

	t4_id2_idx (id2, id1, keyspace_id:id1):
	2 2 2
	3 3 3
	3 4 4
	*/
	vitesst.Exec(t, conn, "delete from t4 where id2 = '42'")
	qr = vitesst.Exec(t, conn, "select * from t4 where id2 = '42'")
	require.Empty(t, qr.Rows)
	qr = vitesst.Exec(t, conn, "select * from t4_id2_idx where id2 = '42'")
	require.Empty(t, qr.Rows)

	// delete all the rows.
	vitesst.Exec(t, conn, "delete from t4")
	qr = vitesst.Exec(t, conn, "select * from t4")
	require.Empty(t, qr.Rows)
	qr = vitesst.Exec(t, conn, "select * from t4_id2_idx")
	require.Empty(t, qr.Rows)
}

func TestSelectMultiEqualLookup(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t10 (id, sharding_key, col1) values (1, 1, 'bar'), (2, 1, 'bar'), (3, 1, 'bar'), (4, 2, 'bar'), (5, 2, 'bar')")

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			vitesst.Exec(t, conn, "set workload = "+workload)

			vitesst.AssertMatches(t, conn, "select id from t10 WHERE (col1, id) IN (('bar', 1), ('baz', 2), ('qux', 3), ('barbar', 4))", "[[INT64(1)]]")
			vitesst.AssertMatches(t, conn, "select id from t10 WHERE (col1 = 'bar' AND id = 1) OR (col1 = 'baz' AND id = 2) OR (col1 = 'qux' AND id = 3) OR (col1 = 'barbar' AND id = 4)", "[[INT64(1)]]")
		})
	}
}

func TestSelectNullLookup(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t6(id1, id2) values(1, 'a'), (2, 'b'), (3, null)")

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			vitesst.Exec(t, conn, "set workload = "+workload)
			vitesst.AssertMatches(t, conn, "select id1, id2 from t6 order by id1", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")] [INT64(3) NULL]]")
			vitesst.AssertIsEmpty(t, conn, "select id1, id2 from t6 where id2 = null")
			vitesst.AssertMatches(t, conn, "select id1, id2 from t6 where id2 is null", "[[INT64(3) NULL]]")
			vitesst.AssertMatches(t, conn, "select id1, id2 from t6 where id2 is not null order by id1", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
			vitesst.AssertIsEmpty(t, conn, "select id1, id2 from t6 where id1 IN (null)")
			vitesst.AssertMatches(t, conn, "select id1, id2 from t6 where id1 IN (1,2,null) order by id1", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
			vitesst.AssertIsEmpty(t, conn, "select id1, id2 from t6 where id1 NOT IN (1,null) order by id1")
			vitesst.AssertMatches(t, conn, "select id1, id2 from t6 where id1 NOT IN (1,3)", "[[INT64(2) VARCHAR(\"b\")]]")
		})
	}
}

func TestUnicodeLooseMD5CaseInsensitive(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t4(id1, id2) values(1, 'test')")

	vitesst.AssertMatches(t, conn, "SELECT id1, id2 from t4 where id2 = 'Test'", `[[INT64(1) VARCHAR("test")]]`)
}

func TestJoinWithPredicateAndJoinOnDifferentVindex(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "select t4.id1 from t4, t3 where t4.id2 = 'foo' and t4.id1 = t3.id6")
}
