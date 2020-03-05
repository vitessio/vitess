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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestConsistentLookup(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn2.Close()

	// Simple insert.
	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	exec(t, conn, "commit")
	qr := exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Inserting again should fail.
	exec(t, conn, "begin")
	_, err = conn.ExecuteFetch("insert into t1(id1, id2) values(1, 4)", 1000, false)
	exec(t, conn, "rollback")
	want := "duplicate entry"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("second insert: %v, must contain %s", err, want)
	}

	// Simple delete.
	exec(t, conn, "begin")
	exec(t, conn, "delete from t1 where id1=1")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Autocommit insert.
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select id2 from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// Autocommit delete.
	exec(t, conn, "delete from t1 where id1=1")

	// Dangling row pointing to existing keyspace id.
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	// Delete the main row only.
	exec(t, conn2, "use `ks:-80`")
	exec(t, conn2, "delete from t1 where id1=1")
	// Verify the lookup row is still there.
	qr = exec(t, conn, "select id2 from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// Insert should still succeed.
	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// Lookup row should be unchanged.
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Dangling row not pointing to existing keyspace id.
	exec(t, conn2, "use `ks:-80`")
	exec(t, conn2, "delete from t1 where id1=1")
	// Update the lookup row with bogus keyspace id.
	exec(t, conn, "update t1_id2_idx set keyspace_id='aaa' where id2=4")
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"aaa\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// Insert should still succeed.
	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// lookup row must be updated.
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Update, but don't change anything. This should not deadlock.
	exec(t, conn, "begin")
	exec(t, conn, "update t1 set id2=4 where id1=1")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Update, and change the lookup value. This should change main and lookup rows.
	exec(t, conn, "begin")
	exec(t, conn, "update t1 set id2=5 where id1=1")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(5)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(5) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	exec(t, conn, "delete from t1 where id1=1")
}

func TestDMLScatter(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

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
	exec(t, conn, "begin")
	exec(t, conn, "insert into t3(id5, id6, id7) values(1, 2, 3), (2, 2, 3), (3, 4, 3), (4, 5, 4)")
	exec(t, conn, "commit")
	qr := exec(t, conn, "select id5, id6, id7 from t3 order by id5")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(2) INT64(3)] [INT64(2) INT64(2) INT64(3)] [INT64(3) INT64(4) INT64(3)] [INT64(4) INT64(5) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
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
	exec(t, conn, "update t3 set id5 = 42 where id5 = 1")
	qr = exec(t, conn, "select id5, id6, id7 from t3 order by id5")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) INT64(2) INT64(3)] [INT64(3) INT64(4) INT64(3)] [INT64(4) INT64(5) INT64(4)] [INT64(42) INT64(2) INT64(3)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
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
	exec(t, conn, "begin")
	exec(t, conn, "update t3 set id7 = 42 where id6 = 2")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select id5, id6, id7 from t3 order by id5")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) INT64(2) INT64(42)] [INT64(3) INT64(4) INT64(3)] [INT64(4) INT64(5) INT64(4)] [INT64(42) INT64(2) INT64(42)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	/* delete one specific keyspace id. after this dml, the tables will contain the following:
	t3 (id5, id6, id7):
	3 4 3
	4 5 4

	t3_id7_idx (id7, keyspace_id:id6):
	3 4
	4 5
	*/
	exec(t, conn, "delete from t3 where id6 = 2")
	qr = exec(t, conn, "select * from t3 where id6 = 2")
	require.Empty(t, qr.Rows)
	qr = exec(t, conn, "select * from t3_id7_idx where id6 = 2")
	require.Empty(t, qr.Rows)

	// delete all the rows.
	exec(t, conn, "delete from t3")
	qr = exec(t, conn, "select * from t3")
	require.Empty(t, qr.Rows)
	qr = exec(t, conn, "select * from t3_id7_idx")
	require.Empty(t, qr.Rows)
}

func TestConsistentLookupMultiInsert(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn2.Close()

	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1,4), (2,5)")
	exec(t, conn, "commit")
	qr := exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)] [INT64(2) INT64(5)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select count(*) from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Delete one row but leave its lookup dangling.
	exec(t, conn2, "use `ks:-80`")
	exec(t, conn2, "delete from t1 where id1=1")
	// Insert a bogus lookup row.
	exec(t, conn, "insert into t1_id2_idx(id2, keyspace_id) values(6, 'aaa')")
	// Insert 3 rows:
	// first row will insert without changing lookup.
	// second will insert and change lookup.
	// third will be a fresh insert for main and lookup.
	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1,2), (3,6), (4,7)")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select id1, id2 from t1 order by id1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(2)] [INT64(2) INT64(5)] [INT64(3) INT64(6)] [INT64(4) INT64(7)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx where id2=6")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(6) VARBINARY(\"N\\xb1\\x90ɢ\\xfa\\x16\\x9c\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select count(*) from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(5)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	exec(t, conn, "delete from t1 where id1=1")
	exec(t, conn, "delete from t1 where id1=2")
	exec(t, conn, "delete from t1 where id1=3")
	exec(t, conn, "delete from t1 where id1=4")
	exec(t, conn, "delete from t1_id2_idx where id2=4")
}

func TestHashLookupMultiInsertIgnore(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn2.Close()

	// DB should start out clean
	qr := exec(t, conn, "select count(*) from t2_id4_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(0)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select count(*) from t2")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(0)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Try inserting a bunch of ids at once
	exec(t, conn, "begin")
	exec(t, conn, "insert ignore into t2(id3, id4) values(50,60), (30,40), (10,20)")
	exec(t, conn, "commit")

	// Verify
	qr = exec(t, conn, "select id3, id4 from t2 order by id3")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(10) INT64(20)] [INT64(30) INT64(40)] [INT64(50) INT64(60)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select id3, id4 from t2_id4_idx order by id3")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(10) INT64(20)] [INT64(30) INT64(40)] [INT64(50) INT64(60)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func TestConsistentLookupUpdate(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

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
	exec(t, conn, "insert into t4(id1, id2) values(1, '2'), (2, '2'), (3, '3'), (4, '3')")
	qr := exec(t, conn, "select id1, id2 from t4 order by id1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("2")] [INT64(2) VARCHAR("2")] [INT64(3) VARCHAR("3")] [INT64(4) VARCHAR("3")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
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
	exec(t, conn, "update t4 set id2 = '42' where id1 = 1")
	qr = exec(t, conn, "select id1, id2 from t4 order by id1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("42")] [INT64(2) VARCHAR("2")] [INT64(3) VARCHAR("3")] [INT64(4) VARCHAR("3")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
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
	exec(t, conn, "delete from t4 where id2 = '42'")
	qr = exec(t, conn, "select * from t4 where id2 = '42'")
	require.Empty(t, qr.Rows)
	qr = exec(t, conn, "select * from t4_id2_idx where id2 = '42'")
	require.Empty(t, qr.Rows)

	// delete all the rows.
	exec(t, conn, "delete from t4")
	qr = exec(t, conn, "select * from t4")
	require.Empty(t, qr.Rows)
	qr = exec(t, conn, "select * from t4_id2_idx")
	require.Empty(t, qr.Rows)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.Nil(t, err)
	return qr
}
