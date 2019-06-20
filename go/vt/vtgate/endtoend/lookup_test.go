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

package endtoend

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func TestConsistentLookup(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
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

func TestConsistentLookupMultiInsert(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
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
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}
