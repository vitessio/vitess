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
	"testing"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestLastInsertId(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// figure out the last inserted id before we run change anything
	qr := exec(t, conn, "select max(id) from t1_last_insert_id")
	oldLastID, err := evalengine.ToUint64(qr.Rows[0][0])
	require.NoError(t, err)

	exec(t, conn, "insert into t1_last_insert_id(id1) values(42)")

	// even without a transaction, we should get the last inserted id back
	qr = exec(t, conn, "select last_insert_id()")
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf("[[UINT64(%d)]]", oldLastID+1)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Error(diff)
	}
}

func TestLastInsertIdWithRollback(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// figure out the last inserted id before we run our tests
	qr := exec(t, conn, "select max(id) from t1_last_insert_id")
	oldLastID, err := evalengine.ToUint64(qr.Rows[0][0])
	require.NoError(t, err)

	// add row inside explicit transaction
	exec(t, conn, "begin")
	exec(t, conn, "insert into t1_last_insert_id(id1) values(42)")
	qr = exec(t, conn, "select last_insert_id()")
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf("[[UINT64(%d)]]", oldLastID+1)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Error(diff)
	}
	// even if we do a rollback, we should still get the same last_insert_id
	exec(t, conn, "rollback")
	qr = exec(t, conn, "select last_insert_id()")
	got = fmt.Sprintf("%v", qr.Rows)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Error(diff)
	}
}
