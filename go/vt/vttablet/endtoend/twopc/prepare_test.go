/*
Copyright 2024 The Vitess Authors.

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
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

// TestCommitPreparedFailNonRetryable tests the case where the commit_prepared fails trying to acquire update lock.
// The transaction updates to failed state.
func TestCommitPreparedFailNonRetryable(t *testing.T) {
	dbaConnector := framework.Server.Config().DB.DbaWithDB()
	conn, err := dbaConnector.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("set global innodb_lock_wait_timeout = 1", 1, false)
	require.NoError(t, err)
	defer conn.ExecuteFetch("set global innodb_lock_wait_timeout = default", 1, false)

	client := framework.NewClient()
	defer client.RollbackPrepared("bb", client.TransactionID())

	_, err = client.BeginExecute(`insert into vitess_test (intval) values(50)`, nil, nil)
	require.NoError(t, err)
	err = client.Prepare("bb")
	require.NoError(t, err)

	client2 := framework.NewClient()
	_, err = client2.BeginExecute(`select * from _vt.redo_state where dtid = 'bb' for update`, nil, nil)
	require.NoError(t, err)

	ch := make(chan any)
	go func() {
		err := client.CommitPrepared("bb")
		ch <- nil
		require.ErrorContains(t, err, "commit_prepared")
	}()
	time.Sleep(1500 * time.Millisecond)

	client2.Release()
	<-ch

	qr, err := client2.Execute("select dtid, state, message from _vt.redo_state where dtid = 'bb'", nil)
	require.NoError(t, err)
	require.Equal(t, `[[VARBINARY("bb") INT64(0) TEXT("Lock wait timeout exceeded; try restarting transaction (errno 1205) (sqlstate HY000) during query: delete from _vt.redo_state where dtid = _binary'bb'")]]`, fmt.Sprintf("%v", qr.Rows))
}

// TestCommitPreparedFailRetryable tests the case where the commit_prepared fails when the query is killed.
// The transaction remains in the prepare state.
func TestCommitPreparedFailRetryable(t *testing.T) {
	client := framework.NewClient()
	defer client.RollbackPrepared("aa", client.TransactionID())

	_, err := client.BeginExecute(`insert into vitess_test (intval) values(40)`, nil, nil)
	require.NoError(t, err)
	connRes, err := client.Execute(`select connection_id()`, nil)
	require.NoError(t, err)
	err = client.Prepare("aa")
	require.NoError(t, err)

	client2 := framework.NewClient()
	_, err = client2.BeginExecute(`select * from _vt.redo_state where dtid = _binary'aa' for update`, nil, nil)
	require.NoError(t, err)

	ch := make(chan any)
	go func() {
		err := client.CommitPrepared("aa")
		ch <- nil
		require.ErrorContains(t, err, "commit_prepared")
	}()
	time.Sleep(100 * time.Millisecond)

	dbaConnector := framework.Server.Config().DB.DbaWithDB()
	conn, err := dbaConnector.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("kill query "+connRes.Rows[0][0].ToString(), 1, false)
	require.NoError(t, err)

	client2.Release()
	<-ch

	qr, err := client2.Execute("select dtid, state, message from _vt.redo_state where dtid = _binary'aa'", nil)
	require.NoError(t, err)
	require.Equal(t, `[[VARBINARY("aa") INT64(1) TEXT("Query execution was interrupted (errno 1317) (sqlstate 70100) during query: delete from _vt.redo_state where dtid = _binary'aa'")]]`, fmt.Sprintf("%v", qr.Rows))
}
