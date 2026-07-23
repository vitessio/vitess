/*
Copyright 2021 The Vitess Authors.

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var procSQL = []string{
	`create procedure proc_select1()
	BEGIN
		select intval from vitess_test;
	END;`,
	`create procedure proc_select4()
	BEGIN
		select intval from vitess_test;
		select intval from vitess_test;
		select intval from vitess_test;
		select intval from vitess_test;
	END;`,
	`create procedure proc_select_then_sleep()
	BEGIN
		select intval from vitess_test;
		select sleep(30);
	END;`,
	`create procedure proc_select2_tx_insert()
	BEGIN
		select intval from vitess_test;
		select intval from vitess_test;
		start transaction;
		insert into vitess_test(intval) values(8675309);
	END;`,
	`create procedure proc_dml()
	BEGIN
	    start transaction;
		insert into vitess_test(intval) values(1432);
		update vitess_test set intval = 2341 where intval = 1432;
		delete from vitess_test where intval = 2341;
	    commit;
	END;`,
	`create procedure proc_tx_begin()
	BEGIN
	    start transaction;
	END;`,
	`create procedure proc_tx_commit()
	BEGIN
	    commit;
	END;`,
	`create procedure proc_tx_rollback()
	BEGIN
	    rollback;
	END;`,
	`create procedure in_parameter(IN val int)
	BEGIN
		insert into vitess_test(intval) values(val);
	END;`,
	`create procedure out_parameter(OUT name varchar(255))
	BEGIN
	    select 42 into name from dual;
	END;`,
}

func TestCallProcedure(t *testing.T) {
	client := framework.NewClient()
	type testcases struct {
		query    string
		wantErr  bool
		wantRows bool
	}
	tcases := []testcases{{
		query: "call proc_dml()",
	}, {
		// A single-resultset procedure is accepted and returns its rows, even
		// though MySQL appends a trailing OK packet after the resultset.
		query:    "call proc_select1()",
		wantRows: true,
	}, {
		query:   "call proc_select4()",
		wantErr: true,
	}, {
		// Again, make sure the connection isn't dirty and does not contain leftover
		// result sets from previous tests.
		query: "call proc_dml()",
	}}

	for _, tc := range tcases {
		t.Run(tc.query, func(t *testing.T) {
			qr, err := client.Execute(tc.query, nil)
			if tc.wantErr {
				require.EqualError(t, err, "Multi-Resultset not supported in stored procedure (CallerID: dev)")
				return
			}
			require.NoError(t, err)
			if tc.wantRows {
				require.NotEmpty(t, qr.Rows, "single-resultset procedure should return its rows")
			}
		})
	}
}

// A stored procedure call with an OUT/INOUT parameter is reported as unsupported
// on both the buffered and streaming paths, rather than leaking the raw MySQL
// "is not a variable" error.
func TestCallProcedureOutParam(t *testing.T) {
	client := framework.NewClient()

	_, err := client.Execute("call out_parameter(123)", nil)
	require.ErrorContains(t, err, "OUT and INOUT parameters are not supported")

	_, err = client.StreamExecute("call out_parameter(123)", nil)
	require.ErrorContains(t, err, "OUT and INOUT parameters are not supported")
}

// On a connection reused across the streaming and buffered paths, a buffered
// single-resultset CALL must drain its own trailing OK packet before returning,
// even when a previous streamed no-resultset CALL left captured OK-packet state
// on the connection. If the buffered CALL mistakes that stale state for its own
// trailing status and skips the drain, the unread trailing packet is consumed by
// the next query and the connection desyncs.
func TestCallProcedureBufferedAfterStreamedInTx(t *testing.T) {
	client := framework.NewClient()

	require.NoError(t, client.Begin(false))
	t.Cleanup(func() { _ = client.Rollback() })

	// A streamed no-resultset CALL inside the transaction captures the
	// connection's OK-packet state without concluding the transaction.
	_, err := client.StreamExecute("call in_parameter(999)", nil)
	require.NoError(t, err)

	// A buffered single-resultset CALL on the same connection returns its rows.
	qr, err := client.Execute("call proc_select1()", nil)
	require.NoError(t, err)
	require.NotEmpty(t, qr.Rows, "single-resultset procedure should return its rows")

	// The next query sees its own result, proving the CALL's trailing packet was
	// drained rather than left on the connection.
	qr, err = client.Execute("select 42", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	require.Equal(t, "42", qr.Rows[0][0].ToString())
}

// With the FetchLastInsertId option set, a single-resultset CALL fetches the
// last insert id with a follow-up query. That follow-up must run only after the
// CALL's trailing OK packet has been drained; issuing it while the trailing
// packet is still pending desyncs the connection.
func TestCallProcedureFetchLastInsertID(t *testing.T) {
	client := framework.NewClient()

	qr, err := client.ExecuteWithOptions("call proc_select1()", nil, &querypb.ExecuteOptions{
		IncludedFields:    querypb.ExecuteOptions_ALL,
		FetchLastInsertId: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, qr.Rows, "single-resultset procedure should return its rows")
}

func TestCallProcedureStreaming(t *testing.T) {
	client := framework.NewClient()
	type testcases struct {
		query   string
		wantErr string
	}
	tcases := []testcases{{
		// A single-resultset procedure legitimately benefits from streaming and
		// is accepted, even though the buffered Execute path rejects it because
		// of the trailing OK packet that follows the resultset.
		query: "call proc_select1()",
	}, {
		// A multi-resultset procedure must be rejected, not silently truncated
		// to its first resultset.
		query:   "call proc_select4()",
		wantErr: "Multi-Resultset not supported in stored procedure (CallerID: dev)",
	}, {
		// A procedure that returns no resultset and concludes its own transaction
		// streams fine.
		query: "call proc_dml()",
	}, {
		// Make sure the streaming connection isn't left dirty by the rejected
		// multi-resultset procedure above.
		query: "call proc_dml()",
	}}

	for _, tc := range tcases {
		t.Run(tc.query, func(t *testing.T) {
			_, err := client.StreamExecute(tc.query, nil)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCallProcedureStreamingMultiResultsetTxLeakClosesConnection(t *testing.T) {
	setStreamPoolSize(t, 1)

	client := framework.NewClient()
	checkClient := framework.NewClient()
	_, _ = checkClient.Execute("delete from vitess_test where intval = 8675309", nil)
	t.Cleanup(func() {
		_, _ = checkClient.Execute("delete from vitess_test where intval = 8675309", nil)
	})

	qr, err := client.StreamExecute("select connection_id()", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	beforeConnID := qr.Rows[0][0].ToString()

	_, err = client.StreamExecute("call proc_select2_tx_insert()", nil)
	require.EqualError(t, err, "Multi-Resultset not supported in stored procedure (CallerID: dev)")

	qr, err = client.StreamExecute("select connection_id()", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	afterConnID := qr.Rows[0][0].ToString()

	_, err = client.StreamExecute("call proc_tx_commit()", nil)
	require.NoError(t, err)

	qr, err = checkClient.Execute("select intval from vitess_test where intval = 8675309", nil)
	require.NoError(t, err)
	assert.NotEqual(t, beforeConnID, afterConnID)
	assert.Empty(t, qr.Rows)
}

func TestCallProcedureStreamingMultiResultsetCleanConnectionReused(t *testing.T) {
	setStreamPoolSize(t, 1)

	client := framework.NewClient()

	qr, err := client.StreamExecute("select connection_id()", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	beforeConnID := qr.Rows[0][0].ToString()

	_, err = client.StreamExecute("call proc_select4()", nil)
	require.EqualError(t, err, "Multi-Resultset not supported in stored procedure (CallerID: dev)")

	qr, err = client.StreamExecute("select connection_id()", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	assert.Equal(t, beforeConnID, qr.Rows[0][0].ToString())
}

func TestCallProcedureStreamingCallbackErrorClosesConnection(t *testing.T) {
	setStreamPoolSize(t, 1)

	client := framework.NewClient()

	qr, err := client.StreamExecute("select connection_id()", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	beforeConnID := qr.Rows[0][0].ToString()

	err = client.Stream("call proc_select1()", nil, func(*sqltypes.Result) error {
		return assert.AnError
	})
	require.ErrorContains(t, err, assert.AnError.Error())

	qr, err = client.StreamExecute("select connection_id(), intval from vitess_test where intval = 1", nil)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	assert.NotEqual(t, beforeConnID, qr.Rows[0][0].ToString())
	assert.Equal(t, "1", qr.Rows[0][1].ToString())
}

func setStreamPoolSize(t *testing.T, size int) {
	t.Helper()

	defaultPoolSize := framework.Server.StreamPoolSize()
	require.NoError(t, framework.Server.SetStreamPoolSize(t.Context(), size))
	t.Cleanup(func() {
		require.NoError(t, framework.Server.SetStreamPoolSize(t.Context(), defaultPoolSize))
	})
}

func TestCallProcedureLeakTxStreaming(t *testing.T) {
	client := framework.NewClient()

	_, err := client.StreamExecute(`call proc_tx_begin()`, nil)
	require.EqualError(t, err, "Transaction not concluded inside the stored procedure, leaking transaction from stored procedure is not allowed (CallerID: dev)")
}

func TestCallProcedureChangedTxStreaming(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	queries := []string{
		`call proc_tx_commit()`,
		`call proc_tx_rollback()`,
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			_, err := client.StreamBeginExecuteWithOptions(query, nil, nil, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
			require.EqualError(t, err, "Transaction state change inside the stored procedure is not allowed (CallerID: dev)")
			client.Release()
		})
	}
}

func TestCallProcedureInsideTx(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	_, err := client.BeginExecute(`call proc_dml()`, nil, nil)
	require.EqualError(t, err, "Transaction state change inside the stored procedure is not allowed (CallerID: dev)")

	_, err = client.Execute(`select 1`, nil)
	require.Contains(t, err.Error(), "ended")
}

func TestCallProcedureInsideReservedConn(t *testing.T) {
	client := framework.NewClient()
	_, err := client.ReserveBeginExecute(`call proc_dml()`, nil, nil, nil)
	require.EqualError(t, err, "Transaction state change inside the stored procedure is not allowed (CallerID: dev)")
	client.Release()

	_, err = client.ReserveExecute(`call proc_dml()`, nil, nil)
	require.NoError(t, err)

	_, err = client.Execute(`call proc_dml()`, nil)
	require.NoError(t, err)

	client.Release()
}

func TestCallProcedureLeakTx(t *testing.T) {
	client := framework.NewClient()

	_, err := client.Execute(`call proc_tx_begin()`, nil)
	require.EqualError(t, err, "Transaction not concluded inside the stored procedure, leaking transaction from stored procedure is not allowed (CallerID: dev)")
}

func TestCallProcedureChangedTx(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	_, err := client.Execute(`call proc_tx_begin()`, nil)
	require.EqualError(t, err, "Transaction not concluded inside the stored procedure, leaking transaction from stored procedure is not allowed (CallerID: dev)")

	queries := []string{
		`call proc_tx_commit()`,
		`call proc_tx_rollback()`,
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			_, err := client.BeginExecute(query, nil, nil)
			require.EqualError(t, err, "Transaction state change inside the stored procedure is not allowed (CallerID: dev)")
			client.Release()
		})
	}

	// This passes as this starts a new transaction by committing the old transaction implicitly.
	_, err = client.BeginExecute(`call proc_tx_begin()`, nil, nil)
	require.NoError(t, err)
}

// A multi-resultset procedure that emits a quick first resultset and then
// blocks must honor the query deadline while its trailing resultsets are being
// drained, rather than blocking until the procedure finishes on its own.
func TestCallProcedureDrainHonorsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		callerid.NewContext(context.Background(), &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "dev"}),
		5*time.Second)
	defer cancel()
	client := framework.NewClientWithContext(ctx)

	start := time.Now()
	_, err := client.Execute("call proc_select_then_sleep()", nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 20*time.Second,
		"draining trailing resultsets must honor the query deadline instead of blocking for the whole procedure")
}
