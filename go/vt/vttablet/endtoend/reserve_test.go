/*
Copyright 2020 The Vitess Authors.

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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

//TODO: Add Counter checks in all the tests.

func TestMultipleReserveHaveDifferentConnection(t *testing.T) {
	client1 := framework.NewClient()
	client2 := framework.NewClient()

	query := "select connection_id()"

	qrc1_1, err := client1.ReserveExecute(query, nil, nil)
	require.NoError(t, err)
	defer client1.Release()
	qrc2_1, err := client2.ReserveExecute(query, nil, nil)
	require.NoError(t, err)
	defer client2.Release()
	require.NotEqual(t, qrc1_1.Rows, qrc2_1.Rows)

	qrc1_2, err := client1.Execute(query, nil)
	require.NoError(t, err)
	qrc2_2, err := client2.Execute(query, nil)
	require.NoError(t, err)
	require.Equal(t, qrc1_1.Rows, qrc1_2.Rows)
	require.Equal(t, qrc2_1.Rows, qrc2_2.Rows)
}

func TestReserveBeginRelease(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	qr1, err := client.ReserveExecute(query, nil, nil)
	require.NoError(t, err)
	defer client.Release()

	qr2, err := client.BeginExecute(query, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, qr1.Rows, qr2.Rows)
	assert.Equal(t, client.ReservedID(), client.TransactionID())

	require.NoError(t, client.Release())
}

func TestBeginReserveRelease(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	qr1, err := client.BeginExecute(query, nil, nil)
	require.NoError(t, err)
	defer client.Release()

	qr2, err := client.ReserveExecute(query, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, qr1.Rows, qr2.Rows)
	assert.Equal(t, client.ReservedID(), client.TransactionID())

	require.NoError(t, client.Release())
}

func TestReserveBeginExecuteRelease(t *testing.T) {
	client := framework.NewClient()

	insQuery := "insert into vitess_test (intval, floatval, charval, binval) values (4, null, null, null)"
	selQuery := "select intval from vitess_test where intval = 4"
	_, err := client.ReserveBeginExecute(insQuery, nil, nil, nil)
	require.NoError(t, err)

	qr, err := client.Execute(selQuery, nil)
	require.NoError(t, err)
	assert.Equal(t, `[[INT32(4)]]`, fmt.Sprintf("%v", qr.Rows))

	err = client.Release()
	require.NoError(t, err)

	qr, err = client.Execute(selQuery, nil)
	require.NoError(t, err)
	assert.Equal(t, `[]`, fmt.Sprintf("%v", qr.Rows))
}

func TestMultipleReserveBeginHaveDifferentConnection(t *testing.T) {
	client1 := framework.NewClient()
	client2 := framework.NewClient()

	query := "select connection_id()"

	qrc1_1, err := client1.ReserveBeginExecute(query, nil, nil, nil)
	require.NoError(t, err)
	defer client1.Release()
	qrc2_1, err := client2.ReserveBeginExecute(query, nil, nil, nil)
	require.NoError(t, err)
	defer client2.Release()
	require.NotEqual(t, qrc1_1.Rows, qrc2_1.Rows)

	qrc1_2, err := client1.Execute(query, nil)
	require.NoError(t, err)
	qrc2_2, err := client2.Execute(query, nil)
	require.NoError(t, err)
	require.Equal(t, qrc1_1.Rows, qrc1_2.Rows)
	require.Equal(t, qrc2_1.Rows, qrc2_2.Rows)
}

func TestCommitOnReserveBeginConn(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	qr1, err := client.ReserveBeginExecute(query, nil, nil, nil)
	require.NoError(t, err)
	defer client.Release()

	oldRID := client.ReservedID()
	err = client.Commit()
	require.NoError(t, err)
	assert.NotEqual(t, client.ReservedID(), oldRID, "reservedID must change after commit")
	assert.EqualValues(t, 0, client.TransactionID(), "transactionID should be 0 after commit")

	qr2, err := client.Execute(query, nil)
	require.NoError(t, err)
	assert.Equal(t, qr1.Rows, qr2.Rows)
}

func TestRollbackOnReserveBeginConn(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	qr1, err := client.ReserveBeginExecute(query, nil, nil, nil)
	require.NoError(t, err)
	defer client.Release()

	oldRID := client.ReservedID()
	err = client.Rollback()
	require.NoError(t, err)
	assert.NotEqual(t, client.ReservedID(), oldRID, "reservedID must change after rollback")
	assert.EqualValues(t, 0, client.TransactionID(), "transactionID should be 0 after commit")

	qr2, err := client.Execute(query, nil)
	require.NoError(t, err)
	assert.Equal(t, qr1.Rows, qr2.Rows)
}

func TestReserveBeginRollbackAndBeginCommitAgain(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	qr1, err := client.ReserveBeginExecute(query, nil, nil, nil)
	require.NoError(t, err)
	defer client.Release()

	oldRID := client.ReservedID()
	err = client.Rollback()
	require.NoError(t, err)
	assert.EqualValues(t, 0, client.TransactionID(), "transactionID should be 0 after rollback")
	assert.NotEqual(t, client.ReservedID(), oldRID, "reservedID must change after rollback")

	oldRID = client.ReservedID()

	qr2, err := client.BeginExecute(query, nil, nil)
	require.NoError(t, err)

	err = client.Commit()
	require.NoError(t, err)
	assert.EqualValues(t, 0, client.TransactionID(), "transactionID should be 0 after commit")
	assert.NotEqual(t, client.ReservedID(), oldRID, "reservedID must change after rollback")

	qr3, err := client.Execute(query, nil)
	require.NoError(t, err)
	assert.Equal(t, qr1.Rows, qr2.Rows)
	assert.Equal(t, qr2.Rows, qr3.Rows)

	require.NoError(t,
		client.Release())
}

func TestReserveBeginCommitFailToReuseTxID(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	_, err := client.ReserveBeginExecute(query, nil, nil, nil)
	require.NoError(t, err)
	defer client.Release()

	oldTxID := client.TransactionID()

	err = client.Commit()
	require.NoError(t, err)

	client.SetTransactionID(oldTxID)

	_, err = client.Execute(query, nil)
	require.Error(t, err)
	require.NoError(t,
		client.Release())
}

func TestReserveBeginRollbackFailToReuseTxID(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	_, err := client.ReserveBeginExecute(query, nil, nil, nil)
	require.NoError(t, err)
	defer client.Release()

	oldTxID := client.TransactionID()

	err = client.Rollback()
	require.NoError(t, err)

	client.SetTransactionID(oldTxID)

	_, err = client.Execute(query, nil)
	require.Error(t, err)
	require.NoError(t,
		client.Release())
}

func TestReserveBeginCommitFailToReuseOldReservedID(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	_, err := client.ReserveBeginExecute(query, nil, nil, nil)
	require.NoError(t, err)

	oldRID := client.ReservedID()

	err = client.Commit()
	require.NoError(t, err)
	newRID := client.ReservedID()

	client.SetReservedID(oldRID)

	_, err = client.Execute(query, nil)
	require.Error(t, err)

	client.SetReservedID(newRID)
	require.NoError(t,
		client.Release())
}

func TestReserveBeginRollbackFailToReuseOldReservedID(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	_, err := client.ReserveBeginExecute(query, nil, nil, nil)
	require.NoError(t, err)

	oldRID := client.ReservedID()

	err = client.Rollback()
	require.NoError(t, err)
	newRID := client.ReservedID()

	client.SetReservedID(oldRID)
	_, err = client.Execute(query, nil)
	require.Error(t, err)

	client.SetReservedID(newRID)
	require.NoError(t,
		client.Release())
}

func TestReserveReleaseAndFailToUseReservedIDAgain(t *testing.T) {
	client := framework.NewClient()

	query := "select 42"

	_, err := client.ReserveExecute(query, nil, nil)
	require.NoError(t, err)

	rID := client.ReservedID()
	require.NoError(t,
		client.Release())

	client.SetReservedID(rID)

	_, err = client.Execute(query, nil)
	require.Error(t, err)
}

func TestReserveAndFailToRunTwiceConcurrently(t *testing.T) {
	client := framework.NewClient()

	query := "select 42"

	_, err := client.ReserveExecute(query, nil, nil)
	require.NoError(t, err)
	defer client.Release()

	// WaitGroup will make defer call to wait for go func to complete.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, err = client.Execute("select sleep(1)", nil)
		wg.Done()
	}()
	_, err2 := client.Execute("select sleep(1)", nil)
	wg.Wait()

	if err == nil && err2 == nil {
		assert.Fail(t, "at least one execution should fail")
	}
}

func TestBeginReserveCommitAndNewTransactionsOnSameReservedID(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	qrTx, err := client.BeginExecute(query, nil, nil)
	require.NoError(t, err)

	qrRID, err := client.ReserveExecute(query, nil, nil)
	require.NoError(t, err)
	require.Equal(t, qrTx.Rows, qrRID.Rows)

	err = client.Commit()
	require.NoError(t, err)

	qrTx, err = client.BeginExecute(query, nil, nil)
	require.NoError(t, err)
	require.Equal(t, qrTx.Rows, qrRID.Rows)

	err = client.Commit()
	require.NoError(t, err)

	qrTx, err = client.BeginExecute(query, nil, nil)
	require.NoError(t, err)
	require.Equal(t, qrTx.Rows, qrRID.Rows)

	err = client.Rollback()
	require.NoError(t, err)

	require.NoError(t,
		client.Release())
}

func TestBeginReserveRollbackAndNewTransactionsOnSameReservedID(t *testing.T) {
	client := framework.NewClient()

	query := "select connection_id()"

	qrTx, err := client.BeginExecute(query, nil, nil)
	require.NoError(t, err)

	qrRID, err := client.ReserveExecute(query, nil, nil)
	require.NoError(t, err)
	require.Equal(t, qrTx.Rows, qrRID.Rows)

	err = client.Rollback()
	require.NoError(t, err)

	qrTx, err = client.BeginExecute(query, nil, nil)
	require.NoError(t, err)
	require.Equal(t, qrTx.Rows, qrRID.Rows)

	err = client.Commit()
	require.NoError(t, err)

	qrTx, err = client.BeginExecute(query, nil, nil)
	require.NoError(t, err)
	require.Equal(t, qrTx.Rows, qrRID.Rows)

	err = client.Rollback()
	require.NoError(t, err)

	require.NoError(t,
		client.Release())
}

func TestBeginReserveReleaseAndFailToUseReservedIDAndTxIDAgain(t *testing.T) {
	client := framework.NewClient()

	query := "select 42"

	_, err := client.BeginExecute(query, nil, nil)
	require.NoError(t, err)

	_, err = client.ReserveExecute(query, nil, nil)
	require.NoError(t, err)

	rID := client.ReservedID()
	txID := client.TransactionID()

	require.NoError(t,
		client.Release())

	client.SetReservedID(rID)
	_, err = client.Execute(query, nil)
	require.Error(t, err)

	client.SetReservedID(0)
	client.SetTransactionID(txID)
	_, err = client.Execute(query, nil)
	require.Error(t, err)
}

func TestReserveBeginReleaseAndFailToUseReservedIDAndTxIDAgain(t *testing.T) {
	client := framework.NewClient()

	query := "select 42"

	_, err := client.ReserveExecute(query, nil, nil)
	require.NoError(t, err)

	_, err = client.BeginExecute(query, nil, nil)
	require.NoError(t, err)

	rID := client.ReservedID()
	txID := client.TransactionID()

	require.NoError(t,
		client.Release())

	client.SetReservedID(rID)
	_, err = client.Execute(query, nil)
	require.Error(t, err)

	client.SetReservedID(0)
	client.SetTransactionID(txID)
	_, err = client.Execute(query, nil)
	require.Error(t, err)
}

func TestReserveExecuteWithFailingQueryAndReserveConnectionRemainsOpen(t *testing.T) {
	client := framework.NewClient()

	_, err := client.ReserveExecute("select foo", nil, nil)
	require.Error(t, err)
	defer client.Release()
	require.NotEqual(t, int64(0), client.ReservedID())

	_, err = client.Execute("select 42", nil)
	require.NoError(t, err)
	require.NoError(t, client.Release())
}

func TestReserveAndExecuteWithFailingQueryAndReserveConnectionRemainsOpen(t *testing.T) {
	client := framework.NewClient()

	qr1, err := client.ReserveExecute("select connection_id()", nil, nil)
	require.NoError(t, err)
	defer client.Release()

	_, err = client.Execute("select foo", nil)
	require.Error(t, err)

	qr2, err := client.Execute("select connection_id()", nil)
	require.NoError(t, err)
	require.Equal(t, qr1.Rows, qr2.Rows)
	require.NoError(t, client.Release())
}

func TestReserveBeginExecuteWithFailingQueryAndReserveConnAndTxRemainsOpen(t *testing.T) {
	client := framework.NewClient()

	_, err := client.ReserveBeginExecute("select foo", nil, nil, nil)
	require.Error(t, err)

	// Save the connection id to check in the end that everything got executed on same connection.
	qr1, err := client.Execute("select connection_id()", nil)
	require.NoError(t, err)

	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values (4, null, null, null)", nil)
	require.NoError(t, err)

	qr, err := client.Execute("select intval from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, "[[INT32(1)] [INT32(2)] [INT32(3)] [INT32(4)]]", fmt.Sprintf("%v", qr.Rows))

	err = client.Rollback()
	require.NoError(t, err)

	qr, err = client.Execute("select intval from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, "[[INT32(1)] [INT32(2)] [INT32(3)]]", fmt.Sprintf("%v", qr.Rows))

	qr2, err := client.Execute("select connection_id()", nil)
	require.NoError(t, err)
	require.Equal(t, qr1.Rows, qr2.Rows)

	require.NoError(t, client.Release())
}

func TestReserveAndBeginExecuteWithFailingQueryAndReserveConnAndTxRemainsOpen(t *testing.T) {
	client := framework.NewClient()

	// Save the connection id to check in the end that everything got executed on same connection.
	qr1, err := client.ReserveExecute("select connection_id()", nil, nil)
	require.NoError(t, err)

	_, err = client.BeginExecute("select foo", nil, nil)
	require.Error(t, err)

	_, err = client.Execute("insert into vitess_test (intval, floatval, charval, binval) values (4, null, null, null)", nil)
	require.NoError(t, err)

	qr, err := client.Execute("select intval from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, "[[INT32(1)] [INT32(2)] [INT32(3)] [INT32(4)]]", fmt.Sprintf("%v", qr.Rows))

	err = client.Rollback()
	require.NoError(t, err)

	qr, err = client.Execute("select intval from vitess_test", nil)
	require.NoError(t, err)
	assert.Equal(t, "[[INT32(1)] [INT32(2)] [INT32(3)]]", fmt.Sprintf("%v", qr.Rows))

	qr2, err := client.Execute("select connection_id()", nil)
	require.NoError(t, err)
	require.Equal(t, qr1.Rows, qr2.Rows)

	require.NoError(t, client.Release())
}

func TestReserveExecuteWithPreQueriesAndCheckConnectionState(t *testing.T) {
	client1 := framework.NewClient()
	client2 := framework.NewClient()

	selQuery := "select str_to_date('00/00/0000', '%m/%d/%Y')"
	warnQuery := "show warnings"
	preQueries1 := []string{
		"set sql_mode = ''",
	}
	preQueries2 := []string{
		"set sql_mode = 'NO_ZERO_DATE'",
	}

	qr1, err := client1.ReserveExecute(selQuery, preQueries1, nil)
	require.NoError(t, err)
	defer client1.Release()

	qr2, err := client2.ReserveExecute(selQuery, preQueries2, nil)
	require.NoError(t, err)
	defer client2.Release()

	assert.NotEqual(t, qr1.Rows, qr2.Rows)
	assert.Equal(t, `[[DATE("0000-00-00")]]`, fmt.Sprintf("%v", qr1.Rows))
	assert.Equal(t, `[[NULL]]`, fmt.Sprintf("%v", qr2.Rows))

	qr1, err = client1.Execute(warnQuery, nil)
	require.NoError(t, err)

	qr2, err = client2.Execute(warnQuery, nil)
	require.NoError(t, err)

	assert.NotEqual(t, qr1.Rows, qr2.Rows)
	assert.Equal(t, `[]`, fmt.Sprintf("%v", qr1.Rows))
	assert.Equal(t, `[[VARCHAR("Warning") UINT32(1411) VARCHAR("Incorrect datetime value: '00/00/0000' for function str_to_date")]]`, fmt.Sprintf("%v", qr2.Rows))
}

func TestReserveExecuteWithPreQueriesAndSavepoint(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	insQuery := "insert into vitess_test (intval) values (5)"
	selQuery := "select intval from vitess_test where intval = 5"
	preQueries := []string{
		"set sql_mode = ''",
	}

	postBeginQueries1 := []string{
		"savepoint a",
	}
	// savepoint there after begin.
	_, err := client.ReserveBeginExecute(insQuery, preQueries, postBeginQueries1, nil)
	require.NoError(t, err)

	qr, err := client.Execute(selQuery, nil)
	require.NoError(t, err)
	assert.Equal(t, `[[INT32(5)]]`, fmt.Sprintf("%v", qr.Rows))

	_, err = client.Execute("rollback to a", nil)
	require.NoError(t, err)

	qr, err = client.Execute(selQuery, nil)
	require.NoError(t, err)
	assert.Equal(t, `[]`, fmt.Sprintf("%v", qr.Rows))

	err = client.Release()
	require.NoError(t, err)

	postBeginQueries2 := []string{
		"savepoint a",
		"release savepoint a",
		"savepoint b",
	}
	// no savepoint after begin
	_, err = client.ReserveBeginExecute(insQuery, preQueries, postBeginQueries2, nil)
	require.NoError(t, err)

	qr, err = client.Execute(selQuery, nil)
	require.NoError(t, err)
	assert.Equal(t, `[[INT32(5)]]`, fmt.Sprintf("%v", qr.Rows))

	// no savepoint a
	_, err = client.Execute("rollback to a", nil)
	require.Error(t, err)

	// no savepoint a.
	_, err = client.Execute("release a", nil)
	require.Error(t, err)

	// record still exists.
	qr, err = client.Execute(selQuery, nil)
	require.NoError(t, err)
	assert.Equal(t, `[[INT32(5)]]`, fmt.Sprintf("%v", qr.Rows))

	_, err = client.Execute("rollback to b", nil)
	require.NoError(t, err)

	qr, err = client.Execute(selQuery, nil)
	require.NoError(t, err)
	assert.Equal(t, `[]`, fmt.Sprintf("%v", qr.Rows))
}

func TestReserveBeginExecuteWithPreQueriesAndCheckConnectionState(t *testing.T) {
	rcClient := framework.NewClient()
	rucClient := framework.NewClient()

	insRcQuery := "insert into vitess_test (intval, floatval, charval, binval) values (4, null, null, null)"
	insRucQuery := "insert into vitess_test (intval, floatval, charval, binval) values (5, null, null, null)"
	selQuery := "select intval from vitess_test"
	delQuery := "delete from vitess_test where intval = 5"
	rcQuery := []string{
		"set session transaction isolation level read committed",
	}
	rucQuery := []string{
		"set session transaction isolation level read uncommitted",
	}

	_, err := rcClient.ReserveBeginExecute(insRcQuery, rcQuery, nil, nil)
	require.NoError(t, err)
	defer rcClient.Release()

	_, err = rucClient.ReserveBeginExecute(insRucQuery, rucQuery, nil, nil)
	require.NoError(t, err)
	defer rucClient.Release()

	qr1, err := rcClient.Execute(selQuery, nil)
	require.NoError(t, err)

	qr2, err := rucClient.Execute(selQuery, nil)
	require.NoError(t, err)

	assert.NotEqual(t, qr1.Rows, qr2.Rows)
	// As the transaction is read commited it is not able to see #5.
	assert.Equal(t, `[[INT32(1)] [INT32(2)] [INT32(3)] [INT32(4)]]`, fmt.Sprintf("%v", qr1.Rows))
	// As the transaction is read uncommited it is able to see #4.
	assert.Equal(t, `[[INT32(1)] [INT32(2)] [INT32(3)] [INT32(4)] [INT32(5)]]`, fmt.Sprintf("%v", qr2.Rows))

	err = rucClient.Commit()
	require.NoError(t, err)

	qr1, err = rcClient.Execute(selQuery, nil)
	require.NoError(t, err)

	qr2, err = rucClient.Execute(selQuery, nil)
	require.NoError(t, err)

	// As the transaction on read uncommitted client got committed, transaction with read committed will be able to see #5.
	assert.Equal(t, qr1.Rows, qr2.Rows)
	assert.Equal(t, `[[INT32(1)] [INT32(2)] [INT32(3)] [INT32(4)] [INT32(5)]]`, fmt.Sprintf("%v", qr1.Rows))

	err = rcClient.Rollback()
	require.NoError(t, err)

	qr1, err = rcClient.Execute(selQuery, nil)
	require.NoError(t, err)

	qr2, err = rucClient.Execute(selQuery, nil)
	require.NoError(t, err)

	// As the transaction on read committed client got rollbacked back, table will forget #4.
	assert.Equal(t, qr1.Rows, qr2.Rows)
	assert.Equal(t, `[[INT32(1)] [INT32(2)] [INT32(3)] [INT32(5)]]`, fmt.Sprintf("%v", qr2.Rows))

	// This is executed on reserved connection without transaction as the transaction was committed.
	_, err = rucClient.Execute(delQuery, nil)
	require.NoError(t, err)
}

func TestReserveExecuteWithFailingPreQueriesAndCheckConnectionState(t *testing.T) {
	client := framework.NewClient()

	selQuery := "select 42"
	preQueries := []string{
		"set @@no_sys_var = 42",
	}

	_, err := client.ReserveExecute(selQuery, preQueries, nil)
	require.Error(t, err)

	err = client.Release()
	require.Error(t, err)
}

func TestReserveBeginExecuteWithFailingPreQueriesAndCheckConnectionState(t *testing.T) {
	client := framework.NewClient()

	selQuery := "select 42"
	preQueries := []string{
		"set @@no_sys_var = 42",
	}

	_, err := client.ReserveBeginExecute(selQuery, preQueries, nil, nil)
	require.Error(t, err)

	err = client.Commit()
	require.Error(t, err)

	err = client.Release()
	require.Error(t, err)
}

func TestBeginReserveExecuteWithFailingPreQueriesAndCheckConnectionState(t *testing.T) {
	client := framework.NewClient()

	selQuery := "select 42"
	preQueries := []string{
		"set @@no_sys_var = 42",
	}

	_, err := client.BeginExecute(selQuery, nil, nil)
	require.NoError(t, err)

	_, err = client.ReserveExecute(selQuery, preQueries, nil)
	require.Error(t, err)

	err = client.Commit()
	require.Error(t, err)

	err = client.Release()
	require.Error(t, err)
}

func TestReserveBeginExecuteWithCommitFailureAndCheckConnectionAndDBState(t *testing.T) {
	client := framework.NewClient()

	connQuery := "select connection_id()"
	insQuery := "insert into vitess_test (intval, floatval, charval, binval) values (4, null, null, null)"
	selQuery := "select intval from vitess_test where intval = 4"

	connQr, err := client.ReserveBeginExecute(connQuery, nil, nil, nil)
	require.NoError(t, err)

	_, err = client.Execute(insQuery, nil)
	require.NoError(t, err)

	killConnection(t, connQr.Rows[0][0].ToString())

	err = client.Commit()
	require.Error(t, err)
	require.Zero(t, client.ReservedID())

	qr, err := client.Execute(selQuery, nil)
	require.NoError(t, err)
	require.Empty(t, qr.Rows)

	qr, err = client.Execute(connQuery, nil)
	require.NoError(t, err)
	require.NotEqual(t, connQr.Rows, qr.Rows)

	require.Error(t, client.Release())
}

func TestReserveBeginExecuteWithRollbackFailureAndCheckConnectionAndDBState(t *testing.T) {
	client := framework.NewClient()

	connQuery := "select connection_id()"
	insQuery := "insert into vitess_test (intval, floatval, charval, binval) values (4, null, null, null)"
	selQuery := "select intval from vitess_test where intval = 4"

	connQr, err := client.ReserveBeginExecute(connQuery, nil, nil, nil)
	require.NoError(t, err)

	_, err = client.Execute(insQuery, nil)
	require.NoError(t, err)

	killConnection(t, connQr.Rows[0][0].ToString())

	err = client.Rollback()
	require.Error(t, err)
	require.Zero(t, client.ReservedID())

	qr, err := client.Execute(selQuery, nil)
	require.NoError(t, err)
	require.Empty(t, qr.Rows)

	qr, err = client.Execute(connQuery, nil)
	require.NoError(t, err)
	require.NotEqual(t, connQr.Rows, qr.Rows)

	require.Error(t, client.Release())
}

func TestReserveExecuteWithExecuteFailureAndCheckConnectionAndDBState(t *testing.T) {
	client := framework.NewClient()

	connQuery := "select connection_id()"
	insQuery := "insert into vitess_test (intval, floatval, charval, binval) values (4, null, null, null)"
	selQuery := "select intval from vitess_test where intval = 4"

	connQr, err := client.ReserveExecute(connQuery, nil, nil)
	require.NoError(t, err)

	killConnection(t, connQr.Rows[0][0].ToString())

	_, err = client.Execute(insQuery, nil)
	require.Error(t, err)
	// Expectation  - require.Zero(t, client.ReservedID())
	// Reality
	require.NotZero(t, client.ReservedID())

	// Client still has transaction id and client id as non-zero.
	_, err = client.Execute(selQuery, nil)
	require.Error(t, err)
	client.SetTransactionID(0)

	_, err = client.Execute(selQuery, nil)
	require.Error(t, err)
	client.SetReservedID(0)

	qr, err := client.Execute(selQuery, nil)
	require.NoError(t, err)
	require.Empty(t, qr.Rows)

	qr, err = client.Execute(connQuery, nil)
	require.NoError(t, err)
	require.NotEqual(t, connQr.Rows, qr.Rows)

	require.Error(t, client.Release())
}

func TestReserveExecuteDDLWithoutTx(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	connQuery := "select connection_id()"
	createQuery := "create table vitess_test_ddl(id bigint primary key)"
	dropQuery := "drop table vitess_test_ddl"
	descQuery := "describe vitess_test_ddl"

	qr1, err := client.ReserveExecute(connQuery, nil, nil)
	require.NoError(t, err)

	_, err = client.Execute(createQuery, nil)
	require.NoError(t, err)
	require.Zero(t, client.TransactionID())
	defer client.Execute(dropQuery, nil)

	qr2, err := client.Execute(connQuery, nil)
	require.NoError(t, err)
	assert.Equal(t, qr1.Rows, qr2.Rows)

	qr3, err := client.Execute(descQuery, nil)
	require.NoError(t, err)
	require.NotZero(t, qr3.Rows)
}

func TestReserveExecuteDDLWithTx(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	connQuery := "select connection_id()"
	createQuery := "create table vitess_test_ddl(id bigint primary key)"
	dropQuery := "drop table vitess_test_ddl"
	descQuery := "describe vitess_test_ddl"

	qr1, err := client.ReserveBeginExecute(connQuery, nil, nil, nil)
	require.NoError(t, err)

	_, err = client.Execute(createQuery, nil)
	require.NoError(t, err)
	require.NotZero(t, client.TransactionID())
	defer client.Execute(dropQuery, nil)

	qr2, err := client.Execute(connQuery, nil)
	require.NoError(t, err)
	assert.Equal(t, qr1.Rows, qr2.Rows)

	qr3, err := client.Execute(descQuery, nil)
	require.NoError(t, err)
	require.NotZero(t, qr3.Rows)
}

func killConnection(t *testing.T, connID string) {
	client := framework.NewClient()
	_, err := client.ReserveExecute("select 1", []string{fmt.Sprintf("kill %s", connID)}, nil)
	require.NoError(t, err)
	defer client.Release()
}

func BenchmarkPreQueries(b *testing.B) {
	client := framework.NewClient()

	tcases := []struct {
		name     string
		settings []string
	}{{
		name: "split_1",
		settings: []string{
			"set @@sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"},
	}, {
		name: "split_2",
		settings: []string{
			"set @@sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
			"set @@sql_safe_updates = false"},
	}, {
		name: "split_3",
		settings: []string{
			"set @@sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
			"set @@sql_safe_updates = false",
			"set @@read_buffer_size = 9191181919"},
	}, {
		name: "split_4",
		settings: []string{
			"set @@sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
			"set @@sql_safe_updates = false", "set @@read_buffer_size = 9191181919",
			"set @@max_heap_table_size = 10204023"},
	}, {
		name:     "combined_2",
		settings: []string{"set @@sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION', @@sql_safe_updates = false"},
	}, {
		name:     "combined_3",
		settings: []string{"set @@sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION', @@sql_safe_updates = false, @@read_buffer_size = 9191181919"},
	}, {
		name:     "combined_4",
		settings: []string{"set @@sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION', @@sql_safe_updates = false, @@read_buffer_size = 9191181919, @@max_heap_table_size = 10204023"},
	}}
	query := "select connection_id()"

	for _, tcase := range tcases {
		b.Run(tcase.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := client.ReserveExecute(query, tcase.settings, nil); err != nil {
					b.Error(err)
				}
				if err := client.Release(); err != nil {
					b.Error(err)
				}
			}
		})
	}
}

func TestFailInfiniteSessions(t *testing.T) {
	client := framework.NewClient()
	qr, err := client.Execute("select @@max_connections", nil)
	require.NoError(t, err)
	maxConn, err := qr.Rows[0][0].ToInt64()
	require.NoError(t, err)

	// twice the number of sessions than the pool size.
	numOfSessions := int(maxConn * 2)

	var clients []*framework.QueryClient

	// read session
	var failed bool
	for i := 0; i < numOfSessions; i++ {
		client := framework.NewClient()
		_, err := client.ReserveExecute("select 1", []string{"set sql_mode = ''"}, nil)
		if err != nil {
			failed = true
			require.Contains(t, err.Error(), "immediate error from server errorCode=1040 errorMsg=Too many connections")
			break
		}
		clients = append(clients, client)
	}
	require.True(t, failed, "should have failed to create more sessions than the max mysql connection")

	// Release all the sessions.
	for _, client := range clients {
		require.NoError(t,
			client.Release())
	}
	clients = nil

	// write session
	failed = false
	for i := 0; i < numOfSessions; i++ {
		client := framework.NewClient()
		_, err := client.ReserveBeginExecute("select 1", []string{"set sql_mode = ''"}, nil, nil)
		if err != nil {
			failed = true
			require.Contains(t, err.Error(), "immediate error from server errorCode=1040 errorMsg=Too many connections")
			break
		}
		require.NoError(t,
			client.Commit())
		clients = append(clients, client)
	}
	require.True(t, failed, "should have failed to create more sessions than the max mysql connection")

	// Release all the sessions.
	for _, client := range clients {
		require.NoError(t,
			client.Release())
	}
}
