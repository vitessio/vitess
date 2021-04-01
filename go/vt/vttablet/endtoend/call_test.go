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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
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
		query   string
		wantErr bool
	}
	tcases := []testcases{{
		query:   "call proc_select1()",
		wantErr: true,
	}, {
		query:   "call proc_select4()",
		wantErr: true,
	}, {
		query: "call proc_dml()",
	}}

	for _, tc := range tcases {
		t.Run(tc.query, func(t *testing.T) {
			_, err := client.Execute(tc.query, nil)
			if tc.wantErr {
				require.EqualError(t, err, "Multi-Resultset not supported in stored procedure (CallerID: dev)")
				return
			}
			require.NoError(t, err)

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
	_, err := client.ReserveBeginExecute(`call proc_dml()`, nil, nil)
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

	_, err := client.Execute(`call proc_tx_begin()`, nil)
	require.EqualError(t, err, "Transaction not concluded inside the stored procedure, leaking transaction from stored procedure is not allowed (CallerID: dev)")

	queries := []string{
		`call proc_tx_commit()`,
		`call proc_tx_rollback()`,
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			_, err := client.BeginExecute(query, nil, nil)
			assert.EqualError(t, err, "Transaction state change inside the stored procedure is not allowed (CallerID: dev)")
			client.Release()
		})
	}

	// This passes as this starts a new transaction by commiting the old transaction implicitly.
	_, err = client.BeginExecute(`call proc_tx_begin()`, nil, nil)
	require.NoError(t, err)
}
