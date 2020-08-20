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

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/vterrors"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutorSet(t *testing.T) {
	executorEnv, _, _, _ := createExecutorEnv()

	testcases := []struct {
		in  string
		out *vtgatepb.Session
		err string
	}{{
		in:  "set @@autocommit = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = 1, client_found_rows = 1",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{ClientFoundRows: true}},
	}, {
		in:  "set @@session.autocommit = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@session.`autocommit` = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = on",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = ON",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = 'on'",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = `on`",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = \"on\"",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = false",
		out: &vtgatepb.Session{},
	}, {
		in:  "set autocommit = off",
		out: &vtgatepb.Session{},
	}, {
		in:  "set autocommit = OFF",
		out: &vtgatepb.Session{},
	}, {
		in:  "set AUTOCOMMIT = 0",
		out: &vtgatepb.Session{},
	}, {
		in:  "set AUTOCOMMIT = 'aa'",
		err: "System setting 'autocommit' can't be set to this value: 'aa' is not a boolean",
	}, {
		in:  "set autocommit = 2",
		err: "System setting 'autocommit' can't be set to this value: 2 is not a boolean",
	}, {
		in:  "set client_found_rows = 1",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{ClientFoundRows: true}},
	}, {
		in:  "set client_found_rows = true",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{ClientFoundRows: true}},
	}, {
		in:  "set client_found_rows = 0",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{}},
	}, {
		in:  "set client_found_rows = false",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{}},
	}, {
		in:  "set @@global.client_found_rows = 1",
		err: "unsupported global scope in set: global client_found_rows = 1",
	}, {
		in:  "set global client_found_rows = 1",
		err: "unsupported global scope in set: global client_found_rows = 1",
	}, {
		in:  "set global @@session.client_found_rows = 1",
		err: "cannot use scope and @@",
	}, {
		in:  "set client_found_rows = 'aa'",
		err: "System setting 'client_found_rows' can't be set to this value: 'aa' is not a boolean",
	}, {
		in:  "set client_found_rows = 2",
		err: "System setting 'client_found_rows' can't be set to this value: 2 is not a boolean",
	}, {
		in:  "set transaction_mode = 'unspecified'",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_UNSPECIFIED},
	}, {
		in:  "set transaction_mode = 'single'",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_SINGLE},
	}, {
		in:  "set transaction_mode = 'multi'",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_MULTI},
	}, {
		in:  "set transaction_mode = 'twopc'",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_TWOPC},
	}, {
		in:  "set transaction_mode = twopc",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_TWOPC},
	}, {
		in:  "set transaction_mode = 'aa'",
		err: "invalid transaction_mode: aa",
	}, {
		in:  "set transaction_mode = 1",
		err: "unexpected value type for transaction_mode: INT64",
	}, {
		in:  "set workload = 'unspecified'",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_UNSPECIFIED}},
	}, {
		in:  "set workload = 'oltp'",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLTP}},
	}, {
		in:  "set workload = 'olap'",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLAP}},
	}, {
		in:  "set workload = 'dba'",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_DBA}},
	}, {
		in:  "set workload = 'aa'",
		err: "invalid workload: aa",
	}, {
		in:  "set workload = 1",
		err: "unexpected value type for workload: INT64",
	}, {
		in:  "set transaction_mode = 'twopc', autocommit=1",
		out: &vtgatepb.Session{Autocommit: true, TransactionMode: vtgatepb.TransactionMode_TWOPC},
	}, {
		in:  "set sql_select_limit = 5",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{SqlSelectLimit: 5}},
	}, {
		in:  "set sql_select_limit = DEFAULT",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{SqlSelectLimit: 0}},
	}, {
		in:  "set sql_select_limit = 'asdfasfd'",
		err: "unexpected value type for sql_select_limit: string",
	}, {
		in:  "set autocommit = 1+1",
		err: "System setting 'autocommit' can't be set to this value: 2 is not a boolean",
	}, {
		in:  "set autocommit = 1+0",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = default",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set foo = 1",
		err: "unsupported construct in set: session foo = 1",
	}, {
		in:  "set names utf8",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set names ascii",
		err: "unexpected value for charset/names: ascii",
	}, {
		in:  "set charset utf8",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character set default",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character set ascii",
		err: "unexpected value for charset/names: ascii",
	}, {
		in:  "set skip_query_plan_cache = 1",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{SkipQueryPlanCache: true}},
	}, {
		in:  "set skip_query_plan_cache = 0",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{}},
	}, {
		in:  "set tx_read_only = 2",
		err: "System setting 'tx_read_only' can't be set to this value: 2 is not a boolean",
	}, {
		in:  "set transaction_read_only = 2",
		err: "System setting 'transaction_read_only' can't be set to this value: 2 is not a boolean",
	}, {
		in:  "set session transaction isolation level repeatable read",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set session transaction isolation level read committed",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set session transaction isolation level read uncommitted",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set session transaction isolation level serializable",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set transaction isolation level serializable",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set transaction read only",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set transaction read write",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set session transaction read write",
		out: &vtgatepb.Session{Autocommit: true},
	}}
	for i, tcase := range testcases {
		t.Run(fmt.Sprintf("%d-%s", i, tcase.in), func(t *testing.T) {
			session := NewSafeSession(&vtgatepb.Session{Autocommit: true})
			_, err := executorEnv.Execute(context.Background(), "TestExecute", session, tcase.in, nil)
			if tcase.err == "" {
				require.NoError(t, err)
				utils.MustMatch(t, tcase.out, session.Session, "new executor")
			} else {
				require.EqualError(t, err, tcase.err)
			}
		})
	}
}

func TestExecutorSetOp(t *testing.T) {
	executor, _, _, sbclookup := createLegacyExecutorEnv()
	*sysVarSetEnabled = true

	sbclookup.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("sql_mode", "varchar"), "STRICT_ALL_TABLES,NO_AUTO_UPDATES"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("sql_safe_updates", "int64"), "1"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("tx_isolation", "varchar"), "read-committed"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("sql_quote_show_create", "int64"), "0"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("foreign_key_checks", "int64")),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("unique_checks", "int64"), "0"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("net_write_timeout", "int64"), "600"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("net_read_timeout", "int64"), "300"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("character_set_client", "varchar"), "utf8"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("character_set_results", "varchar")),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("character_set_results", "varchar")),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("character_set_results", "varchar")),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("character_set_results", "varchar")),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("character_set_results", "varchar")),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("character_set_results", "varchar")),
	})

	testcases := []struct {
		in      string
		warning []*querypb.QueryWarning
		sysVars map[string]string
	}{{
		in: "set big_tables = 1", //ignore
	}, {
		in:      "set sql_mode = 'STRICT_ALL_TABLES,NO_AUTO_UPDATES'",
		sysVars: map[string]string{"sql_mode": "'STRICT_ALL_TABLES,NO_AUTO_UPDATES'"},
	}, {
		in:      "set sql_safe_updates = 1",
		sysVars: map[string]string{"sql_safe_updates": "1"},
	}, {
		in:      "set tx_isolation = 'read-committed'",
		sysVars: map[string]string{"tx_isolation": "'read-committed'"},
	}, {
		in:      "set sql_quote_show_create = 0",
		sysVars: map[string]string{"sql_quote_show_create": "0"},
	}, {
		in: "set foreign_key_checks = 1",
	}, {
		in:      "set unique_checks = 0",
		sysVars: map[string]string{"unique_checks": "0"},
	}, {
		in: "set net_write_timeout = 600",
	}, {
		in: "set net_read_timeout = 600",
	}, {
		in: "set character_set_client = utf8",
	}, {
		in: "set character_set_results=null",
	}, {
		in: "set character_set_results='binary'",
	}, {
		in: "set character_set_results='utf8'",
	}, {
		in: "set character_set_results=utf8mb4",
	}, {
		in: "set character_set_results='latin1'",
	}, {
		in: "set character_set_results='abcd'",
	}}
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			session := NewAutocommitSession(masterSession)
			session.TargetString = KsTestUnsharded
			_, err := executor.Execute(
				context.Background(),
				"TestExecute",
				session,
				tcase.in,
				nil)
			require.NoError(t, err)
			utils.MustMatch(t, tcase.warning, session.Warnings, "")
			utils.MustMatch(t, tcase.sysVars, session.SystemVariables, "")
		})
	}
}

func TestExecutorSetMetadata(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master", Autocommit: true})

	set := "set @@vitess_metadata.app_keyspace_v1= '1'"
	_, err := executor.Execute(context.Background(), "TestExecute", session, set, nil)
	assert.Equalf(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err), "expected error %v, got error: %v", vtrpcpb.Code_PERMISSION_DENIED, err)

	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()

	executor, _, _, _ = createLegacyExecutorEnv()
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@master", Autocommit: true})

	set = "set @@vitess_metadata.app_keyspace_v1= '1'"
	_, err = executor.Execute(context.Background(), "TestExecute", session, set, nil)
	assert.NoError(t, err, "%s error: %v", set, err)

	show := `show vitess_metadata variables like 'app\\_keyspace\\_v_'`
	result, err := executor.Execute(context.Background(), "TestExecute", session, show, nil)
	assert.NoError(t, err)

	want := "1"
	got := string(result.Rows[0][1].ToString())
	assert.Equalf(t, want, got, "want migrations %s, result %s", want, got)

	// Update metadata
	set = "set @@vitess_metadata.app_keyspace_v2='2'"
	_, err = executor.Execute(context.Background(), "TestExecute", session, set, nil)
	assert.NoError(t, err, "%s error: %v", set, err)

	show = `show vitess_metadata variables like 'app\\_keyspace\\_v%'`
	gotqr, err := executor.Execute(context.Background(), "TestExecute", session, show, nil)
	assert.NoError(t, err)

	wantqr := &sqltypes.Result{
		Fields: buildVarCharFields("Key", "Value"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("app_keyspace_v1", "1"),
			buildVarCharRow("app_keyspace_v2", "2"),
		},
		RowsAffected: 2,
	}

	assert.Equal(t, wantqr.Fields, gotqr.Fields)
	assert.ElementsMatch(t, wantqr.Rows, gotqr.Rows)

	show = "show vitess_metadata variables"
	gotqr, err = executor.Execute(context.Background(), "TestExecute", session, show, nil)
	require.NoError(t, err)

	assert.Equal(t, wantqr.Fields, gotqr.Fields)
	assert.ElementsMatch(t, wantqr.Rows, gotqr.Rows)
}

func TestPlanExecutorSetUDV(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()

	testcases := []struct {
		in  string
		out *vtgatepb.Session
		err string
	}{{
		in:  "set @FOO = 'bar'",
		out: &vtgatepb.Session{UserDefinedVariables: createMap([]string{"foo"}, []interface{}{"bar"}), Autocommit: true},
	}, {
		in:  "set @foo = 2",
		out: &vtgatepb.Session{UserDefinedVariables: createMap([]string{"foo"}, []interface{}{2}), Autocommit: true},
	}, {
		in:  "set @foo = 2.1, @bar = 'baz'",
		out: &vtgatepb.Session{UserDefinedVariables: createMap([]string{"foo", "bar"}, []interface{}{2.1, "baz"}), Autocommit: true},
	}}
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			session := NewSafeSession(&vtgatepb.Session{Autocommit: true})
			_, err := executor.Execute(context.Background(), "TestExecute", session, tcase.in, nil)
			if err != nil {
				require.EqualError(t, err, tcase.err)
			} else {
				utils.MustMatch(t, tcase.out, session.Session, "session output was not as expected")
			}
		})
	}
}

func TestSetUDVFromTabletInput(t *testing.T) {
	executor, sbc1, _, _ := createLegacyExecutorEnv()

	fields := sqltypes.MakeTestFields("some", "VARBINARY")
	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(
			fields,
			"abc",
		),
	})

	masterSession.TargetString = "TestExecutor"
	defer func() {
		masterSession.TargetString = ""
	}()
	_, err := executorExec(executor, "set @foo = concat('a','b','c')", nil)
	require.NoError(t, err)

	want := map[string]*querypb.BindVariable{"foo": sqltypes.StringBindVariable("abc")}
	utils.MustMatch(t, want, masterSession.UserDefinedVariables, "")
}

func createMap(keys []string, values []interface{}) map[string]*querypb.BindVariable {
	result := make(map[string]*querypb.BindVariable)
	for i, key := range keys {
		variable, err := sqltypes.BuildBindVariable(values[i])
		if err != nil {
			panic(err)
		}
		result[key] = variable
	}
	return result
}
