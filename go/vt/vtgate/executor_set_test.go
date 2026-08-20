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
	"log/slog"
	"maps"
	"testing"

	mysqlconfig "vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sysvars"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutorSet(t *testing.T) {
	executorEnv, _, _, _, ctx := createExecutorEnv(t)

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
		err: "variable 'autocommit' can't be set to the value: 'aa' is not a boolean",
	}, {
		in:  "set autocommit = 2",
		err: "variable 'autocommit' can't be set to the value: 2 is not a boolean",
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
		in:  "set global @@session.client_found_rows = 1",
		err: "syntax error at position 39 near 'session.client_found_rows'",
	}, {
		in:  "set client_found_rows = 'aa'",
		err: "variable 'client_found_rows' can't be set to the value: 'aa' is not a boolean",
	}, {
		in:  "set client_found_rows = 2",
		err: "variable 'client_found_rows' can't be set to the value: 2 is not a boolean",
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
		err: "incorrect argument type to variable 'transaction_mode': INT64",
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
		err: "incorrect argument type to variable 'workload': INT64",
	}, {
		in:  "set tx_isolation = 'read-committed'",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set transaction_isolation = 'read-committed'",
		out: &vtgatepb.Session{Autocommit: true},
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
		err: "incorrect argument type to variable 'sql_select_limit': VARCHAR",
	}, {
		in:  "set autocommit = 1+1",
		err: "variable 'autocommit' can't be set to the value: 2 is not a boolean",
	}, {
		in:  "set autocommit = 1+0",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set autocommit = default",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set foo = 1",
		err: "VT05006: unknown system variable '@@foo = 1'",
	}, {
		in:  "set names utf8",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set names ascii",
		err: "charset/name ascii is not supported",
	}, {
		in:  "set charset utf8",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character set default",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character set ascii",
		err: "charset/name ascii is not supported",
	}, {
		in:  "set skip_query_plan_cache = 1",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{SkipQueryPlanCache: true}},
	}, {
		in:  "set skip_query_plan_cache = 0",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{}},
	}, {
		in:  "set tx_read_only = 2",
		err: "variable 'tx_read_only' can't be set to the value: 2 is not a boolean",
	}, {
		in:  "set transaction_read_only = 2",
		err: "variable 'transaction_read_only' can't be set to the value: 2 is not a boolean",
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
		in: "set transaction isolation level serializable",
		out: &vtgatepb.Session{
			Autocommit: true,
			Warnings:   []*querypb.QueryWarning{{Code: uint32(sqlerror.ERNotSupportedYet), Message: "converted 'next transaction' scope to 'session' scope"}},
		},
	}, {
		in:  "set transaction read only",
		out: &vtgatepb.Session{Autocommit: true, Warnings: []*querypb.QueryWarning{{Code: uint32(sqlerror.ERNotSupportedYet), Message: "converted 'next transaction' scope to 'session' scope"}}},
	}, {
		in:  "set transaction read write",
		out: &vtgatepb.Session{Autocommit: true, Warnings: []*querypb.QueryWarning{{Code: uint32(sqlerror.ERNotSupportedYet), Message: "converted 'next transaction' scope to 'session' scope"}}},
	}, {
		in:  "set session transaction read write",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@enable_system_settings = on",
		out: &vtgatepb.Session{Autocommit: true, EnableSystemSettings: true},
	}, {
		in:  "set @@enable_system_settings = off",
		out: &vtgatepb.Session{Autocommit: true, EnableSystemSettings: false},
	}, {
		in:  "set @@enable_system_settings = 1",
		out: &vtgatepb.Session{Autocommit: true, EnableSystemSettings: true},
	}, {
		in:  "set @@enable_system_settings = 0",
		out: &vtgatepb.Session{Autocommit: true, EnableSystemSettings: false},
	}, {
		in:  "set @@enable_system_settings = true",
		out: &vtgatepb.Session{Autocommit: true, EnableSystemSettings: true},
	}, {
		in:  "set @@enable_system_settings = false",
		out: &vtgatepb.Session{Autocommit: true, EnableSystemSettings: false},
	}, {
		in:  "set @@socket = '/tmp/change.sock'",
		err: "VT03010: variable 'socket' is a read only variable",
	}, {
		in:  "set @@query_timeout = 50",
		out: &vtgatepb.Session{Autocommit: true, QueryTimeout: 50},
	}, {
		in:  "set @@query_timeout = 50, query_timeout = 75",
		out: &vtgatepb.Session{Autocommit: true, QueryTimeout: 75},
	}, {
		in:  "set @@transaction_timeout = 50",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{TransactionTimeout: new(int64(50))}},
	}, {
		in:  "set @@transaction_timeout = 50, transaction_timeout = 75",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{TransactionTimeout: new(int64(75))}},
	}}
	for i, tcase := range testcases {
		t.Run(fmt.Sprintf("%d-%s", i, tcase.in), func(t *testing.T) {
			session := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true})
			_, err := executorExecSession(ctx, executorEnv, session, tcase.in, nil)
			if tcase.err == "" {
				require.NoError(t, err)
				assertSeededSQLMode(t, session)
				utils.MustMatch(t, tcase.out, session.Session, "new executor")
			} else {
				require.EqualError(t, err, tcase.err)
			}
		})
	}
}

func TestExecutorInitVConfigUsesSetVarFlag(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	oldSetVarEnabled := setVarEnabled
	oldSysVarSetEnabled := sysVarSetEnabled
	t.Cleanup(func() {
		setVarEnabled = oldSetVarEnabled
		sysVarSetEnabled = oldSysVarSetEnabled
	})

	sysVarSetEnabled = true
	setVarEnabled = false
	executor.initVConfig(false, querypb.ExecuteOptions_Gen4)
	assert.False(t, executor.vConfig.SetVarEnabled)

	sysVarSetEnabled = false
	setVarEnabled = true
	executor.initVConfig(false, querypb.ExecuteOptions_Gen4)
	assert.True(t, executor.vConfig.SetVarEnabled)
}

func TestBuildDeniedSystemVariablesWarnsForUnknownNames(t *testing.T) {
	oldWarn := log.Warn
	t.Cleanup(func() {
		log.Warn = oldWarn
	})

	var gotMessage string
	var gotAttrs []slog.Attr
	log.Warn = func(msg string, attrs ...slog.Attr) {
		gotMessage = msg
		gotAttrs = append([]slog.Attr(nil), attrs...)
	}

	denied := buildDeniedSystemVariables([]string{"unique_checks", "not_a_real_sysvar", " "})

	assert.Equal(t, map[string]struct{}{
		"unique_checks":     {},
		"not_a_real_sysvar": {},
	}, denied)
	assert.Equal(t, "unknown system variable in --denied-system-variables", gotMessage)
	require.Len(t, gotAttrs, 1)
	assert.Equal(t, "name", gotAttrs[0].Key)
	assert.Equal(t, "not_a_real_sysvar", gotAttrs[0].Value.String())
}

func TestExecutorSetOp(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)
	sysVarSetEnabled = true

	returnResult := func(columnName, typ, value string) *sqltypes.Result {
		return sqltypes.MakeTestResult(sqltypes.MakeTestFields(columnName, typ), value)
	}
	returnNoResult := func(columnName, typ string) *sqltypes.Result {
		return sqltypes.MakeTestResult(sqltypes.MakeTestFields(columnName, typ))
	}

	testcases := []struct {
		in              string
		warning         []*querypb.QueryWarning
		sysVars         map[string]string
		disallowResConn bool
		result          *sqltypes.Result
	}{{
		in: "set big_tables = 1", // ignore
	}, {
		in:      "set sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'",
		sysVars: map[string]string{"sql_mode": "'STRICT_ALL_TABLES,NO_ZERO_DATE'"},
		result:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"), "|STRICT_ALL_TABLES,NO_ZERO_DATE"),
	}, {
		// even though the tablet is saying that the value has changed,
		// useReservedConn is false, so we won't allow this change
		in:              "set sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'",
		result:          returnResult("sql_mode", "varchar", "STRICT_ALL_TABLES,NO_ZERO_DATE"),
		sysVars:         nil,
		disallowResConn: true,
	}, {
		in:      "set sql_safe_updates = 1",
		sysVars: map[string]string{"sql_safe_updates": "1"},
		result:  returnResult("sql_safe_updates", "int64", "1"),
	}, {
		in:      "set sql_quote_show_create = 0",
		sysVars: map[string]string{"sql_quote_show_create": "0"},
		result:  returnResult("sql_quote_show_create", "int64", "0"),
	}, {
		in:     "set foreign_key_checks = 1",
		result: returnNoResult("foreign_key_checks", "int64"),
	}, {
		in:      "set foreign_key_checks = 0",
		sysVars: map[string]string{"foreign_key_checks": "0"},
		result:  returnResult("foreign_key_checks", "int64", "0"),
	}, {
		in:      "set unique_checks = 0",
		sysVars: map[string]string{"unique_checks": "0"},
		result:  returnResult("unique_checks", "int64", "0"),
	}, {
		in:     "set net_write_timeout = 600",
		result: returnResult("net_write_timeout", "int64", "600"),
	}, {
		in:     "set net_read_timeout = 600",
		result: returnResult("net_read_timeout", "int64", "300"),
	}, {
		in:     "set character_set_client = utf8",
		result: returnResult("character_set_client", "varchar", "utf8"),
	}, {
		in:     "set character_set_results=null",
		result: returnNoResult("character_set_results", "varchar"),
	}, {
		in:     "set character_set_results='binary'",
		result: returnNoResult("character_set_results", "varchar"),
	}, {
		in:     "set character_set_results='utf8'",
		result: returnNoResult("character_set_results", "varchar"),
	}, {
		in:     "set character_set_results=utf8mb4",
		result: returnNoResult("character_set_results", "varchar"),
	}, {
		in:     "set character_set_results='latin1'",
		result: returnNoResult("character_set_results", "varchar"),
	}, {
		in:     "set character_set_results='abcd'",
		result: returnNoResult("character_set_results", "varchar"),
	}, {
		in:     "set @@global.client_found_rows = 1",
		result: returnNoResult("client_found_rows", "int64"),
	}, {
		in:     "set global client_found_rows = 1",
		result: returnNoResult("client_found_rows", "int64"),
	}, {
		in:      "set tx_isolation = 'read-committed'",
		sysVars: map[string]string{"tx_isolation": "'read-committed'"},
		result:  returnResult("tx_isolation", "varchar", "read-committed"),
	}, {
		in:      "set @@innodb_lock_wait_timeout=120",
		sysVars: map[string]string{"innodb_lock_wait_timeout": "120"},
		result:  returnResult("innodb_lock_wait_timeout", "int64", "120"),
	}, {
		in:     "set @@global.innodb_lock_wait_timeout=120",
		result: returnResult("innodb_lock_wait_timeout", "int64", "120"),
	}}
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			session := econtext.NewAutocommitSession(&vtgatepb.Session{
				TargetString: "@primary",
			})
			session.TargetString = KsTestUnsharded
			session.EnableSystemSettings = !tcase.disallowResConn
			sbclookup.SetResults([]*sqltypes.Result{tcase.result})
			_, err := executorExecSession(ctx, executor, session, tcase.in, nil)
			require.NoError(t, err)
			utils.MustMatch(t, tcase.warning, session.Warnings, "")
			// every session is seeded with the default sql_mode; an explicit expectation
			// for sql_mode in the test case wins
			wantSysVars := map[string]string{sysvars.SQLMode.Name: sqltypes.EncodeStringSQL(mysqlconfig.DefaultSQLMode)}
			maps.Copy(wantSysVars, tcase.sysVars)
			utils.MustMatch(t, wantSysVars, session.SystemVariables, "")
		})
	}
}

func TestExecutorSetDeniedSystemVariables(t *testing.T) {
	cases := []struct {
		name    string
		denied  map[string]struct{}
		query   string
		wantErr string // empty = expect success
	}{{
		name:    "unique_checks denied",
		denied:  map[string]struct{}{"unique_checks": {}},
		query:   "set unique_checks = 0",
		wantErr: "VT12001: unsupported: system setting: unique_checks",
	}, {
		name:   "unique_checks allowed when flag empty",
		denied: nil,
		query:  "set unique_checks = 0",
	}, {
		name:    "case-insensitive match",
		denied:  map[string]struct{}{"unique_checks": {}},
		query:   "set UNIQUE_CHECKS = 0",
		wantErr: "VT12001: unsupported: system setting: UNIQUE_CHECKS",
	}, {
		name:    "global scope denied",
		denied:  map[string]struct{}{"unique_checks": {}},
		query:   "set @@global.unique_checks = 0",
		wantErr: "VT12001: unsupported: system setting: unique_checks",
	}, {
		name:   "unrelated sysvars unaffected",
		denied: map[string]struct{}{"unique_checks": {}},
		query:  "set foreign_key_checks = 0",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor, _, _, _, ctx := createExecutorEnv(t)
			executor.vConfig.DeniedSystemVariables = tc.denied

			session := econtext.NewAutocommitSession(&vtgatepb.Session{
				TargetString:         KsTestUnsharded,
				EnableSystemSettings: true,
			})
			_, err := executorExecSession(ctx, executor, session, tc.query, nil)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantErr, err.Error())
			assert.Equal(t, vtrpcpb.Code_UNIMPLEMENTED, vterrors.Code(err))
		})
	}
}

func TestExecutorSetMetadata(t *testing.T) {
	t.Run("Session 1", func(t *testing.T) {
		executor, _, _, _, ctx := createExecutorEnv(t)
		session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: "@primary", Autocommit: true})

		set := "set @@vitess_metadata.app_keyspace_v1= '1'"
		_, err := executorExecSession(ctx, executor, session, set, nil)
		assert.Equalf(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err), "expected error %v, got error: %v", vtrpcpb.Code_PERMISSION_DENIED, err)
	})

	t.Run("Session 2", func(t *testing.T) {
		vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers("%"))
		defer func() {
			vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers(""))
		}()

		executor, _, _, _, ctx := createExecutorEnv(t)
		session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: "@primary", Autocommit: true})

		set := "set @@vitess_metadata.app_keyspace_v1= '1'"
		_, err := executorExecSession(ctx, executor, session, set, nil)
		require.NoError(t, err, "%s error: %v", set, err)

		show := `show vitess_metadata variables like 'app\\_keyspace\\_v_'`
		result, err := executorExecSession(ctx, executor, session, show, nil)
		require.NoError(t, err)

		want := "1"
		got := result.Rows[0][1].ToString()
		assert.Equalf(t, want, got, "want migrations %s, result %s", want, got)

		// Update metadata
		set = "set @@vitess_metadata.app_keyspace_v2='2'"
		_, err = executorExecSession(ctx, executor, session, set, nil)
		require.NoError(t, err, "%s error: %v", set, err)

		show = `show vitess_metadata variables like 'app\\_keyspace\\_v%'`
		gotqr, err := executorExecSession(ctx, executor, session, show, nil)
		require.NoError(t, err)

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
		gotqr, err = executorExecSession(ctx, executor, session, show, nil)
		require.NoError(t, err)

		assert.Equal(t, wantqr.Fields, gotqr.Fields)
		assert.ElementsMatch(t, wantqr.Rows, gotqr.Rows)
	})
}

func TestPlanExecutorSetUDV(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	testcases := []struct {
		in  string
		out *vtgatepb.Session
		err string
	}{{
		in:  "set @FOO = 'bar'",
		out: &vtgatepb.Session{UserDefinedVariables: createMap([]string{"foo"}, []any{"bar"}), Autocommit: true},
	}, {
		in:  "set @foo = 2",
		out: &vtgatepb.Session{UserDefinedVariables: createMap([]string{"foo"}, []any{2}), Autocommit: true},
	}, {
		in:  "set @foo = 2.1, @bar = 'baz'",
		out: &vtgatepb.Session{UserDefinedVariables: createMap([]string{"foo", "bar"}, []any{sqltypes.DecimalString("2.1"), "baz"}), Autocommit: true},
	}}
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			session := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true})
			_, err := executorExecSession(ctx, executor, session, tcase.in, nil)
			if err != nil {
				require.EqualError(t, err, tcase.err)
			} else {
				assertSeededSQLMode(t, session)
				utils.MustMatch(t, tcase.out, session.Session, "session output was not as expected")
			}
		})
	}
}

func TestSetUDVFromTabletInput(t *testing.T) {
	executor, sbc1, _, _, ctx := createExecutorEnv(t)

	fields := sqltypes.MakeTestFields("some", "VARCHAR")
	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(
			fields,
			"abc",
		),
	})

	session := &vtgatepb.Session{TargetString: "TestExecutor"}
	_, err := executorExec(ctx, executor, session, "set @foo = concat('a','b','c')", nil)
	require.NoError(t, err)

	want := map[string]*querypb.BindVariable{"foo": sqltypes.StringBindVariable("abc")}
	utils.MustMatch(t, want, session.UserDefinedVariables, "")
}

func createMap(keys []string, values []any) map[string]*querypb.BindVariable {
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

func TestSetVar(t *testing.T) {
	executor, _, _, sbc, ctx := createCustomExecutor(t, "{}", "8.0.0")
	executor.config.Normalize = true

	session := econtext.NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestUnsharded})

	sbc.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
		"|only_full_group_by")})

	_, err := executorExecSession(ctx, executor, session, "set @@sql_mode = only_full_group_by", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	tcases := []struct {
		sql string
		rc  bool
	}{
		{sql: "select 1 from user"},
		{sql: "update user set col = 2"},
		{sql: "delete from user"},
		{sql: "insert into user (id) values (1)"},
		{sql: "replace into user(id, col) values (1, 'new')"},
		{sql: "set autocommit = 0"},
		{sql: "show create table user"}, // reserved connection should not be set.
		{sql: "create table foo(bar bigint)", rc: true},
	}

	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			// reset reserved conn need.
			session.SetReservedConn(false)

			_, err = executorExecSession(ctx, executor, session, tc.sql, map[string]*querypb.BindVariable{})
			require.NoError(t, err)
			assert.Equal(t, tc.rc, session.InReservedConn())
		})
	}
}

func TestSQLModeFlag(t *testing.T) {
	var f sqlModeFlag
	require.NoError(t, f.Set("no_zero_date,strict_trans_tables"))
	assert.Equal(t, "STRICT_TRANS_TABLES,NO_ZERO_DATE", f.String())
	require.NoError(t, f.Set("TRADITIONAL"))
	assert.Equal(t, "STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_ENGINE_SUBSTITUTION", f.String())
	require.EqualError(t, f.Set("NO_BACKSLASH_ESCAPES"), "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported")
	require.EqualError(t, f.Set("IGNORE_SPACE"), "setting the IGNORE_SPACE sql_mode is unsupported")
	require.EqualError(t, f.Set("BOGUS"), "Variable 'sql_mode' can't be set to the value of 'BOGUS'")
}

func TestSetSQLModeDefault(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())

	session := econtext.NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestUnsharded})
	_, err := executorExecSession(ctx, executor, session, "set sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'", nil)
	require.NoError(t, err)
	require.Equal(t, "'STRICT_ALL_TABLES,NO_ZERO_DATE'", session.SystemVariables[sysvars.SQLMode.Name])

	// DEFAULT restores the configured default the session started with, in canonical form
	_, err = executorExecSession(ctx, executor, session, "set sql_mode = default", nil)
	require.NoError(t, err)
	require.Equal(t, sqltypes.EncodeStringSQL(mysqlconfig.DefaultSQLMode), session.SystemVariables[sysvars.SQLMode.Name])
}

func TestSQLModeLexerModesNotSentToBackends(t *testing.T) {
	executor, _, _, lookup, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())

	// setting a mode that changes how SQL text is interpreted is rejected outright
	session := econtext.NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestUnsharded})
	_, err := executorExecSession(ctx, executor, session, "set sql_mode = 'IGNORE_SPACE,STRICT_TRANS_TABLES'", nil)
	require.EqualError(t, err, "setting the IGNORE_SPACE sql_mode is unsupported")
	_, err = executorExecSession(ctx, executor, session, "set sql_mode = 'HIGH_NOT_PRECEDENCE'", nil)
	require.EqualError(t, err, "setting the HIGH_NOT_PRECEDENCE sql_mode is unsupported")

	// a session proto constructed by a gRPC client can still carry lexer modes:
	// the session reports them, but they are never sent to the backends, which only
	// receive vtgate's canonically-formatted queries
	session = econtext.NewAutocommitSession(&vtgatepb.Session{
		EnableSystemSettings: true,
		TargetString:         KsTestUnsharded,
		SystemVariables:      map[string]string{"sql_mode": "'IGNORE_SPACE,STRICT_TRANS_TABLES'"},
	})
	qr, err := executorExecSession(ctx, executor, session, "select @@sql_mode", nil)
	require.NoError(t, err)
	require.Nil(t, lookup.Queries)
	assert.Equal(t, `[[VARCHAR("IGNORE_SPACE,STRICT_TRANS_TABLES")]]`, fmt.Sprintf("%v", qr.Rows))

	_, err = executorExecSession(ctx, executor, session, "select id from main1", nil)
	require.NoError(t, err)
	require.Len(t, lookup.Queries, 1)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'STRICT_TRANS_TABLES') */ id from main1", lookup.Queries[0].Sql)
}

func TestSessionDefaultSQLMode(t *testing.T) {
	cfg := createExecutorConfigWithNormalizer()
	cfg.SQLMode = "STRICT_TRANS_TABLES,NO_ZERO_DATE"
	executor, _, _, lookup, ctx := createExecutorEnvWithConfig(t, cfg)

	session := econtext.NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestUnsharded})

	// @@sql_mode resolves at the vtgate to the configured default even though the session
	// never set it, without any shard round trip
	qr, err := executorExecSession(ctx, executor, session, "select @@sql_mode", nil)
	require.NoError(t, err)
	require.Nil(t, lookup.Queries)
	assert.Equal(t, `[[VARCHAR("STRICT_TRANS_TABLES,NO_ZERO_DATE")]]`, fmt.Sprintf("%v", qr.Rows))

	// expressions over @@sql_mode are evaluated at the vtgate against the session default,
	// with no shard round trip
	_, err = executorExecSession(ctx, executor, session, "set sql_mode = concat(@@sql_mode, ',NO_ZERO_IN_DATE')", nil)
	require.NoError(t, err)
	require.Nil(t, lookup.Queries)
	assert.Equal(t, "'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'", session.SystemVariables["sql_mode"])

	// @@global.sql_mode is the configured default, never a backend's value
	qr, err = executorExecSession(ctx, executor, session, "select @@global.sql_mode", nil)
	require.NoError(t, err)
	require.Nil(t, lookup.Queries)
	assert.Equal(t, `[[VARCHAR("STRICT_TRANS_TABLES,NO_ZERO_DATE")]]`, fmt.Sprintf("%v", qr.Rows))

	// setting @@global.sql_mode's value restores the session default
	_, err = executorExecSession(ctx, executor, session, "set sql_mode = @@global.sql_mode", nil)
	require.NoError(t, err)
	require.Nil(t, lookup.Queries)
	assert.Equal(t, "'STRICT_TRANS_TABLES,NO_ZERO_DATE'", session.SystemVariables["sql_mode"])

	// setting the session default's own value is a no-op: the seeded value is untouched
	// and no reserved connection is needed
	session2 := econtext.NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestUnsharded})
	_, err = executorExecSession(ctx, executor, session2, "set sql_mode = 'no_zero_date,STRICT_TRANS_TABLES'", nil)
	require.NoError(t, err)
	require.Nil(t, lookup.Queries)
	assert.Equal(t, "'STRICT_TRANS_TABLES,NO_ZERO_DATE'", session2.SystemVariables["sql_mode"])
	assert.False(t, session2.InReservedConn())
}

func TestSessionSQLModeSeedingDisabled(t *testing.T) {
	cfg := createExecutorConfigWithNormalizer()
	cfg.SystemSettingsDisabled = true
	executor, _, _, lookup, ctx := createExecutorEnvWithConfig(t, cfg)

	session := econtext.NewAutocommitSession(&vtgatepb.Session{TargetString: KsTestUnsharded})

	// a deployment that opted out of vtgate-managed system settings keeps its backends'
	// configured sql_mode: no seeding, no SET_VAR hint, no forced settings connection
	_, err := executorExecSession(ctx, executor, session, "select id from main1", nil)
	require.NoError(t, err)
	assert.NotContains(t, session.SystemVariables, "sql_mode")
	assert.False(t, session.InReservedConn())
	require.Len(t, lookup.Queries, 1)
	assert.Equal(t, "select id from main1", lookup.Queries[0].Sql)
}

func TestSetSQLModeRepairsUndecodableSessionValue(t *testing.T) {
	cfg := createExecutorConfigWithNormalizer()
	cfg.SQLMode = "STRICT_TRANS_TABLES,NO_ZERO_DATE"
	executor, _, _, lookup, ctx := createExecutorEnvWithConfig(t, cfg)

	// gRPC clients own their session proto, so the stored sql_mode may not be a valid SQL
	// string literal (e.g. state written by a different vtgate version)
	session := econtext.NewAutocommitSession(&vtgatepb.Session{
		EnableSystemSettings: true,
		TargetString:         KsTestUnsharded,
		SystemVariables:      map[string]string{"sql_mode": "garbage"},
	})

	// every read surface substitutes the configured default for the undecodable value, so
	// setting that same default must not be treated as a no-op: it must repair the stored
	// value instead of leaving the garbage in place
	_, err := executorExecSession(ctx, executor, session, "set sql_mode = 'no_zero_date,STRICT_TRANS_TABLES'", nil)
	require.NoError(t, err)
	require.Nil(t, lookup.Queries)
	assert.Equal(t, "'STRICT_TRANS_TABLES,NO_ZERO_DATE'", session.SystemVariables["sql_mode"])
	assert.False(t, session.InReservedConn())

	// and the SET_VAR hint must carry the repaired value
	_, err = executorExecSession(ctx, executor, session, "select id from main1", nil)
	require.NoError(t, err)
	require.Len(t, lookup.Queries, 1)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE') */ id from main1", lookup.Queries[0].Sql)
}

func TestSetVarShowVariables(t *testing.T) {
	executor, _, _, sbc, ctx := createCustomExecutor(t, "{}", "8.0.0")
	executor.config.Normalize = true

	session := econtext.NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestUnsharded})

	sbc.SetResults([]*sqltypes.Result{
		// show query result
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("Variable_name|Value", "varchar|varchar"),
			"sql_mode|ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE"),
	})

	_, err := executorExecSession(ctx, executor, session, "set @@sql_mode = only_full_group_by", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	// this should return the updated value of sql_mode.
	qr, err := executorExecSession(ctx, executor, session, "show variables like 'sql_mode'", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	assert.False(t, session.InReservedConn(), "reserved connection should not be used")
	assert.Equal(t, `[[VARCHAR("sql_mode") VARCHAR("ONLY_FULL_GROUP_BY")]]`, fmt.Sprintf("%v", qr.Rows))
}

func TestExecutorSetAndSelect(t *testing.T) {
	e, _, _, sbc, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())

	testcases := []struct {
		sysVar string
		val    string
		exp    string
	}{{
		sysVar: "transaction_isolation",
		exp:    `[[VARCHAR("REPEATABLE-READ")]]`,
	}, {
		sysVar: "transaction_isolation",
		val:    "READ-COMMITTED",
		exp:    `[[VARCHAR("READ-COMMITTED")]]`,
	}, {
		sysVar: "tx_isolation",
		val:    "READ-UNCOMMITTED",
		exp:    `[[VARCHAR("READ-UNCOMMITTED")]]`,
	}, {
		sysVar: "tx_isolation",
		exp:    `[[VARCHAR("READ-UNCOMMITTED")]]`, // this returns the value set in previous query.
	}}
	session := econtext.NewAutocommitSession(&vtgatepb.Session{TargetString: KsTestUnsharded, EnableSystemSettings: true})
	for _, tcase := range testcases {
		t.Run(fmt.Sprintf("%s-%s", tcase.sysVar, tcase.val), func(t *testing.T) {
			sbc.ExecCount.Store(0) // reset the value

			if tcase.val != "" {
				// check query result for `select <new_setting> from dual where @@transaction_isolation != <new_setting>
				// not always the check query is the first query, so setting it two times, as it will use one of those results.
				sbc.SetResults([]*sqltypes.Result{
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(tcase.sysVar, "varchar"), tcase.val), // one for set prequeries
					sqltypes.MakeTestResult(sqltypes.MakeTestFields(tcase.sysVar, "varchar"), tcase.val), // second for check query
					sqltypes.MakeTestResult(nil),
				}) // third one for new set query

				setQ := fmt.Sprintf("set %s = '%s'", tcase.sysVar, tcase.val)
				_, err := executorExecSession(ctx, e, session, setQ, nil)
				require.NoError(t, err)
			}

			selectQ := "select @@" + tcase.sysVar
			// if the query reaches the shard, it will return REPEATABLE-READ isolation level.
			sbc.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields(tcase.sysVar, "varchar"), "REPEATABLE-READ")})

			qr, err := executorExecSession(ctx, e, session, selectQ, nil)
			require.NoError(t, err)
			assert.Equal(t, tcase.exp, fmt.Sprintf("%v", qr.Rows))
		})
	}
}

// TestTimeZone verifies that setting different time zones in the session
// results in different outputs for the `now()` function.
func TestExecutorTimeZone(t *testing.T) {
	e, _, _, _, ctx := createExecutorEnv(t)

	session := econtext.NewAutocommitSession(&vtgatepb.Session{TargetString: KsTestUnsharded, EnableSystemSettings: true})
	session.SetSystemVariable("time_zone", "'+08:00'")

	qr, err := executorExecSession(ctx, e, session, "select now()", nil)

	require.NoError(t, err)
	session.SetSystemVariable("time_zone", "'+02:00'")

	qrWith, err := executorExecSession(ctx, e, session, "select now()", nil)
	require.NoError(t, err)

	assert.False(t, qr.Rows[0][0].Equal(qrWith.Rows[0][0]), "%v vs %v", qr.Rows[0][0].ToString(), qrWith.Rows[0][0].ToString())
}
