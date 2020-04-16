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
	"testing"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/vterrors"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutorSet(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	testcases := []struct {
		in  string
		out *vtgatepb.Session
		err string
	}{{
		in:  "set autocommit = 1",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@autocommit = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@session.autocommit = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@session.`autocommit` = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@session.'autocommit' = true",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set @@session.\"autocommit\" = true",
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
		err: "unexpected value for autocommit: aa",
	}, {
		in:  "set autocommit = 2",
		err: "unexpected value for autocommit: 2",
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
		err: "unsupported in set: global",
	}, {
		in:  "set global client_found_rows = 1",
		err: "unsupported in set: global",
	}, {
		in:  "set global @@session.client_found_rows = 1",
		err: "unsupported in set: global",
	}, {
		in:  "set client_found_rows = 'aa'",
		err: "unexpected value type for client_found_rows: string",
	}, {
		in:  "set client_found_rows = 2",
		err: "unexpected value for client_found_rows: 2",
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
		err: "unexpected value type for transaction_mode: int64",
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
		err: "unexpected value type for workload: int64",
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
		err: "unexpected string value for sql_select_limit: asdfasfd",
	}, {
		in:  "set autocommit = 1+1",
		err: "invalid syntax: 1 + 1",
	}, {
		in:  "set character_set_results=null",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set character_set_results='abcd'",
		err: "disallowed value for character_set_results: abcd",
	}, {
		in:  "set foo = 1",
		err: "unsupported construct: set foo = 1",
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
		in:  "set net_write_timeout = 600",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set sql_mode = 'STRICT_ALL_TABLES'",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set net_read_timeout = 600",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set sql_quote_show_create = 1",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set foreign_key_checks = 0",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set unique_checks = 0",
		out: &vtgatepb.Session{Autocommit: true},
	}, {
		in:  "set skip_query_plan_cache = 1",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{SkipQueryPlanCache: true}},
	}, {
		in:  "set skip_query_plan_cache = 0",
		out: &vtgatepb.Session{Autocommit: true, Options: &querypb.ExecuteOptions{}},
	}, {
		in:  "set sql_auto_is_null = 0",
		out: &vtgatepb.Session{Autocommit: true}, // no effect
	}, {
		in:  "set sql_auto_is_null = 1",
		err: "sql_auto_is_null is not currently supported",
	}, {
		in:  "set tx_read_only = 2",
		err: "unexpected value for tx_read_only: 2",
	}, {
		in:  "set transaction_read_only = 2",
		err: "unexpected value for transaction_read_only: 2",
	}, {
		in:  "set tx_isolation = 'invalid'",
		err: "unexpected value for tx_isolation: invalid",
	}, {
		in:  "set sql_safe_updates = 2",
		err: "unexpected value for sql_safe_updates: 2",
	}, {
		in:  "set @foo = 'bar'",
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

func TestExecutorSetMetadata(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master", Autocommit: true})

	set := "set @@vitess_metadata.app_keyspace_v1= '1'"
	_, err := executor.Execute(context.Background(), "TestExecute", session, set, nil)
	assert.Equalf(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err), "expected error %v, got error: %v", vtrpcpb.Code_PERMISSION_DENIED, err)

	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()

	executor, _, _, _ = createExecutorEnv()
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
