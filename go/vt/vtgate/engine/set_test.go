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

package engine

import (
	"errors"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestSetSystemVariableAsString(t *testing.T) {
	setOp := SysVarReservedConn{
		Name: "x",
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Expr: "dummy_expr",
	}

	set := &Set{
		Ops:   []SetOp{&setOp},
		Input: &SingleRow{},
	}
	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id",
				"varchar",
			),
			"foobar",
		)},
		shardSession: []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: "ks", Shard: "-20"}}},
	}
	_, err := set.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		"ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)",
		"ExecuteMultiShard ks.-20: select dummy_expr from dual where @@x != dummy_expr {} false false",
		"SysVar set with (x,'foobar')",
		"Needs Reserved Conn",
		"ExecuteMultiShard ks.-20: set x = dummy_expr {} false false",
	})
}

func TestSetTable(t *testing.T) {
	type testCase struct {
		testName         string
		setOps           []SetOp
		qr               []*sqltypes.Result
		expectedQueryLog []string
		expectedWarning  []*querypb.QueryWarning
		expectedError    string
		input            Primitive
		execErr          error
		mysqlVersion     string
		disableSetVar    bool
		shardSession     []*srvtopo.ResolvedShard
	}

	ks := &vindexes.Keyspace{Name: "ks", Sharded: true}
	tests := []testCase{{
		testName:         "nil set ops",
		expectedQueryLog: []string{},
	}, {
		testName: "udv",
		setOps: []SetOp{
			&UserDefinedVariable{
				Name: "x",
				Expr: evalengine.NewLiteralInt(42),
			},
		},
		expectedQueryLog: []string{
			`UDV set with (x,INT64(42))`,
		},
	}, {
		testName: "udv with input",
		setOps: []SetOp{
			&UserDefinedVariable{
				Name: "x",
				Expr: evalengine.NewColumn(0, evalengine.Type{}, nil),
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col0",
				"datetime",
			),
			"2020-10-28 00:00:00",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select now() from dual {} false false`,
			`UDV set with (x,DATETIME("2020-10-28 00:00:00"))`,
		},
		input: &Send{
			Keyspace:          ks,
			TargetDestination: key.DestinationAnyShard{},
			Query:             "select now() from dual",
			SingleShardOnly:   true,
		},
	}, {
		testName: "sysvar ignore",
		setOps: []SetOp{
			&SysVarIgnore{
				Name: "x",
				Expr: "42",
			},
		},
	}, {
		testName: "sysvar check and ignore",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "dummy_expr",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id",
				"int64",
			),
			"1",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select 1 from dual where @@x = dummy_expr {} false false`,
		},
	}, {
		testName: "sysvar check and error",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "dummy_expr",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select 1 from dual where @@x = dummy_expr {} false false`,
		},
	}, {
		// with system settings disabled a SET is ignored rather than applied, but an
		// invalid sql_mode is still an error: constants are rejected at plan time, and
		// a non-constant value is judged here once evaluated
		testName: "sysvar check and ignore rejects an invalid non-constant sql_mode",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "concat('BO', 'GUS')",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"orig|new",
				"varchar|varchar",
			),
			"STRICT_TRANS_TABLES|BOGUS",
		)},
		expectedError: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('BO', 'GUS') new {} false false`,
		},
	}, {
		testName: "sysvar check and ignore ignores a supported non-constant sql_mode",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "concat('STRICT_TRANS', '_TABLES')",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"orig|new",
				"varchar|varchar",
			),
			"NO_ZERO_DATE|STRICT_TRANS_TABLES",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('STRICT_TRANS', '_TABLES') new {} false false`,
		},
	}, {
		testName: "sysvar checkAndIgnore multi destination error",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAllShards{},
				Expr:              "dummy_expr",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		},
		expectedError: "Unexpected error, DestinationKeyspaceID mapping to multiple shards: DestinationAllShards()",
	}, {
		testName: "sysvar checkAndIgnore execute error",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "dummy_expr",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select 1 from dual where @@x = dummy_expr {} false false`,
		},
		execErr: errors.New("some random error"),
	}, {
		testName: "udv ignore checkAndIgnore ",
		setOps: []SetOp{
			&UserDefinedVariable{
				Name: "x",
				Expr: evalengine.NewLiteralInt(1),
			},
			&SysVarIgnore{
				Name: "y",
				Expr: "2",
			},
			&SysVarCheckAndIgnore{
				Name:              "z",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "dummy_expr",
			},
		},
		expectedQueryLog: []string{
			`UDV set with (x,INT64(1))`,
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select 1 from dual where @@z = dummy_expr {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id",
				"int64",
			),
			"1",
		)},
	}, {
		testName: "sysvar set without destination",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "dummy_expr",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.-20: set x = dummy_expr {} false false`,
			`SysVar set with (x,dummy_expr)`,
		},
	}, {
		// a failed targeted SET must not leave its value in the session, where the
		// settings transport would replay it on every subsequent query
		testName: "targeted set failure does not store the value",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "dummy_expr",
			},
		},
		execErr:       errors.New("some random error"),
		expectedError: "some random error",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.-20: set x = dummy_expr {} false false`,
		},
	}, {
		// a targeted session's SET gets the same sql_mode judgment as an untargeted
		// one; a non-constant expression is evaluated on the target shard and judged
		// before any state changes
		testName: "targeted sql_mode judges a non-constant value on the target shard",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "concat('BO', 'GUS')",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"orig|new",
				"varchar|varchar",
			),
			"STRICT_TRANS_TABLES|BOGUS",
		)},
		expectedError: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('BO', 'GUS') new {} false false`,
		},
	}, {
		// the SET carries the judged value rather than the expression: evaluating the
		// expression a second time could apply a value the session never judged
		testName: "targeted sql_mode applies and stores the judged value, not the expression",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "concat('STRICT_TRANS', '_TABLES')",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"orig|new",
				"varchar|varchar",
			),
			"NO_ZERO_DATE|STRICT_TRANS_TABLES",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('STRICT_TRANS', '_TABLES') new {} false false`,
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.-20: set sql_mode = 'STRICT_TRANS_TABLES' {} false false`,
			`SysVar set with (sql_mode,'STRICT_TRANS_TABLES')`,
		},
	}, {
		testName: "sysvar set not modifying setting",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "x",
				Keyspace: ks,
				Expr:     "dummy_expr",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select dummy_expr from dual where @@x != dummy_expr {} false false`,
		},
	}, {
		testName: "sysvar set modifying setting",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "x",
				Keyspace: ks,
				Expr:     "dummy_expr",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select dummy_expr from dual where @@x != dummy_expr {} false false`,
			`SysVar set with (x,123456)`,
			`Needs Reserved Conn`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id",
				"int64",
			),
			"123456",
		)},
	}, {
		testName: "sql_mode no change - same",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "sql_mode",
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:     "'STRICT_TRANS_TABLES,NO_ZERO_DATE'",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'STRICT_TRANS_TABLES,NO_ZERO_DATE' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES,NO_ZERO_DATE|STRICT_TRANS_TABLES,NO_ZERO_DATE",
		)},
	}, {
		testName: "sql_mode no change - jumbled orig",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "sql_mode",
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:     "'STRICT_TRANS_TABLES,NO_ZERO_DATE'",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'STRICT_TRANS_TABLES,NO_ZERO_DATE' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"NO_ZERO_DATE,STRICT_TRANS_TABLES|STRICT_TRANS_TABLES,NO_ZERO_DATE",
		)},
	}, {
		testName: "sql_mode no change - jumbled new",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "sql_mode",
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:     "'NO_ZERO_DATE,STRICT_TRANS_TABLES'",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'NO_ZERO_DATE,STRICT_TRANS_TABLES' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES,NO_ZERO_DATE|NO_ZERO_DATE,STRICT_TRANS_TABLES",
		)},
	}, {
		testName: "sql_mode no change - same mixed case",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "sql_mode",
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:     "'no_zero_date,STRICT_TRANS_TABLES'",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'no_zero_date,STRICT_TRANS_TABLES' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES,NO_ZERO_DATE|no_zero_date,STRICT_TRANS_TABLES",
		)},
	}, {
		testName: "sql_mode no change - same multiple",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'no_zero_date,STRICT_TRANS_TABLES,strict_trans_tables,no_zero_date,NO_ZERO_DATE,STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'no_zero_date,STRICT_TRANS_TABLES,strict_trans_tables,no_zero_date,NO_ZERO_DATE,STRICT_TRANS_TABLES' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES,NO_ZERO_DATE|no_zero_date,STRICT_TRANS_TABLES,strict_trans_tables,no_zero_date,NO_ZERO_DATE,STRICT_TRANS_TABLES",
		)},
	}, {
		testName:     "sql_mode change - changed additional - MySQL57",
		mysqlVersion: "5.7.9",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'no_zero_date,STRICT_TRANS_TABLES,strict_trans_tables,NO_ZERO_IN_DATE'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'no_zero_date,STRICT_TRANS_TABLES,strict_trans_tables,NO_ZERO_IN_DATE' new {} false false`,
			"SysVar set with (sql_mode,'no_zero_date,STRICT_TRANS_TABLES,strict_trans_tables,NO_ZERO_IN_DATE')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES,NO_ZERO_DATE|no_zero_date,STRICT_TRANS_TABLES,strict_trans_tables,NO_ZERO_IN_DATE",
		)},
	}, {
		testName:     "sql_mode change - changed less - MySQL57",
		mysqlVersion: "5.7.9",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'no_zero_date,NO_ZERO_DATE'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'no_zero_date,NO_ZERO_DATE' new {} false false`,
			"SysVar set with (sql_mode,'no_zero_date,NO_ZERO_DATE')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES,NO_ZERO_DATE|no_zero_date,NO_ZERO_DATE",
		)},
	}, {
		testName: "sql_mode no change - empty list",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "''",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, '' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|",
		)},
	}, {
		testName:     "sql_mode change - empty orig - MySQL57",
		mysqlVersion: "5.7.9",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'STRICT_TRANS_TABLES' new {} false false`,
			"SysVar set with (sql_mode,'STRICT_TRANS_TABLES')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|STRICT_TRANS_TABLES",
		)},
	}, {
		testName: "sql_mode change - empty new",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "''",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, '' new {} false false`,
			"SysVar set with (sql_mode,'')",
			"SET_VAR can be used",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|",
		)},
	}, {
		testName:     "sql_mode change - empty orig - MySQL80",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'STRICT_TRANS_TABLES' new {} false false`,
			"SysVar set with (sql_mode,'STRICT_TRANS_TABLES')",
			"SET_VAR can be used",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|STRICT_TRANS_TABLES",
		)},
	}, {
		testName:     "sql_mode change to empty - non empty orig - MySQL80 - set_var allowed",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "''",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, '' new {} false false`,
			"SysVar set with (sql_mode,'')",
			"SET_VAR can be used",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|",
		)},
	}, {
		testName:     "sql_mode change - empty orig - MySQL80 - SET_VAR disabled",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'STRICT_TRANS_TABLES' new {} false false`,
			"SysVar set with (sql_mode,'STRICT_TRANS_TABLES')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|STRICT_TRANS_TABLES",
		)},
		disableSetVar: true,
	}, {
		// on the reserved-connection path the SET carries the judged value rather than
		// the expression, so the shard applies exactly what the session stores
		testName:     "sql_mode applies the judged value to the shard sessions, not the expression",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "concat('STRICT_TRANS', '_TABLES')",
				SupportSetVar: true,
			},
		},
		shardSession: []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: "ks", Shard: "-20"}}},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('STRICT_TRANS', '_TABLES') new {} false false`,
			"SysVar set with (sql_mode,'STRICT_TRANS_TABLES')",
			"Needs Reserved Conn",
			`ExecuteMultiShard ks.-20: set sql_mode = 'STRICT_TRANS_TABLES' {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|STRICT_TRANS_TABLES",
		)},
		disableSetVar: true,
	}, {
		testName:     "sql_mode set a parse-relevant mode",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'REAL_AS_FLOAT'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'REAL_AS_FLOAT' new {} false false`,
			"SysVar set with (sql_mode,'REAL_AS_FLOAT')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|REAL_AS_FLOAT",
		)},
		disableSetVar: true,
	}, {
		testName:     "sql_mode set a parse-relevant mode the backend already runs with",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'REAL_AS_FLOAT,STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		// the assignment does not change the value, so nothing is stored
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'REAL_AS_FLOAT,STRICT_TRANS_TABLES' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"REAL_AS_FLOAT,STRICT_TRANS_TABLES|REAL_AS_FLOAT,STRICT_TRANS_TABLES",
		)},
	}, {
		testName:     "sql_mode set to a numeric bitmask",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "1048576",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 1048576 new {} false false`,
			"SysVar set with (sql_mode,1048576)",
			"SET_VAR can be used",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|int64"),
			"|1048576",
		)},
	}, {
		testName:     "sql_mode set to the ANSI combination mode",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'ansi'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'ansi' new {} false false`,
			"SysVar set with (sql_mode,'ansi')",
			"SET_VAR can be used",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|ansi",
		)},
	}, {
		testName:     "sql_mode set to IGNORE_SPACE",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'IGNORE_SPACE'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'IGNORE_SPACE' new {} false false`,
			"SysVar set with (sql_mode,'IGNORE_SPACE')",
			"SET_VAR can be used",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|IGNORE_SPACE",
		)},
	}, {
		testName:     "sql_mode set to an unknown mode name",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'BOGUS'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'BOGUS' new {} false false`,
		},
		expectedError: "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|BOGUS",
		)},
	}, {
		testName:     "sql_mode verification result with an unexpected shape fails",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'STRICT_TRANS_TABLES' new {} false false`,
		},
		expectedError: "unexpected result reading sql_mode: 1 fields, 1 rows",
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig", "varchar"),
			"whatever",
		)},
	}, {
		testName:     "sql_mode verification result with no rows fails",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'STRICT_TRANS_TABLES' new {} false false`,
		},
		expectedError: "unexpected result reading sql_mode: 2 fields, 0 rows",
		qr:            []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"))},
	}, {
		testName:     "sql_mode verification result with several rows fails",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'STRICT_TRANS_TABLES' new {} false false`,
		},
		expectedError: "unexpected result reading sql_mode: 2 fields, 2 rows",
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|STRICT_TRANS_TABLES",
			"|STRICT_TRANS_TABLES",
		)},
	}, {
		testName:     "sql_mode set to a removed mode bit",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "256",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 256 new {} false false`,
		},
		expectedError: "sql_mode=0x00000100 is not supported.",
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|int64"),
			"|256",
		)},
	}, {
		testName:     "default_week_format change - empty orig - MySQL80",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "default_week_format",
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:     "'a'",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select 'a' from dual where @@default_week_format != 'a' {} false false`,
			"SysVar set with (default_week_format,'a')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("new", "varchar"),
			"a",
		)},
	}}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			if tc.input == nil {
				tc.input = &SingleRow{}
			}

			set := &Set{
				Ops:   tc.setOps,
				Input: tc.input,
			}
			parser, err := sqlparser.New(sqlparser.Options{
				MySQLServerVersion: tc.mysqlVersion,
			})
			require.NoError(t, err)
			vc := &loggingVCursor{
				shards:         []string{"-20", "20-"},
				results:        tc.qr,
				multiShardErrs: []error{tc.execErr},
				disableSetVar:  tc.disableSetVar,
				parser:         parser,
				shardSession:   tc.shardSession,
			}
			_, err = set.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
			if tc.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}

			vc.ExpectLog(t, tc.expectedQueryLog)
			vc.ExpectWarnings(t, tc.expectedWarning)
		})
	}
}

func TestSysVarSetErr(t *testing.T) {
	setOps := []SetOp{
		&SysVarReservedConn{
			Name: "x",
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: true,
			},
			TargetDestination: key.DestinationAnyShard{},
			Expr:              "dummy_expr",
		},
	}

	// the failed SET must not leave its value in the session: no "SysVar set with"
	expectedQueryLog := []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		"Needs Reserved Conn",
		`ExecuteMultiShard ks.-20: set x = dummy_expr {} false false`,
	}

	set := &Set{
		Ops:   setOps,
		Input: &SingleRow{},
	}
	vc := &loggingVCursor{
		shards:         []string{"-20", "20-"},
		multiShardErrs: []error{errors.New("error")},
	}
	_, err := set.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, "error")
	vc.ExpectLog(t, expectedQueryLog)
}
