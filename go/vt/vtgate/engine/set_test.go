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

	"vitess.io/vitess/go/mysql/sqlmode"
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
		"Needs Reserved Conn",
		"ExecuteMultiShard ks.-20: set x = dummy_expr {} false false",
		"SysVar set with (x,'foobar')",
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
		storedSQLMode    sqlmode.Mode
		hasStoredSQLMode bool
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
		// unsupported sql_mode is still an error: constants are rejected at plan time,
		// and a non-constant value is judged here once evaluated
		testName: "sysvar check and ignore rejects an unsupported non-constant sql_mode",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "concat('AN', 'SI')",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"orig|new",
				"varchar|varchar",
			),
			"STRICT_TRANS_TABLES|ANSI",
		)},
		expectedError: "setting the ANSI sql_mode is unsupported",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('AN', 'SI') new {} false false`,
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
				Expr:              "concat('AN', 'SI')",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"orig|new",
				"varchar|varchar",
			),
			"STRICT_TRANS_TABLES|ANSI",
		)},
		expectedError: "setting the ANSI sql_mode is unsupported",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('AN', 'SI') new {} false false`,
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
			`Needs Reserved Conn`,
			`SysVar set with (x,123456)`,
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
			"Needs Reserved Conn",
			"SysVar set with (sql_mode,'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE')",
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
			"Needs Reserved Conn",
			"SysVar set with (sql_mode,'NO_ZERO_DATE')",
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
			"Needs Reserved Conn",
			"SysVar set with (sql_mode,'STRICT_TRANS_TABLES')",
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
			"SET_VAR can be used",
			"SysVar set with (sql_mode,'')",
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
			"SET_VAR can be used",
			"SysVar set with (sql_mode,'STRICT_TRANS_TABLES')",
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
			"SET_VAR can be used",
			"SysVar set with (sql_mode,'')",
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
			"Needs Reserved Conn",
			"SysVar set with (sql_mode,'STRICT_TRANS_TABLES')",
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
			"Needs Reserved Conn",
			`ExecuteMultiShard ks.-20: set sql_mode = 'STRICT_TRANS_TABLES' {} false false`,
			"SysVar set with (sql_mode,'STRICT_TRANS_TABLES')",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|STRICT_TRANS_TABLES",
		)},
		disableSetVar: true,
	}, {
		testName:     "sql_mode set an unsupported mode",
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
		},
		expectedError: "setting the REAL_AS_FLOAT sql_mode is unsupported",
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|REAL_AS_FLOAT",
		)},
		disableSetVar: true,
	}, {
		testName:     "sql_mode set an unsupported mode the backend already runs with",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'REAL_AS_FLOAT,STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'REAL_AS_FLOAT,STRICT_TRANS_TABLES' new {} false false`,
		},
		// the assignment would not change the value, but it is rejected regardless:
		// such sessions were never parsed correctly by the vtgate
		expectedError: "setting the REAL_AS_FLOAT sql_mode is unsupported",
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"REAL_AS_FLOAT,STRICT_TRANS_TABLES|REAL_AS_FLOAT,STRICT_TRANS_TABLES",
		)},
	}, {
		testName:     "sql_mode set to a numeric bitmask decodes to mode names",
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
		},
		expectedError: "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported",
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
		},
		expectedError: "setting the ANSI sql_mode is unsupported",
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|ansi",
		)},
	}, {
		testName:     "sql_mode set to PIPES_AS_CONCAT, which vtgate's parser honors",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'pipes_as_concat,STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'pipes_as_concat,STRICT_TRANS_TABLES' new {} false false`,
			"SET_VAR can be used",
			"SysVar set with (sql_mode,'PIPES_AS_CONCAT,STRICT_TRANS_TABLES')",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|pipes_as_concat,STRICT_TRANS_TABLES",
		)},
	}, {
		// a targeted session's SET sends the shard the judged value as it is, the
		// honored lexer mode included: the tablet parses under it
		testName:     "sql_mode set to PIPES_AS_CONCAT on a targeted session",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:              "sql_mode",
				Keyspace:          &vindexes.Keyspace{Name: "ks", Sharded: true},
				TargetDestination: key.DestinationShard("-20"),
				Expr:              "'pipes_as_concat'",
				SupportSetVar:     true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationShard(-20)`,
			`ExecuteMultiShard ks.DestinationShard(-20): select @@sql_mode orig, 'pipes_as_concat' new {} false false`,
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.DestinationShard(-20): set sql_mode = 'PIPES_AS_CONCAT' {} false false`,
			"SysVar set with (sql_mode,'PIPES_AS_CONCAT')",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|pipes_as_concat",
		)},
	}, {
		// open shard sessions of a reserved-connection session receive the judged
		// value as it is too
		testName:     "sql_mode set to PIPES_AS_CONCAT with open shard sessions",
		mysqlVersion: "5.7.9",
		shardSession: []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: "ks", Shard: "-20"}}},
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'PIPES_AS_CONCAT,STRICT_TRANS_TABLES'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'PIPES_AS_CONCAT,STRICT_TRANS_TABLES' new {} false false`,
			"Needs Reserved Conn",
			`ExecuteMultiShard ks.-20: set sql_mode = 'PIPES_AS_CONCAT,STRICT_TRANS_TABLES' {} false false`,
			"SysVar set with (sql_mode,'PIPES_AS_CONCAT,STRICT_TRANS_TABLES')",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|PIPES_AS_CONCAT,STRICT_TRANS_TABLES",
		)},
	}, {
		// a session that stores a sql_mode is judged against it, not against the
		// shard the judgment ran on: a pooled connection carries the backend's default
		testName:         "sql_mode reset from the session's own mode to the shard's",
		mysqlVersion:     "8.0.0",
		storedSQLMode:    sqlmode.PipesAsConcat,
		hasStoredSQLMode: true,
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
			"SET_VAR can be used",
			"SysVar set with (sql_mode,'')",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|",
		)},
	}, {
		// no change: nothing is sent, but the session's stored value is set to the
		// canonical form, which canonicalizes a value stored as an older vtgate spelled it
		testName:         "sql_mode set to the session's own mode is no change",
		mysqlVersion:     "8.0.0",
		storedSQLMode:    sqlmode.PipesAsConcat,
		hasStoredSQLMode: true,
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'pipes_as_concat'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'pipes_as_concat' new {} false false`,
			"SysVar set with (sql_mode,'PIPES_AS_CONCAT')",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|pipes_as_concat",
		)},
	}, {
		// with system settings disabled the assignment stores nothing, so the session
		// would not be parsed under the mode either: it is rejected, as before
		testName: "sysvar check and ignore rejects a lexer mode the parser honors",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "concat('PIPES_AS', '_CONCAT')",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|PIPES_AS_CONCAT",
		)},
		expectedError: "setting the PIPES_AS_CONCAT sql_mode is unsupported",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('PIPES_AS', '_CONCAT') new {} false false`,
		},
	}, {
		// unless the session already stores that mode: re-assigning it is a no-op
		testName:         "sysvar check and ignore accepts the session's own lexer mode",
		storedSQLMode:    sqlmode.PipesAsConcat,
		hasStoredSQLMode: true,
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "'PIPES_AS_CONCAT'",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|PIPES_AS_CONCAT",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'PIPES_AS_CONCAT' new {} false false`,
		},
	}, {
		// a change of the runtime modes alongside the stored lexer mode is ignored
		// like any other ignored assignment
		testName:         "sysvar check and ignore ignores a runtime change beside the session's lexer mode",
		storedSQLMode:    sqlmode.PipesAsConcat,
		hasStoredSQLMode: true,
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "'PIPES_AS_CONCAT,NO_ZERO_DATE'",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|PIPES_AS_CONCAT,NO_ZERO_DATE",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'PIPES_AS_CONCAT,NO_ZERO_DATE' new {} false false`,
		},
	}, {
		// an assignment that would take the session out of its stored lexer mode is
		// rejected: ignored, it would leave the session parsed under the mode
		testName:         "sysvar check and ignore rejects leaving the session's lexer mode",
		storedSQLMode:    sqlmode.PipesAsConcat,
		hasStoredSQLMode: true,
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "''",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|",
		)},
		expectedError: "changing the session's sql_mode from PIPES_AS_CONCAT is unsupported while system settings are disabled",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, '' new {} false false`,
		},
	}, {
		// a global assignment does not change the session: it is validated and
		// ignored, whatever lexer mode the session stores
		testName:         "sysvar check and ignore validates and ignores a global sql_mode assignment",
		storedSQLMode:    sqlmode.PipesAsConcat,
		hasStoredSQLMode: true,
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "'NO_ZERO_DATE'",
				Global:            true,
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|NO_ZERO_DATE",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'NO_ZERO_DATE' new {} false false`,
		},
	}, {
		testName: "sysvar check and ignore ignores a global assignment of a lexer mode the parser honors",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "'PIPES_AS_CONCAT'",
				Global:            true,
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|PIPES_AS_CONCAT",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'PIPES_AS_CONCAT' new {} false false`,
		},
	}, {
		testName: "sysvar check and ignore rejects a global assignment of an unsupported mode",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "'ANSI_QUOTES'",
				Global:            true,
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"STRICT_TRANS_TABLES|ANSI_QUOTES",
		)},
		expectedError: "setting the ANSI_QUOTES sql_mode is unsupported",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'ANSI_QUOTES' new {} false false`,
		},
	}, {
		// a judgment that cannot run cannot pass: unlike another ignored variable, an
		// unjudged sql_mode assignment could put the client under a lexer mode the
		// session does not run under
		testName: "sysvar check and ignore fails when the sql_mode judgment fails",
		setOps: []SetOp{
			&SysVarCheckAndIgnore{
				Name:              "sql_mode",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "'PIPES_AS_CONCAT'",
			},
		},
		execErr:       errors.New("some random error"),
		expectedError: "unable to judge the sql_mode assignment: some random error",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'PIPES_AS_CONCAT' new {} false false`,
		},
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
		},
		expectedError: "setting the IGNORE_SPACE sql_mode is unsupported",
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
			"Needs Reserved Conn",
			"SysVar set with (default_week_format,'a')",
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

				storedSQLMode:    tc.storedSQLMode,
				hasStoredSQLMode: tc.hasStoredSQLMode,
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
