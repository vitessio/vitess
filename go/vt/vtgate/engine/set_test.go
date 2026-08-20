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

	"vitess.io/vitess/go/mysql/collations"
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

func TestSetSQLModeAppliedToOpenShardSessions(t *testing.T) {
	set := &Set{
		Ops: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("STRICT_ALL_TABLES"), collations.SystemCollation)},
		},
		Input: &SingleRow{},
	}
	vc := &loggingVCursor{
		shards:        []string{"-20", "20-"},
		shardSession:  []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: "ks", Shard: "-20"}}},
		disableSetVar: true,
	}
	_, err := set.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		"SysVar set with (sql_mode,'STRICT_ALL_TABLES')",
		"Needs Reserved Conn",
		"ExecuteMultiShard ks.-20: set sql_mode = 'STRICT_ALL_TABLES' {} false false",
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
			`SysVar set with (x,dummy_expr)`,
			`ExecuteMultiShard ks.-20: set x = dummy_expr {} false false`,
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
		testName:     "sql_mode set to a new value - SET_VAR",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("strict_trans_tables"), collations.SystemCollation)},
		},
		expectedQueryLog: []string{
			`SysVar set with (sql_mode,'STRICT_TRANS_TABLES')`,
			`SET_VAR can be used`,
		},
	}, {
		testName:     "sql_mode set to the session's current value is re-stored canonically",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			// the session default, jumbled and lowercased
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("no_engine_substitution,only_full_group_by,strict_trans_tables,no_zero_in_date,no_zero_date,error_for_division_by_zero"), collations.SystemCollation)},
		},
		expectedQueryLog: []string{
			`SysVar set with (sql_mode,'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION')`,
			`SET_VAR can be used`,
		},
	}, {
		testName:      "sql_mode set to the current value still converges open shard sessions when SET_VAR is unavailable",
		mysqlVersion:  "8.0.0",
		disableSetVar: true,
		shardSession:  []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: "ks", Shard: "-20"}}},
		setOps: []SetOp{
			// the session default: an unchanged value must still be sent, because a client
			// retrying a partially-failed SET needs the statement to reach the shard
			// sessions the previous attempt did not update
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("no_engine_substitution,only_full_group_by,strict_trans_tables,no_zero_in_date,no_zero_date,error_for_division_by_zero"), collations.SystemCollation)},
		},
		expectedQueryLog: []string{
			`SysVar set with (sql_mode,'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION')`,
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.-20: set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' {} false false`,
		},
	}, {
		testName:     "sql_mode set to a numeric value stores the canonical names",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralInt(1<<21 | 1<<24)},
		},
		expectedQueryLog: []string{
			`SysVar set with (sql_mode,'STRICT_TRANS_TABLES,NO_ZERO_DATE')`,
			`SET_VAR can be used`,
		},
	}, {
		testName:     "sql_mode combination mode is stored expanded",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("TRADITIONAL"), collations.SystemCollation)},
		},
		expectedQueryLog: []string{
			`SysVar set with (sql_mode,'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_ENGINE_SUBSTITUTION')`,
			`SET_VAR can be used`,
		},
	}, {
		testName:     "sql_mode needs a reserved connection when SET_VAR is unavailable",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("STRICT_ALL_TABLES"), collations.SystemCollation)},
		},
		expectedQueryLog: []string{
			`SysVar set with (sql_mode,'STRICT_ALL_TABLES')`,
			`Needs Reserved Conn`,
		},
		disableSetVar: true,
	}, {
		testName:     "sql_mode set from a backend variable fetched through the input",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewColumn(0, evalengine.Type{}, nil)},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("orig", "varchar"),
			"STRICT_TRANS_TABLES,NO_ZERO_DATE",
		)},
		input: &Send{
			Keyspace:          ks,
			TargetDestination: key.DestinationAnyShard{},
			Query:             "select @modes from dual",
			SingleShardOnly:   true,
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.-20: select @modes from dual {} false false`,
			`SysVar set with (sql_mode,'STRICT_TRANS_TABLES,NO_ZERO_DATE')`,
			`SET_VAR can be used`,
		},
	}, {
		testName:     "sql_mode set to an unsupported mode",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("NO_BACKSLASH_ESCAPES"), collations.SystemCollation)},
		},
		expectedQueryLog: []string{},
		expectedError:    "setting the NO_BACKSLASH_ESCAPES sql_mode is unsupported",
	}, {
		testName:     "sql_mode set to the ANSI combination mode bit",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralInt(1 << 18)},
		},
		expectedQueryLog: []string{},
		expectedError:    "setting the ANSI sql_mode is unsupported",
	}, {
		testName:     "sql_mode set to IGNORE_SPACE is unsupported",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("IGNORE_SPACE"), collations.SystemCollation)},
		},
		expectedQueryLog: []string{},
		expectedError:    "setting the IGNORE_SPACE sql_mode is unsupported",
	}, {
		testName:     "sql_mode set to HIGH_NOT_PRECEDENCE is unsupported",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("HIGH_NOT_PRECEDENCE"), collations.SystemCollation)},
		},
		expectedQueryLog: []string{},
		expectedError:    "setting the HIGH_NOT_PRECEDENCE sql_mode is unsupported",
	}, {
		testName:     "sql_mode set to an unknown mode name",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralString([]byte("BOGUS"), collations.SystemCollation)},
		},
		expectedQueryLog: []string{},
		expectedError:    "Variable 'sql_mode' can't be set to the value of 'BOGUS'",
	}, {
		testName:     "sql_mode set to a removed mode bit",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarSQLMode{Expr: evalengine.NewLiteralInt(256)},
		},
		expectedQueryLog: []string{},
		expectedError:    "sql_mode=0x00000100 is not supported.",
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
				shardSession:   tc.shardSession,
				parser:         parser,
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

	expectedQueryLog := []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		"Needs Reserved Conn",
		"SysVar set with (x,dummy_expr)",
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
