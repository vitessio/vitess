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
	"context"
	"errors"
	"fmt"
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
	_, err := set.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
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
		// the session replays the stored text as a connection setting, so the
		// value is stored, not the expression
		testName: "targeted set evaluates the expression and stores the value",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "dummy_expr",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"dummy_expr",
				"int64",
			),
			"123456",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`Needs Reserved Conn`,
<<<<<<< HEAD
			`SysVar set with (x,dummy_expr)`,
			`ExecuteMultiShard ks.-20: set x = dummy_expr {} false false`,
||||||| parent of bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
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
=======
			`ExecuteMultiShard ks.-20: select dummy_expr from dual {} false false`,
			`ExecuteMultiShard ks.-20: set x = 123456 {} false false`,
			`SysVar set with (x,123456)`,
		},
	}, {
		// the expression is evaluated on the reserved connection the SET is then
		// applied to: the tablet refuses a lock function outside a reserved
		// connection, and the lock get_lock() takes must be held by the
		// connection the session keeps, as it was when the SET carried the
		// expression itself
		testName: "targeted set evaluates a lock function on the reserved connection",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "get_lock('x', 0)",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"get_lock('x', 0)",
				"int64",
			),
			"1",
		)},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.-20: select get_lock('x', 0) from dual {} false false`,
			`ExecuteMultiShard ks.-20: set x = 1 {} false false`,
			`SysVar set with (x,1)`,
		},
	}, {
		// a targeted SET whose evaluation fails must not leave its value in the
		// session, where the settings transport would replay it on every
		// subsequent query (TestSysVarSetErr covers the SET itself failing)
		testName: "targeted set evaluation failure does not store the value",
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
			`ExecuteMultiShard ks.-20: select dummy_expr from dual {} false false`,
			`Reset Reserved Conn`,
		},
	}, {
		// the same failure on a session that already holds a shard session,
		// as after a reservation the tablet made before refusing the query,
		// keeps the mark: the reservation is recorded in the session
		testName: "targeted set evaluation failure keeps the mark of a session with a shard session",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "dummy_expr",
			},
		},
		shardSession:  []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: "ks", Shard: "-20"}}},
		execErr:       errors.New("some random error"),
		expectedError: "some random error",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.-20: select dummy_expr from dual {} false false`,
		},
	}, {
		// the evaluation returns one value; anything else cannot be applied or
		// stored and fails rather than pass the expression through
		testName: "targeted set rejects an evaluation that is not a single value",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:              "x",
				Keyspace:          ks,
				TargetDestination: key.DestinationAnyShard{},
				Expr:              "dummy_expr",
			},
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"dummy_expr",
				"int64",
			),
			"1",
			"2",
		)},
		expectedError: "unexpected result evaluating x: 2 rows",
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.-20: select dummy_expr from dual {} false false`,
			`Reset Reserved Conn`,
>>>>>>> bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
		},
	}, {
<<<<<<< HEAD
||||||| parent of bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
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
=======
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
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('AN', 'SI') new {} false false`,
			`Reset Reserved Conn`,
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
			`Needs Reserved Conn`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, concat('STRICT_TRANS', '_TABLES') new {} false false`,
			`ExecuteMultiShard ks.-20: set sql_mode = 'STRICT_TRANS_TABLES' {} false false`,
			`SysVar set with (sql_mode,'STRICT_TRANS_TABLES')`,
		},
	}, {
>>>>>>> bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
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
				Expr:     "'a,b'",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'a,b' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"a,b|a,b",
		)},
	}, {
		testName: "sql_mode no change - jumbled orig",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "sql_mode",
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:     "'a,b'",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'a,b' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"b,a|a,b",
		)},
	}, {
		testName: "sql_mode no change - jumbled new",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "sql_mode",
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:     "'b,a'",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'b,a' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"a,b|b,a",
		)},
	}, {
		testName: "sql_mode no change - same mixed case",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:     "sql_mode",
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:     "'B,a'",
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'B,a' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"a,b|B,a",
		)},
	}, {
		testName: "sql_mode no change - same multiple",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'B,a,A,B,b,a'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'B,a,A,B,b,a' new {} false false`,
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"a,b|B,a,A,B,b,a",
		)},
	}, {
		testName:     "sql_mode change - changed additional - MySQL57",
		mysqlVersion: "5.7.9",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'B,a,A,B,b,a,c'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'B,a,A,B,b,a,c' new {} false false`,
			"SysVar set with (sql_mode,'B,a,A,B,b,a,c')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"a,b|B,a,A,B,b,a,c",
		)},
	}, {
		testName:     "sql_mode change - changed less - MySQL57",
		mysqlVersion: "5.7.9",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'B,b,B,b'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'B,b,B,b' new {} false false`,
			"SysVar set with (sql_mode,'B,b,B,b')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"a,b|B,b,B,b",
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
				Expr:          "'a'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'a' new {} false false`,
			"SysVar set with (sql_mode,'a')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|a",
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
			"a|",
		)},
	}, {
		testName:     "sql_mode change - empty orig - MySQL80",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'a'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'a' new {} false false`,
			"SysVar set with (sql_mode,'a')",
			"SET_VAR can be used",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|a",
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
			"a|",
		)},
	}, {
		testName:     "sql_mode change - empty orig - MySQL80 - SET_VAR disabled",
		mysqlVersion: "8.0.0",
		setOps: []SetOp{
			&SysVarReservedConn{
				Name:          "sql_mode",
				Keyspace:      &vindexes.Keyspace{Name: "ks", Sharded: true},
				Expr:          "'a'",
				SupportSetVar: true,
			},
		},
		expectedQueryLog: []string{
			`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
			`ExecuteMultiShard ks.-20: select @@sql_mode orig, 'a' new {} false false`,
			"SysVar set with (sql_mode,'a')",
			"Needs Reserved Conn",
		},
		qr: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("orig|new", "varchar|varchar"),
			"|a",
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
			}
			_, err = set.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
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

<<<<<<< HEAD
||||||| parent of bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
	// the failed SET must not leave its value in the session: no "SysVar set with"
=======
	// the evaluation succeeds and the SET itself fails: it must not leave its
	// value in the session, so no "SysVar set with"
>>>>>>> bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
	expectedQueryLog := []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		"Needs Reserved Conn",
<<<<<<< HEAD
		"SysVar set with (x,dummy_expr)",
		`ExecuteMultiShard ks.-20: set x = dummy_expr {} false false`,
||||||| parent of bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
		`ExecuteMultiShard ks.-20: set x = dummy_expr {} false false`,
=======
		`ExecuteMultiShard ks.-20: select dummy_expr from dual {} false false`,
		`ExecuteMultiShard ks.-20: set x = 123456 {} false false`,
>>>>>>> bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
	}

	set := &Set{
		Ops:   setOps,
		Input: &SingleRow{},
	}
	vc := &loggingVCursor{
<<<<<<< HEAD
		shards:         []string{"-20", "20-"},
		multiShardErrs: []error{fmt.Errorf("error")},
||||||| parent of bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
		shards:         []string{"-20", "20-"},
		multiShardErrs: []error{errors.New("error")},
=======
		shards: []string{"-20", "20-"},
		// the first result answers the evaluation; the nil entry makes the SET
		// return resultErr
		results:   []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("dummy_expr", "int64"), "123456"), nil},
		resultErr: errors.New("error"),
>>>>>>> bbcfdb17ba (VTTablet: check the reads embedded in CREATE TABLE ... AS SELECT, EXPLAIN ANALYZE, SHOW ... WHERE and SET under table ACL (#21139))
	}
	_, err := set.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, "error")
	vc.ExpectLog(t, expectedQueryLog)
}
