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
	"fmt"
	"testing"

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
	}
	_, err := set.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		"ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)",
		"ExecuteMultiShard ks.-20: select dummy_expr from dual where @@x != dummy_expr {} false false",
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
	}

	tests := []testCase{
		{
			testName:         "nil set ops",
			expectedQueryLog: []string{},
		},
		{
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
		},
		{
			testName: "udv with input",
			setOps: []SetOp{
				&UserDefinedVariable{
					Name: "x",
					Expr: evalengine.NewColumn(0),
				},
			},
			qr: []*sqltypes.Result{sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col0",
					"datetime",
				),
				"2020-10-28",
			)},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
				`ExecuteMultiShard ks.-20: select now() from dual {} false false`,
				`UDV set with (x,DATETIME("2020-10-28"))`,
			},
			input: &Send{
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				TargetDestination: key.DestinationAnyShard{},
				Query:             "select now() from dual",
				SingleShardOnly:   true,
			},
		},
		{
			testName: "sysvar ignore",
			setOps: []SetOp{
				&SysVarIgnore{
					Name: "x",
					Expr: "42",
				},
			},
		},
		{
			testName: "sysvar check and ignore",
			setOps: []SetOp{
				&SysVarCheckAndIgnore{
					Name: "x",
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
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
		},
		{
			testName: "sysvar check and error",
			setOps: []SetOp{
				&SysVarCheckAndIgnore{
					Name: "x",
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
					TargetDestination: key.DestinationAnyShard{},
					Expr:              "dummy_expr",
				},
			},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
				`ExecuteMultiShard ks.-20: select 1 from dual where @@x = dummy_expr {} false false`,
			},
		},
		{
			testName: "sysvar checkAndIgnore multi destination error",
			setOps: []SetOp{
				&SysVarCheckAndIgnore{
					Name: "x",
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
					TargetDestination: key.DestinationAllShards{},
					Expr:              "dummy_expr",
				},
			},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			},
			expectedError: "Unexpected error, DestinationKeyspaceID mapping to multiple shards: DestinationAllShards()",
		},
		{
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
					Name: "z",
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
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
		},
		{
			testName: "sysvar set without destination",
			setOps: []SetOp{
				&SysVarReservedConn{
					Name: "x",
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
					TargetDestination: key.DestinationAnyShard{},
					Expr:              "dummy_expr",
				},
			},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
				`ExecuteMultiShard ks.-20: set @@x = dummy_expr {} false false`,
			},
		},
		{
			testName: "sysvar set not modifying setting",
			setOps: []SetOp{
				&SysVarReservedConn{
					Name: "x",
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
					Expr: "dummy_expr",
				},
			},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
				`ExecuteMultiShard ks.-20: select dummy_expr from dual where @@x != dummy_expr {} false false`,
			},
		},
		{
			testName: "sysvar set modifying setting",
			setOps: []SetOp{
				&SysVarReservedConn{
					Name: "x",
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
					Expr: "dummy_expr",
				},
			},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(00)`,
				`ExecuteMultiShard ks.-20: select dummy_expr from dual where @@x != dummy_expr {} false false`,
				`SysVar set with (x,123456)`,
			},
			qr: []*sqltypes.Result{sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id",
					"int64",
				),
				"123456",
			)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			if tc.input == nil {
				tc.input = &SingleRow{}
			}
			set := &Set{
				Ops:   tc.setOps,
				Input: tc.input,
			}
			vc := &loggingVCursor{
				shards:  []string{"-20", "20-"},
				results: tc.qr,
			}
			_, err := set.Execute(vc, map[string]*querypb.BindVariable{}, false)
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
		`ExecuteMultiShard ks.-20: set @@x = dummy_expr {} false false`,
	}

	set := &Set{
		Ops:   setOps,
		Input: &SingleRow{},
	}
	vc := &loggingVCursor{
		shards:         []string{"-20", "20-"},
		multiShardErrs: []error{fmt.Errorf("error")},
	}
	_, err := set.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, "error")
	vc.ExpectLog(t, expectedQueryLog)
}
