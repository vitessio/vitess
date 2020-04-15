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
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestSetTable(t *testing.T) {
	type testCase struct {
		testName         string
		setOps           []SetOp
		qr               []*sqltypes.Result
		expectedQueryLog []string
		expectedWarning  []*querypb.QueryWarning
		expectedError    string
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
					PlanValue: sqltypes.PlanValue{
						Value: sqltypes.NewInt64(42),
					},
				},
			},
			expectedQueryLog: []string{
				`UDV set with (x,INT64(42))`,
			},
		},
		{
			testName: "sysvar ignore",
			setOps: []SetOp{
				&SysVarIgnore{
					Name: "x",
					PlanValue: sqltypes.PlanValue{
						Value: sqltypes.NewInt64(42),
					},
				},
			},
			expectedWarning: []*querypb.QueryWarning{
				{Code: 1235, Message: "Ignored inapplicable SET x = INT64(42)"},
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
					CheckSysVarQuery:  "dummy_query",
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
				`ExecuteMultiShard ks.-20: dummy_query {} false false`,
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
					CheckSysVarQuery:  "dummy_query",
				},
			},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
				`ExecuteMultiShard ks.-20: dummy_query {} false false`,
			},
			expectedError: "Modification not allowed using set construct for: x",
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
					CheckSysVarQuery:  "dummy_query",
				},
			},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			},
			expectedError: "Unexpected error, DestinationKeyspaceID mapping to multiple shards: DestinationAllShards()",
		},
		{
			testName: "udv_ignr_chignr",
			setOps: []SetOp{
				&UserDefinedVariable{
					Name: "x",
					PlanValue: sqltypes.PlanValue{
						Value: sqltypes.NewInt64(1),
					},
				},
				&SysVarIgnore{
					Name: "y",
					PlanValue: sqltypes.PlanValue{
						Value: sqltypes.NewInt64(2),
					},
				},
				&SysVarCheckAndIgnore{
					Name: "z",
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
					TargetDestination: key.DestinationAnyShard{},
					CheckSysVarQuery:  "dummy_query",
				},
			},
			expectedQueryLog: []string{
				`UDV set with (x,INT64(1))`,
				`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
				`ExecuteMultiShard ks.-20: dummy_query {} false false`,
			},
			expectedWarning: []*querypb.QueryWarning{
				{Code: 1235, Message: "Ignored inapplicable SET y = INT64(2)"},
			},
			qr: []*sqltypes.Result{sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id",
					"int64",
				),
				"1",
			)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			set := &Set{
				Ops: tc.setOps,
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
