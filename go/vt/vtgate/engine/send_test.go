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

	"vitess.io/vitess/go/sqltypes"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestSendTable(t *testing.T) {
	type testCase struct {
		testName         string
		sharded          bool
		shards           []string
		destination      key.Destination
		expectedQueryLog []string
		expectedError    string
		isDML            bool
		singleShardOnly  bool
	}

	singleShard := []string{"0"}
	twoShards := []string{"-20", "20-"}
	tests := []testCase{
		{
			testName:    "unsharded with no autocommit",
			sharded:     false,
			shards:      singleShard,
			destination: key.DestinationAllShards{},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
				`ExecuteMultiShard ks.0: dummy_query {} false false`,
			},
			isDML: false,
		},
		{
			testName:    "sharded with no autocommit",
			sharded:     true,
			shards:      twoShards,
			destination: key.DestinationShard("20-"),
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationShard(20-)`,
				`ExecuteMultiShard ks.DestinationShard(20-): dummy_query {} false false`,
			},
			isDML: false,
		},
		{
			testName:    "unsharded",
			sharded:     false,
			shards:      singleShard,
			destination: key.DestinationAllShards{},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
				`ExecuteMultiShard ks.0: dummy_query {} true true`,
			},
			isDML: true,
		},
		{
			testName:    "sharded with single shard destination",
			sharded:     true,
			shards:      twoShards,
			destination: key.DestinationShard("20-"),
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationShard(20-)`,
				`ExecuteMultiShard ks.DestinationShard(20-): dummy_query {} true true`,
			},
			isDML: true,
		},
		{
			testName:    "sharded with multi shard destination",
			sharded:     true,
			shards:      twoShards,
			destination: key.DestinationAllShards{},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
				`ExecuteMultiShard ks.-20: dummy_query {} ks.20-: dummy_query {} true false`,
			},
			isDML: true,
		},
		{
			testName:    "sharded with multi shard destination",
			sharded:     true,
			shards:      twoShards,
			destination: key.DestinationAllShards{},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			},
			expectedError:   "Unexpected error, DestinationKeyspaceID mapping to multiple shards: dummy_query, got: DestinationAllShards()",
			isDML:           true,
			singleShardOnly: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			send := &Send{
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: tc.sharded,
				},
				Query:             "dummy_query",
				TargetDestination: tc.destination,
				IsDML:             tc.isDML,
				SingleShardOnly:   tc.singleShardOnly,
			}
			vc := &loggingVCursor{shards: tc.shards}
			_, err := send.Execute(vc, map[string]*querypb.BindVariable{}, false)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
			vc.ExpectLog(t, tc.expectedQueryLog)

			// Failure cases
			vc = &loggingVCursor{shardErr: errors.New("shard_error")}
			_, err = send.Execute(vc, map[string]*querypb.BindVariable{}, false)
			require.EqualError(t, err, "shard_error")

			if !tc.sharded {
				vc = &loggingVCursor{}
				_, err = send.Execute(vc, map[string]*querypb.BindVariable{}, false)
				require.EqualError(t, err, "Keyspace does not have exactly one shard: []")
			}
		})
	}
}

func TestSendTable_StreamExecute(t *testing.T) {
	type testCase struct {
		testName         string
		sharded          bool
		shards           []string
		destination      key.Destination
		expectedQueryLog []string
		expectedError    string
		isDML            bool
		singleShardOnly  bool
	}

	singleShard := []string{"0"}
	twoShards := []string{"-20", "20-"}
	tests := []testCase{
		{
			testName:    "unsharded with no autocommit",
			sharded:     false,
			shards:      singleShard,
			destination: key.DestinationAllShards{},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
				`StreamExecuteMulti dummy_query ks.0: {} `,
			},
			isDML: false,
		},
		{
			testName:    "sharded with no autocommit",
			sharded:     true,
			shards:      twoShards,
			destination: key.DestinationShard("20-"),
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationShard(20-)`,
				`StreamExecuteMulti dummy_query ks.DestinationShard(20-): {} `,
			},
			isDML: false,
		},
		{
			testName:    "unsharded",
			sharded:     false,
			shards:      singleShard,
			destination: key.DestinationAllShards{},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
				`StreamExecuteMulti dummy_query ks.0: {} `,
			},
			isDML: true,
		},
		{
			testName:    "sharded with single shard destination",
			sharded:     true,
			shards:      twoShards,
			destination: key.DestinationShard("20-"),
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationShard(20-)`,
				`StreamExecuteMulti dummy_query ks.DestinationShard(20-): {} `,
			},
			isDML: true,
		},
		{
			testName:    "sharded with multi shard destination",
			sharded:     true,
			shards:      twoShards,
			destination: key.DestinationAllShards{},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
				`StreamExecuteMulti dummy_query ks.-20: {} ks.20-: {} `,
			},
			isDML: true,
		},
		{
			testName:    "sharded with multi shard destination single shard setting",
			sharded:     true,
			shards:      twoShards,
			destination: key.DestinationAllShards{},
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			},
			expectedError:   "Unexpected error, DestinationKeyspaceID mapping to multiple shards: dummy_query, got: DestinationAllShards()",
			isDML:           true,
			singleShardOnly: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			send := &Send{
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: tc.sharded,
				},
				Query:             "dummy_query",
				TargetDestination: tc.destination,
				IsDML:             tc.isDML,
				SingleShardOnly:   tc.singleShardOnly,
			}
			vc := &loggingVCursor{shards: tc.shards}
			_, err := wrapStreamExecute(send, vc, map[string]*querypb.BindVariable{}, false)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
			vc.ExpectLog(t, tc.expectedQueryLog)

			// Failure cases
			vc = &loggingVCursor{shardErr: errors.New("shard_error")}
			_, err = wrapStreamExecute(send, vc, map[string]*querypb.BindVariable{}, false)
			require.EqualError(t, err, "shard_error")

			if !tc.sharded {
				vc = &loggingVCursor{}
				_, err = wrapStreamExecute(send, vc, map[string]*querypb.BindVariable{}, false)
				require.EqualError(t, err, "Keyspace does not have exactly one shard: []")
			}
		})
	}
}

func TestSendGetFields(t *testing.T) {
	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3",
			"int64|int64|int64|int64",
		),
		"1|4|5|6",
		"2|7|8|9",
	)}

	send := &Send{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:             "dummy_query",
		TargetDestination: key.DestinationAllShards{},
		IsDML:             true,
		SingleShardOnly:   false,
	}
	vc := &loggingVCursor{shards: []string{"-20", "20-"}, results: results}
	qr, err := send.GetFields(vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_query {} ks.20-: dummy_query {} true false`,
	})
	require.Nil(t, qr.Rows)
	require.Equal(t, 4, len(qr.Fields))
}
