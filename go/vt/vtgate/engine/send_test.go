package engine

import (
	"errors"
	"testing"

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
		noAutoCommit     bool
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
				`ExecuteMultiShard ks.0: dummy_query {} true true`,
			},
			noAutoCommit: true,
		},
		{
			testName:    "sharded with no autocommit",
			sharded:     true,
			shards:      twoShards,
			destination: key.DestinationShard("20-"),
			expectedQueryLog: []string{
				`ResolveDestinations ks [] Destinations:DestinationShard(20-)`,
				`ExecuteMultiShard ks.DestinationShard(20-): dummy_query {} true true`,
			},
			noAutoCommit: true,
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
			noAutoCommit: false,
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
			noAutoCommit: false,
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
			noAutoCommit: false,
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
				NoAutoCommit:      false,
			}
			vc := &loggingVCursor{shards: tc.shards}
			_, err := send.Execute(vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			vc.ExpectLog(t, tc.expectedQueryLog)

			// Failure cases
			vc = &loggingVCursor{shardErr: errors.New("shard_error")}
			_, err = send.Execute(vc, map[string]*querypb.BindVariable{}, false)
			require.EqualError(t, err, "sendExecute: shard_error")

			if !tc.sharded {
				vc = &loggingVCursor{}
				_, err = send.Execute(vc, map[string]*querypb.BindVariable{}, false)
				require.EqualError(t, err, "Keyspace does not have exactly one shard: []")
			}
		})
	}
}
