package engine

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestSendUnsharded(t *testing.T) {
	send := &Send{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query:             "dummy_query",
		TargetDestination: key.DestinationAllShards{},
	}

	vc := &loggingVCursor{shards: []string{"0"}}
	_, err := send.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_query {} false true`,
	})

	// Failure cases
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = send.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "sendExecute: shard_error")

	vc = &loggingVCursor{}
	_, err = send.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "Keyspace does not have exactly one shard: []")
}

func TestSendSharded(t *testing.T) {
	send := &Send{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:             "dummy_query",
		TargetDestination: key.DestinationShard("20-"),
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	_, err := send.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationShard(20-)`,
		`ExecuteMultiShard ks.DestinationShard(20-): dummy_query {} false true`,
	})

	// Failure cases
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = send.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "sendExecute: shard_error")
}
