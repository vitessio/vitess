/*
Copyright 2023 The Vitess Authors.

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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type ddlConfig struct{}

func (ddlConfig) DirectEnabled() bool {
	return true
}

func (ddlConfig) OnlineEnabled() bool {
	return true
}

func TestDDL(t *testing.T) {
	ddl := &DDL{
		DDL: &sqlparser.CreateTable{
			Table: sqlparser.NewTableName("a"),
		},
		Config:    ddlConfig{},
		OnlineDDL: &OnlineDDL{},
		NormalDDL: &Send{
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: true,
			},
			TargetDestination: key.DestinationAllShards{},
			Query:             "ddl query",
		},
	}

	vc := &loggingVCursor{}
	_, err := ddl.TryExecute(t.Context(), vc, nil, true)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		"commit",
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"ExecuteMultiShard false false",
	})
}

func TestDDLTempTable(t *testing.T) {
	ddl := &DDL{
		TempTableDDL: true,
		DDL: &sqlparser.CreateTable{
			Temp:  true,
			Table: sqlparser.NewTableName("a"),
		},
		NormalDDL: &Send{
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: true,
			},
			TargetDestination: key.DestinationAllShards{},
			Query:             "ddl query",
		},
	}

	vc := &loggingVCursor{shards: []string{"0"}}
	_, err := ddl.TryExecute(t.Context(), vc, nil, true)
	require.NoError(t, err)

	// The single-shard routing check resolves before the session is touched;
	// the session is marked as holding temp tables only around the create.
	vc.ExpectLog(t, []string{
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"Needs Reserved Conn",
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"ExecuteMultiShard ks.0: ddl query {} false false",
		"temp table getting created",
	})

	// A CREATE that returns an error still marks the session: the tablet may
	// have reserved a connection and created the table before failing, and
	// heartbeats keyed on the reserved connection must be able to keep it
	// alive. (A create that reserved nothing is simply never beaten.)
	vc = &loggingVCursor{shards: []string{"0"}, multiShardErrs: []error{errors.New("create failed")}}
	_, err = ddl.TryExecute(t.Context(), vc, nil, true)
	require.ErrorContains(t, err, "create failed")
	vc.ExpectLog(t, []string{
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"Needs Reserved Conn",
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"ExecuteMultiShard ks.0: ddl query {} false false",
		"temp table getting created",
	})

	// DROP TEMPORARY TABLE is also a temporary-table DDL: it must run on the
	// reserved connection (no implicit commit, no online-DDL path — the
	// OnlineDDL primitive is nil for temporary DDLs, so falling through
	// would panic), but it must NOT reserve a connection or mark the session
	// as holding temp tables (a drop-only session created nothing).
	dropDDL := &DDL{
		TempTableDDL: true,
		DDL:          &sqlparser.DropTable{FromTables: sqlparser.TableNames{sqlparser.NewTableName("a")}},
		NormalDDL: &Send{
			Keyspace:          &vindexes.Keyspace{Name: "ks", Sharded: true},
			TargetDestination: key.DestinationAllShards{},
			Query:             "drop query",
		},
	}
	vc = &loggingVCursor{}
	_, err = dropDDL.TryExecute(t.Context(), vc, nil, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"ExecuteMultiShard false false",
	})

	// A create whose destination resolves to more than one shard is rejected
	// before anything executes: no tablet RPC is sent, so the session must
	// not be marked — marking would force every subsequent query onto a
	// pointless reserved connection with plan caching disabled for the rest
	// of the connection's life, for a statement that provably did nothing.
	vc = &loggingVCursor{shards: []string{"-80", "80-"}}
	_, err = ddl.TryExecute(t.Context(), vc, nil, true)
	require.ErrorContains(t, err, "exactly one shard")
	vc.ExpectLog(t, []string{
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
	})
}
