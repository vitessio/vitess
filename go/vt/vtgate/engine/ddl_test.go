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

	vc := &loggingVCursor{}
	_, err := ddl.TryExecute(t.Context(), vc, nil, true)
	require.NoError(t, err)

	// The session is marked as holding temp tables only after the create
	// succeeded.
	vc.ExpectLog(t, []string{
		"Needs Reserved Conn",
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"ExecuteMultiShard false false",
		"temp table getting created",
	})

	// A CREATE that returns an error still marks the session: the tablet may
	// have reserved a connection and created the table before failing, and
	// heartbeats keyed on the reserved connection must be able to keep it
	// alive. (A create that reserved nothing is simply never beaten.)
	vc = &loggingVCursor{multiShardErrs: []error{errors.New("create failed")}}
	_, err = ddl.TryExecute(t.Context(), vc, nil, true)
	require.ErrorContains(t, err, "create failed")
	vc.ExpectLog(t, []string{
		"Needs Reserved Conn",
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"ExecuteMultiShard false false",
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
}
