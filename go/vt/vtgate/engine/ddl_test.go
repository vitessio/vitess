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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestDDL(t *testing.T) {
	ddl := &DDL{
		DDL: &sqlparser.CreateTable{
			Table: sqlparser.NewTableName("a"),
		},
		DirectDDLEnabled: true,
		OnlineDDL:        &OnlineDDL{},
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
	_, err := ddl.TryExecute(context.Background(), vc, nil, true)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		"commit",
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"ExecuteMultiShard false false",
	})
}

func TestDDLTempTable(t *testing.T) {
	ddl := &DDL{
		CreateTempTable: true,
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
	_, err := ddl.TryExecute(context.Background(), vc, nil, true)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		"temp table getting created",
		"Needs Reserved Conn",
		"ResolveDestinations ks [] Destinations:DestinationAllShards()",
		"ExecuteMultiShard false false",
	})
}
