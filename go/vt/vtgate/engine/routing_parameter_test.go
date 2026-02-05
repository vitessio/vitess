/*
Copyright 2025 The Vitess Authors.

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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestFindRouteValuesJoin(t *testing.T) {
	vindex, err := vindexes.CreateVindex("hash", "", nil)
	require.NoError(t, err)

	const valueBvName = "v"
	rp := &RoutingParameters{
		Opcode: MultiEqual,

		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},

		Vindex: vindex,

		Values: []evalengine.Expr{
			&evalengine.TupleBindVariable{Key: valueBvName, Index: 0, Collation: collations.Unknown},
		},
	}

	bv := &querypb.BindVariable{
		Type: querypb.Type_TUPLE,
		Values: []*querypb.Value{
			sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarBinary("hello")}),
			sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(2), sqltypes.NewVarBinary("good morning")}),
			sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(3), sqltypes.NewVarBinary("bonjour")}),
			sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(4), sqltypes.NewVarBinary("bonjour")}),
		},
	}

	vc := newTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"-20", "-20", "20-", "20-"}
	rss, bvs, err := rp.findRoute(context.Background(), vc, map[string]*querypb.BindVariable{
		valueBvName: bv,
	})

	require.NoError(t, err)
	require.Len(t, rss, 2)
	require.Len(t, bvs, 2)
}
