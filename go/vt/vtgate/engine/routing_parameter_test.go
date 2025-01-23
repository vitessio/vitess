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
		Opcode: Values,

		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},

		Vindex: vindex,

		Values: []evalengine.Expr{
			evalengine.NewBindVar(valueBvName, evalengine.NewType(sqltypes.Tuple, collations.Unknown)),
			evalengine.NewColumn(0, evalengine.NewType(sqltypes.Int64, collations.Unknown), nil),
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

	expectedIdsPerShard := [][]int64{
		{1, 2},
		{3, 4},
	}
	for i, ids := range expectedIdsPerShard {
		var s []int64
		for _, value := range bvs[i][valueBvName].Values {
			v := sqltypes.ProtoToValue(value)
			require.Equal(t, sqltypes.Int64, v.Type())
			i, err := v.ToInt64()
			require.NoError(t, err)
			s = append(s, i)
		}
		require.Equal(t, ids, s)
	}
}
