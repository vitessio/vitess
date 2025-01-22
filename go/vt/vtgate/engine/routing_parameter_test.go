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
	}
	bv.Values = append(
		bv.Values,
		sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarBinary("hello")}),
	)
	bv.Values = append(
		bv.Values,
		sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(2), sqltypes.NewVarBinary("good morning")}),
	)

	vc := newTestVCursor("0")
	rss, bvs, err := rp.findRoute(context.Background(), vc, map[string]*querypb.BindVariable{
		valueBvName: bv,
	})
	require.NoError(t, err)
	require.Len(t, rss, 1)
	require.Len(t, bvs, 1)
	var s []int64
	for _, value := range bvs[0][valueBvName].Values {
		v := sqltypes.ProtoToValue(value)
		require.Equal(t, sqltypes.Int64, v.Type())
		i, err := v.ToInt64()
		require.NoError(t, err)
		s = append(s, i)
	}
	require.Equal(t, []int64{1, 2}, s)
}
