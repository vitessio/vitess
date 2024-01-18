/*
Copyright 2024 The Vitess Authors.

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

package evalengine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestTupleBindVarEval tests TupleBindVariable eval function.
func TestTupleBindVarEval(t *testing.T) {
	key := "vals"
	c := &TupleBindVariable{
		Key:   key,
		Index: 1,
	}
	collation := collations.TypedCollation{
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireUnicode,
	}

	tcases := []struct {
		tName string
		bv    *querypb.BindVariable

		expEval []eval
		expErr  string
	}{{
		tName:  "bind variable not provided",
		expErr: "query arguments missing for vals",
	}, {
		tName:  "bind variable provided - wrong type",
		bv:     sqltypes.Int64BindVariable(1),
		expErr: "query argument 'vals' must be a tuple (is INT64)",
	}, {
		tName: "bind variable provided",
		bv: &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.TestTuple(sqltypes.NewInt64(1), sqltypes.NewVarChar("a")))},
		},
		expEval: []eval{newEvalText([]byte("a"), collation)},
	}, {
		tName: "bind variable provided - multi values",
		bv: &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
			Values: []*querypb.Value{
				sqltypes.ValueToProto(sqltypes.TestTuple(sqltypes.NewInt64(1), sqltypes.NewVarChar("a"))),
				sqltypes.ValueToProto(sqltypes.TestTuple(sqltypes.NewInt64(2), sqltypes.NewVarChar("b"))),
				sqltypes.ValueToProto(sqltypes.TestTuple(sqltypes.NewInt64(3), sqltypes.NewVarChar("c"))),
			},
		},
		expEval: []eval{
			newEvalText([]byte("a"), collation),
			newEvalText([]byte("b"), collation),
			newEvalText([]byte("c"), collation)},
	}}

	for _, tcase := range tcases {
		t.Run(tcase.tName, func(t *testing.T) {
			env := &ExpressionEnv{
				BindVars: make(map[string]*querypb.BindVariable),
			}
			if tcase.bv != nil {
				env.BindVars[key] = tcase.bv
			}

			res, err := c.eval(env)
			if tcase.expErr != "" {
				require.ErrorContains(t, err, tcase.expErr)
				return
			}
			require.Equal(t, sqltypes.Tuple, res.SQLType())
			resTuple := res.(*evalTuple)
			require.Len(t, resTuple.t, len(tcase.expEval))
			for idx, e := range tcase.expEval {
				require.Equal(t, e, resTuple.t[idx])
			}
		})
	}
}

// TestTupleBindVarTypeOf tests TupleBindVariable typeOf function.
func TestTupleBindVarTypeOf(t *testing.T) {
	key := "vals"
	c := &TupleBindVariable{
		Key:   key,
		Index: 1,
	}

	tcases := []struct {
		tName string
		bv    *querypb.BindVariable

		expErr string
	}{{
		tName:  "bind variable not provided",
		expErr: "query arguments missing for vals",
	}, {
		// typeOf does not evaluate the bind variable value
		tName: "bind variable provided - wrong type",
		bv:    sqltypes.Int64BindVariable(1),
	}, {
		tName: "bind variable provided",
		bv: &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.TestTuple(sqltypes.NewInt64(1), sqltypes.NewVarChar("a")))},
		},
	}, {
		tName: "bind variable provided - multi values",
		bv: &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
			Values: []*querypb.Value{
				sqltypes.ValueToProto(sqltypes.TestTuple(sqltypes.NewInt64(1), sqltypes.NewVarChar("a"))),
				sqltypes.ValueToProto(sqltypes.TestTuple(sqltypes.NewInt64(2), sqltypes.NewVarChar("b"))),
				sqltypes.ValueToProto(sqltypes.TestTuple(sqltypes.NewInt64(3), sqltypes.NewVarChar("c"))),
			},
		},
	}}

	for _, tcase := range tcases {
		t.Run(tcase.tName, func(t *testing.T) {
			env := &ExpressionEnv{
				BindVars: make(map[string]*querypb.BindVariable),
			}
			if tcase.bv != nil {
				env.BindVars[key] = tcase.bv
			}

			res, err := c.typeof(env)
			if tcase.expErr != "" {
				require.ErrorContains(t, err, tcase.expErr)
				return
			}
			require.Equal(t, sqltypes.Tuple, res.Type)
		})
	}
}
