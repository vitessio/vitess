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

package evalengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestEquals(t *testing.T) {
	T := true
	F := false
	tests := []struct {
		name   string
		v1, v2 Expr
		out    *bool
		err    string
	}{
		{
			name: "All Nulls",
			v1:   &Null{},
			v2:   &Null{},
			out:  nil,
		}, {
			name: "Second value null.",
			v1:   NewLiteralInt(1),
			v2:   &Null{},
			out:  nil,
		}, {
			name: "First value null.",
			v1:   &Null{},
			v2:   NewLiteralInt(1),
			out:  nil,
		}, {
			name: "int with int",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralInt(1),
			out:  &T,
		}, {
			name: "wrong int",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralInt(2),
			out:  &F,
		}, {
			name: "int with string",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralString([]byte("1")),
			out:  &T,
		}, {
			name: "varbinary column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("1")),
			out:  &T,
		}, {
			name: "int column with string",
			v1:   NewColumn(1),
			v2:   NewLiteralString([]byte("1")),
			out:  &T,
		}, {
			name: "wrong varbinary column with string",
			v1:   NewColumn(0),
			v2:   NewLiteralString([]byte("42")),
			out:  &F,
		}, {
			name: "string with int",
			v1:   NewLiteralString([]byte("1")),
			v2:   NewLiteralInt(1),
			out:  &T,
		}, {
			name: "wrong int with string",
			v1:   NewLiteralInt(1),
			v2:   NewLiteralString([]byte("42")),
			out:  &F,
		}, {
			name: "wrong string with int",
			v1:   NewLiteralString([]byte("42")),
			v2:   NewLiteralInt(1),
			out:  &F,
		}, {
			name: "float with float",
			v1:   NewLiteralFloat(1.7),
			v2:   NewLiteralFloat(1.7),
			out:  &T,
		}, {
			name: "wrong float with float",
			v1:   NewLiteralFloat(1.7),
			v2:   NewLiteralFloat(9.1),
			out:  &F,
		}, {
			name: "wrong float with int",
			v1:   NewLiteralFloat(1.7),
			v2:   NewLiteralInt(1),
			out:  &F,
		}, {
			name: "float with int",
			v1:   NewLiteralFloat(1.00),
			v2:   NewLiteralInt(1),
			out:  &T,
		}, {
			name: "float with float column",
			v1:   NewLiteralFloat(42.21),
			v2:   NewColumn(2),
			out:  &T,
		},
	}

	for i, tcase := range tests {
		name := fmt.Sprintf("%d_%s_%s%s%s", i+1, tcase.name, tcase.v1.String(), "=", tcase.v2.String())
		t.Run(name, func(t *testing.T) {
			eq := &ComparisonExpr{
				Left:  tcase.v1,
				Right: tcase.v2,
			}

			env := ExpressionEnv{
				BindVars: map[string]*querypb.BindVariable{},
				Row:      []sqltypes.Value{sqltypes.NewVarBinary("1"), sqltypes.NewInt32(1), sqltypes.NewFloat64(42.21)},
			}
			got, err := eq.Evaluate(env)
			if tcase.err == "" {
				require.NoError(t, err)
				if tcase.out != nil && *tcase.out {
					require.EqualValues(t, 1, got.ival)
				} else if tcase.out != nil && !*tcase.out {
					require.EqualValues(t, 0, got.ival)
				} else {
					require.EqualValues(t, 0, got.ival)
					require.EqualValues(t, sqltypes.Null, got.typ)
				}
			} else {
				require.EqualError(t, err, tcase.err)
			}
		})
	}
}
