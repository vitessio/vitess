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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
)

// TestExpressionEnvTypeOf tests the functionality of the TypeOf method on ExpressionEnv
func TestExpressionEnvTypeOf(t *testing.T) {
	sumCol := &Column{
		Type:   sqltypes.Unknown,
		Offset: 0,
		Original: &sqlparser.Sum{
			Arg: sqlparser.NewColName("l_discount"),
		},
		dynamicTypeOffset: 0,
	}
	countCol := &Column{
		Type:   sqltypes.Unknown,
		Offset: 1,
		Original: &sqlparser.Count{
			Args: sqlparser.Exprs{
				sqlparser.NewColName("l_discount"),
			},
		},
		dynamicTypeOffset: 1,
	}

	tests := []struct {
		name        string
		env         *ExpressionEnv
		expr        Expr
		wantedScale int32
		wantedType  sqltypes.Type
	}{
		{
			name: "Decimal divided by integer",
			env: &ExpressionEnv{
				Fields: []*querypb.Field{
					{
						Name:         "avg_disc",
						Type:         querypb.Type_DECIMAL,
						ColumnLength: 39,
						Decimals:     2,
					},
					{
						Name:         "count(l_discount)",
						Type:         querypb.Type_INT64,
						ColumnLength: 21,
					},
				},
				sqlmode: 3,
			},
			expr: &UntypedExpr{
				env:       vtenv.NewTestEnv(),
				mu:        sync.Mutex{},
				collation: 255,
				typed:     nil,
				needTypes: []typedIR{sumCol, countCol},
				ir: &ArithmeticExpr{
					Op: &opArithDiv{},
					BinaryExpr: BinaryExpr{
						Left:  sumCol,
						Right: countCol,
					},
				},
			},
			wantedScale: 6,
			wantedType:  sqltypes.Decimal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.env.TypeOf(tt.expr)
			require.NoError(t, err)
			require.EqualValues(t, tt.wantedType, got.Type())
			require.EqualValues(t, tt.wantedScale, got.Scale())
		})
	}
}
