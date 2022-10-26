/*
Copyright 2022 The Vitess Authors.

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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type dummyTranslator struct{}

func (d dummyTranslator) ColumnLookup(col *sqlparser.ColName) (int, error) {
	switch col.Name.String() {
	case "left":
		return 0, nil
	case "right":
		return 1, nil
	default:
		panic("unexpected column name")
	}
}

func (d dummyTranslator) CollationForExpr(_ sqlparser.Expr) collations.ID {
	return collationEnv.LookupByName("utf8mb4_bin").ID()
}

func (d dummyTranslator) DefaultCollation() collations.ID {
	return collationEnv.LookupByName("utf8mb4_bin").ID()
}

func TestFilterPass(t *testing.T) {
	predicate := &sqlparser.ComparisonExpr{
		Operator: sqlparser.GreaterThanOp,
		Left:     sqlparser.NewColName("left"),
		Right:    sqlparser.NewColName("right"),
	}

	pred, err := evalengine.Translate(predicate, &dummyTranslator{})
	require.NoError(t, err)

	tcases := []struct {
		name   string
		res    *sqltypes.Result
		expRes string
	}{{
		name:   "int32",
		res:    sqltypes.MakeTestResult(sqltypes.MakeTestFields("a|b", "int32|int32"), "0|1", "1|0", "2|3"),
		expRes: `[[INT32(1) INT32(0)]]`,
	}, {
		name:   "uint16",
		res:    sqltypes.MakeTestResult(sqltypes.MakeTestFields("a|b", "uint16|uint16"), "0|1", "1|0", "2|3"),
		expRes: `[[UINT16(1) UINT16(0)]]`,
	}, {
		name:   "uint64_int64",
		res:    sqltypes.MakeTestResult(sqltypes.MakeTestFields("a|b", "uint64|int64"), "0|1", "1|0", "2|3"),
		expRes: `[[UINT64(1) INT64(0)]]`,
	}, {
		name:   "int32_uint32",
		res:    sqltypes.MakeTestResult(sqltypes.MakeTestFields("a|b", "int32|uint32"), "0|1", "1|0", "2|3"),
		expRes: `[[INT32(1) UINT32(0)]]`,
	}, {
		name:   "uint16_int8",
		res:    sqltypes.MakeTestResult(sqltypes.MakeTestFields("a|b", "uint16|int8"), "0|1", "1|0", "2|3"),
		expRes: `[[UINT16(1) INT8(0)]]`,
	}, {
		name:   "uint64_int32",
		res:    sqltypes.MakeTestResult(sqltypes.MakeTestFields("a|b", "uint64|int32"), "0|1", "1|0", "2|3"),
		expRes: `[[UINT64(1) INT32(0)]]`,
	}}
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			filter := &Filter{
				Predicate: pred,
				Input:     &fakePrimitive{results: []*sqltypes.Result{tc.res}},
			}
			qr, err := filter.TryExecute(context.Background(), &noopVCursor{}, nil, false)
			require.NoError(t, err)
			require.Equal(t, tc.expRes, fmt.Sprintf("%v", qr.Rows))
		})
	}
}
