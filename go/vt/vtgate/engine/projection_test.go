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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func TestMultiply(t *testing.T) {
	expr := &sqlparser.BinaryExpr{
		Operator: sqlparser.MultOp,
		Left:     &sqlparser.Offset{V: 0},
		Right:    &sqlparser.Offset{V: 1},
	}
	evalExpr, err := evalengine.Translate(expr, nil)
	require.NoError(t, err)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("a|b", "uint64|uint64"),
			"3|2",
			"1|0",
			"1|2",
		)},
	}
	proj := &Projection{
		Cols:       []string{"apa"},
		Exprs:      []evalengine.Expr{evalExpr},
		Input:      fp,
		noTxNeeded: noTxNeeded{},
	}
	qr, err := proj.TryExecute(&noopVCursor{}, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	assert.Equal(t, "[[UINT64(6)] [UINT64(0)] [UINT64(2)]]", fmt.Sprintf("%v", qr.Rows))

	fp = &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("a|b", "uint64|uint64"),
			"3|2",
			"1|0",
			"1|2",
		)},
	}
	proj.Input = fp
	qr, err = wrapStreamExecute(proj, newNoopVCursor(context.Background()), nil, true)
	require.NoError(t, err)
	assert.Equal(t, "[[UINT64(6)] [UINT64(0)] [UINT64(2)]]", fmt.Sprintf("%v", qr.Rows))
}
