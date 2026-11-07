/*
Copyright 2026 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// execStmtVCursor supplies the session user-defined variables ExecStmt reads.
// It is self-contained so the test applies unchanged on the release branches,
// whose shared fakes do not implement GetUDV.
type execStmtVCursor struct {
	noopVCursor
	udvs map[string]*querypb.BindVariable
}

func (f *execStmtVCursor) Session() SessionActions {
	return f
}

func (f *execStmtVCursor) GetUDV(key string) *querypb.BindVariable {
	return f.udvs[key]
}

// TestExecStmtValidatesUserDefinedVariables pins that EXECUTE ... USING @x
// validates the session variable it copies into the inner plan's bind
// variables the same way a request bind variable is validated. The session
// arrives over the wire with the request, and this path bypasses the
// normalizer's rewrite of @x, so the check has to happen here.
func TestExecStmtValidatesUserDefinedVariables(t *testing.T) {
	malformed := sqltypes.HexNumBindVariable([]byte("1; drop table t #"))
	wellFormed := sqltypes.HexNumBindVariable([]byte("0x41"))

	newExecStmt := func(input Primitive) *ExecStmt {
		return &ExecStmt{
			Params: []*sqlparser.Variable{sqlparser.NewVariableExpression("x", sqlparser.SingleAt)},
			Input:  input,
		}
	}

	t.Run("TryExecute rejects a malformed variable before running the input", func(t *testing.T) {
		input := &fakePrimitive{results: []*sqltypes.Result{{}}}
		vc := &execStmtVCursor{udvs: map[string]*querypb.BindVariable{"x": malformed}}

		_, err := newExecStmt(input).TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
		require.ErrorContains(t, err, "@x")
		require.ErrorContains(t, err, "invalid HEXNUM literal")
		assert.Empty(t, input.log, "input primitive must not run")
	})

	t.Run("TryStreamExecute rejects a malformed variable before running the input", func(t *testing.T) {
		input := &fakePrimitive{results: []*sqltypes.Result{{}}}
		vc := &execStmtVCursor{udvs: map[string]*querypb.BindVariable{"x": malformed}}

		err := newExecStmt(input).TryStreamExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false, func(*sqltypes.Result) error { return nil })
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
		require.ErrorContains(t, err, "@x")
		require.ErrorContains(t, err, "invalid HEXNUM literal")
		assert.Empty(t, input.log, "input primitive must not run")
	})

	t.Run("TryExecute passes a well-formed variable through as v1", func(t *testing.T) {
		input := &fakePrimitive{results: []*sqltypes.Result{{}}}
		vc := &execStmtVCursor{udvs: map[string]*querypb.BindVariable{"x": wellFormed}}

		_, err := newExecStmt(input).TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		input.ExpectLog(t, []string{`Execute v1: type:HEXNUM value:"0x41" false`})
	})
}
