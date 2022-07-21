/*
Copyright 2021 The Vitess Authors.

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

package planbuilder

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vtgate/engine"
)

type testPlanner struct {
	panic       any
	err         error
	res         engine.Primitive
	messWithAST func(sqlparser.Statement)
	called      bool
}

var _ stmtPlanner = (*testPlanner)(nil).plan

func (tp *testPlanner) plan(statement sqlparser.Statement, vars *sqlparser.ReservedVars, schema plancontext.VSchema) (*planResult, error) {
	tp.called = true
	if tp.panic != nil {
		panic(tp.panic)
	}
	if tp.messWithAST != nil {
		tp.messWithAST(statement)
	}
	return newPlanResult(tp.res), tp.err
}

func TestFallbackPlanner(t *testing.T) {
	a := &testPlanner{}
	b := &testPlanner{}
	fb := &fallbackPlanner{
		primary:  a.plan,
		fallback: b.plan,
	}

	stmt := &sqlparser.Select{}
	var vschema plancontext.VSchema

	// first planner succeeds
	_, _ = fb.plan(stmt, nil, vschema)
	assert.True(t, a.called)
	assert.False(t, b.called)
	a.called = false

	// first planner errors
	a.err = fmt.Errorf("fail")
	_, _ = fb.plan(stmt, nil, vschema)
	assert.True(t, a.called)
	assert.True(t, b.called)

	a.called = false
	b.called = false

	// first planner panics
	a.panic = "oh noes"
	_, _ = fb.plan(stmt, nil, vschema)
	assert.True(t, a.called)
	assert.True(t, b.called)
}

func TestFallbackClonesBeforePlanning(t *testing.T) {
	a := &testPlanner{
		messWithAST: func(statement sqlparser.Statement) {
			sel := statement.(*sqlparser.Select)
			sel.SelectExprs = nil
		},
	}
	b := &testPlanner{}
	fb := &fallbackPlanner{
		primary:  a.plan,
		fallback: b.plan,
	}

	stmt := &sqlparser.Select{
		SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
	}
	var vschema plancontext.VSchema

	// first planner succeeds
	_, _ = fb.plan(stmt, nil, vschema)

	assert.NotNilf(t, stmt.SelectExprs, "should not have changed")
}
