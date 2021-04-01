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

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vtgate/engine"
)

type testPlanner struct {
	panic  interface{}
	err    error
	res    engine.Primitive
	called bool
}

var _ selectPlanner = (*testPlanner)(nil).plan

func (tp *testPlanner) plan(_ string) func(sqlparser.Statement, sqlparser.BindVars, ContextVSchema) (engine.Primitive, error) {
	return func(statement sqlparser.Statement, vars sqlparser.BindVars, schema ContextVSchema) (engine.Primitive, error) {
		tp.called = true
		if tp.panic != nil {
			panic(tp.panic)
		}
		return tp.res, tp.err
	}
}

func TestFallbackPlanner(t *testing.T) {
	a := &testPlanner{}
	b := &testPlanner{}
	fb := &fallbackPlanner{
		primary:  a.plan,
		fallback: b.plan,
	}

	stmt := &sqlparser.Select{}
	var vschema ContextVSchema

	// first planner succeeds
	_, _ = fb.plan("query")(stmt, nil, vschema)
	assert.True(t, a.called)
	assert.False(t, b.called)
	a.called = false

	// first planner errors
	a.err = fmt.Errorf("fail")
	_, _ = fb.plan("query")(stmt, nil, vschema)
	assert.True(t, a.called)
	assert.True(t, b.called)

	a.called = false
	b.called = false

	// first planner panics
	a.panic = "oh noes"
	_, _ = fb.plan("query")(stmt, nil, vschema)
	assert.True(t, a.called)
	assert.True(t, b.called)
}
