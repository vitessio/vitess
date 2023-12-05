/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type fakeOp struct {
	id     semantics.TableSet
	inputs []Operator
	cols   []*sqlparser.AliasedExpr
}

var _ Operator = (*fakeOp)(nil)

func (f *fakeOp) Clone(inputs []Operator) Operator {
	return f
}

func (f *fakeOp) Inputs() []Operator {
	return f.inputs
}

func (f *fakeOp) SetInputs(operators []Operator) {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, _ bool, expr *sqlparser.AliasedExpr) int {
	if offset := f.FindCol(ctx, expr.Expr, false); reuseExisting && offset >= 0 {
		return offset
	}
	f.cols = append(f.cols, expr)
	return len(f.cols) - 1
}

func (f *fakeOp) FindCol(ctx *plancontext.PlanningContext, a sqlparser.Expr, underRoute bool) int {
	return slices.IndexFunc(f.cols, func(b *sqlparser.AliasedExpr) bool {
		return a == b.Expr
	})
}

func (f *fakeOp) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) ShortDescription() string {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) introducesTableID() semantics.TableSet {
	return f.id
}
