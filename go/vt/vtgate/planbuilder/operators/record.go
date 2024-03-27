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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Record struct {
	Source Operator
}

func newRecord(src Operator) *Record {
	return &Record{
		Source: src,
	}
}

// Clone implements the Operator interface
func (rc *Record) Clone(inputs []Operator) Operator {
	newOp := *rc
	newOp.Source = inputs[0]
	return &newOp
}

func (rc *Record) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

// Inputs implements the Operator interface
func (rc *Record) Inputs() []Operator {
	return []Operator{rc.Source}
}

// SetInputs implements the Operator interface
func (rc *Record) SetInputs(ops []Operator) {
	rc.Source = ops[0]
}

func (rc *Record) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	newSource := rc.Source.AddPredicate(ctx, expr)
	rc.Source = newSource
	return rc
}

func (rc *Record) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) int {
	return rc.Source.AddColumn(ctx, reuse, gb, expr)
}

func (rc *Record) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return rc.Source.FindCol(ctx, expr, underRoute)
}

func (rc *Record) GetColumns(ctx *plancontext.PlanningContext) (result []*sqlparser.AliasedExpr) {
	return rc.Source.GetColumns(ctx)
}

func (rc *Record) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return rc.Source.GetSelectExprs(ctx)
}

func (rc *Record) ShortDescription() string {
	return ""
}
