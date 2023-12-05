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
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// LockAndComment contains any comments or locking directives we want on all queries down from this operator
type LockAndComment struct {
	Source   Operator
	Comments *sqlparser.ParsedComments
	Lock     sqlparser.Lock
}

func (l *LockAndComment) Clone(inputs []Operator) Operator {
	klon := *l
	klon.Source = inputs[0]
	return &klon
}

func (l *LockAndComment) Inputs() []Operator {
	return []Operator{l.Source}
}

func (l *LockAndComment) SetInputs(operators []Operator) {
	l.Source = operators[0]
}

func (l *LockAndComment) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	l.Source = l.Source.AddPredicate(ctx, expr)
	return l
}

func (l *LockAndComment) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	return l.Source.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (l *LockAndComment) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return l.Source.FindCol(ctx, expr, underRoute)
}

func (l *LockAndComment) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return l.Source.GetColumns(ctx)
}

func (l *LockAndComment) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return l.Source.GetSelectExprs(ctx)
}

func (l *LockAndComment) ShortDescription() string {
	s := slices.Clone(l.Comments.GetComments())
	if l.Lock != sqlparser.NoLock {
		s = append(s, l.Lock.ToString())
	}

	return strings.Join(s, " ")
}

func (l *LockAndComment) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return l.Source.GetOrdering(ctx)
}
