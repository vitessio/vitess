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

// SaveToSession is used to save a value to the session.
// At the moment it's only used for last_insert_id, but it could be used for other things in the future.
type SaveToSession struct {
	unaryOperator
	Offset int
}

var _ Operator = (*SaveToSession)(nil)

func (s *SaveToSession) Clone(inputs []Operator) Operator {
	k := *s
	k.Source = inputs[0]
	return &k
}

func (s *SaveToSession) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	src := s.Source.AddPredicate(ctx, expr)
	s.Source = src
	return s
}

func (s *SaveToSession) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	return s.Source.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (s *SaveToSession) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return s.Source.AddWSColumn(ctx, offset, underRoute)
}

func (s *SaveToSession) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return s.Source.FindCol(ctx, expr, underRoute)
}

func (s *SaveToSession) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return s.Source.GetColumns(ctx)
}

func (s *SaveToSession) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return s.Source.GetSelectExprs(ctx)
}

func (s *SaveToSession) ShortDescription() string {
	return ""
}

func (s *SaveToSession) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return s.Source.GetOrdering(ctx)
}
