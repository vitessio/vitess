/*
Copyright 2019 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*simpleProjection)(nil)

// simpleProjection is used for wrapping a derived table.
// This primitive wraps any derived table that results
// in something that's not a route. It builds a
// 'table' for the derived table allowing higher level
// constructs to reference its columns. If a derived table
// results in a route primitive, we instead build
// a new route that keeps the subquery in the FROM
// clause, because a route is more versatile than
// a simpleProjection.
type simpleProjection struct {
	logicalPlanCommon
	eSimpleProj *engine.SimpleProjection
}

// Primitive implements the logicalPlan interface
func (sq *simpleProjection) Primitive() engine.Primitive {
	sq.eSimpleProj.Input = sq.input.Primitive()
	return sq.eSimpleProj
}

// OutputColumns implements the logicalPlan interface
func (sq *simpleProjection) OutputColumns() []sqlparser.SelectExpr {
	exprs := make([]sqlparser.SelectExpr, 0, len(sq.eSimpleProj.Cols))
	outputCols := sq.input.OutputColumns()
	for _, colID := range sq.eSimpleProj.Cols {
		exprs = append(exprs, outputCols[colID])
	}
	return exprs
}
