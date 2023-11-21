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

// Package operators contains the operators used to plan queries.
/*
The operators go through a few phases while planning:
1.	Initial plan
	In this first pass, we build an operator tree from the incoming parsed query.
	At the leaves, it will contain QueryGraphs - these are the tables in the FROM clause
	that we can easily do join ordering on because they are all inner joins.
	All the post-processing - aggregations, sorting, limit etc. are at this stage
	contained in Horizon structs. We try to push these down under routes, and expand
	the ones that can't be pushed down into individual operators such as Projection,
	Aggregation, Limit, etc.
2.	Planning
	Once the initial plan has been fully built, we go through a number of phases.
	recursively running rewriters on the tree in a fixed point fashion, until we've gone
	over all phases and the tree has stop changing.
3.	Offset planning
	Now is the time to stop working with AST objects and transform remaining expressions being
	used on top of vtgate to either offsets on inputs or evalengine expressions.
*/
package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// Operator forms the tree of operators, representing the declarative query provided.
	// The operator tree is no actually runnable, it's an intermediate representation used
	// while query planning
	// The mental model are operators that pull data from each other, the root being the
	// full query output, and the leaves are most often `Route`s, representing communication
	// with one or more shards. We want to push down as much work as possible under these Routes
	Operator interface {
		// Clone will return a copy of this operator, protected so changed to the original will not impact the clone
		Clone(inputs []Operator) Operator

		// Inputs returns the inputs for this operator
		Inputs() []Operator

		// SetInputs changes the inputs for this op
		SetInputs([]Operator)

		// AddPredicate is used to push predicates. It pushed it as far down as is possible in the tree.
		// If we encounter a join and the predicate depends on both sides of the join, the predicate will be split into two parts,
		// where data is fetched from the LHS of the join to be used in the evaluation on the RHS
		// TODO: we should remove this and replace it with rewriters
		AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator

		AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int

		FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int

		GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr
		GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs

		ShortDescription() string

		GetOrdering(ctx *plancontext.PlanningContext) []OrderBy
	}

	// OrderBy contains the expression to used in order by and also if ordering is needed at VTGate level then what the weight_string function expression to be sent down for evaluation.
	OrderBy struct {
		Inner *sqlparser.Order

		// See GroupBy#SimplifiedExpr for more details about this
		SimplifiedExpr sqlparser.Expr
	}
)

// Map takes in a mapping function and applies it to both the expression in OrderBy.
func (ob OrderBy) Map(mappingFunc func(sqlparser.Expr) sqlparser.Expr) OrderBy {
	return OrderBy{
		Inner: &sqlparser.Order{
			Expr:      mappingFunc(ob.Inner.Expr),
			Direction: ob.Inner.Direction,
		},
		SimplifiedExpr: mappingFunc(ob.SimplifiedExpr),
	}
}
