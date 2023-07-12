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

package ops

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// Operator forms the tree of operators, representing the declarative query provided.
	// While planning, the operator tree starts with logical operators, and later moves to physical operators.
	// The difference between the two is that when we get to a physical operator, we have made decisions on in
	// which order to do the joins, and how to split them up across shards and keyspaces.
	// In some situation we go straight to the physical operator - when there are no options to consider,
	// we can go straight to the end result.
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
		AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Operator, error)

		// AddColumn tells an operator to also output an additional column specified.
		// The offset to the column is returned.
		AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseExisting, addToGroupBy bool) (Operator, int, error)

		GetColumns() ([]*sqlparser.AliasedExpr, error)

		GetSelectExprs() (sqlparser.SelectExprs, error)

		ShortDescription() string

		GetOrdering() ([]OrderBy, error)
	}

	// OrderBy contains the expression to used in order by and also if ordering is needed at VTGate level then what the weight_string function expression to be sent down for evaluation.
	OrderBy struct {
		Inner *sqlparser.Order

		// See GroupBy#SimplifiedExpr for more details about this
		SimplifiedExpr sqlparser.Expr
	}
)
