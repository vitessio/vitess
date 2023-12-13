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
	Agreggation, Limit, etc.
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
	"fmt"
	"runtime"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// helper type that implements Inputs() returning nil
	noInputs struct{}

	// helper type that implements AddColumn() returning an error
	noColumns struct{}

	// helper type that implements AddPredicate() returning an error
	noPredicates struct{}
)

// PlanQuery creates a query plan for a given SQL statement
func PlanQuery(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (result Operator, err error) {
	defer PanicHandler(&err)

	op := translateQueryToOp(ctx, stmt)

	if DebugOperatorTree {
		fmt.Println("Initial tree:")
		fmt.Println(ToTree(op))
	}

	op = compact(ctx, op)
	checkValid(op)
	op = planQuery(ctx, op)

	_, isRoute := op.(*Route)
	if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
		// If we got here, we don't have a single shard plan
		return nil, ctx.SemTable.NotSingleRouteErr
	}

	return op, err
}

func PanicHandler(err *error) {
	if r := recover(); r != nil {
		switch badness := r.(type) {
		case runtime.Error:
			panic(r)
		case error:
			*err = badness
		default:
			panic(r)
		}
	}
}

// Inputs implements the Operator interface
func (noInputs) Inputs() []Operator {
	return nil
}

// SetInputs implements the Operator interface
func (noInputs) SetInputs(ops []Operator) {
	if len(ops) > 0 {
		panic("the noInputs operator does not have inputs")
	}
}

// AddColumn implements the Operator interface
func (noColumns) AddColumn(*plancontext.PlanningContext, bool, bool, *sqlparser.AliasedExpr) int {
	panic(vterrors.VT13001("noColumns operators have no column"))
}

func (noColumns) GetColumns(*plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	panic(vterrors.VT13001("noColumns operators have no column"))
}

func (noColumns) FindCol(*plancontext.PlanningContext, sqlparser.Expr, bool) int {
	panic(vterrors.VT13001("noColumns operators have no column"))
}

func (noColumns) GetSelectExprs(*plancontext.PlanningContext) sqlparser.SelectExprs {
	panic(vterrors.VT13001("noColumns operators have no column"))
}

// AddPredicate implements the Operator interface
func (noPredicates) AddPredicate(*plancontext.PlanningContext, sqlparser.Expr) Operator {
	panic(vterrors.VT13001("the noColumns operator cannot accept predicates"))
}

// tryTruncateColumnsAt will see if we can truncate the columns by just asking the operator to do it for us
func tryTruncateColumnsAt(op Operator, truncateAt int) bool {
	type columnTruncator interface {
		setTruncateColumnCount(offset int)
	}

	truncator, ok := op.(columnTruncator)
	if ok {
		truncator.setTruncateColumnCount(truncateAt)
		return true
	}

	switch op := op.(type) {
	case *Limit:
		return tryTruncateColumnsAt(op.Source, truncateAt)
	case *SubQuery:
		for _, offset := range op.Vars {
			if offset >= truncateAt {
				return false
			}
		}
		return tryTruncateColumnsAt(op.Outer, truncateAt)
	default:
		return false
	}
}

func transformColumnsToSelectExprs(ctx *plancontext.PlanningContext, op Operator) sqlparser.SelectExprs {
	columns := op.GetColumns(ctx)
	selExprs := slice.Map(columns, func(from *sqlparser.AliasedExpr) sqlparser.SelectExpr {
		return from
	})
	return selExprs
}
