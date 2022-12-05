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
1. Logical
   In this first pass, we build an operator tree from the incoming parsed query.
   It will contain logical joins - we still haven't decided on the join algorithm to use yet.
   At the leaves, it will contain QueryGraphs - these are the tables in the FROM clause
   that we can easily do join ordering on. The logical tree will represent the full query,
   including projections, grouping, ordering and so on.
2. Physical
   Once the logical plan has been fully built, we go bottom up and plan which routes that will be used.
   During this phase, we will also decide which join algorithms should be used on the vtgate level
3. Columns & Aggregation
   Once we know which queries will be sent to the tablets, we go over the tree and decide which
   columns each operator should output. At this point, we also do offset lookups,
   so we know at runtime from which columns in the input table we need to read.
*/
package operators

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
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

func PlanQuery(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) (ops.Operator, error) {
	op, err := createLogicalOperatorFromAST(ctx, selStmt)
	if err != nil {
		return nil, err
	}

	if err = CheckValid(op); err != nil {
		return nil, err
	}

	op, err = transformToPhysical(ctx, op)
	if err != nil {
		return nil, err
	}

	backup := Clone(op)

	op, err = planHorizons(op)
	if err == errNotHorizonPlanned {
		op = backup
	} else if err != nil {
		return nil, err
	}

	if op, err = Compact(ctx, op); err != nil {
		return nil, err
	}

	return op, err
}

// Inputs implements the Operator interface
func (noInputs) Inputs() []ops.Operator {
	return nil
}

// AddColumn implements the Operator interface
func (noColumns) AddColumn(*plancontext.PlanningContext, sqlparser.Expr) (int, error) {
	return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "this operator cannot accept columns")
}

// AddPredicate implements the Operator interface
func (noPredicates) AddPredicate(*plancontext.PlanningContext, sqlparser.Expr) (ops.Operator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "this operator cannot accept predicates")
}
