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
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// Operator forms the tree of operators, representing the declarative query provided.
	// While planning, the operator tree starts with logical operators, and later moves to physical operators.
	// The difference between the two is that when we get to a physical operator, we have made decisions on in
	// which order to do the joins, and how to split them up across shards and keyspaces.
	// In some situation we go straight to the physical operator - when there are no options to consider,
	// we can go straight to the end result.
	Operator interface {
		clone(inputs []Operator) Operator
		inputs() []Operator

		// AddPredicate is used to push predicates. It pushed it as far down as is possible in the tree.
		// If we encounter a join and the predicate depends on both sides of the join, the predicate will be split into two parts,
		// where data is fetched from the LHS of the join to be used in the evaluation on the RHS
		AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Operator, error)

		// AddColumn tells an operator to also output an additional column specified.
		// The offset to the column is returned.
		AddColumn(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error)
	}

	// PhysicalOperator means that this operator is ready to be turned into a logical plan
	PhysicalOperator interface {
		Operator
		IPhysical()
	}

	// tableIDIntroducer is used to signal that this operator introduces data from a new source
	tableIDIntroducer interface {
		Introduces() semantics.TableSet
	}

	unresolved interface {
		// UnsolvedPredicates returns any predicates that have dependencies on the given Operator and
		// on the outside of it (a parent Select expression, any other table not used by Operator, etc).
		// This is used for sub-queries. An example query could be:
		// SELECT * FROM tbl WHERE EXISTS (SELECT 1 FROM otherTbl WHERE tbl.col = otherTbl.col)
		// The subquery would have one unsolved predicate: `tbl.col = otherTbl.col`
		// It's a predicate that belongs to the inner query, but it needs data from the outer query
		// These predicates dictate which data we have to send from the outer side to the inner
		UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr
	}

	costly interface {
		// Cost returns the cost for this operator. All the costly operators in the tree are summed together to get the
		// total cost of the operator tree.
		// TODO: We should really calculate this using cardinality estimation,
		//       but until then this is better than nothing
		Cost() int
	}

	checkable interface {
		// checkValid allows operators that need a final check before being used, to make sure that
		// all the necessary information is in the operator
		checkValid() error
	}

	compactable interface {
		// implement this interface for operators that have easy to see optimisations
		compact(ctx *plancontext.PlanningContext) (Operator, bool, error)
	}

	// helper type that implements Inputs() returning nil
	noInputs struct{}

	// helper type that implements AddColumn() returning an error
	noColumns struct{}

	// helper type that implements AddPredicate() returning an error
	noPredicates struct{}
)

func PlanQuery(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) (Operator, error) {
	op, err := createLogicalOperatorFromAST(ctx, selStmt)
	if err != nil {
		return nil, err
	}

	if err = checkValid(op); err != nil {
		return nil, err
	}

	op, err = transformToPhysical(ctx, op)
	if err != nil {
		return nil, err
	}

	backup := clone(op)

	op, err = planHorizons(ctx, op)
	if err == errNotHorizonPlanned {
		op = backup
	} else if err != nil {
		return nil, err
	}

	if op, err = compact(ctx, op); err != nil {
		return nil, err
	}

	return op, err
}

// inputs implements the Operator interface
func (noInputs) inputs() []Operator {
	return nil
}

// AddColumn implements the Operator interface
func (noColumns) AddColumn(*plancontext.PlanningContext, sqlparser.Expr) (int, error) {
	return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "this operator cannot accept columns")
}

// AddPredicate implements the Operator interface
func (noPredicates) AddPredicate(*plancontext.PlanningContext, sqlparser.Expr) (Operator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "this operator cannot accept predicates")
}

func VisitTopDown(root Operator, visitor func(Operator) error) error {
	queue := []Operator{root}
	for len(queue) > 0 {
		this := queue[0]
		queue = append(queue[1:], this.inputs()...)
		err := visitor(this)
		if err != nil {
			return err
		}
	}
	return nil
}

func TableID(op Operator) (result semantics.TableSet) {
	_ = VisitTopDown(op, func(this Operator) error {
		if tbl, ok := this.(tableIDIntroducer); ok {
			result.MergeInPlace(tbl.Introduces())
		}
		return nil
	})
	return
}

func unresolvedPredicates(op Operator, st *semantics.SemTable) (result []sqlparser.Expr) {
	_ = VisitTopDown(op, func(this Operator) error {
		if tbl, ok := this.(unresolved); ok {
			result = append(result, tbl.UnsolvedPredicates(st)...)
		}

		return nil
	})
	return
}

func checkValid(op Operator) error {
	return VisitTopDown(op, func(this Operator) error {
		if chk, ok := this.(checkable); ok {
			return chk.checkValid()
		}
		return nil
	})
}

func CostOf(op Operator) (cost int) {
	_ = VisitTopDown(op, func(op Operator) error {
		if costlyOp, ok := op.(costly); ok {
			cost += costlyOp.Cost()
		}
		return nil
	})
	return
}

func clone(op Operator) Operator {
	inputs := op.inputs()
	clones := make([]Operator, len(inputs))
	for i, input := range inputs {
		clones[i] = clone(input)
	}
	return op.clone(clones)
}

func checkSize(inputs []Operator, shouldBe int) {
	if len(inputs) != shouldBe {
		panic(fmt.Sprintf("BUG: got the wrong number of inputs: got %d, expected %d", len(inputs), shouldBe))
	}
}

type rewriterFunc func(*plancontext.PlanningContext, Operator) (newOp Operator, changed bool, err error)
type rewriterBreakableFunc func(*plancontext.PlanningContext, Operator) (newOp Operator, visitChildren bool, err error)

func rewriteBottomUp(ctx *plancontext.PlanningContext, root Operator, rewriter rewriterFunc) (Operator, bool, error) {
	oldInputs := root.inputs()
	anythingChanged := false
	newInputs := make([]Operator, len(oldInputs))
	for i, operator := range oldInputs {
		in, changed, err := rewriteBottomUp(ctx, operator, rewriter)
		if err != nil {
			return nil, false, err
		}
		if changed {
			anythingChanged = true
		}
		newInputs[i] = in
	}

	if anythingChanged {
		root = root.clone(newInputs)
	}

	newOp, b, err := rewriter(ctx, root)
	if err != nil {
		return nil, false, err
	}
	return newOp, anythingChanged || b, nil
}

func rewriteBreakableTopDown(ctx *plancontext.PlanningContext, in Operator, rewriterF rewriterBreakableFunc) (
	newOp Operator,
	err error,
) {
	newOp, visitChildren, err := rewriterF(ctx, in)
	if err != nil || !visitChildren {
		return
	}

	oldInputs := newOp.inputs()
	newInputs := make([]Operator, len(oldInputs))
	for i, oldInput := range oldInputs {
		newInputs[i], err = rewriteBreakableTopDown(ctx, oldInput, rewriterF)
		if err != nil {
			return
		}
	}
	newOp = newOp.clone(newInputs)
	return
}
