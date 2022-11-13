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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Compact will optimise the operator tree into a smaller but equivalent version
func Compact(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error) {
	type compactable interface {
		// Compact implement this interface for operators that have easy to see optimisations
		Compact(ctx *plancontext.PlanningContext) (ops.Operator, rewrite.TreeIdentity, error)
	}

	newOp, err := rewrite.BottomUp(op, func(op ops.Operator) (ops.Operator, rewrite.TreeIdentity, error) {
		newOp, ok := op.(compactable)
		if !ok {
			return op, rewrite.SameTree, nil
		}
		return newOp.Compact(ctx)
	})
	return newOp, err
}

func CheckValid(op ops.Operator) error {
	type checkable interface {
		CheckValid() error
	}

	return rewrite.Visit(op, func(this ops.Operator) error {
		if chk, ok := this.(checkable); ok {
			return chk.CheckValid()
		}
		return nil
	})
}

func Clone(op ops.Operator) ops.Operator {
	inputs := op.Inputs()
	clones := make([]ops.Operator, len(inputs))
	for i, input := range inputs {
		clones[i] = Clone(input)
	}
	return op.Clone(clones)
}

// TableIDIntroducer is used to signal that this operator introduces data from a new source
type TableIDIntroducer interface {
	Introduces() semantics.TableSet
}

func TableID(op ops.Operator) (result semantics.TableSet) {
	_ = rewrite.Visit(op, func(this ops.Operator) error {
		if tbl, ok := this.(TableIDIntroducer); ok {
			result.MergeInPlace(tbl.Introduces())
		}
		return nil
	})
	return
}

func UnresolvedPredicates(op ops.Operator, st *semantics.SemTable) (result []sqlparser.Expr) {
	type unresolved interface {
		// UnsolvedPredicates returns any predicates that have dependencies on the given Operator and
		// on the outside of it (a parent Select expression, any other table not used by Operator, etc).
		// This is used for sub-queries. An example query could be:
		// SELECT * FROM tbl WHERE EXISTS (SELECT 1 FROM otherTbl WHERE tbl.col = otherTbl.col)
		// The subquery would have one unsolved predicate: `tbl.col = otherTbl.col`
		// It's a predicate that belongs to the inner query, but it needs data from the outer query
		// These predicates dictate which data we have to send from the outer side to the inner
		UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr
	}

	_ = rewrite.Visit(op, func(this ops.Operator) error {
		if tbl, ok := this.(unresolved); ok {
			result = append(result, tbl.UnsolvedPredicates(st)...)
		}

		return nil
	})
	return
}

func CostOf(op ops.Operator) (cost int) {
	type costly interface {
		// Cost returns the cost for this operator. All the costly operators in the tree are summed together to get the
		// total cost of the operator tree.
		// TODO: We should really calculate this using cardinality estimation,
		//       but until then this is better than nothing
		Cost() int
	}

	_ = rewrite.Visit(op, func(op ops.Operator) error {
		if costlyOp, ok := op.(costly); ok {
			cost += costlyOp.Cost()
		}
		return nil
	})
	return
}
