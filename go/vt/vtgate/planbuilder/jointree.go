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

package planbuilder

import (
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type joinTree struct {
	// columns needed to feed other plans
	columns []int

	// arguments that need to be copied from the LHS/RHS
	vars map[string]int

	// the children of this plan
	lhs, rhs queryTree

	leftJoin bool
}

var _ queryTree = (*joinTree)(nil)

func (jp *joinTree) tableID() semantics.TableSet {
	return jp.lhs.tableID().Merge(jp.rhs.tableID())
}

func (jp *joinTree) clone() queryTree {
	result := &joinTree{
		lhs:      jp.lhs.clone(),
		rhs:      jp.rhs.clone(),
		leftJoin: jp.leftJoin,
		vars:     jp.vars,
	}
	return result
}

func (jp *joinTree) cost() int {
	return jp.lhs.cost() + jp.rhs.cost()
}

func (jp *joinTree) pushOutputColumns(columns []*sqlparser.ColName, semTable *semantics.SemTable) ([]int, error) {
	var toTheLeft []bool
	var lhs, rhs []*sqlparser.ColName
	for _, col := range columns {
		col.Qualifier.Qualifier = sqlparser.NewTableIdent("")
		if semTable.RecursiveDeps(col).IsSolvedBy(jp.lhs.tableID()) {
			lhs = append(lhs, col)
			toTheLeft = append(toTheLeft, true)
		} else {
			rhs = append(rhs, col)
			toTheLeft = append(toTheLeft, false)
		}
	}
	lhsOffset, err := jp.lhs.pushOutputColumns(lhs, semTable)
	if err != nil {
		return nil, err
	}
	rhsOffset, err := jp.rhs.pushOutputColumns(rhs, semTable)
	if err != nil {
		return nil, err
	}

	outputColumns := make([]int, len(toTheLeft))
	var l, r int
	for i, isLeft := range toTheLeft {
		outputColumns[i] = i
		if isLeft {
			jp.columns = append(jp.columns, -lhsOffset[l]-1)
			l++
		} else {
			jp.columns = append(jp.columns, rhsOffset[r]+1)
			r++
		}
	}
	return outputColumns, nil
}

func (jp *joinTree) pushPredicate(ctx *planningContext, expr sqlparser.Expr) error {
	if ctx.semTable.RecursiveDeps(expr).IsSolvedBy(jp.lhs.tableID()) {
		return jp.lhs.pushPredicate(ctx, expr)
	} else if ctx.semTable.RecursiveDeps(expr).IsSolvedBy(jp.rhs.tableID()) {
		return jp.rhs.pushPredicate(ctx, expr)
	}
	return vterrors.New(vtrpc.Code_UNIMPLEMENTED, "pushPredicate does not work on joinTrees with predicates having dependencies from both the sides")
}

func (jp *joinTree) removePredicate(ctx *planningContext, expr sqlparser.Expr) error {
	if ctx.semTable.RecursiveDeps(expr).IsSolvedBy(jp.lhs.tableID()) {
		return jp.lhs.removePredicate(ctx, expr)
	} else if ctx.semTable.RecursiveDeps(expr).IsSolvedBy(jp.rhs.tableID()) {
		return jp.rhs.removePredicate(ctx, expr)
	}
	return vterrors.New(vtrpc.Code_UNIMPLEMENTED, "removePredicate does not work on joinTrees with predicates having dependencies from both the sides")
}
