// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

type filterBuilder struct {
	order   int
	Input   planBuilder
	symtab  *symtab
	colsyms []*colsym
	Filter  *Filter
}

func newFilterBuilder(boolExpr sqlparser.BoolExpr, plan planBuilder) (*filterBuilder, error) {
	comparison, ok := boolExpr.(*sqlparser.ComparisonExpr)
	if !ok {
		return nil, errors.New("unsupported: subquery: not a comparison")
	}
	if comparison.Operator != sqlparser.InStr {
		return nil, errors.New("unsupported: subquery: not an IN clause")
	}
	// TODO(sougou): allow more constructs here.
	left, ok := comparison.Left.(*sqlparser.ColName)
	if !ok {
		return nil, errors.New("unsupported: subquery: no col ref in IN clause")
	}
	right, ok := comparison.Right.(*sqlparser.Subquery)
	if !ok {
		return nil, errors.New("unsupported: subquery: RHS is not a subquery")
	}
	sub, ok := right.Select.(*sqlparser.Select)
	if !ok {
		return nil, errors.New("unsupported: subquery: subquery is not a SELECT")
	}
	// TODO(sougou): Verify that only one column is being returned.
	subplan, err := processSelect(sub, plan.Symtab().VSchema, plan)
	if err != nil {
		return nil, err
	}
	fl := &filterBuilder{
		Input:   plan,
		symtab:  plan.Symtab(),
		colsyms: plan.Colsyms(),
		order:   plan.Order(),
		Filter: &Filter{
			Input: getUnderlyingPlan(plan),
			Condition: &INClause{
				// TODO(sougou): This should be just 'left'
				// once wire-up is implemented.
				Left: sqlparser.String(left),
				Right: &Pivot{
					Input: getUnderlyingPlan(subplan),
				},
			},
		},
	}
	fl.Filter.Cols = make([]int, len(plan.Colsyms()))
	for i := range fl.Filter.Cols {
		fl.Filter.Cols[i] = i
	}
	return fl, nil
}

// Symtab returns the associated symtab.
func (fl *filterBuilder) Symtab() *symtab {
	return fl.symtab
}

// Colsyms returns the colsyms.
func (fl *filterBuilder) Colsyms() []*colsym {
	return fl.colsyms
}

// Order returns the order of the node.
func (fl *filterBuilder) Order() int {
	return fl.order
}

// PushSelect pushes the select expression into the join and
// recursively down.
func (fl *filterBuilder) PushSelect(expr *sqlparser.NonStarExpr, route *routeBuilder) (colsym *colsym, colnum int, err error) {
	colsym, colnum, err = fl.Input.PushSelect(expr, route)
	if err != nil {
		return nil, 0, err
	}
	fl.Filter.Cols = append(fl.Filter.Cols, colnum)
	fl.colsyms = append(fl.colsyms, colsym)
	return colsym, len(fl.colsyms) - 1, nil
}

// SupplyVar updates the filter to make it supply the requested
// column as a join variable. If the column is not already in
// its list, it requests the Input node to supply it using SupplyCol.
func (fl *filterBuilder) SupplyVar(col *sqlparser.ColName, varname string) {
	if _, ok := fl.Filter.Vars[varname]; ok {
		// Looks like somebody else already requested this.
		return
	}
	switch meta := col.Metadata.(type) {
	case *colsym:
		for i, colsym := range fl.colsyms {
			if meta == colsym {
				fl.Filter.Vars[varname] = fl.Filter.Cols[i]
				return
			}
		}
		panic("unexpected: column not found")
	case *tableAlias:
		ref := newColref(col)
		for i, colsym := range fl.colsyms {
			if colsym.Underlying == ref {
				fl.Filter.Vars[varname] = fl.Filter.Cols[i]
				return
			}
		}
		fl.Filter.Vars[varname] = fl.Input.SupplyCol(col)
		return
	}
	panic("unreachable")
}

// SupplyCol changes the filter to supply the requested column
// name, and returns the result column number. If the column
// is already in the list, it's reused.
func (fl *filterBuilder) SupplyCol(col *sqlparser.ColName) int {
	ref := newColref(col)
	for i, colsym := range fl.colsyms {
		if colsym.Underlying == ref {
			return i
		}
	}
	ret := fl.Input.SupplyCol(col)
	fl.Filter.Cols = append(fl.Filter.Cols, ret)
	fl.colsyms = append(fl.colsyms, &colsym{Underlying: ref})
	return len(fl.Filter.Cols) - 1
}
