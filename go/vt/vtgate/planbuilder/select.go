// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

// buildSelectPlan2 is the new function to build a Select plan.
// TODO(sougou): rename after deprecating old one.
func buildSelectPlan(sel *sqlparser.Select, vschema *VSchema) (plan interface{}, err error) {
	builder, _, err := processSelect(sel, vschema, nil)
	if err != nil {
		return nil, err
	}
	err = newGenerator(builder).Generate()
	if err != nil {
		return nil, err
	}
	return getUnderlyingPlan(builder), nil
}

// processSelect builds a plan for the given query or subquery.
func processSelect(sel *sqlparser.Select, vschema *VSchema, outer *symtab) (planBuilder, *symtab, error) {
	plan, syms, err := processTableExprs(sel.From, vschema)
	if err != nil {
		return nil, nil, err
	}
	syms.Outer = outer
	if sel.Where != nil {
		err = processBoolExpr(sel.Where.Expr, syms, sqlparser.WhereStr)
		if err != nil {
			return nil, nil, err
		}
	}
	err = processSelectExprs(sel, plan, syms)
	if err != nil {
		return nil, nil, err
	}
	if sel.Having != nil {
		err = processBoolExpr(sel.Having.Expr, syms, sqlparser.HavingStr)
		if err != nil {
			return nil, nil, err
		}
	}
	err = processOrderBy(sel.OrderBy, syms)
	if err != nil {
		return nil, nil, err
	}
	err = processLimit(sel.Limit, plan)
	if err != nil {
		return nil, nil, err
	}
	processMisc(sel, plan)
	return plan, syms, nil
}
