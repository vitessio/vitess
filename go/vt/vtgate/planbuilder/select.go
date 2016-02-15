// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

// buildSelectPlan2 is the new function to build a Select plan.
func buildSelectPlan(sel *sqlparser.Select, vschema *VSchema) (plan interface{}, err error) {
	builder, err := processSelect(sel, vschema, nil)
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
func processSelect(sel *sqlparser.Select, vschema *VSchema, outer planBuilder) (planBuilder, error) {
	plan, err := processTableExprs(sel.From, vschema)
	if err != nil {
		return nil, err
	}
	if outer != nil {
		plan.Symtab().Outer = outer.Symtab()
	}
	if sel.Where != nil {
		err = processBoolExpr(sel.Where.Expr, plan, sqlparser.WhereStr)
		if err != nil {
			return nil, err
		}
	}
	err = processSelectExprs(sel, plan)
	if err != nil {
		return nil, err
	}
	if sel.Having != nil {
		err = processBoolExpr(sel.Having.Expr, plan, sqlparser.HavingStr)
		if err != nil {
			return nil, err
		}
	}
	err = processOrderBy(sel.OrderBy, plan)
	if err != nil {
		return nil, err
	}
	err = processLimit(sel.Limit, plan)
	if err != nil {
		return nil, err
	}
	processMisc(sel, plan)
	return plan, nil
}
