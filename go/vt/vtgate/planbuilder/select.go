// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// buildSelectPlan is the new function to build a Select plan.
func buildSelectPlan(sel *sqlparser.Select, vschema VSchema) (primitive engine.Primitive, err error) {
	bindvars := sqlparser.GetBindvars(sel)
	builder, err := processSelect(sel, vschema, nil)
	if err != nil {
		return nil, err
	}
	jt := newJointab(bindvars)
	err = builder.Wireup(builder, jt)
	if err != nil {
		return nil, err
	}
	return builder.Primitive(), nil
}

// processSelect builds a primitive tree for the given query or subquery.
func processSelect(sel *sqlparser.Select, vschema VSchema, outer builder) (builder, error) {
	bldr, err := processTableExprs(sel.From, vschema)
	if err != nil {
		return nil, err
	}
	if outer != nil {
		bldr.Symtab().Outer = outer.Symtab()
	}
	if sel.Where != nil {
		err = pushFilter(sel.Where.Expr, bldr, sqlparser.WhereStr)
		if err != nil {
			return nil, err
		}
	}
	err = pushSelectExprs(sel, bldr)
	if err != nil {
		return nil, err
	}
	if sel.Having != nil {
		err = pushFilter(sel.Having.Expr, bldr, sqlparser.HavingStr)
		if err != nil {
			return nil, err
		}
	}
	err = pushOrderBy(sel.OrderBy, bldr)
	if err != nil {
		return nil, err
	}
	err = pushLimit(sel.Limit, bldr)
	if err != nil {
		return nil, err
	}
	bldr.PushMisc(sel)
	return bldr, nil
}

// pushFilter identifies the target route for the specified bool expr,
// pushes it down, and updates the route info if the new constraint improves
// the primitive. This function can push to a WHERE or HAVING clause.
func pushFilter(boolExpr sqlparser.Expr, bldr builder, whereType string) error {
	filters := splitAndExpression(nil, boolExpr)
	reorderBySubquery(filters)
	for _, filter := range filters {
		rb, err := findRoute(filter, bldr)
		if err != nil {
			return err
		}
		err = rb.PushFilter(filter, whereType)
		if err != nil {
			return err
		}
	}
	return nil
}

// reorderBySubquery reorders the filters by pushing subqueries
// to the end. This allows the non-subquery filters to be
// pushed first because they can potentially improve the routing
// plan, which can later allow a filter containing a subquery
// to successfully merge with the corresponding route.
func reorderBySubquery(filters []sqlparser.Expr) {
	max := len(filters)
	for i := 0; i < max; i++ {
		if !hasSubquery(filters[i]) {
			continue
		}
		saved := filters[i]
		for j := i; j < len(filters)-1; j++ {
			filters[j] = filters[j+1]
		}
		filters[len(filters)-1] = saved
		max--
	}
}

// pushSelectExprs identifies the target route for the
// select expressions and pushes them down.
func pushSelectExprs(sel *sqlparser.Select, bldr builder) error {
	err := checkAggregates(sel, bldr)
	if err != nil {
		return err
	}
	if sel.Distinct != "" {
		// We know it's a route, but this may change
		// in the distant future.
		bldr.(*route).MakeDistinct()
	}
	colsyms, err := pushSelectRoutes(sel.SelectExprs, bldr)
	if err != nil {
		return err
	}
	bldr.Symtab().Colsyms = colsyms
	err = pushGroupBy(sel.GroupBy, bldr)
	if err != nil {
		return err
	}
	return nil
}

// checkAggregates returns an error if the select statement
// has aggregates that cannot be pushed down due to a complex
// plan.
func checkAggregates(sel *sqlparser.Select, bldr builder) error {
	hasAggregates := false
	if sel.Distinct != "" {
		hasAggregates = true
	} else {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.FuncExpr:
				if node.IsAggregate() {
					hasAggregates = true
					return false, errors.New("dummy")
				}
			case *sqlparser.GroupConcatExpr:
				hasAggregates = true
				return false, errors.New("dummy")
			}
			return true, nil
		}, sel.SelectExprs)
	}
	if !hasAggregates {
		return nil
	}

	// Check if we can allow aggregates.
	rb, ok := bldr.(*route)
	if !ok {
		return errors.New("unsupported: complex join with aggregates")
	}
	if rb.IsSingle() {
		return nil
	}
	// It's a scatter rb. We can allow aggregates if there is a unique
	// vindex in the select list.
	for _, selectExpr := range sel.SelectExprs {
		switch selectExpr := selectExpr.(type) {
		case *sqlparser.NonStarExpr:
			vindex := bldr.Symtab().Vindex(selectExpr.Expr, rb, true)
			if vindex != nil && vindexes.IsUnique(vindex) {
				return nil
			}
		}
	}
	return errors.New("unsupported: scatter with aggregates")
}

// pusheSelectRoutes is a convenience function that pushes all the select
// expressions and returns the list of colsyms generated for it.
func pushSelectRoutes(selectExprs sqlparser.SelectExprs, bldr builder) ([]*colsym, error) {
	colsyms := make([]*colsym, len(selectExprs))
	for i, node := range selectExprs {
		switch node := node.(type) {
		case *sqlparser.NonStarExpr:
			rb, err := findRoute(node.Expr, bldr)
			if err != nil {
				return nil, err
			}
			colsyms[i], _, err = bldr.PushSelect(node, rb)
			if err != nil {
				return nil, err
			}
		case *sqlparser.StarExpr:
			// We'll allow select * for simple routes.
			rb, ok := bldr.(*route)
			if !ok {
				return nil, errors.New("unsupported: '*' expression in complex join")
			}
			// Validate keyspace reference if any.
			if !node.TableName.IsEmpty() {
				if qual := node.TableName.Qualifier; !qual.IsEmpty() {
					if qual.String() != rb.ERoute.Keyspace.Name {
						return nil, fmt.Errorf("cannot resolve %s to keyspace %s", sqlparser.String(node), rb.ERoute.Keyspace.Name)
					}
				}
			}
			// We can push without validating the reference because
			// MySQL will fail if it's invalid.
			colsyms[i] = rb.PushStar(node)
		case sqlparser.Nextval:
			// For now, this is only supported as an implicit feature
			// for auto_inc in inserts.
			return nil, errors.New("unsupported: NEXT VALUES construct")
		}
	}
	return colsyms, nil
}
