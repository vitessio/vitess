/*
Copyright 2017 Google Inc.

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
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
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
	bldr, err = checkAggregates(sel, bldr)
	if err != nil {
		return nil, err
	}
	bldr, err = pushSelectExprs(sel, bldr)
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
	bldr, err = pushLimit(sel.Limit, bldr)
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
		origin, err := findOrigin(filter, bldr)
		if err != nil {
			return err
		}
		if err := bldr.PushFilter(filter, whereType, origin); err != nil {
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
func pushSelectExprs(sel *sqlparser.Select, bldr builder) (builder, error) {
	resultColumns, err := pushSelectRoutes(sel.SelectExprs, bldr)
	if err != nil {
		return nil, err
	}
	bldr.Symtab().ResultColumns = resultColumns

	if err := pushGroupBy(sel, bldr); err != nil {
		return nil, err
	}
	return bldr, nil
}

// pusheSelectRoutes is a convenience function that pushes all the select
// expressions and returns the list of resultColumns generated for it.
func pushSelectRoutes(selectExprs sqlparser.SelectExprs, bldr builder) ([]*resultColumn, error) {
	resultColumns := make([]*resultColumn, len(selectExprs))
	for i, node := range selectExprs {
		switch node := node.(type) {
		case *sqlparser.AliasedExpr:
			origin, err := findOrigin(node.Expr, bldr)
			if err != nil {
				return nil, err
			}
			resultColumns[i], _, err = bldr.PushSelect(node, origin)
			if err != nil {
				return nil, err
			}
		case *sqlparser.StarExpr:
			// We'll allow select * for simple routes.
			rb, ok := bldr.(*route)
			if !ok {
				return nil, errors.New("unsupported: '*' expression in cross-shard query")
			}
			// Validate keyspace reference if any.
			if !node.TableName.IsEmpty() {
				if qual := node.TableName.Qualifier; !qual.IsEmpty() {
					if qual.String() != rb.ERoute.Keyspace.Name {
						return nil, fmt.Errorf("cannot resolve %s to keyspace %s", sqlparser.String(node), rb.ERoute.Keyspace.Name)
					}
				}
			}
			resultColumns[i] = rb.PushAnonymous(node)
		case sqlparser.Nextval:
			rb, ok := bldr.(*route)
			if !ok {
				// This code is unreachable because the parser doesn't allow joins for next val statements.
				return nil, errors.New("unsupported: SELECT NEXT query in cross-shard query")
			}
			if err := rb.SetOpcode(engine.SelectNext); err != nil {
				return nil, err
			}
			resultColumns[i] = rb.PushAnonymous(node)
		default:
			panic(fmt.Sprintf("BUG: unexpceted select expression type: %T", node))
		}
	}
	return resultColumns, nil
}
