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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// buildSelectPlan is the new function to build a Select plan.
func buildSelectPlan(sel *sqlparser.Select, vschema ContextVSchema) (primitive engine.Primitive, err error) {
	pb := newPrimitiveBuilder(vschema, newJointab(sqlparser.GetBindvars(sel)))
	if err := pb.processSelect(sel, nil); err != nil {
		return nil, err
	}
	if err := pb.bldr.Wireup(pb.bldr, pb.jt); err != nil {
		return nil, err
	}
	return pb.bldr.Primitive(), nil
}

// processSelect builds a primitive tree for the given query or subquery.
// The tree built by this function has the following general structure:
//
// The leaf nodes can be a route, vindexFunc or subquery. In the symtab,
// the tables map has columns that point to these leaf nodes. A subquery
// itself contains a builder tree, but it's opaque and is made to look
// like a table for the analysis of the current tree.
//
// The leaf nodes are usually tied together by join nodes. While the join
// nodes are built, they have ON clauses. Those are analyzed and pushed
// down into the leaf nodes as the tree is formed. Join nodes are formed
// during analysis of the FROM clause.
//
// During the WHERE clause analysis, the target leaf node is identified
// for each part, and the PushFilter function is used to push the condition
// down. The same strategy is used for the other clauses.
//
// So, a typical plan would either be a simple leaf node, or may consist
// of leaf nodes tied together by join nodes.
//
// If a query has aggregates that cannot be pushed down, an aggregator
// primitive is built. The current orderedAggregate primitive can only
// be built on top of a route. The orderedAggregate expects the rows
// to be ordered as they are returned. This work is performed by the
// underlying route. This means that a compatible ORDER BY clause
// can also be handled by this combination of primitives. In this case,
// the tree would consist of an orderedAggregate whose input is a route.
//
// If a query has an ORDER BY, but the route is a scatter, then the
// ordering is pushed down into the route itself. This results in a simple
// route primitive.
//
// The LIMIT clause is the last construct of a query. If it cannot be
// pushed into a route, then a primitve is created on top of any
// of the above trees to make it discard unwanted rows.
func (pb *primitiveBuilder) processSelect(sel *sqlparser.Select, outer *symtab) error {
	if err := pb.processTableExprs(sel.From); err != nil {
		return err
	}

	if rb, ok := pb.bldr.(*route); ok {
		// TODO(sougou): this can probably be improved.
		for _, ro := range rb.routeOptions {
			directives := sqlparser.ExtractCommentDirectives(sel.Comments)
			ro.eroute.QueryTimeout = queryTimeout(directives)
			if ro.eroute.TargetDestination != nil {
				return errors.New("unsupported: SELECT with a target destination")
			}

			if directives.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings) {
				ro.eroute.ScatterErrorsAsWarnings = true
			}
		}
	}

	// Set the outer symtab after processing of FROM clause.
	// This is because correlation is not allowed there.
	pb.st.Outer = outer
	if sel.Where != nil {
		if err := pb.pushFilter(sel.Where.Expr, sqlparser.WhereStr); err != nil {
			return err
		}
	}
	if err := pb.checkAggregates(sel); err != nil {
		return err
	}
	if err := pb.pushSelectExprs(sel); err != nil {
		return err
	}
	if sel.Having != nil {
		if err := pb.pushFilter(sel.Having.Expr, sqlparser.HavingStr); err != nil {
			return err
		}
	}
	if err := pb.pushOrderBy(sel.OrderBy); err != nil {
		return err
	}
	if err := pb.pushLimit(sel.Limit); err != nil {
		return err
	}
	pb.bldr.PushMisc(sel)
	return nil
}

// pushFilter identifies the target route for the specified bool expr,
// pushes it down, and updates the route info if the new constraint improves
// the primitive. This function can push to a WHERE or HAVING clause.
func (pb *primitiveBuilder) pushFilter(boolExpr sqlparser.Expr, whereType string) error {
	filters := splitAndExpression(nil, boolExpr)
	reorderBySubquery(filters)
	for _, filter := range filters {
		pullouts, origin, expr, err := pb.findOrigin(filter)
		if err != nil {
			return err
		}
		// The returned expression may be complex. Resplit before pushing.
		for _, subexpr := range splitAndExpression(nil, expr) {
			if err := pb.bldr.PushFilter(pb, subexpr, whereType, origin); err != nil {
				return err
			}
		}
		pb.addPullouts(pullouts)
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

// addPullouts adds the pullout subqueries to the primitiveBuilder.
func (pb *primitiveBuilder) addPullouts(pullouts []*pulloutSubquery) {
	for _, pullout := range pullouts {
		pullout.setUnderlying(pb.bldr)
		pb.bldr = pullout
		pb.bldr.Reorder(0)
	}
}

// pushSelectExprs identifies the target route for the
// select expressions and pushes them down.
func (pb *primitiveBuilder) pushSelectExprs(sel *sqlparser.Select) error {
	resultColumns, err := pb.pushSelectRoutes(sel.SelectExprs)
	if err != nil {
		return err
	}
	pb.st.SetResultColumns(resultColumns)
	return pb.pushGroupBy(sel)
}

// pusheSelectRoutes is a convenience function that pushes all the select
// expressions and returns the list of resultColumns generated for it.
func (pb *primitiveBuilder) pushSelectRoutes(selectExprs sqlparser.SelectExprs) ([]*resultColumn, error) {
	resultColumns := make([]*resultColumn, 0, len(selectExprs))
	for _, node := range selectExprs {
		switch node := node.(type) {
		case *sqlparser.AliasedExpr:
			pullouts, origin, expr, err := pb.findOrigin(node.Expr)
			if err != nil {
				return nil, err
			}
			node.Expr = expr
			rc, _, err := pb.bldr.PushSelect(pb, node, origin)
			if err != nil {
				return nil, err
			}
			resultColumns = append(resultColumns, rc)
			pb.addPullouts(pullouts)
		case *sqlparser.StarExpr:
			var expanded bool
			var err error
			resultColumns, expanded, err = pb.expandStar(resultColumns, node)
			if err != nil {
				return nil, err
			}
			if expanded {
				continue
			}
			// We'll allow select * for simple routes.
			rb, ok := pb.bldr.(*route)
			if !ok {
				return nil, errors.New("unsupported: '*' expression in cross-shard query")
			}
			// Validate keyspace reference if any.
			if !node.TableName.IsEmpty() {
				if _, err := pb.st.FindTable(node.TableName); err != nil {
					return nil, err
				}
			}
			resultColumns = append(resultColumns, rb.PushAnonymous(node))
		case sqlparser.Nextval:
			rb, ok := pb.bldr.(*route)
			if !ok {
				// This code is unreachable because the parser doesn't allow joins for next val statements.
				return nil, errors.New("unsupported: SELECT NEXT query in cross-shard query")
			}
			for _, ro := range rb.routeOptions {
				if ro.eroute.Opcode != engine.SelectNext {
					return nil, errors.New("NEXT used on a non-sequence table")
				}
				ro.eroute.Opcode = engine.SelectNext
			}
			resultColumns = append(resultColumns, rb.PushAnonymous(node))
		default:
			panic(fmt.Sprintf("BUG: unexpceted select expression type: %T", node))
		}
	}
	return resultColumns, nil
}

// expandStar expands a StarExpr and pushes the expanded
// expressions down if the tables have authoritative column lists.
// If not, it returns false.
// This function breaks the abstraction a bit: it directly sets the
// the Metadata for newly created expressions. In all other cases,
// the Metadata is set through a symtab Find.
func (pb *primitiveBuilder) expandStar(inrcs []*resultColumn, expr *sqlparser.StarExpr) (outrcs []*resultColumn, expanded bool, err error) {
	tables := pb.st.AllTables()
	if tables == nil {
		// no table metadata available.
		return inrcs, false, nil
	}
	if expr.TableName.IsEmpty() {
		for _, t := range tables {
			// All tables must have authoritative column lists.
			if !t.isAuthoritative {
				return inrcs, false, nil
			}
		}
		singleTable := false
		if len(tables) == 1 {
			singleTable = true
		}
		for _, t := range tables {
			for _, col := range t.columnNames {
				var expr *sqlparser.AliasedExpr
				if singleTable {
					// If there's only one table, we use unqualifed column names.
					expr = &sqlparser.AliasedExpr{
						Expr: &sqlparser.ColName{
							Metadata: t.columns[col.Lowered()],
							Name:     col,
						},
					}
				} else {
					// If a and b have id as their column, then
					// select * from a join b should result in
					// select a.id as id, b.id as id from a join b.
					expr = &sqlparser.AliasedExpr{
						Expr: &sqlparser.ColName{
							Metadata:  t.columns[col.Lowered()],
							Name:      col,
							Qualifier: t.alias,
						},
						As: col,
					}
				}
				rc, _, err := pb.bldr.PushSelect(pb, expr, t.Origin())
				if err != nil {
					// Unreachable because PushSelect won't fail on ColName.
					return inrcs, false, err
				}
				inrcs = append(inrcs, rc)
			}
		}
		return inrcs, true, nil
	}

	// Expression qualified with table name.
	t, err := pb.st.FindTable(expr.TableName)
	if err != nil {
		return inrcs, false, err
	}
	if !t.isAuthoritative {
		return inrcs, false, nil
	}
	for _, col := range t.columnNames {
		expr := &sqlparser.AliasedExpr{
			Expr: &sqlparser.ColName{
				Metadata:  t.columns[col.Lowered()],
				Name:      col,
				Qualifier: expr.TableName,
			},
		}
		rc, _, err := pb.bldr.PushSelect(pb, expr, t.Origin())
		if err != nil {
			// Unreachable because PushSelect won't fail on ColName.
			return inrcs, false, err
		}
		inrcs = append(inrcs, rc)
	}
	return inrcs, true, nil
}
