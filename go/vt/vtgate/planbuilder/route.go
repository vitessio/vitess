/*
Copyright 2019 The Vitess Authors.

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ logicalPlan = (*route)(nil)

// route is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
type route struct {
	order int

	// Redirect may point to another route if this route
	// was merged with it. The Resolve function chases
	// this pointer till the last un-redirected route.
	Redirect *route

	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select sqlparser.SelectStatement

	// resultColumns represent the columns returned by this route.
	resultColumns []*resultColumn

	// weight_string keeps track of the weight_string expressions
	// that were added additionally for each column. These expressions
	// are added to be used for collation of text columns.
	weightStrings map[*resultColumn]int

	// substitutions contain the list of table expressions that
	// have to be substituted in the route's query.
	substitutions []*tableSubstitution

	// condition stores the AST condition that will be used
	// to resolve the ERoute Values field.
	condition sqlparser.Expr

	// eroute is the primitive being built.
	eroute *engine.Route

	// tables keeps track of which tables this route is covering
	tables semantics.TableSet
}

// Order implements the logicalPlan interface
func (rb *route) Order() int {
	return rb.order
}

// Reorder implements the logicalPlan interface
func (rb *route) Reorder(order int) {
	rb.order = order + 1
}

// Primitive implements the logicalPlan interface
func (rb *route) Primitive() engine.Primitive {
	return rb.eroute
}

// ResultColumns implements the logicalPlan interface
func (rb *route) ResultColumns() []*resultColumn {
	return rb.resultColumns
}

// PushAnonymous pushes an anonymous expression like '*' or NEXT VALUES
// into the select expression list of the route. This function is
// similar to PushSelect.
func (rb *route) PushAnonymous(expr sqlparser.SelectExpr) *resultColumn {
	// TODO: we should not assume that the query is a SELECT
	sel := rb.Select.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, expr)

	// We just create a place-holder resultColumn. It won't
	// match anything.
	rc := &resultColumn{column: &column{origin: rb}}
	rb.resultColumns = append(rb.resultColumns, rc)

	return rc
}

// SetLimit adds a LIMIT clause to the route.
func (rb *route) SetLimit(limit *sqlparser.Limit) {
	rb.Select.SetLimit(limit)
}

// WireupGen4 implements the logicalPlan interface
func (rb *route) WireupGen4(_ *semantics.SemTable) error {
	rb.prepareTheAST()

	rb.eroute.Query = sqlparser.String(rb.Select)

	buffer := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery)
	node := buffer.WriteNode(rb.Select)
	query := node.ParsedQuery()
	rb.eroute.FieldQuery = query.Query
	return nil
}

// ContainsTables implements the logicalPlan interface
func (rb *route) ContainsTables() semantics.TableSet {
	return rb.tables
}

// Wireup implements the logicalPlan interface
func (rb *route) Wireup(plan logicalPlan, jt *jointab) error {
	// Precaution: update ERoute.Values only if it's not set already.
	if rb.eroute.Values == nil {
		// Resolve values stored in the logical plan.
		switch vals := rb.condition.(type) {
		case *sqlparser.ComparisonExpr:
			pv, err := rb.procureValues(plan, jt, vals.Right)
			if err != nil {
				return err
			}
			rb.eroute.Values = []sqltypes.PlanValue{pv}
			vals.Right = sqlparser.ListArg(engine.ListVarName)
		case nil:
			// no-op.
		default:
			pv, err := rb.procureValues(plan, jt, vals)
			if err != nil {
				return err
			}
			rb.eroute.Values = []sqltypes.PlanValue{pv}
		}
	}

	// Fix up the AST.
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Select:
			if len(node.SelectExprs) == 0 {
				node.SelectExprs = []sqlparser.SelectExpr{
					&sqlparser.AliasedExpr{
						Expr: sqlparser.NewIntLiteral("1"),
					},
				}
			}
		case *sqlparser.ComparisonExpr:
			if node.Operator == sqlparser.EqualOp {
				if rb.exprIsValue(node.Left) && !rb.exprIsValue(node.Right) {
					node.Left, node.Right = node.Right, node.Left
				}
			}
		}
		return true, nil
	}, rb.Select)

	// Substitute table names
	for _, sub := range rb.substitutions {
		*sub.oldExpr = *sub.newExpr
	}

	// Generate query while simultaneously resolving values.
	varFormatter := func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			if !rb.isLocal(node) {
				joinVar := jt.Procure(plan, node, rb.Order())
				buf.WriteArg(":", joinVar)
				return
			}
		case sqlparser.TableName:
			if !sqlparser.SystemSchema(node.Qualifier.String()) {
				node.Name.Format(buf)
				return
			}
			node.Format(buf)
			return
		}
		node.Format(buf)
	}
	buf := sqlparser.NewTrackedBuffer(varFormatter)
	varFormatter(buf, rb.Select)
	rb.eroute.Query = buf.ParsedQuery().Query
	rb.eroute.FieldQuery = rb.generateFieldQuery(rb.Select, jt)
	return nil
}

// prepareTheAST does minor fixups of the SELECT struct before producing the query string
func (rb *route) prepareTheAST() {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Select:
			if len(node.SelectExprs) == 0 {
				node.SelectExprs = []sqlparser.SelectExpr{
					&sqlparser.AliasedExpr{
						Expr: sqlparser.NewIntLiteral("1"),
					},
				}
			}
		case *sqlparser.ComparisonExpr:
			// 42 = colName -> colName = 42
			b := node.Operator == sqlparser.EqualOp
			value := sqlparser.IsValue(node.Left)
			name := sqlparser.IsColName(node.Right)
			if b &&
				value &&
				name {
				node.Left, node.Right = node.Right, node.Left
			}
		}
		return true, nil
	}, rb.Select)
}

// procureValues procures and converts the input into
// the expected types for rb.Values.
func (rb *route) procureValues(plan logicalPlan, jt *jointab, val sqlparser.Expr) (sqltypes.PlanValue, error) {
	switch val := val.(type) {
	case sqlparser.ValTuple:
		pv := sqltypes.PlanValue{}
		for _, val := range val {
			v, err := rb.procureValues(plan, jt, val)
			if err != nil {
				return pv, err
			}
			pv.Values = append(pv.Values, v)
		}
		return pv, nil
	case *sqlparser.ColName:
		joinVar := jt.Procure(plan, val, rb.Order())
		return sqltypes.PlanValue{Key: joinVar}, nil
	default:
		return sqlparser.NewPlanValue(val)
	}
}

func (rb *route) isLocal(col *sqlparser.ColName) bool {
	return col.Metadata.(*column).Origin() == rb
}

// generateFieldQuery generates a query with an impossible where.
// This will be used on the RHS node to fetch field info if the LHS
// returns no result.
func (rb *route) generateFieldQuery(sel sqlparser.SelectStatement, jt *jointab) string {
	formatter := func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			if !rb.isLocal(node) {
				_, joinVar := jt.Lookup(node)
				buf.WriteArg(":", joinVar)
				return
			}
		case sqlparser.TableName:
			if !sqlparser.SystemSchema(node.Qualifier.String()) {
				node.Name.Format(buf)
				return
			}
			node.Format(buf)
			return
		}
		sqlparser.FormatImpossibleQuery(buf, node)
	}

	buffer := sqlparser.NewTrackedBuffer(formatter)
	node := buffer.WriteNode(sel)
	query := node.ParsedQuery()
	return query.Query
}

// SupplyVar implements the logicalPlan interface
func (rb *route) SupplyVar(int, int, *sqlparser.ColName, string) {
	// route is an atomic primitive. So, SupplyVar cannot be
	// called on it.
	panic("BUG: route is an atomic node.")
}

// SupplyCol implements the logicalPlan interface
func (rb *route) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	c := col.Metadata.(*column)
	for i, rc := range rb.resultColumns {
		if rc.column == c {
			return rc, i
		}
	}

	// A new result has to be returned.
	rc = &resultColumn{column: c}
	rb.resultColumns = append(rb.resultColumns, rc)
	// TODO: we should not assume that the query is a SELECT query
	sel := rb.Select.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, &sqlparser.AliasedExpr{Expr: col})
	return rc, len(rb.resultColumns) - 1
}

// SupplyWeightString implements the logicalPlan interface
func (rb *route) SupplyWeightString(colNumber int, alsoAddToGroupBy bool) (weightcolNumber int, err error) {
	rc := rb.resultColumns[colNumber]
	s, ok := rb.Select.(*sqlparser.Select)
	if !ok {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected AST struct for query")
	}

	aliasExpr, ok := s.SelectExprs[colNumber].(*sqlparser.AliasedExpr)
	if !ok {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected AST struct for query %T", s.SelectExprs[colNumber])
	}
	weightStringExpr := &sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent("weight_string"),
		Exprs: []sqlparser.SelectExpr{
			&sqlparser.AliasedExpr{
				Expr: aliasExpr.Expr,
			},
		},
	}
	expr := &sqlparser.AliasedExpr{
		Expr: weightStringExpr,
	}
	if alsoAddToGroupBy {
		sel, isSelect := rb.Select.(*sqlparser.Select)
		if !isSelect {
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "cannot add weight string in %T", rb.Select)
		}
		sel.GroupBy = append(sel.GroupBy, weightStringExpr)
	}

	if weightcolNumber, ok := rb.weightStrings[rc]; ok {
		return weightcolNumber, nil
	}
	// It's ok to pass nil for pb and logicalPlan because PushSelect doesn't use them.
	// TODO: we are ignoring a potential error here. need to clean this up
	_, _, weightcolNumber, err = planProjection(nil, rb, expr, nil)
	if err != nil {
		return 0, err
	}
	rb.weightStrings[rc] = weightcolNumber
	return weightcolNumber, nil
}

// Rewrite implements the logicalPlan interface
func (rb *route) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 0 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "route: wrong number of inputs")
	}
	return nil
}

// Inputs implements the logicalPlan interface
func (rb *route) Inputs() []logicalPlan {
	return []logicalPlan{}
}

func (rb *route) isSingleShard() bool {
	switch rb.eroute.Opcode {
	case engine.SelectUnsharded, engine.SelectDBA, engine.SelectNext, engine.SelectEqualUnique, engine.SelectReference:
		return true
	}
	return false
}

func (rb *route) unionCanMerge(other *route, distinct bool) bool {
	if rb.eroute.Keyspace.Name != other.eroute.Keyspace.Name {
		return false
	}
	switch rb.eroute.Opcode {
	case engine.SelectUnsharded, engine.SelectReference:
		return rb.eroute.Opcode == other.eroute.Opcode
	case engine.SelectDBA:
		return other.eroute.Opcode == engine.SelectDBA &&
			len(rb.eroute.SysTableTableSchema) == 0 &&
			len(rb.eroute.SysTableTableName) == 0 &&
			len(other.eroute.SysTableTableSchema) == 0 &&
			len(other.eroute.SysTableTableName) == 0
	case engine.SelectEqualUnique:
		// Check if they target the same shard.
		if other.eroute.Opcode == engine.SelectEqualUnique && rb.eroute.Vindex == other.eroute.Vindex && valEqual(rb.condition, other.condition) {
			return true
		}
	case engine.SelectScatter:
		return other.eroute.Opcode == engine.SelectScatter && !distinct
	case engine.SelectNext:
		return false
	}
	return false
}

func (rb *route) updateRoute(opcode engine.RouteOpcode, vindex vindexes.SingleColumn, condition sqlparser.Expr) {
	rb.eroute.Opcode = opcode
	rb.eroute.Vindex = vindex
	rb.condition = condition
}

// computeNotInPlan looks for null values to produce a SelectNone if found
func (rb *route) computeNotInPlan(right sqlparser.Expr) engine.RouteOpcode {
	switch node := right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if sqlparser.IsNull(n) {
				return engine.SelectNone
			}
		}
	}

	return engine.SelectScatter
}

// exprIsValue returns true if the expression can be treated as a value
// for the routeOption. External references are treated as value.
func (rb *route) exprIsValue(expr sqlparser.Expr) bool {
	if node, ok := expr.(*sqlparser.ColName); ok {
		return node.Metadata.(*column).Origin() != rb
	}
	return sqlparser.IsValue(expr)
}
