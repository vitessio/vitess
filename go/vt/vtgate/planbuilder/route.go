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
)

var _ logicalPlan = (*route)(nil)

// route is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
type route struct {
	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select sqlparser.SelectStatement

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
	return 1
}

// Reorder implements the logicalPlan interface
func (rb *route) Reorder(int) {}

// Primitive implements the logicalPlan interface
func (rb *route) Primitive() engine.Primitive { return rb.eroute }

// ResultColumns implements the logicalPlan interface
func (rb *route) ResultColumns() []*resultColumn { return nil }

// SetLimit adds a LIMIT clause to the route.
func (rb *route) SetLimit(limit *sqlparser.Limit) {rb.Select.SetLimit(limit)}

// WireupGen4 implements the logicalPlan interface
func (rb *route) WireupGen4(*semantics.SemTable) error {
	rb.prepareTheAST()

	rb.eroute.Query = sqlparser.String(rb.Select)

	buffer := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery)
	node := buffer.WriteNode(rb.Select)
	query := node.ParsedQuery()
	rb.eroute.FieldQuery = query.Query
	return nil
}

func (rb *route) ContainsTables() semantics.TableSet {
	return rb.tables
}

// Wireup implements the logicalPlan interface
func (rb *route) Wireup(logicalPlan, *jointab) error {
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
func (rb *route) SupplyCol(*sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("gen4 only, baby")
}

// SupplyWeightString implements the logicalPlan interface
func (rb *route) SupplyWeightString(int, bool) (weightcolNumber int, err error) {
	panic("gen4 only, baby")
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
