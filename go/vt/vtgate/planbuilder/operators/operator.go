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

package operators

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// Operator forms the tree of operators, representing the declarative query provided.
	Operator interface {
		ThisIsAnOperator()

		Clone(inputs []Operator) Operator
		Inputs() []Operator
	}

	// PhysicalOperator means that this operator is ready to be turned into a logical plan
	PhysicalOperator interface {
		Operator
		IPhysical()
	}

	clonable interface {
		// Clone creates a copy of the operator that can be updated without changing the original
		Clone(inputs []Operator) Operator
	}

	// tableIDIntroducer is used to signal that this operator introduces data from a new source
	tableIDIntroducer interface {
		Introduces() semantics.TableSet
	}

	unresolved interface {
		// UnsolvedPredicates returns any predicates that have dependencies on the given Operator and
		// on the outside of it (a parent Select expression, any other table not used by Operator, etc).
		UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr
	}

	costly interface {
		// Cost returns the cost for this operator. All the costly operators in the tree are summed together to get the
		// total cost of the operator tree.
		// TODO: We should really calculate this using cardinality estimation,
		//       but until then this is better than nothing
		Cost() int
	}

	checked interface {
		// CheckValid allows operators that need a final check before being used, to make sure that
		// all the necessary information is in the operator
		CheckValid() error
	}

	noInputs struct{}
)

func (noInputs) Inputs() []Operator {
	return nil
}

func getOperatorFromTableExpr(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr) (Operator, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tbl := tableExpr.Expr.(type) {
		case sqlparser.TableName:
			tableID := ctx.SemTable.TableSetFor(tableExpr)
			tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
			if err != nil {
				return nil, err
			}

			if vt, isVindex := tableInfo.(*semantics.VindexTable); isVindex {
				return &Vindex{Table: VindexTable{
					TableID: tableID,
					Alias:   tableExpr,
					Table:   tbl,
					VTable:  vt.Table.GetVindexTable(),
				}, Vindex: vt.Vindex}, nil
			}
			qg := newQueryGraph()
			isInfSchema := tableInfo.IsInfSchema()
			qt := &QueryTable{Alias: tableExpr, Table: tbl, ID: tableID, IsInfSchema: isInfSchema}
			qg.Tables = append(qg.Tables, qt)
			return qg, nil
		case *sqlparser.DerivedTable:
			inner, err := CreateLogicalOperatorFromAST(ctx, tbl.Select)
			if err != nil {
				return nil, err
			}
			return &Derived{Alias: tableExpr.As.String(), Source: inner, Query: tbl.Select, ColumnAliases: tableExpr.Columns}, nil
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unable to use: %T", tbl)
		}
	case *sqlparser.JoinTableExpr:
		switch tableExpr.Join {
		case sqlparser.NormalJoinType:
			lhs, err := getOperatorFromTableExpr(ctx, tableExpr.LeftExpr)
			if err != nil {
				return nil, err
			}
			rhs, err := getOperatorFromTableExpr(ctx, tableExpr.RightExpr)
			if err != nil {
				return nil, err
			}
			op := createJoin(lhs, rhs)
			if tableExpr.Condition.On != nil {
				op, err = LogicalPushPredicate(ctx, op, sqlparser.RemoveKeyspaceFromColName(tableExpr.Condition.On))
				if err != nil {
					return nil, err
				}
			}
			return op, nil
		case sqlparser.LeftJoinType, sqlparser.RightJoinType:
			lhs, err := getOperatorFromTableExpr(ctx, tableExpr.LeftExpr)
			if err != nil {
				return nil, err
			}
			rhs, err := getOperatorFromTableExpr(ctx, tableExpr.RightExpr)
			if err != nil {
				return nil, err
			}
			if tableExpr.Join == sqlparser.RightJoinType {
				lhs, rhs = rhs, lhs
			}
			return &Join{LHS: lhs, RHS: rhs, LeftJoin: true, Predicate: sqlparser.RemoveKeyspaceFromColName(tableExpr.Condition.On)}, nil
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: %s", tableExpr.Join.ToString())
		}
	case *sqlparser.ParenTableExpr:
		return crossJoin(ctx, tableExpr.Exprs)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unable to use: %T table type", tableExpr)
	}
}

func crossJoin(ctx *plancontext.PlanningContext, exprs sqlparser.TableExprs) (Operator, error) {
	var output Operator
	for _, tableExpr := range exprs {
		op, err := getOperatorFromTableExpr(ctx, tableExpr)
		if err != nil {
			return nil, err
		}
		if output == nil {
			output = op
		} else {
			output = createJoin(output, op)
		}
	}
	return output, nil
}

func getSelect(s sqlparser.SelectStatement) *sqlparser.Select {
	switch s := s.(type) {
	case *sqlparser.Select:
		return s
	default:
		return nil
	}
}

// CreateLogicalOperatorFromAST creates an operator tree that represents the input SELECT or UNION query
func CreateLogicalOperatorFromAST(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) (op Operator, err error) {
	switch node := selStmt.(type) {
	case *sqlparser.Select:
		op, err = createOperatorFromSelect(ctx, node)
	case *sqlparser.Union:
		op, err = createOperatorFromUnion(ctx, node)
	case *sqlparser.Update:
		op, err = createOperatorFromUpdate(ctx, node)
	case *sqlparser.Delete:
		op, err = createOperatorFromDelete(ctx, node)
	default:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T: operator not yet supported", selStmt)
	}
	if err != nil {
		return nil, err
	}
	return Compact(ctx, op)
}

func createOperatorFromUnion(ctx *plancontext.PlanningContext, node *sqlparser.Union) (Operator, error) {
	opLHS, err := CreateLogicalOperatorFromAST(ctx, node.Left)
	if err != nil {
		return nil, err
	}

	_, isRHSUnion := node.Right.(*sqlparser.Union)
	if isRHSUnion {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "nesting of unions at the right-hand side is not yet supported")
	}
	opRHS, err := CreateLogicalOperatorFromAST(ctx, node.Right)
	if err != nil {
		return nil, err
	}
	return &Concatenate{
		Distinct:    node.Distinct,
		SelectStmts: []*sqlparser.Select{getSelect(node.Left), getSelect(node.Right)},
		Sources:     []Operator{opLHS, opRHS},
		OrderBy:     node.OrderBy,
		Limit:       node.Limit,
	}, nil
}

// createOperatorFromSelect creates an operator tree that represents the input SELECT query
func createOperatorFromSelect(ctx *plancontext.PlanningContext, sel *sqlparser.Select) (Operator, error) {
	subq, err := createSubqueryFromStatement(ctx, sel)
	if err != nil {
		return nil, err
	}
	op, err := crossJoin(ctx, sel.From)
	if err != nil {
		return nil, err
	}
	if sel.Where != nil {
		exprs := sqlparser.SplitAndExpression(nil, sel.Where.Expr)
		for _, expr := range exprs {
			op, err = LogicalPushPredicate(ctx, op, sqlparser.RemoveKeyspaceFromColName(expr))
			if err != nil {
				return nil, err
			}
			addColumnEquality(ctx, expr)
		}
	}
	if subq == nil {
		return op, nil
	}
	subq.Outer = op
	return subq, nil
}

func createOperatorFromUpdate(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update) (Operator, error) {
	tableInfo, qt, err := createQueryTableForDML(ctx, updStmt.TableExprs[0], updStmt.Where)
	if err != nil {
		return nil, err
	}

	assignments := make(map[string]sqlparser.Expr)
	for _, set := range updStmt.Exprs {
		assignments[set.Name.Name.String()] = set.Expr
	}

	u := &Update{
		Table:       qt,
		Assignments: assignments,
		AST:         updStmt,
		TableInfo:   tableInfo,
	}

	subq, err := createSubqueryFromStatement(ctx, updStmt)
	if err != nil {
		return nil, err
	}
	if subq == nil {
		return u, nil
	}
	subq.Outer = u
	return subq, nil
}

func createQueryTableForDML(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr, whereClause *sqlparser.Where) (semantics.TableInfo, *QueryTable, error) {
	alTbl, ok := tableExpr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected AliasedTableExpr")
	}
	tblName, ok := alTbl.Expr.(sqlparser.TableName)
	if !ok {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected TableName")
	}

	tableID := ctx.SemTable.TableSetFor(alTbl)
	tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
	if err != nil {
		return nil, nil, err
	}

	if tableInfo.IsInfSchema() {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't update information schema tables")
	}

	var predicates []sqlparser.Expr
	if whereClause != nil {
		predicates = sqlparser.SplitAndExpression(nil, whereClause.Expr)
	}
	qt := &QueryTable{
		ID:          tableID,
		Alias:       alTbl,
		Table:       tblName,
		Predicates:  predicates,
		IsInfSchema: false,
	}
	return tableInfo, qt, nil
}

func createOperatorFromDelete(ctx *plancontext.PlanningContext, deleteStmt *sqlparser.Delete) (Operator, error) {
	tableInfo, qt, err := createQueryTableForDML(ctx, deleteStmt.TableExprs[0], deleteStmt.Where)
	if err != nil {
		return nil, err
	}

	u := &Delete{
		Table:     qt,
		AST:       deleteStmt,
		TableInfo: tableInfo,
	}

	subq, err := createSubqueryFromStatement(ctx, deleteStmt)
	if err != nil {
		return nil, err
	}
	if subq == nil {
		return u, nil
	}
	subq.Outer = u
	return subq, nil
}

func createSubqueryFromStatement(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (*SubQuery, error) {
	if len(ctx.SemTable.SubqueryMap[stmt]) == 0 {
		return nil, nil
	}
	subq := &SubQuery{}
	for _, sq := range ctx.SemTable.SubqueryMap[stmt] {
		opInner, err := CreateLogicalOperatorFromAST(ctx, sq.Subquery.Select)
		if err != nil {
			return nil, err
		}
		subq.Inner = append(subq.Inner, &SubQueryInner{
			ExtractedSubquery: sq,
			Inner:             opInner,
		})
	}
	return subq, nil
}

func addColumnEquality(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if expr.Operator != sqlparser.EqualOp {
			return
		}

		if left, isCol := expr.Left.(*sqlparser.ColName); isCol {
			ctx.SemTable.AddColumnEquality(left, expr.Right)
		}
		if right, isCol := expr.Right.(*sqlparser.ColName); isCol {
			ctx.SemTable.AddColumnEquality(right, expr.Left)
		}
	}
}

func createJoin(LHS, RHS Operator) Operator {
	lqg, lok := LHS.(*QueryGraph)
	rqg, rok := RHS.(*QueryGraph)
	if lok && rok {
		op := &QueryGraph{
			Tables:     append(lqg.Tables, rqg.Tables...),
			innerJoins: append(lqg.innerJoins, rqg.innerJoins...),
			NoDeps:     sqlparser.AndExpressions(lqg.NoDeps, rqg.NoDeps),
		}
		return op
	}
	return &Join{LHS: LHS, RHS: RHS}
}
