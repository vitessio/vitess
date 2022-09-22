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

package abstract

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	// Operator forms the tree of operators, representing the declarative query provided.
	Operator interface {
		// TableID returns a TableSet of the tables contained within
		TableID() semantics.TableSet

		// UnsolvedPredicates returns any predicates that have dependencies on the given Operator and
		// on the outside of it (a parent Select expression, any other table not used by Operator, etc).
		UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr

		// CheckValid checks if we have a valid operator tree, and returns an error if something is wrong
		CheckValid() error
	}

	LogicalOperator interface {
		Operator
		iLogical()

		// PushPredicate pushes a predicate to the closest possible operator
		PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error)

		// Compact will optimise the operator tree into a smaller but equivalent version
		Compact(semTable *semantics.SemTable) (LogicalOperator, error)
	}

	PhysicalOperator interface {
		Operator
		IPhysical()
		// Cost is simply the number of routes in the operator tree
		Cost() int
		// Clone creates a copy of the operator that can be updated without changing the original
		Clone() PhysicalOperator
	}

	// IntroducesTable is used to make it possible to gather information about the table an operator introduces
	IntroducesTable interface {
		GetQTable() *QueryTable
		GetVTable() *vindexes.Table
	}
)

func getOperatorFromTableExpr(tableExpr sqlparser.TableExpr, semTable *semantics.SemTable) (LogicalOperator, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tbl := tableExpr.Expr.(type) {
		case sqlparser.TableName:
			tableID := semTable.TableSetFor(tableExpr)
			tableInfo, err := semTable.TableInfoFor(tableID)
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
			inner, err := CreateLogicalOperatorFromAST(tbl.Select, semTable)
			if err != nil {
				return nil, err
			}
			return &Derived{Alias: tableExpr.As.String(), Inner: inner, Sel: tbl.Select, ColumnAliases: tableExpr.Columns}, nil
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unable to use: %T", tbl)
		}
	case *sqlparser.JoinTableExpr:
		switch tableExpr.Join {
		case sqlparser.NormalJoinType:
			lhs, err := getOperatorFromTableExpr(tableExpr.LeftExpr, semTable)
			if err != nil {
				return nil, err
			}
			rhs, err := getOperatorFromTableExpr(tableExpr.RightExpr, semTable)
			if err != nil {
				return nil, err
			}
			op := createJoin(lhs, rhs)
			if tableExpr.Condition.On != nil {
				op, err = op.PushPredicate(sqlparser.RemoveKeyspaceFromColName(tableExpr.Condition.On), semTable)
				if err != nil {
					return nil, err
				}
			}
			return op, nil
		case sqlparser.LeftJoinType, sqlparser.RightJoinType:
			lhs, err := getOperatorFromTableExpr(tableExpr.LeftExpr, semTable)
			if err != nil {
				return nil, err
			}
			rhs, err := getOperatorFromTableExpr(tableExpr.RightExpr, semTable)
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
		return crossJoin(tableExpr.Exprs, semTable)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unable to use: %T table type", tableExpr)
	}
}

func crossJoin(exprs sqlparser.TableExprs, semTable *semantics.SemTable) (LogicalOperator, error) {
	var output LogicalOperator
	for _, tableExpr := range exprs {
		op, err := getOperatorFromTableExpr(tableExpr, semTable)
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
func CreateLogicalOperatorFromAST(selStmt sqlparser.Statement, semTable *semantics.SemTable) (op LogicalOperator, err error) {
	switch node := selStmt.(type) {
	case *sqlparser.Select:
		op, err = createOperatorFromSelect(node, semTable)
	case *sqlparser.Union:
		op, err = createOperatorFromUnion(node, semTable)
	case *sqlparser.Update:
		op, err = createOperatorFromUpdate(node, semTable)
	case *sqlparser.Delete:
		op, err = createOperatorFromDelete(node, semTable)
	default:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T: operator not yet supported", selStmt)
	}
	if err != nil {
		return nil, err
	}
	return op.Compact(semTable)
}

func createOperatorFromUnion(node *sqlparser.Union, semTable *semantics.SemTable) (LogicalOperator, error) {
	opLHS, err := CreateLogicalOperatorFromAST(node.Left, semTable)
	if err != nil {
		return nil, err
	}

	_, isRHSUnion := node.Right.(*sqlparser.Union)
	if isRHSUnion {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "nesting of unions at the right-hand side is not yet supported")
	}
	opRHS, err := CreateLogicalOperatorFromAST(node.Right, semTable)
	if err != nil {
		return nil, err
	}
	return &Concatenate{
		Distinct:    node.Distinct,
		SelectStmts: []*sqlparser.Select{getSelect(node.Left), getSelect(node.Right)},
		Sources:     []LogicalOperator{opLHS, opRHS},
		OrderBy:     node.OrderBy,
		Limit:       node.Limit,
	}, nil
}

// createOperatorFromSelect creates an operator tree that represents the input SELECT query
func createOperatorFromSelect(sel *sqlparser.Select, semTable *semantics.SemTable) (LogicalOperator, error) {
	subq, err := createSubqueryFromStatement(sel, semTable)
	if err != nil {
		return nil, err
	}
	op, err := crossJoin(sel.From, semTable)
	if err != nil {
		return nil, err
	}
	if sel.Where != nil {
		exprs := sqlparser.SplitAndExpression(nil, sel.Where.Expr)
		for _, expr := range exprs {
			op, err = op.PushPredicate(sqlparser.RemoveKeyspaceFromColName(expr), semTable)
			if err != nil {
				return nil, err
			}
			addColumnEquality(semTable, expr)
		}
	}
	if subq == nil {
		return op, nil
	}
	subq.Outer = op
	return subq, nil
}

func createOperatorFromUpdate(updStmt *sqlparser.Update, semTable *semantics.SemTable) (LogicalOperator, error) {
	tableInfo, qt, err := createQueryTableForDML(updStmt.TableExprs[0], semTable, updStmt.Where)
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

	subq, err := createSubqueryFromStatement(updStmt, semTable)
	if err != nil {
		return nil, err
	}
	if subq == nil {
		return u, nil
	}
	subq.Outer = u
	return subq, nil
}

func createQueryTableForDML(tableExpr sqlparser.TableExpr, semTable *semantics.SemTable, whereClause *sqlparser.Where) (semantics.TableInfo, *QueryTable, error) {
	alTbl, ok := tableExpr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected AliasedTableExpr")
	}
	tblName, ok := alTbl.Expr.(sqlparser.TableName)
	if !ok {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected TableName")
	}

	tableID := semTable.TableSetFor(alTbl)
	tableInfo, err := semTable.TableInfoFor(tableID)
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

func createOperatorFromDelete(deleteStmt *sqlparser.Delete, semTable *semantics.SemTable) (LogicalOperator, error) {
	tableInfo, qt, err := createQueryTableForDML(deleteStmt.TableExprs[0], semTable, deleteStmt.Where)
	if err != nil {
		return nil, err
	}

	u := &Delete{
		Table:     qt,
		AST:       deleteStmt,
		TableInfo: tableInfo,
	}

	subq, err := createSubqueryFromStatement(deleteStmt, semTable)
	if err != nil {
		return nil, err
	}
	if subq == nil {
		return u, nil
	}
	subq.Outer = u
	return subq, nil
}

func createSubqueryFromStatement(stmt sqlparser.Statement, semTable *semantics.SemTable) (*SubQuery, error) {
	if len(semTable.SubqueryMap[stmt]) == 0 {
		return nil, nil
	}
	subq := &SubQuery{}
	for _, sq := range semTable.SubqueryMap[stmt] {
		opInner, err := CreateLogicalOperatorFromAST(sq.Subquery.Select, semTable)
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

func addColumnEquality(semTable *semantics.SemTable, expr sqlparser.Expr) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if expr.Operator != sqlparser.EqualOp {
			return
		}

		if left, isCol := expr.Left.(*sqlparser.ColName); isCol {
			semTable.AddColumnEquality(left, expr.Right)
		}
		if right, isCol := expr.Right.(*sqlparser.ColName); isCol {
			semTable.AddColumnEquality(right, expr.Left)
		}
	}
}

func createJoin(LHS, RHS LogicalOperator) LogicalOperator {
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
