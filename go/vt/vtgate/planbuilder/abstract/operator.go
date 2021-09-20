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
)

type (
	// Operator forms the tree of operators, representing the declarative query provided.
	// An operator can be:
	//	*  Derived - which represents an expression that generates a table.
	//  *  QueryGraph - which represents a group of tables and predicates that can be evaluated in any order
	//     while still preserving the results
	//	*  LeftJoin - A left join. These can't be evaluated in any order, so we keep them separate
	//	*  Join - A join represents inner join.
	//  *  SubQuery - Represents a query that encapsulates one or more sub-queries (SubQueryInner).
	//  *  Vindex - Represents a query that selects from vindex tables.
	//  *  Concatenate - Represents concatenation of the outputs of all the input sources
	//  *  Distinct - Represents elimination of duplicates from the output of the input source
	Operator interface {
		// TableID returns a TableSet of the tables contained within
		TableID() semantics.TableSet

		// PushPredicate pushes a predicate to the closest possible operator
		PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error

		// UnsolvedPredicates returns any predicates that have dependencies on the given Operator and
		// on the outside of it (a parent Select expression, any other table not used by Operator, etc).
		UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr

		// CheckValid checks if we have a valid operator tree, and returns an error if something is wrong
		CheckValid() error
	}
)

func getOperatorFromTableExpr(tableExpr sqlparser.TableExpr, semTable *semantics.SemTable) (Operator, error) {
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
			qt := &QueryTable{Alias: tableExpr, Table: tbl, TableID: tableID, IsInfSchema: isInfSchema}
			qg.Tables = append(qg.Tables, qt)
			return qg, nil
		case *sqlparser.DerivedTable:
			inner, err := CreateOperatorFromAST(tbl.Select, semTable)
			if err != nil {
				return nil, err
			}
			return &Derived{Alias: tableExpr.As.String(), Inner: inner, Sel: tbl.Select}, nil
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
				err = op.PushPredicate(tableExpr.Condition.On, semTable)
				if err != nil {
					return nil, err
				}
			}
			return op, nil
		case sqlparser.LeftJoinType, sqlparser.RightJoinType:
			inner, err := getOperatorFromTableExpr(tableExpr.LeftExpr, semTable)
			if err != nil {
				return nil, err
			}
			outer, err := getOperatorFromTableExpr(tableExpr.RightExpr, semTable)
			if err != nil {
				return nil, err
			}
			if tableExpr.Join == sqlparser.RightJoinType {
				inner, outer = outer, inner
			}
			op := &LeftJoin{
				Left:      inner,
				Right:     outer,
				Predicate: tableExpr.Condition.On,
			}
			return op, nil
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: %s", tableExpr.Join.ToString())
		}
	case *sqlparser.ParenTableExpr:
		return crossJoin(tableExpr.Exprs, semTable)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unable to use: %T table type", tableExpr)
	}
}

func crossJoin(exprs sqlparser.TableExprs, semTable *semantics.SemTable) (Operator, error) {
	var output Operator
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
	case *sqlparser.ParenSelect:
		return getSelect(s.Select)
	default:
		return nil
	}
}

// CreateOperatorFromAST creates an operator tree that represents the input SELECT or UNION query
func CreateOperatorFromAST(selStmt sqlparser.SelectStatement, semTable *semantics.SemTable) (Operator, error) {
	switch node := selStmt.(type) {
	case *sqlparser.Select:
		return createOperatorFromSelect(node, semTable)
	case *sqlparser.Union:
		return createOperatorFromUnion(node, semTable)
	case *sqlparser.ParenSelect:
		return CreateOperatorFromAST(node.Select, semTable)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T: operator not yet supported", selStmt)
}

func createOperatorFromUnion(node *sqlparser.Union, semTable *semantics.SemTable) (Operator, error) {
	op, err := CreateOperatorFromAST(node.FirstStatement, semTable)
	if err != nil {
		return nil, err
	}
	sources := []Operator{op}
	sel := getSelect(node.FirstStatement)
	selectStmts := []*sqlparser.Select{sel}

	var addToSource func(op Operator, sel *sqlparser.Select)

	switch op := op.(type) {
	case *Distinct:
		switch src := op.Source.(type) {
		case *Concatenate:
			addToSource = func(op Operator, sel *sqlparser.Select) {
				src.Sources = append(src.Sources, op)
				src.SelectStmts = append(src.SelectStmts, sel)
			}
		}
	}

	// we only need a single DISTINCT, so we'll go over the UNION to find the last DISTINCT, and that is the one we will keep.
	// Example: S1 UNION S2 UNION ALL S3 UNION S4 UNION ALL S5
	// To plan this query, we can do concatenate on S1, S2, S3, and S4, and then distinct, and lastly we concatenate S5

	distinctAt := lastDistinctAt(node)

	for i, unionSelect := range node.UnionSelects {
		op, err = CreateOperatorFromAST(unionSelect.Statement, semTable)
		if err != nil {
			return nil, err
		}

		sel = getSelect(unionSelect.Statement)
		if addToSource != nil && i <= distinctAt {
			// if we can, let's add it to the input instead of building up a new UNION
			addToSource(op, sel)
		} else {
			sources = append(sources, op)
			selectStmts = append(selectStmts, sel)

			if i == distinctAt {
				sources = []Operator{&Distinct{Source: &Concatenate{Sources: sources, SelectStmts: selectStmts}}}
				selectStmts = []*sqlparser.Select{nil}
			}
		}
	}
	return createConcatenateIfRequired(sources, selectStmts), nil
}

// lastDistinctAt finds the last DISTINCT in a list of queries UNIONed together
func lastDistinctAt(node *sqlparser.Union) int {
	distinctAt := -1
	for i := len(node.UnionSelects) - 1; i >= 0; i-- {
		if node.UnionSelects[i].Distinct {
			distinctAt = i
			break
		}
	}
	return distinctAt
}

// createOperatorFromSelect creates an operator tree that represents the input SELECT query
func createOperatorFromSelect(sel *sqlparser.Select, semTable *semantics.SemTable) (Operator, error) {
	var resultantOp *SubQuery
	if len(semTable.SubqueryMap[sel]) > 0 {
		resultantOp = &SubQuery{}
		for _, sq := range semTable.SubqueryMap[sel] {
			subquerySelectStatement := sq.SubQuery.Select.(*sqlparser.Select)
			opInner, err := createOperatorFromSelect(subquerySelectStatement, semTable)
			if err != nil {
				return nil, err
			}
			resultantOp.Inner = append(resultantOp.Inner, &SubQueryInner{
				SelectStatement: subquerySelectStatement,
				Inner:           opInner,
				Type:            sq.OpCode,
				ArgName:         sq.ArgName,
			})
		}
	}
	op, err := crossJoin(sel.From, semTable)
	if err != nil {
		return nil, err
	}
	if sel.Where != nil {
		exprs := sqlparser.SplitAndExpression(nil, sel.Where.Expr)
		for _, expr := range exprs {
			err := op.PushPredicate(expr, semTable)
			if err != nil {
				return nil, err
			}
			addColumnEquality(semTable, expr)
		}
	}
	if resultantOp == nil {
		return op, nil
	}
	resultantOp.Outer = op
	return resultantOp, nil
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

func createJoin(LHS, RHS Operator) Operator {
	lqg, lok := LHS.(*QueryGraph)
	rqg, rok := RHS.(*QueryGraph)
	if lok && rok {
		op := &QueryGraph{
			Tables:     append(lqg.Tables, rqg.Tables...),
			innerJoins: map[semantics.TableSet][]sqlparser.Expr{},
			NoDeps:     sqlparser.AndExpressions(lqg.NoDeps, rqg.NoDeps),
		}
		for k, v := range lqg.innerJoins {
			op.innerJoins[k] = v
		}
		for k, v := range rqg.innerJoins {
			op.innerJoins[k] = v
		}
		return op
	}
	return &Join{LHS: LHS, RHS: RHS}
}
