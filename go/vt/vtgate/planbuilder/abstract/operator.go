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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// Operator forms the tree of operators, representing the declarative query provided.
	// An operator can be:
	//  *  QueryGraph - which represents a group of tables and predicates that can be evaluated in any order
	//     while still preserving the results
	//	*  LeftJoin - A left join. These can't be evaluated in any order, so we keep them separate
	//	*  Join - A join represents inner join.
	Operator interface {
		// TableID returns a TableSet of the tables contained within
		TableID() semantics.TableSet

		// PushPredicate pushes a predicate to the closest possible operator
		PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error
	}
)

func getOperatorFromTableExpr(tableExpr sqlparser.TableExpr, semTable *semantics.SemTable) (Operator, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		qg := newQueryGraph()
		tableName := tableExpr.Expr.(sqlparser.TableName)
		qt := &QueryTable{Alias: tableExpr, Table: tableName, TableID: semTable.TableSetFor(tableExpr)}
		qg.Tables = append(qg.Tables, qt)
		return qg, nil
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
			err = op.PushPredicate(tableExpr.Condition.On, semTable)
			if err != nil {
				return nil, err
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
			return nil, semantics.Gen4NotSupportedF("%s joins", tableExpr.Join.ToString())
		}
	case *sqlparser.ParenTableExpr:
		return crossJoin(tableExpr.Exprs, semTable)
	default:
		return nil, semantics.Gen4NotSupportedF("%T table type", tableExpr)
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

// CreateOperatorFromSelect creates an operator tree that represents the input SELECT query
func CreateOperatorFromSelect(sel *sqlparser.Select, semTable *semantics.SemTable) (Operator, error) {
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
		}
	}
	return op, nil
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
