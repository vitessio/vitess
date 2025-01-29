/*
Copyright 2025 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func TestToSQLValues(t *testing.T) {
	ctx := plancontext.CreateEmptyPlanningContext()

	tableName := sqlparser.NewTableName("x")
	tableColumn := sqlparser.NewColName("id")
	op := &Values{
		unaryOperator: newUnaryOp(&Table{
			QTable: &QueryTable{
				Table: tableName,
				Alias: sqlparser.NewAliasedTableExpr(tableName, ""),
			},
			Columns: []*sqlparser.ColName{tableColumn},
		}),
		Columns: sqlparser.Columns{sqlparser.NewIdentifierCI("user_id")},
		Name:    "t",
		Arg:     "toto",
	}

	stmt, _, err := ToSQL(ctx, op)
	require.NoError(t, err)
	require.Equal(t, "select id from x, (values ::toto) as t(user_id)", sqlparser.String(stmt))

	// Now do the same test but with a projection on top
	proj := newAliasedProjection(op)
	proj.addUnexploredExpr(sqlparser.NewAliasedExpr(tableColumn, ""), tableColumn)

	userIdColName := sqlparser.NewColNameWithQualifier("user_id", sqlparser.NewTableName("t"))
	proj.addUnexploredExpr(
		sqlparser.NewAliasedExpr(userIdColName, ""),
		userIdColName,
	)

	stmt, _, err = ToSQL(ctx, proj)
	require.NoError(t, err)
	require.Equal(t, "select id, t.user_id from x, (values ::toto) as t(user_id)", sqlparser.String(stmt))
}

func TestToSQLValuesJoin(t *testing.T) {
	ctx := plancontext.CreateEmptyPlanningContext()
	parser := sqlparser.NewTestParser()

	lhsTableName := sqlparser.NewTableName("x")
	lhsTableColumn := sqlparser.NewColName("id")
	lhsFilterPred, err := parser.ParseExpr("x.id = 42")
	require.NoError(t, err)

	LHS := &Filter{
		unaryOperator: newUnaryOp(&Table{
			QTable: &QueryTable{
				Table: lhsTableName,
				Alias: sqlparser.NewAliasedTableExpr(lhsTableName, ""),
			},
			Columns: []*sqlparser.ColName{lhsTableColumn},
		}),
		Predicates: []sqlparser.Expr{lhsFilterPred},
	}

	const argumentName = "v"

	rhsTableName := sqlparser.NewTableName("y")
	rhsTableColumn := sqlparser.NewColName("tata")
	rhsFilterPred, err := parser.ParseExpr("y.tata = 42")
	require.NoError(t, err)
	rhsJoinFilterPred, err := parser.ParseExpr("y.tata = x.id")
	require.NoError(t, err)

	RHS := &Filter{
		unaryOperator: newUnaryOp(&Values{
			unaryOperator: newUnaryOp(&Table{
				QTable: &QueryTable{
					Table: rhsTableName,
					Alias: sqlparser.NewAliasedTableExpr(rhsTableName, ""),
				},
				Columns: []*sqlparser.ColName{rhsTableColumn},
			}),
			Columns: sqlparser.Columns{sqlparser.NewIdentifierCI("id")},
			Name:    lhsTableName.Name.String(),
			Arg:     argumentName,
		}),
		Predicates: []sqlparser.Expr{rhsFilterPred, rhsJoinFilterPred},
	}

	vj := &ValuesJoin{
		binaryOperator: newBinaryOp(LHS, RHS),
		bindVarName:    argumentName,
	}

	stmt, _, err := ToSQL(ctx, vj)
	require.NoError(t, err)
	require.Equal(t, "select id, tata from x, y where x.id = 42 and y.tata = 42 and y.tata = x.id", sqlparser.String(stmt))
}
