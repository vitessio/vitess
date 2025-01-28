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
	ctx := plancontext.PlanningContext{}

	tableName := sqlparser.NewTableName("x")
	tableColumn := sqlparser.NewColName("id")
	valuesColumn := sqlparser.NewIdentifierCI("user_id")
	op := &Values{
		unaryOperator: newUnaryOp(&Table{
			QTable: &QueryTable{
				Table: tableName,
				Alias: sqlparser.NewAliasedTableExpr(tableName, ""),
			},
			Columns: []*sqlparser.ColName{tableColumn},
		}),
		Columns: sqlparser.Columns{valuesColumn},
		Name:    "t",
		Arg:     "toto",
	}

	stmt, _, err := ToSQL(&ctx, op)
	require.NoError(t, err)
	require.Equal(t, "select id from x, (values ::toto) as t(user_id)", sqlparser.String(stmt))

	proj := newAliasedProjection(op)
	proj.addUnexploredExpr(sqlparser.NewAliasedExpr(tableColumn, ""), tableColumn)
	proj.addUnexploredExpr(sqlparser.NewAliasedExpr(sqlparser.NewColNameWithQualifier("user_id", sqlparser.NewTableName("t")), ""), sqlparser.NewColNameWithQualifier("user_id", sqlparser.NewTableName("t")))

	stmt, _, err = ToSQL(&ctx, proj)
	require.NoError(t, err)
	require.Equal(t, "select id, t.user_id from x, (values ::toto) as t(user_id)", sqlparser.String(stmt))
}
