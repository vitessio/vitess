/*
Copyright 2024 The Vitess Authors.

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

package plancontext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestOuterTableNullability(t *testing.T) {
	// Tests that columns from outer tables are nullable,
	// even though the semantic state says that they are not nullable.
	// This is because the outer table may not have a matching row.
	// All columns are marked as NOT NULL in the schema.
	query := "select * from t1 left join t2 on t1.a = t2.a where t1.a+t2.a/abs(t2.boing)"
	ctx, columns := prepareContextAndFindColumns(t, query)

	// Check if the columns are correctly marked as nullable.
	for _, col := range columns {
		colName := "column: " + sqlparser.String(col)
		t.Run(colName, func(t *testing.T) {
			// Extract the column type from the context and the semantic state.
			// The context should mark the column as nullable.
			ctxType, found := ctx.TypeForExpr(col)
			require.True(t, found, colName)
			stType, found := ctx.SemTable.TypeForExpr(col)
			require.True(t, found, colName)
			ctxNullable := ctxType.Nullable()
			stNullable := stType.Nullable()

			switch col.Qualifier.Name.String() {
			case "t1":
				assert.False(t, ctxNullable, colName)
				assert.False(t, stNullable, colName)
			case "t2":
				assert.True(t, ctxNullable, colName)

				// The semantic state says that the column is not nullable. Don't trust it.
				assert.False(t, stNullable, colName)
			}
		})
	}
}

func prepareContextAndFindColumns(t *testing.T, query string) (ctx *PlanningContext, columns []*sqlparser.ColName) {
	parser := sqlparser.NewTestParser()
	ast, err := parser.Parse(query)
	require.NoError(t, err)
	semTable := semantics.EmptySemTable()
	t1 := semantics.SingleTableSet(0)
	t2 := semantics.SingleTableSet(1)
	stmt := ast.(*sqlparser.Select)
	expr := stmt.Where.Expr

	// Instead of using the semantic analysis, we manually set the types for the columns.
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		col, ok := node.(*sqlparser.ColName)
		if !ok {
			return true, nil
		}

		switch col.Qualifier.Name.String() {
		case "t1":
			semTable.Recursive[col] = t1
		case "t2":
			semTable.Recursive[col] = t2
		}

		intNotNull := evalengine.NewType(sqltypes.Int64, collations.Unknown)
		intNotNull.SetNullability(false)
		semTable.ExprTypes[col] = intNotNull
		columns = append(columns, col)
		return false, nil
	}, nil, expr)

	ctx = &PlanningContext{
		SemTable:          semTable,
		joinPredicates:    map[sqlparser.Expr][]sqlparser.Expr{},
		skipPredicates:    map[sqlparser.Expr]any{},
		ReservedArguments: map[sqlparser.Expr]string{},
		Statement:         stmt,
		OuterTables:       t2, // t2 is the outer table.
	}
	return
}
