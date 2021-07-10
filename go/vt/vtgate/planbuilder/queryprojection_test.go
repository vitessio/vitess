package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestQP(t *testing.T) {
	tcases := []struct {
		sql string

		expErr   string
		expOrder []orderBy
	}{
		{
			sql:    "select * from user",
			expErr: "gen4 does not yet support: *sqlparser.StarExpr in select list",
		},
		{
			sql:    "select next value from user_seq",
			expErr: "gen4 does not yet support: *sqlparser.Nextval in select list",
		},
		{
			sql: "select (select 1) from user",
		},
		{
			sql:    "select 1, count(1) from user",
			expErr: "gen4 does not yet support: aggregation and non-aggregation expressions, together are not supported in cross-shard query",
		},
		{
			sql: "select max(id) from user",
		},
		{
			sql:    "select max(a, b) from user",
			expErr: "aggregate functions take a single argument 'max(a, b)'",
		},
		{
			sql:    "select func(max(id)) from user",
			expErr: "unsupported: in scatter query: complex aggregate expression",
		},
		{
			sql:    "select 1, count(1) from user order by 1",
			expErr: "gen4 does not yet support: aggregation and non-aggregation expressions, together are not supported in cross-shard query",
		},
		{
			sql: "select id from user order by col, id, 1",
			expOrder: []orderBy{
				{inner: &sqlparser.Order{Expr: sqlparser.NewColName("col")}, weightStrExpr: sqlparser.NewColName("col")},
				{inner: &sqlparser.Order{Expr: sqlparser.NewColName("id")}, weightStrExpr: sqlparser.NewColName("id")},
				{inner: &sqlparser.Order{Expr: sqlparser.NewColName("id")}, weightStrExpr: sqlparser.NewColName("id")},
			},
		},
		{
			sql:    "select id from user order by 2", // positional order not supported
			expErr: "Unknown column '2' in 'order clause'",
		},
		{
			sql: "SELECT CONCAT(last_name,', ',first_name) AS full_name FROM mytable, tbl2 ORDER BY full_name", // alias in order not supported
			expOrder: []orderBy{
				{
					inner: &sqlparser.Order{Expr: sqlparser.NewColName("full_name")},
					weightStrExpr: &sqlparser.FuncExpr{
						Name: sqlparser.NewColIdent("CONCAT"),
						Exprs: sqlparser.SelectExprs{
							&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("last_name")},
							&sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral(", ")},
							&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("first_name")},
						},
					},
				},
			},
		},
	}

	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tcase.sql)
			require.NoError(t, err)

			sel := stmt.(*sqlparser.Select)
			qp, err := createQPFromSelect(sel)
			if tcase.expErr != "" {
				require.EqualError(t, err, tcase.expErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, len(sel.SelectExprs), len(qp.selectExprs)+len(qp.aggrExprs))
				for index, expOrder := range tcase.expOrder {
					assert.True(t, sqlparser.EqualsSQLNode(expOrder.inner, qp.orderExprs[index].inner), "want: %+v, got %+v", sqlparser.String(expOrder.inner), sqlparser.String(qp.orderExprs[index].inner))
					assert.True(t, sqlparser.EqualsSQLNode(expOrder.weightStrExpr, qp.orderExprs[index].weightStrExpr), "want: %v, got %v", sqlparser.String(expOrder.weightStrExpr), sqlparser.String(qp.orderExprs[index].weightStrExpr))
				}
			}
		})
	}
}
