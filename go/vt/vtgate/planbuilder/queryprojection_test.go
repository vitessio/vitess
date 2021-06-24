package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestQP(t *testing.T) {
	type order struct {
		found bool
		pos   int
	}
	tcases := []struct {
		sql string

		expErr   string
		expOrder []order
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
			sql: "select 1, count(1) from user",
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
			expErr: "",
			expOrder: []order{
				{found: true, pos: 0},
			},
		},
		{
			sql: "select id from user order by col, id",
			expOrder: []order{
				{found: false},
				{found: true, pos: 0},
			},
		},
		{
			sql: "select id from user order by 2", // positional order not supported
			expOrder: []order{
				{found: false},
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
				//for index, orderExpr := range qp.orderExprs {
				//	pos, exists := qp.orderExprs[orderExpr]
				//	assert.Contains(t, tcase.expOrder[index].found, exists)
				//	assert.Equal(t, tcase.expOrder[index].pos, pos)
				//}
			}
		})
	}
}
