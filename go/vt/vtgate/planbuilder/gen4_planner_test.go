package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestBindingSubquery(t *testing.T) {
	testcases := []struct {
		query            string
		requiredTableSet semantics.TableSet
		extractor        func(p *sqlparser.Select) sqlparser.Expr
		rewrite          bool
	}{
		{
			query:            "select (select col from tabl limit 1) as a from foo join tabl order by a + 1",
			requiredTableSet: semantics.EmptyTableSet(),
			extractor: func(sel *sqlparser.Select) sqlparser.Expr {
				return sel.OrderBy[0].Expr
			},
			rewrite: true,
		}, {
			query:            "select t.a from (select (select col from tabl limit 1) as a from foo join tabl) t",
			requiredTableSet: semantics.EmptyTableSet(),
			extractor: func(sel *sqlparser.Select) sqlparser.Expr {
				return extractExpr(sel, 0)
			},
			rewrite: true,
		}, {
			query:            "select (select col from tabl where foo.id = 4 limit 1) as a from foo",
			requiredTableSet: semantics.SingleTableSet(0),
			extractor: func(sel *sqlparser.Select) sqlparser.Expr {
				return extractExpr(sel, 0)
			},
			rewrite: false,
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(testcase.query)
			require.NoError(t, err)
			selStmt := parse.(*sqlparser.Select)
			semTable, err := semantics.Analyze(selStmt, "d", &semantics.FakeSI{
				Tables: map[string]*vindexes.Table{
					"tabl": {Name: sqlparser.NewTableIdent("tabl")},
					"foo":  {Name: sqlparser.NewTableIdent("foo")},
				},
			})
			require.NoError(t, err)
			if testcase.rewrite {
				err = queryRewrite(semTable, sqlparser.NewReservedVars("vt", make(sqlparser.BindVars)), selStmt)
				require.NoError(t, err)
			}
			expr := testcase.extractor(selStmt)
			tableset := semTable.RecursiveDeps(expr)
			require.Equal(t, testcase.requiredTableSet, tableset)
		})
	}
}

func extractExpr(in *sqlparser.Select, idx int) sqlparser.Expr {
	return in.SelectExprs[idx].(*sqlparser.AliasedExpr).Expr
}
