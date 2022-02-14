package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestCheckIfAlreadyExists(t *testing.T) {
	tests := []struct {
		name string
		expr *sqlparser.AliasedExpr
		sel  *sqlparser.Select
		want int
	}{
		{
			name: "No alias, both ColName",
			want: 0,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")}}},
		},
		{
			name: "Aliased expression and ColName",
			want: 0,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("user_id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{As: sqlparser.NewColIdent("user_id"), Expr: sqlparser.NewColName("id")}}},
		},
		{
			name: "Non-ColName expressions",
			want: 0,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral("test")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral("test")}}},
		},
		{
			name: "No alias, multiple ColName in projection",
			want: 1,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("foo")}, &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")}}},
		},
		{
			name: "No matching entry",
			want: -1,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("foo")}, &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("name")}}},
		},
		{
			name: "No AliasedExpr in projection",
			want: -1,
			expr: &sqlparser.AliasedExpr{Expr: sqlparser.NewColName("id")},
			sel:  &sqlparser.Select{SelectExprs: []sqlparser.SelectExpr{&sqlparser.StarExpr{TableName: sqlparser.TableName{Name: sqlparser.NewTableIdent("user")}}, &sqlparser.StarExpr{TableName: sqlparser.TableName{Name: sqlparser.NewTableIdent("people")}}}},
		},
	}
	for _, tt := range tests {
		semTable := semantics.EmptySemTable()
		t.Run(tt.name, func(t *testing.T) {
			got := checkIfAlreadyExists(tt.expr, tt.sel, semTable)
			assert.Equal(t, tt.want, got)
		})
	}
}
