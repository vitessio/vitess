package evalengine_test

import (
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

func parseExpr(t testing.TB, expr string) sqlparser.Expr {
	stmt, err := sqlparser.Parse("select " + expr)
	if err != nil {
		t.Fatal(err)
	}
	return stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
}

func makeFields(values []sqltypes.Value) (fields []*querypb.Field) {
	for _, v := range values {
		field := &querypb.Field{
			Type: v.Type(),
		}
		if sqltypes.IsText(field.Type) {
			field.Charset = uint32(collations.CollationUtf8mb4ID)
		} else {
			field.Charset = uint32(collations.CollationBinaryID)
		}
		fields = append(fields, field)
	}
	return
}
