package semantics

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// typer is responsible for setting the type for expressions
// it does it's work after visiting the children (up), since the children types is often needed to type a node.
type typer struct {
	exprTypes map[sqlparser.Expr]Type
}

// Type is the normal querypb.Type with collation
type Type struct {
	Type      querypb.Type
	Collation collations.ID
}

func newTyper() *typer {
	return &typer{
		exprTypes: map[sqlparser.Expr]Type{},
	}
}

var typeInt32 = Type{Type: sqltypes.Int32}
var decimal = Type{Type: sqltypes.Decimal}
var floatval = Type{Type: sqltypes.Float64}

func (t *typer) up(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Literal:
		switch node.Type {
		case sqlparser.IntVal:
			t.exprTypes[node] = typeInt32
		case sqlparser.StrVal:
			t.exprTypes[node] = Type{Type: sqltypes.VarChar} // TODO - add system default collation name
		case sqlparser.DecimalVal:
			t.exprTypes[node] = decimal
		case sqlparser.FloatVal:
			t.exprTypes[node] = floatval
		}
	case *sqlparser.FuncExpr:
		code, ok := engine.SupportedAggregates[node.Name.Lowered()]
		if ok {
			typ, ok := engine.OpcodeType[code]
			if ok {
				t.exprTypes[node] = Type{Type: typ}
			}
		}
	}
	return nil
}

func (t *typer) setTypeFor(node *sqlparser.ColName, typ Type) {
	t.exprTypes[node] = typ
}
