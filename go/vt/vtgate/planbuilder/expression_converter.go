package planbuilder

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type expressionConverter struct {
	tabletExpressions []sqlparser.Expr
}

func booleanValues(astExpr sqlparser.Expr) evalengine.Expr {
	var (
		ON  = evalengine.NewLiteralInt(1)
		OFF = evalengine.NewLiteralInt(0)
	)
	switch node := astExpr.(type) {
	case *sqlparser.Literal:
		//set autocommit = 'on'
		if node.Type == sqlparser.StrVal {
			switch strings.ToLower(node.Val) {
			case "on":
				return ON
			case "off":
				return OFF
			}
		}
	case *sqlparser.ColName:
		//set autocommit = on
		if node.Name.AtCount() == sqlparser.NoAt {
			switch node.Name.Lowered() {
			case "on":
				return ON
			case "off":
				return OFF
			}
		}
	}
	return nil
}

func identifierAsStringValue(astExpr sqlparser.Expr) evalengine.Expr {
	colName, isColName := astExpr.(*sqlparser.ColName)
	if isColName {
		// TODO@collations: proper collation for column name
		return evalengine.NewLiteralString([]byte(colName.Name.Lowered()), collations.TypedCollation{})
	}
	return nil
}

func (ec *expressionConverter) convert(astExpr sqlparser.Expr, boolean, identifierAsString bool) (evalengine.Expr, error) {
	if boolean {
		evalExpr := booleanValues(astExpr)
		if evalExpr != nil {
			return evalExpr, nil
		}
	}
	if identifierAsString {
		evalExpr := identifierAsStringValue(astExpr)
		if evalExpr != nil {
			return evalExpr, nil
		}
	}
	evalExpr, err := evalengine.Convert(astExpr, nil)
	if err != nil {
		if !strings.Contains(err.Error(), evalengine.ErrConvertExprNotSupported) {
			return nil, err
		}
		evalExpr = &evalengine.Column{Offset: len(ec.tabletExpressions)}
		ec.tabletExpressions = append(ec.tabletExpressions, astExpr)
	}
	return evalExpr, nil
}

func (ec *expressionConverter) source(vschema plancontext.VSchema) (engine.Primitive, error) {
	if len(ec.tabletExpressions) == 0 {
		return &engine.SingleRow{}, nil
	}
	ks, dest, err := resolveDestination(vschema)
	if err != nil {
		return nil, err
	}

	var expr []string
	for _, e := range ec.tabletExpressions {
		expr = append(expr, sqlparser.String(e))
	}
	query := fmt.Sprintf("select %s from dual", strings.Join(expr, ","))

	primitive := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
		IsDML:             false,
		SingleShardOnly:   true,
	}
	return primitive, nil
}
