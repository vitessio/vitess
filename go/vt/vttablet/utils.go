package vttablet

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

func IsJoin(parser *sqlparser.Parser, query string) (bool, error) {
	statement, err := parser.Parse(query)
	if err != nil {
		return false, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return false, fmt.Errorf("unsupported non-select statement")
	}
	if sel.Distinct {
		return false, fmt.Errorf("unsupported distinct clause")
	}
	switch sel.From[0].(type) {
	case *sqlparser.JoinTableExpr:
		return true, nil
	default:
		return false, fmt.Errorf("unsupported from expression (%T)", sel.From[0])
	}
}
