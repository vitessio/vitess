package vttablet

import (
	"fmt"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/sqlparser"
)

func IsJoin(parser *sqlparser.Parser, query string) (bool, error) {
	if query == "" {
		return false, nil
	}
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
		return false, nil
	}
}

func GetJoinTables(parser *sqlparser.Parser, query string) ([]string, error) {
	log.Infof("GetJoinTables: %s", query)
	statement, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	var tables []string
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.AliasedTableExpr:
			if tableName, ok := node.Expr.(sqlparser.TableName); ok {
				tables = append(tables, tableName.Name.String())
			}
		}
		return true, nil
	}, statement)
	if err != nil {
		return nil, err
	}
	log.Infof("tables: %v", tables)
	return tables, nil
}
