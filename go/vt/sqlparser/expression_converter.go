package sqlparser

import "vitess.io/vitess/go/sqltypes"

func Convert(e Expr) sqltypes.Expr {
	switch node := e.(type) {
	case *SQLVal:
		return &sqltypes.SQLVal{
			Type: sqltypes.ValType(node.Type), // eeeek! not a clean way of doing it
			Val:  node.Val,
		}
	}
	return nil
}
