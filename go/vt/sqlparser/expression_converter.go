package sqlparser

import "vitess.io/vitess/go/sqltypes"

func Convert(e Expr) sqltypes.Expr {
	switch node := e.(type) {
	case *SQLVal:
		switch node.Type {
		case IntVal:
			return &sqltypes.LiteralInt{Val: node.Val}
		case ValArg:
			return &sqltypes.BindVariable{Key: string(node.Val[1:])}
		}
	}
	return nil
}
