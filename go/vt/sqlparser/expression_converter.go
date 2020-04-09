/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import "vitess.io/vitess/go/sqltypes"

//Convert converts between AST expressions and executable expressions
func Convert(e Expr) sqltypes.Expr {
	switch node := e.(type) {
	case *SQLVal:
		switch node.Type {
		case IntVal:
			return &sqltypes.LiteralInt{Val: node.Val}
		case ValArg:
			return &sqltypes.BindVariable{Key: string(node.Val[1:])}
		}
	case *BinaryExpr:
		var op sqltypes.BinaryExpr
		switch node.Operator {
		case PlusStr:
			op = &sqltypes.Addition{}
		case MinusStr:
			op = &sqltypes.Subtraction{}
		case MultStr:
			op = &sqltypes.Multiplication{}
		case DivStr:
			op = &sqltypes.Division{}
		default:
			return nil
		}
		return &sqltypes.BinaryOp{
			Expr:  op,
			Left:  Convert(node.Left),
			Right: Convert(node.Right),
		}

	}
	return nil
}
