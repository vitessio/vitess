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

import (
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var ExprNotSupported = fmt.Errorf("Expr Not Supported")

//Convert converts between AST expressions and executable expressions
func Convert(e Expr) (evalengine.Expr, error) {
	switch node := e.(type) {
	case *SQLVal:
		switch node.Type {
		case IntVal:
			return evalengine.NewLiteralInt(node.Val)
		case FloatVal:
			return evalengine.NewLiteralFloat(node.Val)
		case ValArg:
			return &evalengine.BindVariable{Key: string(node.Val[1:])}, nil
		}
	case *BinaryExpr:
		var op evalengine.BinaryExpr
		switch node.Operator {
		case PlusStr:
			op = &evalengine.Addition{}
		case MinusStr:
			op = &evalengine.Subtraction{}
		case MultStr:
			op = &evalengine.Multiplication{}
		case DivStr:
			op = &evalengine.Division{}
		default:
			return nil, ExprNotSupported
		}
		left, err := Convert(node.Left)
		if err != nil {
			return nil, err
		}
		right, err := Convert(node.Right)
		if err != nil {
			return nil, err
		}
		return &evalengine.BinaryOp{
			Expr:  op,
			Left:  left,
			Right: right,
		}, nil

	}
	return nil, ExprNotSupported
}
