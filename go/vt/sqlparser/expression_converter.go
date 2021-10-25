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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var ErrConvertExprNotSupported = "expr cannot be converted, not supported"

// translateComparisonOperator takes in a sqlparser.ComparisonExprOperator and
// returns the corresponding evalengine.ComparisonOp
func translateComparisonOperator(op ComparisonExprOperator) evalengine.ComparisonOp {
	switch op {
	case EqualOp:
		return &evalengine.EqualOp{}
	case LessThanOp:
		return &evalengine.LessThanOp{}
	case GreaterThanOp:
		return &evalengine.GreaterThanOp{}
	case LessEqualOp:
		return &evalengine.LessEqualOp{}
	case GreaterEqualOp:
		return &evalengine.GreaterEqualOp{}
	case NotEqualOp:
		return &evalengine.NotEqualOp{}
	case NullSafeEqualOp:
		return &evalengine.NullSafeEqualOp{}
	case InOp:
		return &evalengine.InOp{}
	case NotInOp:
		return &evalengine.NotInOp{}
	case LikeOp:
		return &evalengine.LikeOp{}
	case NotLikeOp:
		return &evalengine.NotLikeOp{}
	case RegexpOp:
		return &evalengine.RegexpOp{}
	case NotRegexpOp:
		return &evalengine.NotRegexpOp{}
	default:
		return nil
	}
}

// Convert converts between AST expressions and executable expressions
func Convert(e Expr, columnLookup func(col *ColName) (int, error)) (evalengine.Expr, error) {
	switch node := e.(type) {
	case *ColName:
		if columnLookup == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s: cannot lookup column", ErrConvertExprNotSupported)
		}
		idx, err := columnLookup(node)
		if err != nil {
			return nil, err
		}
		return &evalengine.Column{Offset: idx}, nil
	case *ComparisonExpr:
		left, err := Convert(node.Left, columnLookup)
		if err != nil {
			return nil, err
		}
		right, err := Convert(node.Right, columnLookup)
		if err != nil {
			return nil, err
		}
		return &evalengine.ComparisonExpr{
			Op:    translateComparisonOperator(node.Operator),
			Left:  left,
			Right: right,
		}, nil
	case Argument:
		return evalengine.NewBindVar(string(node)), nil
	case *Literal:
		switch node.Type {
		case IntVal:
			return evalengine.NewLiteralIntFromBytes(node.Bytes())
		case FloatVal:
			return evalengine.NewLiteralFloatFromBytes(node.Bytes())
		case StrVal:
			return evalengine.NewLiteralString(node.Bytes()), nil
		case HexNum:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: hexadecimal value: %s", ErrConvertExprNotSupported, node.Val)
		}
	case BoolVal:
		if node {
			return evalengine.NewLiteralIntFromBytes([]byte("1"))
		}
		return evalengine.NewLiteralIntFromBytes([]byte("0"))
	case *BinaryExpr:
		var op evalengine.BinaryOp
		switch node.Operator {
		case PlusOp:
			op = &evalengine.Addition{}
		case MinusOp:
			op = &evalengine.Subtraction{}
		case MultOp:
			op = &evalengine.Multiplication{}
		case DivOp:
			op = &evalengine.Division{}
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %T", ErrConvertExprNotSupported, e)
		}
		left, err := Convert(node.Left, columnLookup)
		if err != nil {
			return nil, err
		}
		right, err := Convert(node.Right, columnLookup)
		if err != nil {
			return nil, err
		}
		return &evalengine.BinaryExpr{
			Op:    op,
			Left:  left,
			Right: right,
		}, nil

	}
	return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %T", ErrConvertExprNotSupported, e)
}
