/*
Copyright 2021 The Vitess Authors.

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

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	ConverterLookup interface {
		ColumnLookup(col *sqlparser.ColName) (int, error)
		CollationIDLookup(expr sqlparser.Expr) collations.ID
	}
)

var ErrConvertExprNotSupported = "expr cannot be converted, not supported"

// translateComparisonOperator takes in a sqlparser.ComparisonExprOperator and
// returns the corresponding ComparisonOp
func translateComparisonOperator(op sqlparser.ComparisonExprOperator) ComparisonOp {
	switch op {
	case sqlparser.EqualOp:
		return &EqualOp{}
	case sqlparser.LessThanOp:
		return &LessThanOp{}
	case sqlparser.GreaterThanOp:
		return &GreaterThanOp{}
	case sqlparser.LessEqualOp:
		return &LessEqualOp{}
	case sqlparser.GreaterEqualOp:
		return &GreaterEqualOp{}
	case sqlparser.NotEqualOp:
		return &NotEqualOp{}
	case sqlparser.NullSafeEqualOp:
		return &NullSafeEqualOp{}
	case sqlparser.InOp:
		return &InOp{}
	case sqlparser.NotInOp:
		return &NotInOp{}
	case sqlparser.LikeOp:
		return &LikeOp{}
	case sqlparser.NotLikeOp:
		return &NotLikeOp{}
	case sqlparser.RegexpOp:
		return &RegexpOp{}
	case sqlparser.NotRegexpOp:
		return &NotRegexpOp{}
	default:
		return nil
	}
}

func getCollation(expr sqlparser.Expr, lookup ConverterLookup) collations.ID {
	collation := collations.Unknown
	if lookup != nil {
		collation = lookup.CollationIDLookup(expr)
	}
	return collation
}

// Convert converts between AST expressions and executable expressions
func Convert(e sqlparser.Expr, lookup ConverterLookup) (Expr, error) {
	switch node := e.(type) {
	case *sqlparser.ColName:
		if lookup == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s: cannot lookup column", ErrConvertExprNotSupported)
		}
		idx, err := lookup.ColumnLookup(node)
		if err != nil {
			return nil, err
		}
		collation := getCollation(node, lookup)
		return NewColumn(idx, collation), nil
	case *sqlparser.ComparisonExpr:
		left, err := Convert(node.Left, lookup)
		if err != nil {
			return nil, err
		}
		right, err := Convert(node.Right, lookup)
		if err != nil {
			return nil, err
		}
		return &ComparisonExpr{
			Op:    translateComparisonOperator(node.Operator),
			Left:  left,
			Right: right,
		}, nil
	case sqlparser.Argument:
		collation := collations.Unknown
		if lookup != nil {
			collation = lookup.CollationIDLookup(e)
		}
		return NewBindVar(string(node), collation), nil
	case *sqlparser.Literal:
		switch node.Type {
		case sqlparser.IntVal:
			return NewLiteralIntFromBytes(node.Bytes())
		case sqlparser.FloatVal:
			return NewLiteralFloatFromBytes(node.Bytes())
		case sqlparser.StrVal:
			collation := getCollation(e, lookup)
			return NewLiteralString(node.Bytes(), collation), nil
		case sqlparser.HexNum:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: hexadecimal value: %s", ErrConvertExprNotSupported, node.Val)
		}
	case sqlparser.BoolVal:
		if node {
			return NewLiteralIntFromBytes([]byte("1"))
		}
		return NewLiteralIntFromBytes([]byte("0"))
	case *sqlparser.BinaryExpr:
		var op BinaryOp
		switch node.Operator {
		case sqlparser.PlusOp:
			op = &Addition{}
		case sqlparser.MinusOp:
			op = &Subtraction{}
		case sqlparser.MultOp:
			op = &Multiplication{}
		case sqlparser.DivOp:
			op = &Division{}
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %T", ErrConvertExprNotSupported, e)
		}
		left, err := Convert(node.Left, lookup)
		if err != nil {
			return nil, err
		}
		right, err := Convert(node.Right, lookup)
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{
			Op:    op,
			Left:  left,
			Right: right,
		}, nil
	case sqlparser.ValTuple:
		var res Tuple
		for _, expr := range node {
			convertedExpr, err := Convert(expr, lookup)
			if err != nil {
				return nil, err
			}
			res = append(res, convertedExpr)
		}
		return res, nil
	case *sqlparser.NullVal:
		return Null{}, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %T", ErrConvertExprNotSupported, e)
}
