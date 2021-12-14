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
	"fmt"

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

func convertComparisonExpr(op sqlparser.ComparisonExprOperator, left, right sqlparser.Expr, lookup ConverterLookup) (Expr, error) {
	l, err := convertExpr(left, lookup)
	if err != nil {
		return nil, err
	}
	r, err := convertExpr(right, lookup)
	if err != nil {
		return nil, err
	}
	return convertComparisonExpr2(op, l, r)
}

func convertComparisonExpr2(op sqlparser.ComparisonExprOperator, left, right Expr) (Expr, error) {
	binaryExpr := BinaryExpr{
		Left:  left,
		Right: right,
	}

	if op == sqlparser.InOp || op == sqlparser.NotInOp {
		return &InExpr{
			BinaryExpr: binaryExpr,
			Negate:     op == sqlparser.NotInOp,
			Hashed:     nil,
		}, nil
	}

	coercedExpr := BinaryCoercedExpr{
		BinaryExpr: binaryExpr,
	}
	if err := coercedExpr.coerce(); err != nil {
		return nil, err
	}

	switch op {
	case sqlparser.EqualOp:
		return &ComparisonExpr{coercedExpr, compareEQ{}}, nil
	case sqlparser.NotEqualOp:
		return &ComparisonExpr{coercedExpr, compareNE{}}, nil
	case sqlparser.LessThanOp:
		return &ComparisonExpr{coercedExpr, compareLT{}}, nil
	case sqlparser.LessEqualOp:
		return &ComparisonExpr{coercedExpr, compareLE{}}, nil
	case sqlparser.GreaterThanOp:
		return &ComparisonExpr{coercedExpr, compareGT{}}, nil
	case sqlparser.GreaterEqualOp:
		return &ComparisonExpr{coercedExpr, compareGE{}}, nil
	case sqlparser.NullSafeEqualOp:
		return &NullSafeComparisonExpr{coercedExpr}, nil
	case sqlparser.LikeOp:
		return &LikeExpr{BinaryCoercedExpr: coercedExpr}, nil
	case sqlparser.NotLikeOp:
		return &LikeExpr{BinaryCoercedExpr: coercedExpr, Negate: true}, nil
	default:
		return nil, fmt.Errorf("unsupported comparison operator %q", op.ToString())
	}
}

func convertLogicalExpr(opname string, left, right sqlparser.Expr, lookup ConverterLookup) (Expr, error) {
	l, err := convertExpr(left, lookup)
	if err != nil {
		return nil, err
	}
	r, err := convertExpr(right, lookup)
	if err != nil {
		return nil, err
	}

	var logic func(l, r boolean) boolean
	switch opname {
	case "AND":
		logic = func(l, r boolean) boolean { return l.and(r) }
	case "OR":
		logic = func(l, r boolean) boolean { return l.or(r) }
	case "XOR":
		logic = func(l, r boolean) boolean { return l.xor(r) }
	default:
		panic("unexpected logical operator")
	}

	return &LogicalExpr{
		BinaryExpr: BinaryExpr{
			Left:  l,
			Right: r,
		},
		op:     logic,
		opname: opname,
	}, nil
}

func getCollation(expr sqlparser.Expr, lookup ConverterLookup) collations.TypedCollation {
	collation := collations.TypedCollation{
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireUnicode,
	}
	if lookup != nil {
		collation.Collation = lookup.CollationIDLookup(expr)
	} else {
		sysdefault, _ := collations.Local().ResolveCollation("", "")
		collation.Collation = sysdefault.ID()
	}
	return collation
}

func ConvertEx(e sqlparser.Expr, lookup ConverterLookup, simplify bool) (Expr, error) {
	expr, err := convertExpr(e, lookup)
	if err != nil {
		return nil, err
	}
	if simplify {
		expr, err = simplifyExpr(expr)
	}
	return expr, err
}

// Convert converts between AST expressions and executable expressions
func Convert(e sqlparser.Expr, lookup ConverterLookup) (Expr, error) {
	return ConvertEx(e, lookup, true)
}

func convertExpr(e sqlparser.Expr, lookup ConverterLookup) (Expr, error) {
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
		return convertComparisonExpr(node.Operator, node.Left, node.Right, lookup)
	case sqlparser.Argument:
		collation := getCollation(e, lookup)
		return NewBindVar(string(node), collation), nil
	case sqlparser.ListArg:
		collation := getCollation(e, lookup)
		return NewBindVar(string(node), collation), nil
	case *sqlparser.Literal:
		switch node.Type {
		case sqlparser.IntVal:
			return NewLiteralIntegralFromBytes(node.Bytes())
		case sqlparser.FloatVal:
			return NewLiteralRealFromBytes(node.Bytes())
		case sqlparser.StrVal:
			collation := getCollation(e, lookup)
			return NewLiteralString(node.Bytes(), collation), nil
		case sqlparser.HexNum:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: hexadecimal value: %s", ErrConvertExprNotSupported, node.Val)
		}
	case sqlparser.BoolVal:
		if node {
			return NewLiteralInt(1), nil
		}
		return NewLiteralInt(0), nil
	case *sqlparser.AndExpr:
		return convertLogicalExpr("AND", node.Left, node.Right, lookup)
	case *sqlparser.OrExpr:
		return convertLogicalExpr("OR", node.Left, node.Right, lookup)
	case *sqlparser.XorExpr:
		return convertLogicalExpr("XOR", node.Left, node.Right, lookup)
	case *sqlparser.NotExpr:
		inner, err := convertExpr(node.Expr, lookup)
		if err != nil {
			return nil, err
		}
		return &NotExpr{UnaryExpr{inner}}, nil
	case *sqlparser.BinaryExpr:
		var op ArithmeticOp
		switch node.Operator {
		case sqlparser.PlusOp:
			op = &OpAddition{}
		case sqlparser.MinusOp:
			op = &OpSubstraction{}
		case sqlparser.MultOp:
			op = &OpMultiplication{}
		case sqlparser.DivOp:
			op = &OpDivision{}
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %T", ErrConvertExprNotSupported, e)
		}
		left, err := convertExpr(node.Left, lookup)
		if err != nil {
			return nil, err
		}
		right, err := convertExpr(node.Right, lookup)
		if err != nil {
			return nil, err
		}
		return &ArithmeticExpr{
			BinaryExpr: BinaryExpr{
				Left:  left,
				Right: right,
			},
			Op: op,
		}, nil
	case sqlparser.ValTuple:
		var exprs TupleExpr
		for _, expr := range node {
			convertedExpr, err := convertExpr(expr, lookup)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, convertedExpr)
		}
		return exprs, nil
	case *sqlparser.NullVal:
		return NullExpr, nil
	case *sqlparser.CollateExpr:
		expr, err := convertExpr(node.Expr, lookup)
		if err != nil {
			return nil, err
		}
		coll := collations.Local().LookupByName(node.Collation)
		if coll == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unknown collation: '%s'", node.Collation)
		}
		return &CollateExpr{
			UnaryExpr: UnaryExpr{expr},
			TypedCollation: collations.TypedCollation{
				Collation:    coll.ID(),
				Coercibility: collations.CoerceExplicit,
				Repertoire:   collations.RepertoireUnicode,
			},
		}, nil
	case *sqlparser.IntroducerExpr:
		expr, err := convertExpr(node.Expr, lookup)
		if err != nil {
			return nil, err
		}
		coll := collations.Local().DefaultCollationForCharset(node.CharacterSet[1:])
		if coll == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unknown character set: %s", node.CharacterSet)
		}
		switch lit := expr.(type) {
		case *Literal:
			lit.Val.collation.Collation = coll.ID()
		case *BindVariable:
			lit.collation.Collation = coll.ID()
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] character set introducers are only supported for literals and arguments")
		}
		return expr, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %T", ErrConvertExprNotSupported, e)
}
