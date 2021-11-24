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
		return &EqualOp{"=", func(cmp int) bool { return cmp == 0 }}
	case sqlparser.LessThanOp:
		return &EqualOp{"<", func(cmp int) bool { return cmp < 0 }}
	case sqlparser.GreaterThanOp:
		return &EqualOp{">", func(cmp int) bool { return cmp > 0 }}
	case sqlparser.LessEqualOp:
		return &EqualOp{"<=", func(cmp int) bool { return cmp <= 0 }}
	case sqlparser.GreaterEqualOp:
		return &EqualOp{">=", func(cmp int) bool { return cmp >= 0 }}
	case sqlparser.NotEqualOp:
		return &EqualOp{"!=", func(cmp int) bool { return cmp != 0 }}
	case sqlparser.NullSafeEqualOp:
		return &NullSafeEqualOp{}
	case sqlparser.InOp:
		return &InOp{}
	case sqlparser.NotInOp:
		return &InOp{Negate: true}
	case sqlparser.LikeOp:
		return &LikeOp{}
	case sqlparser.NotLikeOp:
		return &LikeOp{Negate: true}
	case sqlparser.RegexpOp:
		return &RegexpOp{}
	case sqlparser.NotRegexpOp:
		return &RegexpOp{Negate: true}
	default:
		return nil
	}
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

func simplifyExpr(e Expr) (Expr, error) {
	var err error

	switch node := e.(type) {
	case *ComparisonExpr:
		node.Left, err = simplifyExpr(node.Left)
		if err != nil {
			return nil, err
		}
		node.Right, err = simplifyExpr(node.Right)
		if err != nil {
			return nil, err
		}
		lit1, _ := node.Left.(*Literal)
		lit2, _ := node.Right.(*Literal)
		if lit1 != nil && lit2 != nil {
			res, err := node.Evaluate(nil)
			if err != nil {
				return nil, err
			}
			return &Literal{Val: res}, nil
		}

		if lit1 != nil && node.CoerceLeft != nil {
			lit1.Val.bytes, _ = node.CoerceLeft(nil, lit1.Val.bytes)
			lit1.Val.collation = node.TypedCollation
			node.CoerceLeft = nil
		}
		if lit2 != nil && node.CoerceRight != nil {
			lit2.Val.bytes, _ = node.CoerceRight(nil, lit2.Val.bytes)
			lit2.Val.collation = node.TypedCollation
			node.CoerceRight = nil
		}

		switch op := node.Op.(type) {
		case *LikeOp:
			if lit2 != nil {
				coll := collations.Local().LookupByID(node.TypedCollation.Collation)
				op.Match = coll.Wildcard(lit2.Val.bytes, 0, 0, 0)
			}
		}

	case *BinaryExpr:
		node.Left, err = simplifyExpr(node.Left)
		if err != nil {
			return nil, err
		}
		node.Right, err = simplifyExpr(node.Right)
		if err != nil {
			return nil, err
		}
		_, lit1 := node.Left.(*Literal)
		_, lit2 := node.Right.(*Literal)
		if lit1 && lit2 {
			res, err := node.Evaluate(nil)
			if err != nil {
				return nil, err
			}
			return &Literal{Val: res}, nil
		}

	case *CollateExpr:
		lit, _ := node.Expr.(*Literal)
		if lit != nil {
			res, err := node.Evaluate(nil)
			if err != nil {
				return nil, err
			}
			return &Literal{Val: res}, nil
		}

	case Tuple:
		var err error
		var literal = true
		for i, expr := range node {
			expr, err = simplifyExpr(expr)
			if err != nil {
				return nil, err
			}
			if _, isLiteral := expr.(*Literal); !isLiteral {
				literal = false
			}
			node[i] = expr
		}
		if literal {
			// TODO: optimize with a set
		}
	}
	return e, nil
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
		left, err := convertExpr(node.Left, lookup)
		if err != nil {
			return nil, err
		}
		right, err := convertExpr(node.Right, lookup)
		if err != nil {
			return nil, err
		}
		comp := &ComparisonExpr{
			Op:    translateComparisonOperator(node.Operator),
			Left:  left,
			Right: right,
		}

		leftColl := left.Collation()
		rightColl := right.Collation()
		if leftColl.Valid() && rightColl.Valid() {
			env := collations.Local()
			comp.TypedCollation, comp.CoerceLeft, comp.CoerceRight, err =
				env.MergeCollations(leftColl, rightColl, collations.CoercionOptions{
					ConvertToSuperset:   true,
					ConvertWithCoercion: true,
				})
			if err != nil {
				return nil, err
			}
		}

		return comp, nil
	case sqlparser.Argument:
		collation := getCollation(e, lookup)
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
			return NewLiteralInt(1), nil
		}
		return NewLiteralInt(0), nil
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
		left, err := convertExpr(node.Left, lookup)
		if err != nil {
			return nil, err
		}
		right, err := convertExpr(node.Right, lookup)
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
			convertedExpr, err := convertExpr(expr, lookup)
			if err != nil {
				return nil, err
			}
			res = append(res, convertedExpr)
		}
		return res, nil
	case *sqlparser.NullVal:
		return Null{}, nil
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
			Expr: expr,
			TypedCollation: collations.TypedCollation{
				Collation:    coll.ID(),
				Coercibility: collations.CoerceExplicit,
				Repertoire:   collations.RepertoireUnicode,
			},
		}, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %T", ErrConvertExprNotSupported, e)
}
