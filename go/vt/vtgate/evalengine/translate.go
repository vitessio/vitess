/*
Copyright 2023 The Vitess Authors.

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
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	TranslationLookup interface {
		ColumnLookup(col *sqlparser.ColName) (int, error)
		CollationForExpr(expr sqlparser.Expr) collations.ID
		DefaultCollation() collations.ID
	}
)

var ErrTranslateExprNotSupported = "expr cannot be translated, not supported"
var ErrEvaluatedExprNotSupported = "expr cannot be evaluated, not supported"

func translateComparisonExpr(op sqlparser.ComparisonExprOperator, left, right sqlparser.Expr, lookup TranslationLookup) (Expr, error) {
	l, err := translateExpr(left, lookup)
	if err != nil {
		return nil, err
	}
	r, err := translateExpr(right, lookup)
	if err != nil {
		return nil, err
	}
	return translateComparisonExpr2(op, l, r)
}

func translateComparisonExpr2(op sqlparser.ComparisonExprOperator, left, right Expr) (Expr, error) {
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

	switch op {
	case sqlparser.EqualOp:
		return &ComparisonExpr{binaryExpr, compareEQ{}}, nil
	case sqlparser.NotEqualOp:
		return &ComparisonExpr{binaryExpr, compareNE{}}, nil
	case sqlparser.LessThanOp:
		return &ComparisonExpr{binaryExpr, compareLT{}}, nil
	case sqlparser.LessEqualOp:
		return &ComparisonExpr{binaryExpr, compareLE{}}, nil
	case sqlparser.GreaterThanOp:
		return &ComparisonExpr{binaryExpr, compareGT{}}, nil
	case sqlparser.GreaterEqualOp:
		return &ComparisonExpr{binaryExpr, compareGE{}}, nil
	case sqlparser.NullSafeEqualOp:
		return &ComparisonExpr{binaryExpr, compareNullSafeEQ{}}, nil
	case sqlparser.LikeOp:
		return &LikeExpr{BinaryExpr: binaryExpr}, nil
	case sqlparser.NotLikeOp:
		return &LikeExpr{BinaryExpr: binaryExpr, Negate: true}, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, op.ToString())
	}
}

func translateLogicalNot(inner Expr) Expr {
	return &NotExpr{UnaryExpr{inner}}
}

func translateLogicalExpr(opname string, left, right sqlparser.Expr, lookup TranslationLookup) (Expr, error) {
	l, err := translateExpr(left, lookup)
	if err != nil {
		return nil, err
	}

	if opname == "NOT" {
		return translateLogicalNot(l), nil
	}

	r, err := translateExpr(right, lookup)
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

func translateIsExpr(left sqlparser.Expr, op sqlparser.IsExprOperator, lookup TranslationLookup) (Expr, error) {
	expr, err := translateExpr(left, lookup)
	if err != nil {
		return nil, err
	}

	var check func(e eval) bool
	switch op {
	case sqlparser.IsNullOp:
		check = func(e eval) bool { return e == nil }
	case sqlparser.IsNotNullOp:
		check = func(e eval) bool { return e != nil }
	case sqlparser.IsTrueOp:
		check = func(e eval) bool { return evalIsTruthy(e) == boolTrue }
	case sqlparser.IsNotTrueOp:
		check = func(e eval) bool { return evalIsTruthy(e) != boolTrue }
	case sqlparser.IsFalseOp:
		check = func(e eval) bool { return evalIsTruthy(e) == boolFalse }
	case sqlparser.IsNotFalseOp:
		check = func(e eval) bool { return evalIsTruthy(e) != boolFalse }
	}

	return &IsExpr{
		UnaryExpr: UnaryExpr{expr},
		Op:        op,
		Check:     check,
	}, nil
}

func getCollation(expr sqlparser.Expr, lookup TranslationLookup) collations.TypedCollation {
	collation := collations.TypedCollation{
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireUnicode,
	}
	if lookup != nil {
		collation.Collation = lookup.CollationForExpr(expr)
		if collation.Collation == collations.Unknown {
			collation.Collation = lookup.DefaultCollation()
		}
	} else {
		collation.Collation = collations.Default()
	}
	return collation
}

func translateColName(colname *sqlparser.ColName, lookup TranslationLookup) (Expr, error) {
	if lookup == nil {
		return nil, vterrors.Wrap(translateExprNotSupported(colname), "cannot lookup column")
	}
	idx, err := lookup.ColumnLookup(colname)
	if err != nil {
		return nil, err
	}
	collation := getCollation(colname, lookup)
	return NewColumn(idx, collation), nil
}

func translateLiteral(lit *sqlparser.Literal, lookup TranslationLookup) (*Literal, error) {
	switch lit.Type {
	case sqlparser.IntVal:
		return NewLiteralIntegralFromBytes(lit.Bytes())
	case sqlparser.FloatVal:
		return NewLiteralFloatFromBytes(lit.Bytes())
	case sqlparser.DecimalVal:
		return NewLiteralDecimalFromBytes(lit.Bytes())
	case sqlparser.StrVal:
		collation := getCollation(lit, lookup)
		return NewLiteralString(lit.Bytes(), collation), nil
	case sqlparser.HexNum:
		return NewLiteralBinaryFromHexNum(lit.Bytes())
	case sqlparser.HexVal:
		return NewLiteralBinaryFromHex(lit.Bytes())
	case sqlparser.DateVal:
		return NewLiteralDateFromBytes(lit.Bytes())
	case sqlparser.TimeVal:
		return NewLiteralTimeFromBytes(lit.Bytes())
	case sqlparser.TimestampVal:
		return NewLiteralDatetimeFromBytes(lit.Bytes())
	default:
		return nil, translateExprNotSupported(lit)
	}
}

func translateBinaryExpr(binary *sqlparser.BinaryExpr, lookup TranslationLookup) (Expr, error) {
	left, err := translateExpr(binary.Left, lookup)
	if err != nil {
		return nil, err
	}
	right, err := translateExpr(binary.Right, lookup)
	if err != nil {
		return nil, err
	}
	binaryExpr := BinaryExpr{
		Left:  left,
		Right: right,
	}

	switch binary.Operator {
	case sqlparser.PlusOp:
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &opArithAdd{}}, nil
	case sqlparser.MinusOp:
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &opArithSub{}}, nil
	case sqlparser.MultOp:
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &opArithMul{}}, nil
	case sqlparser.DivOp:
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &opArithDiv{}}, nil
	case sqlparser.BitAndOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &opBitAnd{}}, nil
	case sqlparser.BitOrOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &opBitOr{}}, nil
	case sqlparser.BitXorOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &opBitXor{}}, nil
	case sqlparser.ShiftLeftOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &opBitShl{}}, nil
	case sqlparser.ShiftRightOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &opBitShr{}}, nil
	case sqlparser.JSONExtractOp:
		return builtinJSONExtractRewrite(left, right)
	case sqlparser.JSONUnquoteExtractOp:
		return builtinJSONExtractUnquoteRewrite(left, right)
	default:
		return nil, translateExprNotSupported(binary)
	}
}

func translateTuple(tuple sqlparser.ValTuple, lookup TranslationLookup) (Expr, error) {
	var exprs TupleExpr
	for _, expr := range tuple {
		convertedExpr, err := translateExpr(expr, lookup)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, convertedExpr)
	}
	return exprs, nil
}

func translateCollateExpr(collate *sqlparser.CollateExpr, lookup TranslationLookup) (Expr, error) {
	expr, err := translateExpr(collate.Expr, lookup)
	if err != nil {
		return nil, err
	}
	coll := collations.Local().LookupByName(collate.Collation)
	if coll == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unknown collation: '%s'", collate.Collation)
	}
	return &CollateExpr{
		UnaryExpr: UnaryExpr{expr},
		TypedCollation: collations.TypedCollation{
			Collation:    coll.ID(),
			Coercibility: collations.CoerceExplicit,
			Repertoire:   collations.RepertoireUnicode,
		},
	}, nil
}

func translateIntroducerExpr(introduced *sqlparser.IntroducerExpr, lookup TranslationLookup) (Expr, error) {
	expr, err := translateExpr(introduced.Expr, lookup)
	if err != nil {
		return nil, err
	}

	var collation collations.ID
	if strings.ToLower(introduced.CharacterSet) == "_binary" {
		collation = collations.CollationBinaryID
	} else {
		defaultCollation := collations.Local().DefaultCollationForCharset(introduced.CharacterSet[1:])
		if defaultCollation == nil {
			panic(fmt.Sprintf("unknown character set: %s", introduced.CharacterSet))
		}
		collation = defaultCollation.ID()
	}

	switch lit := expr.(type) {
	case *Literal:
		switch collation {
		case collations.CollationBinaryID:
			lit.inner = evalToBinary(lit.inner)
		default:
			lit.inner, err = evalToVarchar(lit.inner, collation, false)
			if err != nil {
				return nil, err
			}
		}
	case *BindVariable:
		if lit.tuple {
			panic("parser allowed introducer before tuple")
		}

		switch collation {
		case collations.CollationBinaryID:
			lit.coerce = sqltypes.VarBinary
			lit.col = collationBinary
		default:
			lit.coerce = sqltypes.VarChar
			lit.col.Collation = collation
		}
	default:
		panic("character set introducers are only supported for literals and arguments")
	}
	return expr, nil
}

func translateIntegral(lit *sqlparser.Literal, lookup TranslationLookup) (int, bool, error) {
	if lit == nil {
		return 0, false, nil
	}
	literal, err := translateLiteral(lit, lookup)
	if err != nil {
		return 0, false, err
	}
	return int(evalToNumeric(literal.inner).toUint64().u), true, nil
}

func translateUnaryExpr(unary *sqlparser.UnaryExpr, lookup TranslationLookup) (Expr, error) {
	expr, err := translateExpr(unary.Expr, lookup)
	if err != nil {
		return nil, err
	}

	switch unary.Operator {
	case sqlparser.UMinusOp:
		return &NegateExpr{UnaryExpr: UnaryExpr{expr}}, nil
	case sqlparser.BangOp:
		return translateLogicalNot(expr), nil
	case sqlparser.TildaOp:
		return &BitwiseNotExpr{UnaryExpr: UnaryExpr{expr}}, nil
	case sqlparser.NStringOp:
		return &ConvertExpr{UnaryExpr: UnaryExpr{expr}, Type: "NCHAR", Collation: collations.CollationUtf8ID}, nil
	default:
		return nil, translateExprNotSupported(unary)
	}
}

func translateCaseExpr(node *sqlparser.CaseExpr, lookup TranslationLookup) (Expr, error) {
	var err error
	var result CaseExpr

	if node.Else != nil {
		result.Else, err = translateExpr(node.Else, lookup)
		if err != nil {
			return nil, err
		}
	}

	var cmpbase Expr
	if node.Expr != nil {
		cmpbase, err = translateExpr(node.Expr, lookup)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range node.Whens {
		var cond, val Expr

		cond, err = translateExpr(when.Cond, lookup)
		if err != nil {
			return nil, err
		}

		val, err = translateExpr(when.Val, lookup)
		if err != nil {
			return nil, err
		}

		if cmpbase != nil {
			cond, err = translateComparisonExpr2(sqlparser.EqualOp, cmpbase, cond)
			if err != nil {
				return nil, err
			}
		}

		result.cases = append(result.cases, WhenThen{
			when: cond,
			then: val,
		})
	}

	return &result, nil
}

func translateBetweenExpr(node *sqlparser.BetweenExpr, lookup TranslationLookup) (Expr, error) {
	// x BETWEEN a AND b => x >= a AND x <= b
	from := &sqlparser.ComparisonExpr{
		Operator: sqlparser.GreaterEqualOp,
		Left:     node.Left,
		Right:    node.From,
	}
	to := &sqlparser.ComparisonExpr{
		Operator: sqlparser.LessEqualOp,
		Left:     node.Left,
		Right:    node.To,
	}

	if !node.IsBetween {
		// x NOT BETWEEN a AND b  => x < a OR x > b
		from.Operator = sqlparser.LessThanOp
		to.Operator = sqlparser.GreaterThanOp
		return translateExpr(&sqlparser.OrExpr{Left: from, Right: to}, lookup)
	}

	return translateExpr(sqlparser.AndExpressions(from, to), lookup)
}

func translateExprNotSupported(e sqlparser.Expr) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %s", ErrTranslateExprNotSupported, sqlparser.String(e))
}

func translateExpr(e sqlparser.Expr, lookup TranslationLookup) (Expr, error) {
	switch node := e.(type) {
	case sqlparser.BoolVal:
		return NewLiteralBool(bool(node)), nil
	case *sqlparser.ColName:
		return translateColName(node, lookup)
	case *sqlparser.Offset:
		return NewColumn(node.V, getCollation(node, lookup)), nil
	case *sqlparser.ComparisonExpr:
		return translateComparisonExpr(node.Operator, node.Left, node.Right, lookup)
	case sqlparser.Argument:
		collation := getCollation(e, lookup)
		return NewBindVar(string(node), collation), nil
	case sqlparser.ListArg:
		return NewBindVarTuple(string(node)), nil
	case *sqlparser.Literal:
		return translateLiteral(node, lookup)
	case *sqlparser.AndExpr:
		return translateLogicalExpr("AND", node.Left, node.Right, lookup)
	case *sqlparser.OrExpr:
		return translateLogicalExpr("OR", node.Left, node.Right, lookup)
	case *sqlparser.XorExpr:
		return translateLogicalExpr("XOR", node.Left, node.Right, lookup)
	case *sqlparser.NotExpr:
		return translateLogicalExpr("NOT", node.Expr, nil, lookup)
	case *sqlparser.BinaryExpr:
		return translateBinaryExpr(node, lookup)
	case sqlparser.ValTuple:
		return translateTuple(node, lookup)
	case *sqlparser.NullVal:
		return NullExpr, nil
	case *sqlparser.CollateExpr:
		return translateCollateExpr(node, lookup)
	case *sqlparser.IntroducerExpr:
		return translateIntroducerExpr(node, lookup)
	case *sqlparser.IsExpr:
		return translateIsExpr(node.Left, node.Right, lookup)
	case sqlparser.Callable:
		return translateCallable(node, lookup)
	case *sqlparser.UnaryExpr:
		return translateUnaryExpr(node, lookup)
	case *sqlparser.CastExpr:
		return translateConvertExpr(node.Expr, node.Type, lookup)
	case *sqlparser.CaseExpr:
		return translateCaseExpr(node, lookup)
	case *sqlparser.BetweenExpr:
		return translateBetweenExpr(node, lookup)
	default:
		return nil, translateExprNotSupported(e)
	}
}

func TranslateEx(e sqlparser.Expr, lookup TranslationLookup, simplify bool) (Expr, error) {
	expr, err := translateExpr(e, lookup)
	if err != nil {
		return nil, err
	}

	if err := (cardinalityCheck{}).expr(expr); err != nil {
		return nil, err
	}

	if simplify {
		var staticEnv ExpressionEnv
		if lookup != nil {
			staticEnv.DefaultCollation = lookup.DefaultCollation()
		} else {
			staticEnv.DefaultCollation = collations.Default()
		}
		expr, err = simplifyExpr(&staticEnv, expr)
	}
	return expr, err
}

// Translate translates between AST expressions and executable expressions
func Translate(e sqlparser.Expr, lookup TranslationLookup) (Expr, error) {
	return TranslateEx(e, lookup, true)
}
