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
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"
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

	var check func(result *EvalResult) bool

	switch op {
	case sqlparser.IsNullOp:
		check = func(er *EvalResult) bool { return er.isNull() }
	case sqlparser.IsNotNullOp:
		check = func(er *EvalResult) bool { return !er.isNull() }
	case sqlparser.IsTrueOp:
		check = func(er *EvalResult) bool { return er.isTruthy() == boolTrue }
	case sqlparser.IsNotTrueOp:
		check = func(er *EvalResult) bool { return er.isTruthy() != boolTrue }
	case sqlparser.IsFalseOp:
		check = func(er *EvalResult) bool { return er.isTruthy() == boolFalse }
	case sqlparser.IsNotFalseOp:
		check = func(er *EvalResult) bool { return er.isTruthy() != boolFalse }
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
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &OpAddition{}}, nil
	case sqlparser.MinusOp:
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &OpSubstraction{}}, nil
	case sqlparser.MultOp:
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &OpMultiplication{}}, nil
	case sqlparser.DivOp:
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &OpDivision{}}, nil
	case sqlparser.BitAndOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &OpBitAnd{}}, nil
	case sqlparser.BitOrOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &OpBitOr{}}, nil
	case sqlparser.BitXorOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &OpBitXor{}}, nil
	case sqlparser.ShiftLeftOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &OpBitShiftLeft{}}, nil
	case sqlparser.ShiftRightOp:
		return &BitwiseExpr{BinaryExpr: binaryExpr, Op: &OpBitShiftRight{}}, nil
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
			lit.Val.makeBinary()
		default:
			lit.Val.makeTextual(collation)
		}
	case *BindVariable:
		switch collation {
		case collations.CollationBinaryID:
			lit.coerceType = sqltypes.VarBinary
			lit.coll = collationBinary
		default:
			lit.coerceType = sqltypes.VarChar
			lit.coll.Collation = collation
		}
	default:
		panic("character set introducers are only supported for literals and arguments")
	}
	return expr, nil
}

func translateFuncExpr(fn *sqlparser.FuncExpr, lookup TranslationLookup) (Expr, error) {
	var args TupleExpr
	var aliases []sqlparser.IdentifierCI
	for _, expr := range fn.Exprs {
		aliased, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, translateExprNotSupported(fn)
		}
		convertedExpr, err := translateExpr(aliased.Expr, lookup)
		if err != nil {
			return nil, err
		}
		args = append(args, convertedExpr)
		aliases = append(aliases, aliased.As)
	}

	method := fn.Name.Lowered()

	if rewrite, ok := builtinFunctionsRewrite[method]; ok {
		return rewrite(args, lookup)
	}

	if call, ok := builtinFunctions[method]; ok {
		return &CallExpr{
			Arguments: args,
			Aliases:   aliases,
			Method:    method,
			F:         call,
		}, nil
	}

	return nil, translateExprNotSupported(fn)
}

func translateIntegral(lit *sqlparser.Literal, lookup TranslationLookup) (int, bool, error) {
	if lit == nil {
		return 0, false, nil
	}
	literal, err := translateLiteral(lit, lookup)
	if err != nil {
		return 0, false, err
	}
	// this conversion is always valid because the SQL parser enforces Length to be an integral
	return int(literal.Val.uint64()), true, nil
}

func translateWeightStringFuncExpr(wsfn *sqlparser.WeightStringFuncExpr, lookup TranslationLookup) (Expr, error) {
	var (
		call WeightStringCallExpr
		err  error
	)
	call.String, err = translateExpr(wsfn.Expr, lookup)
	if err != nil {
		return nil, err
	}
	if wsfn.As != nil {
		call.Cast = strings.ToLower(wsfn.As.Type)
		call.Len, call.HasLen, err = translateIntegral(wsfn.As.Length, lookup)
		if err != nil {
			return nil, err
		}
	}
	return &call, nil
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

func binaryCollationForCollation(collation collations.ID) collations.ID {
	binary := collations.Local().LookupByID(collation)
	if binary == nil {
		return collations.Unknown
	}
	binaryCollation := collations.Local().BinaryCollationForCharset(binary.Charset().Name())
	if binaryCollation == nil {
		return collations.Unknown
	}
	return binaryCollation.ID()
}

func translateConvertCharset(charset string, binary bool, lookup TranslationLookup) (collations.ID, error) {
	if charset == "" {
		collation := lookup.DefaultCollation()
		if binary {
			collation = binaryCollationForCollation(collation)
		}
		if collation == collations.Unknown {
			return collations.Unknown, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No default character set specified")
		}
		return collation, nil
	}
	charset = strings.ToLower(charset)
	collation := collations.Local().DefaultCollationForCharset(charset)
	if collation == nil {
		return collations.Unknown, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unknown character set: '%s'", charset)
	}
	collationID := collation.ID()
	if binary {
		collationID = binaryCollationForCollation(collationID)
		if collationID == collations.Unknown {
			return collations.Unknown, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No binary collation found for character set: %s ", charset)
		}
	}
	return collationID, nil
}

func translateConvertExpr(expr sqlparser.Expr, convertType *sqlparser.ConvertType, lookup TranslationLookup) (Expr, error) {
	var (
		convert ConvertExpr
		err     error
	)

	convert.Inner, err = translateExpr(expr, lookup)
	if err != nil {
		return nil, err
	}

	convert.Length, convert.HasLength, err = translateIntegral(convertType.Length, lookup)
	if err != nil {
		return nil, err
	}

	convert.Scale, convert.HasScale, err = translateIntegral(convertType.Scale, lookup)
	if err != nil {
		return nil, err
	}

	convert.Type = strings.ToUpper(convertType.Type)
	switch convert.Type {
	case "DECIMAL":
		if convert.Length < convert.Scale {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s').",
				"", // TODO: column name
			)
		}
		if convert.Length > decimal.MyMaxPrecision {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"Too-big precision %d specified for '%s'. Maximum is %d.",
				convert.Length, sqlparser.String(expr), decimal.MyMaxPrecision)
		}
		if convert.Scale > decimal.MyMaxScale {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"Too big scale %d specified for column '%s'. Maximum is %d.",
				convert.Scale, sqlparser.String(expr), decimal.MyMaxScale)
		}
	case "NCHAR":
		convert.Collation = collations.CollationUtf8ID
	case "CHAR":
		convert.Collation, err = translateConvertCharset(convertType.Charset.Name, convertType.Charset.Binary, lookup)
		if err != nil {
			return nil, err
		}
	case "BINARY", "DOUBLE", "REAL", "SIGNED", "SIGNED INTEGER", "UNSIGNED", "UNSIGNED INTEGER":
		// Supported types for conv expression
	default:
		// For unsupported types, we should return an error on translation instead of returning an error on runtime.
		return nil, convert.returnUnsupportedError()
	}

	return &convert, nil
}

func translateConvertUsingExpr(expr *sqlparser.ConvertUsingExpr, lookup TranslationLookup) (Expr, error) {
	var (
		using ConvertUsingExpr
		err   error
	)

	using.Inner, err = translateExpr(expr.Expr, lookup)
	if err != nil {
		return nil, err
	}

	using.Collation, err = translateConvertCharset(expr.Type, false, lookup)
	if err != nil {
		return nil, err
	}

	return &using, nil
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

func translateExprNotSupported(e sqlparser.Expr) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %s", ErrTranslateExprNotSupported, sqlparser.String(e))
}

func translateExpr(e sqlparser.Expr, lookup TranslationLookup) (Expr, error) {
	switch node := e.(type) {
	case sqlparser.BoolVal:
		if node {
			return NewLiteralInt(1), nil
		}
		return NewLiteralInt(0), nil
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
		collation := getCollation(e, lookup)
		return NewBindVar(string(node), collation), nil
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
	case *sqlparser.FuncExpr:
		return translateFuncExpr(node, lookup)
	case *sqlparser.WeightStringFuncExpr:
		return translateWeightStringFuncExpr(node, lookup)
	case *sqlparser.UnaryExpr:
		return translateUnaryExpr(node, lookup)
	case *sqlparser.CastExpr:
		return translateConvertExpr(node.Expr, node.Type, lookup)
	case *sqlparser.ConvertExpr:
		return translateConvertExpr(node.Expr, node.Type, lookup)
	case *sqlparser.ConvertUsingExpr:
		return translateConvertUsingExpr(node, lookup)
	case *sqlparser.CaseExpr:
		return translateCaseExpr(node, lookup)
	default:
		return nil, translateExprNotSupported(e)
	}
}

func TranslateEx(e sqlparser.Expr, lookup TranslationLookup, simplify bool) (Expr, error) {
	expr, err := translateExpr(e, lookup)
	if err != nil {
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
