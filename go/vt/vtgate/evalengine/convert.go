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
	ConverterLookup interface {
		ColumnLookup(col *sqlparser.ColName) (int, error)
		CollationForExpr(expr sqlparser.Expr) collations.ID
		DefaultCollation() collations.ID
	}
)

var ErrConvertExprNotSupported = "expr cannot be converted, not supported"
var ErrEvaluatedExprNotSupported = "expr cannot be evaluated, not supported"

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

func convertLogicalNot(inner Expr) Expr {
	return &NotExpr{UnaryExpr{inner}}
}

func convertLogicalExpr(opname string, left, right sqlparser.Expr, lookup ConverterLookup) (Expr, error) {
	l, err := convertExpr(left, lookup)
	if err != nil {
		return nil, err
	}

	if opname == "NOT" {
		return convertLogicalNot(l), nil
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

func convertIsExpr(left sqlparser.Expr, op sqlparser.IsExprOperator, lookup ConverterLookup) (Expr, error) {
	expr, err := convertExpr(left, lookup)
	if err != nil {
		return nil, err
	}

	var check func(result *EvalResult) bool

	switch op {
	case sqlparser.IsNullOp:
		check = func(er *EvalResult) bool { return er.null() }
	case sqlparser.IsNotNullOp:
		check = func(er *EvalResult) bool { return !er.null() }
	case sqlparser.IsTrueOp:
		check = func(er *EvalResult) bool { return er.truthy() == boolTrue }
	case sqlparser.IsNotTrueOp:
		check = func(er *EvalResult) bool { return er.truthy() != boolTrue }
	case sqlparser.IsFalseOp:
		check = func(er *EvalResult) bool { return er.truthy() == boolFalse }
	case sqlparser.IsNotFalseOp:
		check = func(er *EvalResult) bool { return er.truthy() != boolFalse }
	}

	return &IsExpr{
		UnaryExpr: UnaryExpr{expr},
		Op:        op,
		Check:     check,
	}, nil
}

func getCollation(expr sqlparser.Expr, lookup ConverterLookup) collations.TypedCollation {
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

func convertColName(colname *sqlparser.ColName, lookup ConverterLookup) (Expr, error) {
	if lookup == nil {
		return nil, vterrors.Wrap(convertNotSupported(colname), "cannot lookup column")
	}
	idx, err := lookup.ColumnLookup(colname)
	if err != nil {
		return nil, err
	}
	collation := getCollation(colname, lookup)
	return NewColumn(idx, collation), nil
}

func convertLiteral(lit *sqlparser.Literal, lookup ConverterLookup) (*Literal, error) {
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
	default:
		return nil, convertNotSupported(lit)
	}
}

func convertBinaryExpr(binary *sqlparser.BinaryExpr, lookup ConverterLookup) (Expr, error) {
	left, err := convertExpr(binary.Left, lookup)
	if err != nil {
		return nil, err
	}
	right, err := convertExpr(binary.Right, lookup)
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
		return nil, convertNotSupported(binary)
	}
}

func convertTuple(tuple sqlparser.ValTuple, lookup ConverterLookup) (Expr, error) {
	var exprs TupleExpr
	for _, expr := range tuple {
		convertedExpr, err := convertExpr(expr, lookup)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, convertedExpr)
	}
	return exprs, nil
}

func convertCollateExpr(collate *sqlparser.CollateExpr, lookup ConverterLookup) (Expr, error) {
	expr, err := convertExpr(collate.Expr, lookup)
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

func convertIntroducerExpr(introduced *sqlparser.IntroducerExpr, lookup ConverterLookup) (Expr, error) {
	expr, err := convertExpr(introduced.Expr, lookup)
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

func convertFuncExpr(fn *sqlparser.FuncExpr, lookup ConverterLookup) (Expr, error) {
	method := fn.Name.Lowered()
	call, ok := builtinFunctions[method]
	if !ok {
		return nil, convertNotSupported(fn)
	}

	var args TupleExpr
	var aliases []sqlparser.ColIdent
	for _, expr := range fn.Exprs {
		aliased, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, convertNotSupported(fn)
		}
		convertedExpr, err := convertExpr(aliased.Expr, lookup)
		if err != nil {
			return nil, err
		}
		args = append(args, convertedExpr)
		aliases = append(aliases, aliased.As)
	}
	return &CallExpr{
		Arguments: args,
		Aliases:   aliases,
		Method:    method,
		Call:      call,
	}, nil
}

func convertWeightStringFuncExpr(wsfn *sqlparser.WeightStringFuncExpr, lookup ConverterLookup) (Expr, error) {
	inner, err := convertExpr(wsfn.Expr, lookup)
	if err != nil {
		return nil, err
	}
	var length int
	var ttype string
	if wsfn.As != nil {
		ttype = strings.ToLower(wsfn.As.Type)
		literal, err := convertLiteral(wsfn.As.Length, lookup)
		if err != nil {
			return nil, err
		}
		// this conversion is always valid because the SQL parser enforces Length to be an integral
		length = int(literal.Val.uint64())
	}
	return &WeightStringCallExpr{
		String: inner,
		Cast:   ttype,
		Len:    length,
	}, nil
}

func convertUnaryExpr(unary *sqlparser.UnaryExpr, lookup ConverterLookup) (Expr, error) {
	expr, err := convertExpr(unary.Expr, lookup)
	if err != nil {
		return nil, err
	}

	switch unary.Operator {
	case sqlparser.UMinusOp:
		return &NegateExpr{UnaryExpr: UnaryExpr{expr}}, nil
	case sqlparser.BangOp:
		return convertLogicalNot(expr), nil
	case sqlparser.TildaOp:
		return &BitwiseNotExpr{UnaryExpr: UnaryExpr{expr}}, nil
	default:
		return nil, convertNotSupported(unary)
	}
}

func convertNotSupported(e sqlparser.Expr) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %s", ErrConvertExprNotSupported, sqlparser.String(e))
}

func convertExpr(e sqlparser.Expr, lookup ConverterLookup) (Expr, error) {
	switch node := e.(type) {
	case sqlparser.BoolVal:
		if node {
			return NewLiteralInt(1), nil
		}
		return NewLiteralInt(0), nil
	case *sqlparser.ColName:
		return convertColName(node, lookup)
	case *sqlparser.ComparisonExpr:
		return convertComparisonExpr(node.Operator, node.Left, node.Right, lookup)
	case sqlparser.Argument:
		collation := getCollation(e, lookup)
		return NewBindVar(string(node), collation), nil
	case sqlparser.ListArg:
		collation := getCollation(e, lookup)
		return NewBindVar(string(node), collation), nil
	case *sqlparser.Literal:
		return convertLiteral(node, lookup)
	case *sqlparser.AndExpr:
		return convertLogicalExpr("AND", node.Left, node.Right, lookup)
	case *sqlparser.OrExpr:
		return convertLogicalExpr("OR", node.Left, node.Right, lookup)
	case *sqlparser.XorExpr:
		return convertLogicalExpr("XOR", node.Left, node.Right, lookup)
	case *sqlparser.NotExpr:
		return convertLogicalExpr("NOT", node.Expr, nil, lookup)
	case *sqlparser.BinaryExpr:
		return convertBinaryExpr(node, lookup)
	case sqlparser.ValTuple:
		return convertTuple(node, lookup)
	case *sqlparser.NullVal:
		return NullExpr, nil
	case *sqlparser.CollateExpr:
		return convertCollateExpr(node, lookup)
	case *sqlparser.IntroducerExpr:
		return convertIntroducerExpr(node, lookup)
	case *sqlparser.IsExpr:
		return convertIsExpr(node.Left, node.Right, lookup)
	case *sqlparser.FuncExpr:
		return convertFuncExpr(node, lookup)
	case *sqlparser.WeightStringFuncExpr:
		return convertWeightStringFuncExpr(node, lookup)
	case *sqlparser.UnaryExpr:
		return convertUnaryExpr(node, lookup)
	default:
		return nil, convertNotSupported(e)
	}
}

func ConvertEx(e sqlparser.Expr, lookup ConverterLookup, simplify bool) (Expr, error) {
	expr, err := convertExpr(e, lookup)
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

// Convert converts between AST expressions and executable expressions
func Convert(e sqlparser.Expr, lookup ConverterLookup) (Expr, error) {
	return ConvertEx(e, lookup, true)
}
