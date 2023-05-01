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
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

var ErrTranslateExprNotSupported = "expr cannot be translated, not supported"
var ErrEvaluatedExprNotSupported = "expr cannot be evaluated, not supported"

func (ast *astCompiler) translateComparisonExpr(op sqlparser.ComparisonExprOperator, left, right sqlparser.Expr) (Expr, error) {
	l, err := ast.translateExpr(left)
	if err != nil {
		return nil, err
	}
	r, err := ast.translateExpr(right)
	if err != nil {
		return nil, err
	}
	return ast.translateComparisonExpr2(op, l, r)
}

func (ast *astCompiler) translateComparisonExpr2(op sqlparser.ComparisonExprOperator, left, right Expr) (Expr, error) {
	binaryExpr := BinaryExpr{
		Left:  left,
		Right: right,
	}

	if op == sqlparser.InOp || op == sqlparser.NotInOp {
		return &InExpr{
			BinaryExpr: binaryExpr,
			Negate:     op == sqlparser.NotInOp,
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

func (ast *astCompiler) translateLogicalNot(inner Expr) Expr {
	return &NotExpr{UnaryExpr{inner}}
}

func (ast *astCompiler) translateLogicalExpr(opname string, left, right sqlparser.Expr) (Expr, error) {
	l, err := ast.translateExpr(left)
	if err != nil {
		return nil, err
	}

	if opname == "NOT" {
		return ast.translateLogicalNot(l), nil
	}

	r, err := ast.translateExpr(right)
	if err != nil {
		return nil, err
	}

	var logic func(l, r Expr, env *ExpressionEnv) (boolean, error)
	switch opname {
	case "AND":
		logic = func(l, r Expr, env *ExpressionEnv) (boolean, error) { return opAnd(l, r, env) }
	case "OR":
		logic = func(l, r Expr, env *ExpressionEnv) (boolean, error) { return opOr(l, r, env) }
	case "XOR":
		logic = func(l, r Expr, env *ExpressionEnv) (boolean, error) { return opXor(l, r, env) }
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

func (ast *astCompiler) translateIsExpr(left sqlparser.Expr, op sqlparser.IsExprOperator) (Expr, error) {
	expr, err := ast.translateExpr(left)
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

func defaultCoercionCollation(id collations.ID) collations.TypedCollation {
	return collations.TypedCollation{
		Collation:    id,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireUnicode,
	}
}

func (ast *astCompiler) translateBindVar(arg *sqlparser.Argument) (Expr, error) {
	bvar := NewBindVar(arg.Name)
	bvar.Collation.Collation = ast.cfg.Collation

	if arg.Type >= 0 {
		bvar.Type = arg.Type
		bvar.typed = true
	} else {
		ast.untyped++
	}
	return bvar, nil
}

func (ast *astCompiler) translateColOffset(col *sqlparser.Offset) (Expr, error) {
	column := NewColumn(col.V)
	if ast.cfg.ResolveType != nil {
		column.Type, column.Collation.Collation, column.typed = ast.cfg.ResolveType(col.Original)
	}
	if column.Collation.Collation == collations.Unknown {
		column.Collation.Collation = ast.cfg.Collation
	}
	if !column.typed {
		ast.untyped++
	}
	return column, nil
}

func (ast *astCompiler) translateColName(colname *sqlparser.ColName) (Expr, error) {
	if ast.cfg.ResolveColumn == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot lookup column (column access not supported here)")
	}
	idx, err := ast.cfg.ResolveColumn(colname)
	if err != nil {
		return nil, err
	}
	column := NewColumn(idx)
	if ast.cfg.ResolveType != nil {
		column.Type, column.Collation.Collation, column.typed = ast.cfg.ResolveType(colname)
	}
	if column.Collation.Collation == collations.Unknown {
		column.Collation.Collation = ast.cfg.Collation
	}
	if !column.typed {
		ast.untyped++
	}
	return column, nil
}

func translateLiteral(lit *sqlparser.Literal, collation collations.ID) (*Literal, error) {
	switch lit.Type {
	case sqlparser.IntVal:
		return NewLiteralIntegralFromBytes(lit.Bytes())
	case sqlparser.FloatVal:
		return NewLiteralFloatFromBytes(lit.Bytes())
	case sqlparser.DecimalVal:
		return NewLiteralDecimalFromBytes(lit.Bytes())
	case sqlparser.StrVal:
		return NewLiteralString(lit.Bytes(), defaultCoercionCollation(collation)), nil
	case sqlparser.HexNum:
		return NewLiteralBinaryFromHexNum(lit.Bytes())
	case sqlparser.HexVal:
		return NewLiteralBinaryFromHex(lit.Bytes())
	case sqlparser.BitVal:
		return NewLiteralBinaryFromBit(lit.Bytes())
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

func (ast *astCompiler) translateBinaryExpr(binary *sqlparser.BinaryExpr) (Expr, error) {
	left, err := ast.translateExpr(binary.Left)
	if err != nil {
		return nil, err
	}
	right, err := ast.translateExpr(binary.Right)
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
	case sqlparser.IntDivOp:
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &opArithIntDiv{}}, nil
	case sqlparser.ModOp:
		return &ArithmeticExpr{BinaryExpr: binaryExpr, Op: &opArithMod{}}, nil
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

func (ast *astCompiler) translateTuple(tuple sqlparser.ValTuple) (Expr, error) {
	var exprs TupleExpr
	for _, expr := range tuple {
		convertedExpr, err := ast.translateExpr(expr)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, convertedExpr)
	}
	return exprs, nil
}

func (ast *astCompiler) translateCollateExpr(collate *sqlparser.CollateExpr) (Expr, error) {
	expr, err := ast.translateExpr(collate.Expr)
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

func (ast *astCompiler) translateIntroducerExpr(introduced *sqlparser.IntroducerExpr) (Expr, error) {
	expr, err := ast.translateExpr(introduced.Expr)
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
		if lit.Type == sqltypes.Tuple {
			panic("parser allowed introducer before tuple")
		}

		switch collation {
		case collations.CollationBinaryID:
			lit.Type = sqltypes.VarBinary
			lit.Collation = collationBinary
			lit.typed = true
		default:
			lit.Type = sqltypes.VarChar
			lit.Collation.Collation = collation
			lit.typed = true
		}
	default:
		panic("character set introducers are only supported for literals and arguments")
	}
	return expr, nil
}

func (ast *astCompiler) translateIntegral(lit *sqlparser.Literal) (int, bool, error) {
	if lit == nil {
		return 0, false, nil
	}
	literal, err := translateLiteral(lit, ast.cfg.Collation)
	if err != nil {
		return 0, false, err
	}
	return int(evalToInt64(literal.inner).toUint64().u), true, nil
}

func (ast *astCompiler) translateUnaryExpr(unary *sqlparser.UnaryExpr) (Expr, error) {
	expr, err := ast.translateExpr(unary.Expr)
	if err != nil {
		return nil, err
	}

	switch unary.Operator {
	case sqlparser.UMinusOp:
		return &NegateExpr{UnaryExpr: UnaryExpr{expr}}, nil
	case sqlparser.BangOp:
		return ast.translateLogicalNot(expr), nil
	case sqlparser.TildaOp:
		return &BitwiseNotExpr{UnaryExpr: UnaryExpr{expr}}, nil
	case sqlparser.NStringOp:
		return &ConvertExpr{UnaryExpr: UnaryExpr{expr}, Type: "NCHAR", Collation: collations.CollationUtf8ID}, nil
	default:
		return nil, translateExprNotSupported(unary)
	}
}

func (ast *astCompiler) translateCaseExpr(node *sqlparser.CaseExpr) (Expr, error) {
	var err error
	var result CaseExpr

	if node.Else != nil {
		result.Else, err = ast.translateExpr(node.Else)
		if err != nil {
			return nil, err
		}
	}

	var cmpbase Expr
	if node.Expr != nil {
		cmpbase, err = ast.translateExpr(node.Expr)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range node.Whens {
		var cond, val Expr

		cond, err = ast.translateExpr(when.Cond)
		if err != nil {
			return nil, err
		}

		val, err = ast.translateExpr(when.Val)
		if err != nil {
			return nil, err
		}

		if cmpbase != nil {
			cond, err = ast.translateComparisonExpr2(sqlparser.EqualOp, cmpbase, cond)
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

func (ast *astCompiler) translateBetweenExpr(node *sqlparser.BetweenExpr) (Expr, error) {
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
		return ast.translateExpr(&sqlparser.OrExpr{Left: from, Right: to})
	}

	return ast.translateExpr(sqlparser.AndExpressions(from, to))
}

func translateExprNotSupported(e sqlparser.Expr) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %s", ErrTranslateExprNotSupported, sqlparser.String(e))
}

func (ast *astCompiler) translateExpr(e sqlparser.Expr) (Expr, error) {
	switch node := e.(type) {
	case sqlparser.BoolVal:
		return NewLiteralBool(bool(node)), nil
	case *sqlparser.ColName:
		return ast.translateColName(node)
	case *sqlparser.Offset:
		return ast.translateColOffset(node)
	case *sqlparser.ComparisonExpr:
		return ast.translateComparisonExpr(node.Operator, node.Left, node.Right)
	case *sqlparser.Argument:
		return ast.translateBindVar(node)
	case sqlparser.ListArg:
		return NewBindVarTuple(string(node)), nil
	case *sqlparser.Literal:
		return translateLiteral(node, ast.cfg.Collation)
	case *sqlparser.AndExpr:
		return ast.translateLogicalExpr("AND", node.Left, node.Right)
	case *sqlparser.OrExpr:
		return ast.translateLogicalExpr("OR", node.Left, node.Right)
	case *sqlparser.XorExpr:
		return ast.translateLogicalExpr("XOR", node.Left, node.Right)
	case *sqlparser.NotExpr:
		return ast.translateLogicalExpr("NOT", node.Expr, nil)
	case *sqlparser.BinaryExpr:
		return ast.translateBinaryExpr(node)
	case sqlparser.ValTuple:
		return ast.translateTuple(node)
	case *sqlparser.NullVal:
		return NullExpr, nil
	case *sqlparser.CollateExpr:
		return ast.translateCollateExpr(node)
	case *sqlparser.IntroducerExpr:
		return ast.translateIntroducerExpr(node)
	case *sqlparser.IsExpr:
		return ast.translateIsExpr(node.Left, node.Right)
	case sqlparser.Callable:
		return ast.translateCallable(node)
	case *sqlparser.UnaryExpr:
		return ast.translateUnaryExpr(node)
	case *sqlparser.CastExpr:
		return ast.translateConvertExpr(node.Expr, node.Type)
	case *sqlparser.CaseExpr:
		return ast.translateCaseExpr(node)
	case *sqlparser.BetweenExpr:
		return ast.translateBetweenExpr(node)
	default:
		return nil, translateExprNotSupported(e)
	}
}

type astCompiler struct {
	cfg     *Config
	untyped int
}

type ColumnResolver func(name *sqlparser.ColName) (int, error)
type TypeResolver func(expr sqlparser.Expr) (sqltypes.Type, collations.ID, bool)

type OptimizationLevel int8

const (
	OptimizationLevelDefault OptimizationLevel = iota
	OptimizationLevelSimplify
	OptimizationLevelCompile
	OptimizationLevelCompilerDebug
	OptimizationLevelMax
	OptimizationLevelNone OptimizationLevel = -1
)

type Config struct {
	ResolveColumn ColumnResolver
	ResolveType   TypeResolver

	Collation    collations.ID
	Optimization OptimizationLevel
	CompilerErr  error
}

func Translate(e sqlparser.Expr, cfg *Config) (Expr, error) {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.Collation == collations.Unknown {
		cfg.Collation = collations.Default()
	}
	if cfg.Optimization == OptimizationLevelDefault {
		cfg.Optimization = OptimizationLevelSimplify
	}

	ast := astCompiler{cfg: cfg}

	expr, err := ast.translateExpr(e)
	if err != nil {
		return nil, err
	}

	if err := ast.cardExpr(expr); err != nil {
		return nil, err
	}

	if cfg.Optimization >= OptimizationLevelSimplify && cfg.Optimization != OptimizationLevelCompilerDebug {
		staticEnv := EmptyExpressionEnv()
		expr, err = simplifyExpr(staticEnv, expr)
	}

	if cfg.Optimization >= OptimizationLevelCompile && ast.untyped == 0 {
		comp := compiler{cfg: cfg}
		var ct ctype
		if ct, cfg.CompilerErr = comp.compile(expr); cfg.CompilerErr == nil {
			expr = &CompiledExpr{code: comp.asm.ins, original: expr, stack: comp.asm.stack.max, typed: ct.Type}
		}
	}

	return expr, err
}

type FieldResolver []*querypb.Field

func (fields FieldResolver) Column(col *sqlparser.ColName) (int, error) {
	name := col.CompliantName()
	for i, f := range fields {
		if f.Name == name {
			return i, nil
		}
	}
	return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown column: %q", sqlparser.String(col))
}

func (fields FieldResolver) Type(expr sqlparser.Expr) (sqltypes.Type, collations.ID, bool) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		name := expr.CompliantName()
		for _, f := range fields {
			if f.Name == name {
				return f.Type, collations.ID(f.Charset), true
			}
		}
	}
	return 0, 0, false
}
