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
	"slices"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
)

var ErrTranslateExprNotSupported = "expr cannot be translated, not supported"
var ErrEvaluatedExprNotSupported = "expr cannot be evaluated, not supported"

func (ast *astCompiler) translateComparisonExpr(op sqlparser.ComparisonExprOperator, left, right sqlparser.Expr) (IR, error) {
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

func (ast *astCompiler) translateComparisonExpr2(op sqlparser.ComparisonExprOperator, left, right IR) (IR, error) {
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
	case sqlparser.RegexpOp, sqlparser.NotRegexpOp:
		return &builtinRegexpLike{
			CallExpr: CallExpr{
				Arguments: []IR{left, right},
				Method:    "REGEXP_LIKE",
			},
			Negate: op == sqlparser.NotRegexpOp,
		}, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, op.ToString())
	}
}

func (ast *astCompiler) translateLogicalNot(node *sqlparser.NotExpr) (IR, error) {
	inner, err := ast.translateExpr(node.Expr)
	if err != nil {
		return nil, err
	}
	return &NotExpr{UnaryExpr{inner}}, nil
}

func (ast *astCompiler) translateLogicalExpr(node sqlparser.Expr) (IR, error) {
	var left, right sqlparser.Expr

	var logic opLogical
	switch n := node.(type) {
	case *sqlparser.AndExpr:
		left = n.Left
		right = n.Right
		logic = opLogicalAnd{}
	case *sqlparser.OrExpr:
		left = n.Left
		right = n.Right
		logic = opLogicalOr{}
	case *sqlparser.XorExpr:
		left = n.Left
		right = n.Right
		logic = opLogicalXor{}
	default:
		panic("unexpected logical operator")
	}

	l, err := ast.translateExpr(left)
	if err != nil {
		return nil, err
	}

	r, err := ast.translateExpr(right)
	if err != nil {
		return nil, err
	}

	return &LogicalExpr{
		BinaryExpr: BinaryExpr{
			Left:  l,
			Right: r,
		},
		op: logic,
	}, nil
}

func (ast *astCompiler) translateIntervalExpr(needle sqlparser.Expr, haystack []sqlparser.Expr) (IR, error) {
	exprs := make([]IR, 0, len(haystack)+1)

	expr, err := ast.translateExpr(needle)
	if err != nil {
		return nil, err
	}

	exprs = append(exprs, expr)
	for _, e := range haystack {
		expr, err := ast.translateExpr(e)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	return &IntervalExpr{
		CallExpr{
			Arguments: exprs,
			Method:    "INTERVAL",
		},
	}, nil
}

func (ast *astCompiler) translateIsExpr(left sqlparser.Expr, op sqlparser.IsExprOperator) (IR, error) {
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

func (ast *astCompiler) translateBindVar(arg *sqlparser.Argument) (IR, error) {
	bvar := NewBindVar(arg.Name, NewType(arg.Type, ast.cfg.Collation))

	if !bvar.typed() {
		bvar.dynamicTypeOffset = len(ast.untyped)
		ast.untyped = append(ast.untyped, bvar)
	}
	return bvar, nil
}

func (ast *astCompiler) translateColOffset(col *sqlparser.Offset) (IR, error) {
	var typ Type
	if ast.cfg.ResolveType != nil {
		typ, _ = ast.cfg.ResolveType(col.Original)
	}
	if typ.Valid() && typ.collation == collations.Unknown {
		typ.collation = ast.cfg.Collation
	}

	column := NewColumn(col.V, typ, col.Original)
	if !column.typed() {
		column.dynamicTypeOffset = len(ast.untyped)
		ast.untyped = append(ast.untyped, column)
	}
	return column, nil
}

func (ast *astCompiler) translateColName(colname *sqlparser.ColName) (IR, error) {
	if ast.cfg.ResolveColumn == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot lookup column '%s' (column access not supported here)", sqlparser.String(colname))
	}
	idx, err := ast.cfg.ResolveColumn(colname)
	if err != nil {
		return nil, err
	}
	var typ Type
	if ast.cfg.ResolveType != nil {
		typ, _ = ast.cfg.ResolveType(colname)
	}
	if typ.Valid() && typ.collation == collations.Unknown {
		typ.collation = ast.cfg.Collation
	}

	column := NewColumn(idx, typ, colname)

	if !column.typed() {
		column.dynamicTypeOffset = len(ast.untyped)
		ast.untyped = append(ast.untyped, column)
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
		return NewLiteralString(lit.Bytes(), typedCoercionCollation(sqltypes.VarChar, collation)), nil
	case sqlparser.HexNum:
		return NewLiteralBinaryFromHexNum(lit.Bytes())
	case sqlparser.HexVal:
		return NewLiteralBinaryFromHex(lit.Bytes())
	case sqlparser.BitNum:
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

func (ast *astCompiler) translateBinaryExpr(binary *sqlparser.BinaryExpr) (IR, error) {
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

func (ast *astCompiler) translateTuple(tuple sqlparser.ValTuple) (IR, error) {
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

func (ast *astCompiler) translateCollateExpr(collate *sqlparser.CollateExpr) (IR, error) {
	expr, err := ast.translateExpr(collate.Expr)
	if err != nil {
		return nil, err
	}
	coll := ast.cfg.Environment.CollationEnv().LookupByName(collate.Collation)
	if coll == collations.Unknown {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unknown collation: '%s'", collate.Collation)
	}
	return &CollateExpr{
		UnaryExpr: UnaryExpr{expr},
		TypedCollation: collations.TypedCollation{
			Collation:    coll,
			Coercibility: collations.CoerceExplicit,
			Repertoire:   collations.RepertoireUnicode,
		},
		CollationEnv: ast.cfg.Environment.CollationEnv(),
	}, nil
}

func (ast *astCompiler) translateIntroducerExpr(introduced *sqlparser.IntroducerExpr) (IR, error) {
	expr, err := ast.translateExpr(introduced.Expr)
	if err != nil {
		return nil, err
	}

	var collation collations.ID
	if strings.ToLower(introduced.CharacterSet) == "_binary" {
		collation = collations.CollationBinaryID
	} else {
		defaultCollation := ast.cfg.Environment.CollationEnv().DefaultCollationForCharset(introduced.CharacterSet[1:])
		if defaultCollation == collations.Unknown {
			panic(fmt.Sprintf("unknown character set: %s", introduced.CharacterSet))
		}
		collation = defaultCollation
	}

	switch lit := expr.(type) {
	case *Literal:
		switch collation {
		case collations.CollationBinaryID:
			lit.inner = evalToBinary(lit.inner)
		default:
			lit.inner, err = introducerCast(lit.inner, collation)
			if err != nil {
				return nil, err
			}
		}
		return expr, nil
	case *BindVariable:
		if lit.Type == sqltypes.Tuple {
			panic("parser allowed introducer before tuple")
		}

		return &IntroducerExpr{
			UnaryExpr: UnaryExpr{expr},
			TypedCollation: collations.TypedCollation{
				Collation:    collation,
				Coercibility: collations.CoerceExplicit,
				Repertoire:   collations.RepertoireUnicode,
			},
			CollationEnv: ast.cfg.Environment.CollationEnv(),
		}, nil
	default:
		panic("character set introducers are only supported for literals and arguments")
	}
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

func (ast *astCompiler) translateUnaryExpr(unary *sqlparser.UnaryExpr) (IR, error) {
	expr, err := ast.translateExpr(unary.Expr)
	if err != nil {
		return nil, err
	}

	switch unary.Operator {
	case sqlparser.UMinusOp:
		return &NegateExpr{UnaryExpr: UnaryExpr{expr}}, nil
	case sqlparser.BangOp:
		return &NotExpr{UnaryExpr{expr}}, nil
	case sqlparser.TildaOp:
		return &BitwiseNotExpr{UnaryExpr: UnaryExpr{expr}}, nil
	case sqlparser.NStringOp:
		return &ConvertExpr{UnaryExpr: UnaryExpr{expr}, Type: "NCHAR", Collation: collations.CollationUtf8mb3ID, CollationEnv: ast.cfg.Environment.CollationEnv()}, nil
	default:
		return nil, translateExprNotSupported(unary)
	}
}

func (ast *astCompiler) translateCaseExpr(node *sqlparser.CaseExpr) (IR, error) {
	var err error
	var result CaseExpr

	if node.Else != nil {
		result.Else, err = ast.translateExpr(node.Else)
		if err != nil {
			return nil, err
		}
	}

	var cmpbase IR
	if node.Expr != nil {
		cmpbase, err = ast.translateExpr(node.Expr)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range node.Whens {
		var cond, val IR

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

func (ast *astCompiler) translateBetweenExpr(node *sqlparser.BetweenExpr) (IR, error) {
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

func (ast *astCompiler) translateExpr(e sqlparser.Expr) (IR, error) {
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
		return NewBindVarTuple(string(node), ast.cfg.Collation), nil
	case *sqlparser.Literal:
		return translateLiteral(node, ast.cfg.Collation)
	case *sqlparser.AndExpr:
		return ast.translateLogicalExpr(node)
	case *sqlparser.OrExpr:
		return ast.translateLogicalExpr(node)
	case *sqlparser.XorExpr:
		return ast.translateLogicalExpr(node)
	case *sqlparser.NotExpr:
		return ast.translateLogicalNot(node)
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
	case *sqlparser.IntervalFuncExpr:
		return ast.translateIntervalExpr(node.Expr, node.Exprs)
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
	untyped []typedIR
}

type ColumnResolver func(name *sqlparser.ColName) (int, error)
type TypeResolver func(expr sqlparser.Expr) (Type, bool)

type Config struct {
	ResolveColumn ColumnResolver
	ResolveType   TypeResolver

	Collation         collations.ID
	NoConstantFolding bool
	NoCompilation     bool
	SQLMode           SQLMode
	Environment       *vtenv.Environment
}

func Translate(e sqlparser.Expr, cfg *Config) (Expr, error) {
	ast := astCompiler{cfg: cfg}

	expr, err := ast.translateExpr(e)
	if err != nil {
		return nil, err
	}

	if err := ast.cardExpr(expr); err != nil {
		return nil, err
	}

	if !cfg.NoConstantFolding {
		staticEnv := EmptyExpressionEnv(cfg.Environment)
		expr, err = simplifyExpr(staticEnv, expr)
		if err != nil {
			return nil, err
		}
	}

	if expr, ok := expr.(Expr); ok {
		return expr, nil
	}

	if len(ast.untyped) == 0 && !cfg.NoCompilation {
		comp := compiler{collation: cfg.Collation, env: cfg.Environment, sqlmode: cfg.SQLMode}
		return comp.compile(expr)
	}

	return &UntypedExpr{
		env:       cfg.Environment,
		ir:        expr,
		collation: cfg.Collation,
		needTypes: ast.untyped,
	}, nil
}

// typedExpr is a lazily compiled expression from an UntypedExpr. This expression
// can only be compiled when it's evaluated with a fixed set of user-supplied types.
// These static types are stored in the types slice so the next time the expression
// is evaluated with the same set of types, we can match this typedExpr and not have
// to compile it again.
type typedExpr struct {
	once     sync.Once
	types    []ctype
	compiled *CompiledExpr
	err      error
}

func (typed *typedExpr) compile(env *vtenv.Environment, expr IR, collation collations.ID, sqlmode SQLMode) (*CompiledExpr, error) {
	typed.once.Do(func() {
		comp := compiler{
			env:          env,
			collation:    collation,
			dynamicTypes: typed.types,
			sqlmode:      sqlmode,
		}
		typed.compiled, typed.err = comp.compile(expr)
	})
	return typed.compiled, typed.err
}

type typedIR interface {
	IR
	typeof(env *ExpressionEnv) (ctype, error)
}

// UntypedExpr is a translated expression that cannot be compiled ahead of time because it
// contains dynamic types.
type UntypedExpr struct {
	env *vtenv.Environment
	// ir is the translated IR for the expression
	ir IR
	// collation is the default collation for the translated expression
	collation collations.ID
	// needTypes are the IR nodes in ir that could not be typed ahead of time: these must
	// necessarily be either Column or BindVariable nodes, as all other nodes can always
	// be statically typed. The dynamicTypeOffset field on each node is the offset of
	// the node in this slice.
	needTypes []typedIR

	mu sync.Mutex
	// typed contains the lazily compiled versions of ir for every type set
	typed []*typedExpr
}

var _ Expr = (*UntypedExpr)(nil)

func (u *UntypedExpr) IR() IR {
	return u.ir
}

func (u *UntypedExpr) eval(env *ExpressionEnv) (eval, error) {
	return u.ir.eval(env)
}

func (u *UntypedExpr) loadTypedExpression(env *ExpressionEnv) (*typedExpr, error) {
	dynamicTypes := make([]ctype, 0, len(u.needTypes))
	for _, expr := range u.needTypes {
		typ, err := expr.typeof(env)
		if err != nil {
			return nil, err
		}
		dynamicTypes = append(dynamicTypes, typ)
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	for _, typed := range u.typed {
		if slices.EqualFunc(typed.types, dynamicTypes, func(a, b ctype) bool {
			return a.equal(b)
		}) {
			return typed, nil
		}
	}
	typed := &typedExpr{types: dynamicTypes}
	u.typed = append(u.typed, typed)
	return typed, nil
}

func (u *UntypedExpr) Compile(env *ExpressionEnv) (*CompiledExpr, error) {
	typed, err := u.loadTypedExpression(env)
	if err != nil {
		return nil, err
	}
	return typed.compile(u.env, u.ir, u.collation, env.sqlmode)
}

func (u *UntypedExpr) typeof(env *ExpressionEnv) (ctype, error) {
	compiled, err := u.Compile(env)
	if err != nil {
		return ctype{}, err
	}
	return compiled.typeof(env)
}

func (u *UntypedExpr) IsExpr() {}

func (u *UntypedExpr) Format(buf *sqlparser.TrackedBuffer) {
	u.ir.format(buf)
}

func (u *UntypedExpr) FormatFast(buf *sqlparser.TrackedBuffer) {
	u.ir.format(buf)
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

func (fields FieldResolver) Type(expr sqlparser.Expr) (Type, bool) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		name := expr.CompliantName()
		for _, f := range fields {
			if f.Name == name {
				return NewType(f.Type, collations.ID(f.Charset)), true
			}
		}
	}
	return Type{}, false
}
