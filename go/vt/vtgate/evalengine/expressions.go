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

package evalengine

import (
	"encoding/hex"
	"strconv"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		BindVars map[string]*querypb.BindVariable
		Row      []sqltypes.Value
	}

	// Expr is the interface that all evaluating expressions must implement
	Expr interface {
		eval(env *ExpressionEnv, result *EvalResult)
		typeof(env *ExpressionEnv) querypb.Type
		format(buf *formatter, depth int)
		constant() bool
		simplify() error
	}

	Literal struct {
		Val EvalResult
	}

	BindVariable struct {
		Key        string
		coll       collations.TypedCollation
		coerceType querypb.Type
	}

	Column struct {
		Offset int
		coll   collations.TypedCollation
	}

	TupleExpr []Expr

	CollateExpr struct {
		UnaryExpr
		TypedCollation collations.TypedCollation
	}

	UnaryExpr struct {
		Inner Expr
	}

	BinaryExpr struct {
		Left, Right Expr
	}
)

var _ Expr = (*Literal)(nil)
var _ Expr = (*BindVariable)(nil)
var _ Expr = (*Column)(nil)
var _ Expr = (*ArithmeticExpr)(nil)
var _ Expr = (*ComparisonExpr)(nil)
var _ Expr = (*InExpr)(nil)
var _ Expr = (*LikeExpr)(nil)
var _ Expr = (TupleExpr)(nil)
var _ Expr = (*CollateExpr)(nil)
var _ Expr = (*LogicalExpr)(nil)
var _ Expr = (*NotExpr)(nil)

var noenv *ExpressionEnv

type evalError struct {
	error
}

func throwEvalError(err error) {
	panic(evalError{err})
}

func throwCardinalityError(expected int) {
	panic(evalError{
		vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.OperandColumns, "Operand should contain %d column(s)", expected),
	})
}

func (env *ExpressionEnv) cardinality(expr Expr) int {
	switch expr := expr.(type) {
	case *BindVariable:
		if expr.typeof(env) == querypb.Type_TUPLE {
			return len(expr.bvar(env).Values)
		}
		return 1

	case TupleExpr:
		return len(expr)

	default:
		return 1
	}
}

func (env *ExpressionEnv) ensureCardinality(expr Expr, expected int) {
	if env.cardinality(expr) != expected {
		throwCardinalityError(expected)
	}
}

func (env *ExpressionEnv) subexpr(expr Expr, nth int) (Expr, int) {
	switch expr := expr.(type) {
	case *BindVariable:
		if expr.typeof(env) == querypb.Type_TUPLE {
			return nil, 1
		}
	case TupleExpr:
		return expr[nth], env.cardinality(expr[nth])
	}
	panic("subexpr called on non-tuple")
}

func (env *ExpressionEnv) typecheckComparison(expr1 Expr, card1 int, expr2 Expr, card2 int) {
	switch {
	case card1 == 1 && card2 == 1:
		env.typecheck(expr1)
		env.typecheck(expr2)
	case card1 == card2:
		for n := 0; n < card1; n++ {
			left1, leftcard1 := env.subexpr(expr1, n)
			right1, rightcard1 := env.subexpr(expr2, n)
			env.typecheckComparison(left1, leftcard1, right1, rightcard1)
		}
	default:
		env.typecheck(expr1)
		env.typecheck(expr2)
		throwCardinalityError(card1)
	}
}

func (env *ExpressionEnv) typecheck(expr Expr) {
	if expr == nil {
		return
	}

	switch expr := expr.(type) {
	case *ArithmeticExpr:
		env.typecheck(expr.Left)
		env.ensureCardinality(expr.Left, 1)

		env.typecheck(expr.Right)
		env.ensureCardinality(expr.Right, 1)

	case *ComparisonExpr:
		left := env.cardinality(expr.Left)
		right := env.cardinality(expr.Right)
		env.typecheckComparison(expr.Left, left, expr.Right, right)

	case *LogicalExpr:
		env.typecheck(expr.Left)
		env.ensureCardinality(expr.Left, 1)

		env.typecheck(expr.Right)
		env.ensureCardinality(expr.Right, 1)

	case *InExpr:
		env.typecheck(expr.Left)
		left := env.cardinality(expr.Left)
		right := env.cardinality(expr.Right)

		if expr.Right.typeof(env) != querypb.Type_TUPLE {
			throwEvalError(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple"))
		}

		for n := 0; n < right; n++ {
			subexpr, subcard := env.subexpr(expr.Right, n)
			env.typecheck(subexpr)
			if left != subcard {
				throwCardinalityError(left)
			}
		}

	case *LikeExpr:
		env.typecheck(expr.Left)
		env.ensureCardinality(expr.Left, 1)

		env.typecheck(expr.Right)
		env.ensureCardinality(expr.Right, 1)

	case TupleExpr:
		for _, subexpr := range expr {
			env.typecheck(subexpr)
		}

	case *IsExpr:
		env.ensureCardinality(expr.Inner, 1)
	}
}

func (env *ExpressionEnv) Evaluate(expr Expr) (er EvalResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			if ee, ok := r.(evalError); ok {
				err = ee.error
			} else {
				panic(r)
			}
		}
	}()
	env.typecheck(expr)
	expr.eval(env, &er)
	return
}

func (env *ExpressionEnv) TypeOf(expr Expr) (ty querypb.Type, err error) {
	defer func() {
		if r := recover(); r != nil {
			if ee, ok := r.(evalError); ok {
				err = ee.error
			} else {
				panic(r)
			}
		}
	}()
	ty = expr.typeof(env)
	return
}

// EmptyExpressionEnv returns a new ExpressionEnv with no bind vars or row
func EmptyExpressionEnv() *ExpressionEnv {
	return EnvWithBindVars(map[string]*querypb.BindVariable{})
}

// EnvWithBindVars returns an expression environment with no current row, but with bindvars
func EnvWithBindVars(bindVars map[string]*querypb.BindVariable) *ExpressionEnv {
	return &ExpressionEnv{BindVars: bindVars}
}

// NullExpr is just what you are lead to believe
var NullExpr = &Literal{}

func init() {
	NullExpr.Val.setNull()
}

// NewLiteralIntegralFromBytes returns a literal expression.
// It tries to return an int64, but if the value is too large, it tries with an uint64
func NewLiteralIntegralFromBytes(val []byte) (Expr, error) {
	str := string(val)
	ival, err := strconv.ParseInt(str, 10, 64)
	if err == nil {
		return NewLiteralInt(ival), nil
	}

	// let's try with uint if we overflowed
	numError, ok := err.(*strconv.NumError)
	if !ok || numError.Err != strconv.ErrRange {
		return nil, err
	}

	uval, err := strconv.ParseUint(str, 0, 64)
	if err != nil {
		return nil, err
	}
	return NewLiteralUint(uval), nil
}

// NewLiteralInt returns a literal expression
func NewLiteralInt(i int64) Expr {
	lit := &Literal{}
	lit.Val.setInt64(i)
	return lit
}

// NewLiteralUint returns a literal expression
func NewLiteralUint(i uint64) Expr {
	lit := &Literal{}
	lit.Val.setUint64(i)
	return lit
}

// NewLiteralFloat returns a literal expression
func NewLiteralFloat(val float64) Expr {
	lit := &Literal{}
	lit.Val.setFloat(val)
	return lit
}

// NewLiteralFloatFromBytes returns a float literal expression from a slice of bytes
func NewLiteralFloatFromBytes(val []byte) (Expr, error) {
	lit := &Literal{}
	fval, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return nil, err
	}
	lit.Val.setFloat(fval)
	return lit, nil
}

func NewLiteralDecimalFromBytes(val []byte) (Expr, error) {
	lit := &Literal{}
	dec, err := newDecimalString(string(val))
	if err != nil {
		return nil, err
	}
	lit.Val.setDecimal(dec)
	return lit, nil
}

// NewLiteralString returns a literal expression
func NewLiteralString(val []byte, collation collations.TypedCollation) Expr {
	collation.Repertoire = collations.RepertoireASCII
	for _, b := range val {
		if b >= utf8.RuneSelf {
			collation.Repertoire = collations.RepertoireUnicode
			break
		}
	}
	lit := &Literal{}
	lit.Val.setRaw(sqltypes.VarChar, val, collation)
	return lit
}

func NewLiteralBinaryFromHex(val []byte) (Expr, error) {
	raw := make([]byte, hex.DecodedLen(len(val)))
	if _, err := hex.Decode(raw, val); err != nil {
		return nil, err
	}
	lit := &Literal{}
	lit.Val.setRaw(sqltypes.VarBinary, raw, collationBinary)
	return lit, nil
}

// NewBindVar returns a bind variable
func NewBindVar(key string, collation collations.TypedCollation) Expr {
	return &BindVariable{
		Key:        key,
		coll:       collation,
		coerceType: -1,
	}
}

// NewColumn returns a column expression
func NewColumn(offset int, collation collations.TypedCollation) Expr {
	return &Column{
		Offset: offset,
		coll:   collation,
	}
}

// NewTupleExpr returns a tuple expression
func NewTupleExpr(exprs ...Expr) TupleExpr {
	tupleExpr := make(TupleExpr, 0, len(exprs))
	for _, f := range exprs {
		tupleExpr = append(tupleExpr, f)
	}
	return tupleExpr
}

func (c *UnaryExpr) typeof(env *ExpressionEnv) querypb.Type {
	return c.Inner.typeof(env)
}

// eval implements the Expr interface
func (l *Literal) eval(_ *ExpressionEnv, result *EvalResult) {
	*result = l.Val
}

func (t TupleExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var tup = make([]EvalResult, len(t))
	for i, expr := range t {
		tup[i].init(env, expr)
	}
	result.setTuple(tup)
}

func (bv *BindVariable) bvar(env *ExpressionEnv) *querypb.BindVariable {
	val, ok := env.BindVars[bv.Key]
	if !ok {
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", bv.Key))
	}
	return val
}

// eval implements the Expr interface
func (bv *BindVariable) eval(env *ExpressionEnv, result *EvalResult) {
	bvar := bv.bvar(env)
	typ := bvar.Type
	if bv.coerceType >= 0 {
		typ = bv.coerceType
	}

	switch typ {
	case querypb.Type_TUPLE:
		tuple := make([]EvalResult, len(bvar.Values))
		for i, value := range bvar.Values {
			tuple[i].setBindVar1(value.Type, value.Value, collations.TypedCollation{})
		}
		result.setTuple(tuple)

	default:
		result.setBindVar1(typ, bvar.Value, bv.coll)
	}
}

// typeof implements the Expr interface
func (bv *BindVariable) typeof(env *ExpressionEnv) querypb.Type {
	if bv.coerceType >= 0 {
		return bv.coerceType
	}
	return bv.bvar(env).Type
}

// eval implements the Expr interface
func (c *Column) eval(env *ExpressionEnv, result *EvalResult) {
	value := env.Row[c.Offset]
	if err := result.setValue(value); err != nil {
		throwEvalError(err)
	}
	result.replaceCollation(c.coll)
}

// typeof implements the Expr interface
func (l *Literal) typeof(*ExpressionEnv) querypb.Type {
	return l.Val.typeof()
}

// typeof implements the Expr interface
func (t TupleExpr) typeof(*ExpressionEnv) querypb.Type {
	return querypb.Type_TUPLE
}

func (c *Column) typeof(env *ExpressionEnv) querypb.Type {
	value := env.Row[c.Offset]
	return value.Type()
}
