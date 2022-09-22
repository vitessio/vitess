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
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"
)

type (
	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		BindVars         map[string]*querypb.BindVariable
		DefaultCollation collations.ID

		// Row and Fields should line up
		Row    []sqltypes.Value
		Fields []*querypb.Field
	}

	// Expr is the interface that all evaluating expressions must implement
	Expr interface {
		eval(env *ExpressionEnv, result *EvalResult)
		typeof(env *ExpressionEnv) (sqltypes.Type, flag)
		format(buf *formatter, depth int)
		constant() bool
		simplify(env *ExpressionEnv) error
	}

	Literal struct {
		Val EvalResult
	}

	BindVariable struct {
		Key        string
		coll       collations.TypedCollation
		coerceType sqltypes.Type
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

	BinaryExpr struct {
		Left, Right Expr
	}
)

func (expr *BinaryExpr) LeftExpr() Expr {
	return expr.Left
}

func (expr *BinaryExpr) RightExpr() Expr {
	return expr.Right
}

var _ Expr = (*Literal)(nil)
var _ Expr = (*BindVariable)(nil)
var _ Expr = (*Column)(nil)
var _ Expr = (*ArithmeticExpr)(nil)
var _ Expr = (*ComparisonExpr)(nil)
var _ Expr = (*InExpr)(nil)
var _ Expr = (*IsExpr)(nil)
var _ Expr = (*LikeExpr)(nil)
var _ Expr = (TupleExpr)(nil)
var _ Expr = (*CollateExpr)(nil)
var _ Expr = (*LogicalExpr)(nil)
var _ Expr = (*NotExpr)(nil)
var _ Expr = (*CallExpr)(nil)
var _ Expr = (*WeightStringCallExpr)(nil)
var _ Expr = (*BitwiseExpr)(nil)
var _ Expr = (*BitwiseNotExpr)(nil)
var _ Expr = (*ConvertExpr)(nil)
var _ Expr = (*ConvertUsingExpr)(nil)

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
		tt, _ := expr.typeof(env)
		if tt == sqltypes.Tuple {
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
		tt, _ := expr.typeof(env)
		if tt == sqltypes.Tuple {
			return nil, 1
		}
	case *Literal:
		if expr.Val.typeof() == sqltypes.Tuple {
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

func (env *ExpressionEnv) typecheckBinary(left, right Expr) {
	env.typecheck(left)
	env.ensureCardinality(left, 1)

	env.typecheck(right)
	env.ensureCardinality(right, 1)
}

func (env *ExpressionEnv) typecheckUnary(inner Expr) {
	env.typecheck(inner)
	env.ensureCardinality(inner, 1)
}

func (env *ExpressionEnv) typecheck(expr Expr) {
	if expr == nil {
		return
	}

	switch expr := expr.(type) {
	case *ConvertExpr:
		env.typecheckUnary(expr.Inner)
	case *ConvertUsingExpr:
		env.typecheckUnary(expr.Inner)
	case *NegateExpr:
		env.typecheckUnary(expr.Inner)
	case *CollateExpr:
		env.typecheckUnary(expr.Inner)
	case *IsExpr:
		env.typecheckUnary(expr.Inner)
	case *BitwiseNotExpr:
		env.typecheckUnary(expr.Inner)
	case *WeightStringCallExpr:
		env.typecheckUnary(expr.String)
	case *ArithmeticExpr:
		env.typecheckBinary(expr.Left, expr.Right)
	case *LogicalExpr:
		env.typecheckBinary(expr.Left, expr.Right)
	case *BitwiseExpr:
		env.typecheckBinary(expr.Left, expr.Right)
	case *LikeExpr:
		env.typecheckBinary(expr.Left, expr.Right)
	case *ComparisonExpr:
		left := env.cardinality(expr.Left)
		right := env.cardinality(expr.Right)
		env.typecheckComparison(expr.Left, left, expr.Right, right)
	case *InExpr:
		env.typecheck(expr.Left)
		left := env.cardinality(expr.Left)
		right := env.cardinality(expr.Right)

		tt, _ := expr.Right.typeof(env)
		if tt != sqltypes.Tuple {
			throwEvalError(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rhs of an In operation should be a tuple"))
		}

		for n := 0; n < right; n++ {
			subexpr, subcard := env.subexpr(expr.Right, n)
			env.typecheck(subexpr)
			if left != subcard {
				throwCardinalityError(left)
			}
		}
	case TupleExpr:
		for _, subexpr := range expr {
			env.typecheck(subexpr)
		}
	case *CallExpr:
		env.typecheck(expr.Arguments)
	case *Literal, *Column, *BindVariable, *CaseExpr: // noop
	default:
		panic(fmt.Sprintf("unhandled cardinality: %T", expr))
	}
}

func (env *ExpressionEnv) Evaluate(expr Expr) (er EvalResult, err error) {
	if env == nil {
		panic("ExpressionEnv == nil")
	}
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

func (env *ExpressionEnv) TypeOf(expr Expr) (ty sqltypes.Type, err error) {
	defer func() {
		if r := recover(); r != nil {
			if ee, ok := r.(evalError); ok {
				err = ee.error
			} else {
				panic(r)
			}
		}
	}()
	ty, _ = expr.typeof(env)
	return
}

// EmptyExpressionEnv returns a new ExpressionEnv with no bind vars or row
func EmptyExpressionEnv() *ExpressionEnv {
	return EnvWithBindVars(map[string]*querypb.BindVariable{}, collations.Unknown)
}

// EnvWithBindVars returns an expression environment with no current row, but with bindvars
func EnvWithBindVars(bindVars map[string]*querypb.BindVariable, coll collations.ID) *ExpressionEnv {
	if coll == collations.Unknown {
		coll = collations.Default()
	}
	return &ExpressionEnv{BindVars: bindVars, DefaultCollation: coll}
}

// NullExpr is just what you are lead to believe
var NullExpr = &Literal{}

func init() {
	NullExpr.Val.setNull()
	NullExpr.Val.replaceCollation(collationNull)
}

// NewLiteralIntegralFromBytes returns a literal expression.
// It tries to return an int64, but if the value is too large, it tries with an uint64
func NewLiteralIntegralFromBytes(val []byte) (*Literal, error) {
	if val[0] == '-' {
		panic("NewLiteralIntegralFromBytes: negative value")
	}

	uval, err := strconv.ParseUint(string(val), 10, 64)
	if err != nil {
		if numError, ok := err.(*strconv.NumError); ok && numError.Err == strconv.ErrRange {
			return NewLiteralDecimalFromBytes(val)
		}
		return nil, err
	}
	if uval <= math.MaxInt64 {
		return NewLiteralInt(int64(uval)), nil
	}
	return NewLiteralUint(uval), nil
}

// NewLiteralInt returns a literal expression
func NewLiteralInt(i int64) *Literal {
	lit := &Literal{}
	lit.Val.setInt64(i)
	return lit
}

// NewLiteralUint returns a literal expression
func NewLiteralUint(i uint64) *Literal {
	lit := &Literal{}
	lit.Val.setUint64(i)
	return lit
}

// NewLiteralFloat returns a literal expression
func NewLiteralFloat(val float64) *Literal {
	lit := &Literal{}
	lit.Val.setFloat(val)
	return lit
}

// NewLiteralFloatFromBytes returns a float literal expression from a slice of bytes
func NewLiteralFloatFromBytes(val []byte) (*Literal, error) {
	lit := &Literal{}
	fval, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return nil, err
	}
	lit.Val.setFloat(fval)
	return lit, nil
}

func NewLiteralDecimalFromBytes(val []byte) (*Literal, error) {
	lit := &Literal{}
	dec, err := decimal.NewFromMySQL(val)
	if err != nil {
		return nil, err
	}
	lit.Val.setDecimal(dec, -dec.Exponent())
	return lit, nil
}

// NewLiteralString returns a literal expression
func NewLiteralString(val []byte, collation collations.TypedCollation) *Literal {
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

// NewLiteralDateFromBytes returns a literal expression.
func NewLiteralDateFromBytes(val []byte) (*Literal, error) {
	_, err := sqlparser.ParseDate(string(val))
	if err != nil {
		return nil, err
	}
	lit := &Literal{}
	lit.Val.setRaw(querypb.Type_DATE, val, collationNumeric)
	return lit, nil
}

// NewLiteralTimeFromBytes returns a literal expression.
// it validates the time by parsing it and checking the error.
func NewLiteralTimeFromBytes(val []byte) (*Literal, error) {
	_, err := sqlparser.ParseTime(string(val))
	if err != nil {
		return nil, err
	}
	lit := &Literal{}
	lit.Val.setRaw(querypb.Type_TIME, val, collationNumeric)
	return lit, nil
}

// NewLiteralDatetimeFromBytes returns a literal expression.
// it validates the datetime by parsing it and checking the error.
func NewLiteralDatetimeFromBytes(val []byte) (*Literal, error) {
	_, err := sqlparser.ParseDateTime(string(val))
	if err != nil {
		return nil, err
	}
	lit := &Literal{}
	lit.Val.setRaw(querypb.Type_DATETIME, val, collationNumeric)
	return lit, nil
}

func parseHexLiteral(val []byte) ([]byte, error) {
	raw := make([]byte, hex.DecodedLen(len(val)))
	if _, err := hex.Decode(raw, val); err != nil {
		return nil, err
	}
	return raw, nil
}

func parseHexNumber(val []byte) ([]byte, error) {
	if val[0] != '0' || val[1] != 'x' {
		panic("malformed hex literal from parser")
	}
	if len(val)%2 == 0 {
		return parseHexLiteral(val[2:])
	}
	// If the hex literal doesn't have an even amount of hex digits, we need
	// to pad it with a '0' in the left. Instead of allocating a new slice
	// for padding pad in-place by replacing the 'x' in the original slice with
	// a '0', and clean it up after parsing.
	val[1] = '0'
	defer func() {
		val[1] = 'x'
	}()
	return parseHexLiteral(val[1:])
}

func NewLiteralBinary(val []byte) *Literal {
	lit := &Literal{}
	lit.Val.setRaw(sqltypes.VarBinary, val, collationBinary)
	return lit
}

func NewLiteralBinaryFromHex(val []byte) (*Literal, error) {
	raw, err := parseHexLiteral(val)
	if err != nil {
		return nil, err
	}
	lit := &Literal{}
	lit.Val.setBinaryHex(raw)
	return lit, nil
}

func NewLiteralBinaryFromHexNum(val []byte) (*Literal, error) {
	raw, err := parseHexNumber(val)
	if err != nil {
		return nil, err
	}
	lit := &Literal{}
	lit.Val.setBinaryHex(raw)
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
	case sqltypes.Tuple:
		tuple := make([]EvalResult, len(bvar.Values))
		for i, value := range bvar.Values {
			if err := tuple[i].setValue(sqltypes.MakeTrusted(value.Type, value.Value), collations.TypedCollation{}); err != nil {
				throwEvalError(err)
			}
		}
		result.setTuple(tuple)

	default:
		if err := result.setValue(sqltypes.MakeTrusted(typ, bvar.Value), bv.coll); err != nil {
			throwEvalError(err)
		}
	}
}

// typeof implements the Expr interface
func (bv *BindVariable) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	bvar := bv.bvar(env)
	switch bvar.Type {
	case sqltypes.Null:
		return sqltypes.Null, flagNull | flagNullable
	case sqltypes.HexNum, sqltypes.HexVal:
		return sqltypes.VarBinary, flagHex
	default:
		if bv.coerceType >= 0 {
			return bv.coerceType, 0
		}
		return bvar.Type, 0
	}
}

// eval implements the Expr interface
func (c *Column) eval(env *ExpressionEnv, result *EvalResult) {
	if err := result.setValue(env.Row[c.Offset], c.coll); err != nil {
		throwEvalError(err)
	}
}

// typeof implements the Expr interface
func (l *Literal) typeof(*ExpressionEnv) (sqltypes.Type, flag) {
	return l.Val.typeof(), l.Val.flags_
}

// typeof implements the Expr interface
func (t TupleExpr) typeof(*ExpressionEnv) (sqltypes.Type, flag) {
	return sqltypes.Tuple, flagNullable
}

func (c *Column) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	// we'll try to do the best possible with the information we have
	if c.Offset < len(env.Row) {
		value := env.Row[c.Offset]
		if value.IsNull() {
			return sqltypes.Null, flagNull | flagNullable
		}
		return value.Type(), flag(0)
	}

	if c.Offset < len(env.Fields) {
		return env.Fields[c.Offset].Type, flagNullable
	}

	panic("Column missing both data and field")
}
