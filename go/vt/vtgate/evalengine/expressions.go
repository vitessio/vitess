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
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal"
)

type (
	EvalResult struct {
		typ       querypb.Type
		collation collations.TypedCollation
		numval    uint64
		bytes     []byte
		tuple     *[]EvalResult
		decimal   *decimalResult
	}

	decimalResult struct {
		num  decimal.Big
		frac int
	}

	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		BindVars map[string]*querypb.BindVariable
		Row      []sqltypes.Value
	}

	// Expr is the interface that all evaluating expressions must implement
	Expr interface {
		Evaluate(env *ExpressionEnv) (EvalResult, error)
		Type(env *ExpressionEnv) (querypb.Type, error)
		Collation() collations.TypedCollation
		format(buf *strings.Builder, wrap bool)
	}

	RouteValue struct {
		Expr Expr
	}

	Literal struct {
		Val EvalResult
	}
	BindVariable struct {
		Key       string
		collation collations.TypedCollation
	}
	Column struct {
		Offset    int
		collation collations.TypedCollation
	}
	TupleExpr   []Expr
	CollateExpr struct {
		Expr           Expr
		TypedCollation collations.TypedCollation
	}
)

// EmptyExpressionEnv returns a new ExpressionEnv with no bind vars or row
func EmptyExpressionEnv() *ExpressionEnv {
	return EnvWithBindVars(map[string]*querypb.BindVariable{})
}

// EnvWithBindVars returns an expression environment with no current row, but with bindvars
func EnvWithBindVars(bindVars map[string]*querypb.BindVariable) *ExpressionEnv {
	return &ExpressionEnv{BindVars: bindVars}
}

// ResolveValue allows for retrieval of the value we expose for public consumption
func (rv *RouteValue) ResolveValue(bindVars map[string]*querypb.BindVariable) (sqltypes.Value, error) {
	env := EnvWithBindVars(bindVars)
	evalResul, err := rv.Expr.Evaluate(env)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return evalResul.Value(), nil
}

// ResolveList allows for retrieval of the value we expose for public consumption
func (rv *RouteValue) ResolveList(bindVars map[string]*querypb.BindVariable) ([]sqltypes.Value, error) {
	evalResul, err := rv.Expr.Evaluate(EnvWithBindVars(bindVars))
	if err != nil {
		return nil, err
	}
	return evalResul.TupleValues(), nil
}

func (rv *RouteValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(FormatExpr(rv.Expr))
}

func (t TupleExpr) Collation() collations.TypedCollation {
	// a Tuple does not have a collation, but an individual collation for every element of the tuple
	return collations.TypedCollation{}
}

func FormatExpr(expr Expr) string {
	var bld strings.Builder
	expr.format(&bld, false)
	return bld.String()
}

func (l *Literal) format(w *strings.Builder, _ bool) {
	w.WriteString(l.Val.Value().String())
}
func (bv *BindVariable) format(w *strings.Builder, _ bool) {
	w.WriteByte(':')
	w.WriteString(bv.Key)
}
func (c *Column) format(w *strings.Builder, _ bool) {
	fmt.Fprintf(w, "[COLUMN %d]", c.Offset)
}
func (b *BinaryExpr) format(w *strings.Builder, wrap bool) {
	if wrap {
		w.WriteByte('(')
	}

	b.Left.format(w, true)
	w.WriteString(" ")
	w.WriteString(b.Op.String())
	w.WriteString(" ")
	b.Right.format(w, true)

	if wrap {
		w.WriteByte(')')
	}
}
func (c *ComparisonExpr) format(w *strings.Builder, wrap bool) {
	if wrap {
		w.WriteByte('(')
	}

	c.Left.format(w, true)
	w.WriteString(" ")
	w.WriteString(c.Op.String())
	w.WriteString(" ")
	c.Right.format(w, true)

	if wrap {
		w.WriteByte(')')
	}
}
func (t TupleExpr) format(w *strings.Builder, wrap bool) {
	w.WriteByte('(')
	for i, expr := range t {
		if i > 0 {
			w.WriteString(", ")
		}
		expr.format(w, wrap)
	}
	w.WriteByte(')')
}
func (c *CollateExpr) format(w *strings.Builder, wrap bool) {
	c.Expr.format(w, wrap)
	coll := collations.Local().LookupByID(c.TypedCollation.Collation)
	fmt.Fprintf(w, " COLLATE %s", coll.Name())
}

var _ Expr = (*Literal)(nil)
var _ Expr = (*BindVariable)(nil)
var _ Expr = (*Column)(nil)
var _ Expr = (*BinaryExpr)(nil)
var _ Expr = (*ComparisonExpr)(nil)
var _ Expr = (TupleExpr)(nil)
var _ Expr = (*CollateExpr)(nil)

// Value allows for retrieval of the value we expose for public consumption
func (e EvalResult) Value() sqltypes.Value {
	return e.toSQLValue(e.typ)
}

// TupleValues allows for retrieval of the value we expose for public consumption
func (e EvalResult) TupleValues() []sqltypes.Value {
	if e.tuple == nil {
		return nil
	}

	values := *e.tuple
	result := make([]sqltypes.Value, 0, len(values))
	for _, val := range values {
		result = append(result, val.Value())
	}
	return result
}

func (e EvalResult) textual() bool {
	return sqltypes.IsText(e.typ) || sqltypes.IsBinary(e.typ)
}

var collationNull = collations.TypedCollation{
	Collation:    collations.CollationBinaryID,
	Coercibility: collations.CoerceIgnorable,
	Repertoire:   collations.RepertoireASCII,
}

// NullExpr is just what you are lead to believe
var NullExpr = &Literal{Val: resultNull}

// NewLiteralIntFromBytes returns a literal expression
func NewLiteralIntFromBytes(val []byte) (Expr, error) {
	ival, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return nil, err
	}
	return NewLiteralInt(ival), nil
}

var collationNumeric = collations.TypedCollation{
	Collation:    collations.CollationBinaryID,
	Coercibility: collations.CoerceNumeric,
	Repertoire:   collations.RepertoireASCII,
}

// NewLiteralInt returns a literal expression
func NewLiteralInt(i int64) Expr {
	return &Literal{Val: newEvalInt64(i)}
}

// NewLiteralUint returns a literal expression
func NewLiteralUint(i uint64) Expr {
	return &Literal{Val: newEvalUint64(i)}
}

// NewLiteralFloat returns a literal expression
func NewLiteralFloat(val float64) Expr {
	return &Literal{Val: newEvalFloat(val)}
}

// NewLiteralRealFromBytes returns a float literal expression from a slice of bytes
func NewLiteralRealFromBytes(val []byte) (Expr, error) {
	if bytes.IndexByte(val, 'e') >= 0 || bytes.IndexByte(val, 'E') >= 0 {
		fval, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return nil, err
		}
		return &Literal{Val: newEvalFloat(fval)}, nil
	}
	dec, err := newDecimalString(string(val))
	if err != nil {
		return nil, err
	}
	return &Literal{Val: newEvalDecimal(dec)}, nil
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
	return &Literal{Val: EvalResult{typ: sqltypes.VarBinary, bytes: val, collation: collation}}
}

// NewBindVar returns a bind variable
func NewBindVar(key string, collation collations.TypedCollation) Expr {
	return &BindVariable{
		Key:       key,
		collation: collation,
	}
}

// NewColumn returns a column expression
func NewColumn(offset int, collation collations.TypedCollation) Expr {
	return &Column{
		Offset:    offset,
		collation: collation,
	}
}

// NewTupleExpr returns a tuple expression
func NewTupleExpr(exprs ...Expr) Expr {
	tupleExpr := make(TupleExpr, 0, len(exprs))
	for _, f := range exprs {
		tupleExpr = append(tupleExpr, f)
	}
	return tupleExpr
}

// Evaluate implements the Expr interface
func (l *Literal) Evaluate(*ExpressionEnv) (EvalResult, error) {
	return l.Val, nil
}

func (l *Literal) Collation() collations.TypedCollation {
	return l.Val.collation
}

func (t TupleExpr) Evaluate(env *ExpressionEnv) (EvalResult, error) {
	var tup []EvalResult
	for _, expr := range t {
		evalRes, err := expr.Evaluate(env)
		if err != nil {
			return EvalResult{}, err
		}
		tup = append(tup, evalRes)
	}
	return EvalResult{
		typ:   querypb.Type_TUPLE,
		tuple: &tup,
	}, nil
}

// Evaluate implements the Expr interface
func (bv *BindVariable) Evaluate(env *ExpressionEnv) (EvalResult, error) {
	val, ok := env.BindVars[bv.Key]
	if !ok {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Bind variable not found")
	}
	eval, err := evaluateByType(val)
	if err != nil {
		return EvalResult{}, err
	}
	eval.collation = bv.collation
	return eval, nil
}

func (bv *BindVariable) Collation() collations.TypedCollation {
	return bv.collation
}

// Evaluate implements the Expr interface
func (c *Column) Evaluate(env *ExpressionEnv) (EvalResult, error) {
	value := env.Row[c.Offset]
	numeric, err := newEvalResult(value)
	numeric.collation = c.collation
	return numeric, err
}

func (c *Column) Collation() collations.TypedCollation {
	return c.collation
}

// Type implements the Expr interface
func (bv *BindVariable) Type(env *ExpressionEnv) (querypb.Type, error) {
	e := env.BindVars
	v, found := e[bv.Key]
	if !found {
		return querypb.Type_NULL_TYPE, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", bv.Key)
	}
	return v.Type, nil
}

// Type implements the Expr interface
func (l *Literal) Type(*ExpressionEnv) (querypb.Type, error) {
	return l.Val.typ, nil
}

// Type implements the Expr interface
func (t TupleExpr) Type(*ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_TUPLE, nil
}

func (c *Column) Type(*ExpressionEnv) (querypb.Type, error) {
	return sqltypes.Float64, nil
}

func mergeNumericalTypes(ltype, rtype querypb.Type) querypb.Type {
	switch ltype {
	case sqltypes.Int64:
		if rtype == sqltypes.Uint64 || rtype == sqltypes.Float64 {
			return rtype
		}
	case sqltypes.Uint64:
		if rtype == sqltypes.Float64 {
			return rtype
		}
	}
	return ltype
}

func evaluateByTypeSingle(typ querypb.Type, value []byte) (EvalResult, error) {
	switch typ {
	case sqltypes.Int64:
		ival, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			ival = 0
		}
		return EvalResult{typ: sqltypes.Int64, numval: uint64(ival)}, nil
	case sqltypes.Int32:
		ival, err := strconv.ParseInt(string(value), 10, 32)
		if err != nil {
			ival = 0
		}
		return EvalResult{typ: sqltypes.Int32, numval: uint64(ival)}, nil
	case sqltypes.Uint64:
		uval, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			uval = 0
		}
		return EvalResult{typ: sqltypes.Uint64, numval: uval}, nil
	case sqltypes.Float64:
		fval, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			fval = 0
		}
		return EvalResult{typ: sqltypes.Float64, numval: math.Float64bits(fval)}, nil
	case sqltypes.Decimal:
		dec, err := newDecimalString(string(value))
		if err != nil {
			return EvalResult{}, err
		}
		return newEvalDecimal(dec), nil
	case sqltypes.VarChar, sqltypes.Text, sqltypes.VarBinary:
		return EvalResult{typ: sqltypes.VarBinary, bytes: value}, nil
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Timestamp, sqltypes.Date:
		return EvalResult{typ: typ, bytes: value}, nil
	case sqltypes.Null:
		return resultNull, nil
	default:
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Type is not supported: %s", typ.String())
	}

}
func evaluateByType(val *querypb.BindVariable) (EvalResult, error) {
	switch val.Type {
	case querypb.Type_TUPLE:
		tuple := make([]EvalResult, 0, len(val.Values))
		for _, value := range val.Values {
			single, err := evaluateByTypeSingle(value.Type, value.Value)
			if err != nil {
				return EvalResult{}, err
			}
			tuple = append(tuple, single)
		}
		return EvalResult{
			typ:   querypb.Type_TUPLE,
			tuple: &tuple,
		}, nil

	default:
		return evaluateByTypeSingle(val.Type, val.Value)
	}
}

// debugString prints the entire EvalResult in a debug format
func (e *EvalResult) debugString() string {
	return fmt.Sprintf("(%s) 0x%08x %s", querypb.Type_name[int32(e.typ)], e.numval, e.bytes)
}

func (c *CollateExpr) Evaluate(env *ExpressionEnv) (EvalResult, error) {
	res, err := c.Expr.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	if err := collations.Local().EnsureCollate(res.collation.Collation, c.TypedCollation.Collation); err != nil {
		return EvalResult{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
	}
	res.collation = c.TypedCollation
	return res, nil
}

func (c *CollateExpr) Type(env *ExpressionEnv) (querypb.Type, error) {
	return c.Expr.Type(env)
}

func (c *CollateExpr) Collation() collations.TypedCollation {
	return c.TypedCollation
}
