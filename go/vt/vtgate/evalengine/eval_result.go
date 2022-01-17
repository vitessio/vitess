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
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal"
)

type (
	// EvalResult is a lazily computed result of an evaluation
	EvalResult struct {
		// expr is the expression that will be eventually evaluated to fill the other fields.
		// If expr is set, it means that this EvalResult has not been evaluated yet, and the
		// remaining fields are not valid. Once the evaluation engine calls EvalResult.resolve(),
		// the other fields will be set based on the evaluation result of expr and expr will be
		// set to nil, to mark this result as fully resolved.
		expr Expr
		// env is the ExpressionEnv in which the expr is being evaluated
		env *ExpressionEnv
		// type_ is the SQL type of this result.
		// Must not be accessed directly: call EvalResult.typeof() instead.
		// For most expression types, this is known ahead of time and calling typeof() does not require
		// an evaluation, so the type of an expression can be known without evaluating it.
		type_ querypb.Type
		// collation_ is the collation of this result. It may be uninitialized.
		// Must not be accessed directly: call EvalResult.collation() instead.
		collation_ collations.TypedCollation
		// numeric_ is the numeric value of this result. It may be uninitialized.
		// Must not be accessed directly: call one of the numeric getters for EvalResult instead.
		numeric_ uint64
		// bytes_ is the raw byte value this result. It may be uninitialized.
		// Must not be accessed directly: call EvalResult.bytes() instead.
		bytes_ []byte
		// tuple_ is the list of all results contained in this result, if the result is a tuple.
		// It may be uninitialized.
		// Must not be accessed directly: call EvalResult.tuple() instead.
		tuple_ *[]EvalResult
		// decimal_ is the numeric decimal for this result. It may be uninitialized.
		// Must not be accessed directly: call EvalResult.decimal() instead.
		decimal_ *decimalResult
	}

	decimalResult struct {
		num  decimal.Big
		frac int
	}
)

// init initializes this EvalResult with the given expr. The actual value of this result will be
// calculated lazily when required, and will be the output of evaluating the expr.
func (er *EvalResult) init(env *ExpressionEnv, expr Expr) {
	er.expr = expr
	er.env = env
	er.type_ = expr.typeof(env)
}

const typecheckEval = false

// resolve computes the final value of this EvalResult by evaluating the expr embedded in it.
// This function should not be called directly: it will be called by the evaluation engine
// lazily when it needs to know the value of this result and not earlier.
func (er *EvalResult) resolve() {
	if er.expr != nil {
		if typecheckEval {
			before := er.type_
			er.expr.eval(er.env, er)
			if er.type_ != before {
				panic(fmt.Sprintf("did not pre-compute the right type: %v before evaluation, %v after", before.String(), er.type_.String()))
			}
		} else {
			er.expr.eval(er.env, er)
		}
		er.expr = nil
	}
}

func (er *EvalResult) typeof() querypb.Type {
	if er.type_ < 0 {
		er.resolve()
	}
	return er.type_
}

func (er *EvalResult) collation() collations.TypedCollation {
	er.resolve()
	return er.collation_
}

func (er *EvalResult) float64() float64 {
	er.resolve()
	return math.Float64frombits(er.numeric_)
}

func (er *EvalResult) uint64() uint64 {
	er.resolve()
	return er.numeric_
}

func (er *EvalResult) int64() int64 {
	er.resolve()
	return int64(er.numeric_)
}

func (er *EvalResult) decimal() *decimalResult {
	er.resolve()
	return er.decimal_
}

func (er *EvalResult) tuple() []EvalResult {
	er.resolve()
	return *er.tuple_
}

func (er *EvalResult) bytes() []byte {
	er.resolve()
	return er.bytes_
}

func (er *EvalResult) string() string {
	er.resolve()
	return hack.String(er.bytes_)
}

func (er *EvalResult) value() sqltypes.Value {
	return er.toSQLValue(er.typeof())
}

func (er *EvalResult) null() bool {
	return er.typeof() == sqltypes.Null
}

func (er *EvalResult) setNull() {
	er.type_ = sqltypes.Null
	er.collation_ = collationNull
}

var mysql8 = true

func (er *EvalResult) setBool(b bool) {
	er.collation_ = collationNumeric
	if mysql8 {
		er.type_ = sqltypes.Uint64
	} else {
		er.type_ = sqltypes.Int64
	}
	if b {
		er.numeric_ = 1
	} else {
		er.numeric_ = 0
	}
}

func (er *EvalResult) setBoolean(b boolean) {
	if b == boolNULL {
		er.setNull()
	} else {
		er.setBool(b == boolTrue)
	}
}

func (er *EvalResult) setRaw(typ querypb.Type, raw []byte, coll collations.TypedCollation) {
	er.type_ = typ
	er.bytes_ = raw
	er.collation_ = coll
}

func (er *EvalResult) setInt64(i int64) {
	er.type_ = sqltypes.Int64
	er.numeric_ = uint64(i)
	er.collation_ = collationNumeric
}

func (er *EvalResult) setUint64(u uint64) {
	er.type_ = sqltypes.Uint64
	er.numeric_ = u
	er.collation_ = collationNumeric
}

func (er *EvalResult) setFloat(f float64) {
	er.type_ = sqltypes.Float64
	er.numeric_ = math.Float64bits(f)
	er.collation_ = collationNumeric
}

func (er *EvalResult) setDecimal(dec *decimalResult) {
	er.type_ = sqltypes.Decimal
	er.decimal_ = dec
	er.collation_ = collationNumeric
}

func (er *EvalResult) setTuple(t []EvalResult) {
	er.type_ = querypb.Type_TUPLE
	er.tuple_ = &t
	er.collation_ = collations.TypedCollation{}
}

func (er *EvalResult) replaceBytes(b []byte, collation collations.TypedCollation) {
	er.bytes_ = b
	er.collation_ = collation
}

func (er *EvalResult) replaceCollationID(collation collations.ID) {
	er.collation_.Collation = collation
}

func (er *EvalResult) replaceCollation(collation collations.TypedCollation) {
	er.collation_ = collation
}

func (er *EvalResult) setValue(v sqltypes.Value) error {
	switch {
	case v.IsBinary() || v.IsText():
		// TODO: collation
		er.setRaw(sqltypes.VarBinary, v.Raw(), collations.TypedCollation{})
	case v.IsSigned():
		ival, err := strconv.ParseInt(v.RawStr(), 10, 64)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		er.setInt64(ival)
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(v.RawStr(), 10, 64)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		er.setUint64(uval)
	case v.IsFloat():
		fval, err := strconv.ParseFloat(v.RawStr(), 64)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		er.setFloat(fval)
	case v.Type() == sqltypes.Decimal:
		dec, err := newDecimalString(v.RawStr())
		if err != nil {
			return err
		}
		er.setDecimal(dec)
	default:
		er.setRaw(v.Type(), v.Raw(), collations.TypedCollation{})
	}
	return nil
}

func (er *EvalResult) setValueIntegralNumeric(v sqltypes.Value) error {
	switch {
	case v.IsSigned():
		ival, err := strconv.ParseInt(v.RawStr(), 10, 64)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		er.setInt64(ival)
		return nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(v.RawStr(), 10, 64)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		er.setUint64(uval)
		return nil
	}

	// For other types, do best effort.
	if ival, err := strconv.ParseInt(v.RawStr(), 10, 64); err == nil {
		er.setInt64(ival)
		return nil
	}
	if uval, err := strconv.ParseUint(v.RawStr(), 10, 64); err == nil {
		er.setUint64(uval)
		return nil
	}
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: '%s'", v.RawStr())
}

// Value allows for retrieval of the value we expose for public consumption
func (er *EvalResult) Value() sqltypes.Value {
	if er.expr != nil {
		panic("did not resolve EvalResult after evaluation")
	}
	return er.toSQLValue(er.type_)
}

// TupleValues allows for retrieval of the value we expose for public consumption
func (er *EvalResult) TupleValues() []sqltypes.Value {
	if er.expr != nil {
		panic("did not resolve EvalResult after evaluation")
	}
	if er.tuple_ == nil {
		return nil
	}

	values := *er.tuple_
	result := make([]sqltypes.Value, 0, len(values))
	for _, val := range values {
		result = append(result, val.value())
	}
	return result
}

// debugString prints the entire EvalResult in a debug format
func (er *EvalResult) debugString() string {
	return fmt.Sprintf("(%s) 0x%08x %s", querypb.Type_name[int32(er.type_)], er.numeric_, er.bytes_)
}

// ToBooleanStrict is used when the casting to a boolean has to be minimally forgiving,
// such as when assigning to a system variable that is expected to be a boolean
func (er *EvalResult) ToBooleanStrict() (bool, error) {
	if er.expr != nil {
		panic("did not resolve EvalResult after evaluation")
	}

	intToBool := func(i uint64) (bool, error) {
		switch i {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%d is not a boolean", i)
		}
	}

	switch er.type_ {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
		return intToBool(er.uint64())
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		return intToBool(er.uint64())
	case sqltypes.VarBinary:
		lower := strings.ToLower(er.string())
		switch lower {
		case "on":
			return true, nil
		case "off":
			return false, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "'%s' is not a boolean", lower)
		}
	}
	return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "is not a boolean")
}

func (er *EvalResult) textual() bool {
	tt := er.typeof()
	return sqltypes.IsText(tt) || sqltypes.IsBinary(tt)
}

func (er *EvalResult) nonzero() boolean {
	switch er.type_ {
	case sqltypes.Null:
		return boolNULL
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64, sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		return makeboolean(er.uint64() != 0)
	case sqltypes.Float64, sqltypes.Float32:
		return makeboolean(er.float64() != 0.0)
	case sqltypes.Decimal:
		return makeboolean(!er.decimal().num.IsZero())
	case sqltypes.VarBinary:
		return makeboolean(parseStringToFloat(er.string()) != 0.0)
	case querypb.Type_TUPLE:
		panic("did not typecheck tuples")
	default:
		return boolTrue
	}
}

func (er *EvalResult) toSQLValue(resultType querypb.Type) sqltypes.Value {
	switch {
	case sqltypes.IsSigned(resultType):
		switch er.typeof() {
		case sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, er.int64(), 10))
		case sqltypes.Uint64, sqltypes.Uint32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, er.int64(), 10))
		case sqltypes.Float64, sqltypes.Float32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(er.float64()), 10))
		}
	case sqltypes.IsUnsigned(resultType):
		switch er.typeof() {
		case sqltypes.Uint64, sqltypes.Uint32, sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, er.uint64(), 10))
		case sqltypes.Float64, sqltypes.Float32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(er.float64()), 10))
		}
	case sqltypes.IsFloat(resultType) || resultType == sqltypes.Decimal:
		switch er.typeof() {
		case sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, er.int64(), 10))
		case sqltypes.Uint64, sqltypes.Uint32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, er.uint64(), 10))
		case sqltypes.Float64, sqltypes.Float32:
			format := byte('g')
			if resultType == sqltypes.Decimal {
				format = 'f'
			}
			return sqltypes.MakeTrusted(resultType, strconv.AppendFloat(nil, er.float64(), format, -1, 64))
		case sqltypes.Decimal:
			dec := er.decimal()
			return sqltypes.MakeTrusted(resultType, dec.num.FormatCustom(dec.frac, roundingModeFormat))
		}
	default:
		return sqltypes.MakeTrusted(resultType, er.bytes())
	}
	return sqltypes.NULL
}

// HashCode is a type alias to the code easier to read
type HashCode = uintptr

func (er *EvalResult) numeric() bool {
	return sqltypes.IsNumber(er.typeof())
}

func (er *EvalResult) nullSafeHashcode() (HashCode, error) {
	er.resolve()

	switch {
	case er.null():
		return HashCode(math.MaxUint64), nil
	case er.numeric():
		return HashCode(er.uint64()), nil
	case er.textual():
		coll := collations.Local().LookupByID(er.collation().Collation)
		if coll == nil {
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "text type with an unknown/unsupported collation cannot be hashed")
		}
		return coll.Hash(er.bytes(), 0), nil
	case sqltypes.IsDate(er.typeof()):
		time, err := parseDate(er)
		if err != nil {
			return 0, err
		}
		return HashCode(time.UnixNano()), nil
	default:
		return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v", er.typeof())
	}
}

func (er *EvalResult) setValueCast(v sqltypes.Value, typ querypb.Type) error {
	switch {
	case typ == sqltypes.Null:
		er.setNull()
		return nil
	case sqltypes.IsFloat(typ) || typ == sqltypes.Decimal:
		switch {
		case v.IsSigned():
			ival, err := strconv.ParseInt(v.RawStr(), 10, 64)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%v", err)
			}
			er.setFloat(float64(ival))
			return nil
		case v.IsUnsigned():
			uval, err := strconv.ParseUint(v.RawStr(), 10, 64)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%v", err)
			}
			er.setFloat(float64(uval))
			return nil
		case v.IsFloat() || v.Type() == sqltypes.Decimal:
			fval, err := strconv.ParseFloat(v.RawStr(), 64)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%v", err)
			}
			er.setFloat(fval)
			return nil
		case v.IsText() || v.IsBinary():
			er.setFloat(parseStringToFloat(v.RawStr()))
			return nil
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a float: %v", v)
		}

	case sqltypes.IsSigned(typ):
		switch {
		case v.IsSigned():
			ival, err := strconv.ParseInt(v.RawStr(), 10, 64)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%v", err)
			}
			er.setInt64(ival)
			return nil
		case v.IsUnsigned():
			uval, err := strconv.ParseUint(v.RawStr(), 10, 64)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%v", err)
			}
			er.setInt64(int64(uval))
			return nil
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a signed int: %v", v)
		}

	case sqltypes.IsUnsigned(typ):
		switch {
		case v.IsSigned():
			uval, err := strconv.ParseInt(v.RawStr(), 10, 64)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%v", err)
			}
			er.setUint64(uint64(uval))
			return nil
		case v.IsUnsigned():
			uval, err := strconv.ParseUint(v.RawStr(), 10, 64)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%v", err)
			}
			er.setUint64(uval)
			return nil
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a unsigned int: %v", v)
		}

	case sqltypes.IsText(typ) || sqltypes.IsBinary(typ):
		switch {
		case v.IsText() || v.IsBinary():
			// TODO: collation
			er.setRaw(v.Type(), v.Raw(), collations.TypedCollation{})
			return nil
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a text: %v", v)
		}
	}
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value: %v", v)
}

func (er *EvalResult) setBindVar1(typ querypb.Type, value []byte, collation collations.TypedCollation) {
	switch typ {
	case sqltypes.Int64:
		ival, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			ival = 0
		}
		er.setInt64(ival)
	case sqltypes.Int32:
		ival, err := strconv.ParseInt(string(value), 10, 32)
		if err != nil {
			ival = 0
		}
		// TODO: type32
		er.setInt64(ival)
	case sqltypes.Uint64:
		uval, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			uval = 0
		}
		er.setUint64(uval)
	case sqltypes.Float64:
		fval, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			fval = 0
		}
		er.setFloat(fval)
	case sqltypes.Decimal:
		dec, err := newDecimalString(string(value))
		if err != nil {
			throwEvalError(err)
		}
		er.setDecimal(dec)
	case sqltypes.VarChar, sqltypes.Text, sqltypes.VarBinary:
		er.setRaw(sqltypes.VarBinary, value, collation)
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Timestamp, sqltypes.Date:
		er.setRaw(typ, value, collationNumeric)
	case sqltypes.Null:
		er.setNull()
	default:
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Type is not supported: %s", typ.String()))
	}
}

func (er *EvalResult) setBindVar(val *querypb.BindVariable, collation collations.TypedCollation) {
	switch val.Type {
	case querypb.Type_TUPLE:
		tuple := make([]EvalResult, len(val.Values))
		for i, value := range val.Values {
			t := &tuple[i]
			t.setBindVar1(value.Type, value.Value, collations.TypedCollation{})
		}
		er.setTuple(tuple)

	default:
		er.setBindVar1(val.Type, val.Value, collation)
	}
}

// CoerceTo takes two input types, and decides how they should be coerced before compared
func CoerceTo(v1, v2 querypb.Type) (querypb.Type, error) {
	if v1 == v2 {
		return v1, nil
	}
	if sqltypes.IsNull(v1) || sqltypes.IsNull(v2) {
		return sqltypes.Null, nil
	}
	if (sqltypes.IsText(v1) || sqltypes.IsBinary(v1)) && (sqltypes.IsText(v2) || sqltypes.IsBinary(v2)) {
		return sqltypes.VarChar, nil
	}
	if sqltypes.IsNumber(v1) || sqltypes.IsNumber(v2) {
		switch {
		case sqltypes.IsText(v1) || sqltypes.IsBinary(v1) || sqltypes.IsText(v2) || sqltypes.IsBinary(v2):
			return sqltypes.Float64, nil
		case sqltypes.IsFloat(v2) || v2 == sqltypes.Decimal || sqltypes.IsFloat(v1) || v1 == sqltypes.Decimal:
			return sqltypes.Float64, nil
		case sqltypes.IsSigned(v1):
			switch {
			case sqltypes.IsUnsigned(v2):
				return sqltypes.Uint64, nil
			case sqltypes.IsSigned(v2):
				return sqltypes.Int64, nil
			default:
				return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
			}
		case sqltypes.IsUnsigned(v1):
			switch {
			case sqltypes.IsSigned(v2) || sqltypes.IsUnsigned(v2):
				return sqltypes.Uint64, nil
			default:
				return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
			}
		}
	}
	return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
}

// NullsafeHashcode returns an int64 hashcode that is guaranteed to be the same
// for two values that are considered equal by `NullsafeCompare`.
func NullsafeHashcode(v sqltypes.Value, collation collations.ID, coerceType querypb.Type) (HashCode, error) {
	var cast EvalResult
	if err := cast.setValueCast(v, coerceType); err != nil {
		return 0, err
	}
	cast.replaceCollationID(collation)
	return cast.nullSafeHashcode()
}

func (er *EvalResult) makeFloat() {
	switch er.typeof() {
	case sqltypes.Float64, sqltypes.Float32:
		return
	case sqltypes.Decimal:
		if f, ok := er.coerceDecimalToFloat(); ok {
			er.setFloat(f)
			return
		}
	case sqltypes.Uint64:
		er.setFloat(float64(er.uint64()))
		return
	case sqltypes.Int64:
		er.setFloat(float64(er.int64()))
		return
	}
	if er.bytes() != nil {
		er.setFloat(parseStringToFloat(er.string()))
		return
	}
	er.setFloat(0)
}

func (er *EvalResult) makeNumeric() {
	if er.numeric() {
		er.resolve()
		return
	}
	if ival, err := strconv.ParseInt(er.string(), 10, 64); err == nil {
		er.setInt64(ival)
		return
	}
	if fval, err := strconv.ParseFloat(er.string(), 64); err == nil {
		er.setFloat(fval)
		return
	}
	er.setFloat(0)
}

func (er *EvalResult) coerceDecimalToFloat() (float64, bool) {
	dec := &er.decimal().num
	if f, ok := dec.Float64(); ok {
		return f, true
	}

	// normal form for decimal did not fit in float64, attempt reduction before giving up
	var reduced decimal.Big
	reduced.Copy(dec)
	reduced.Reduce()
	return reduced.Float64()
}

func (er *EvalResult) coerceToFloat() (float64, error) {
	switch er.typeof() {
	case sqltypes.Int64:
		return float64(er.int64()), nil
	case sqltypes.Uint64:
		return float64(er.uint64()), nil
	case sqltypes.Decimal:
		if f, ok := er.coerceDecimalToFloat(); ok {
			return f, nil
		}
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")
	default:
		return er.float64(), nil
	}
}

func (er *EvalResult) coerceToDecimal() *decimalResult {
	switch er.typeof() {
	case sqltypes.Int64:
		return newDecimalInt64(er.int64())
	case sqltypes.Uint64:
		return newDecimalUint64(er.uint64())
	case sqltypes.Float64:
		panic("should never coerce FLOAT64 to DECIMAL")
	case sqltypes.Decimal:
		return er.decimal()
	default:
		panic("bad numeric type")
	}
}

func newEvalUint64(u uint64) (er EvalResult) {
	er.setUint64(u)
	return
}

func newEvalInt64(i int64) (er EvalResult) {
	er.setInt64(i)
	return
}

func newEvalFloat(f float64) (er EvalResult) {
	er.setFloat(f)
	return
}

func newEvalDecimal(dec *decimalResult) (er EvalResult) {
	er.setDecimal(dec)
	return
}

func newEvalResult(v sqltypes.Value) (er EvalResult, err error) {
	err = er.setValue(v)
	return
}

func newEvalResultNumeric(v sqltypes.Value) (er EvalResult, err error) {
	err = er.setValueIntegralNumeric(v)
	return
}

func newEvalRaw(typ querypb.Type, raw []byte) (er EvalResult) {
	er.setRaw(typ, raw, collations.TypedCollation{})
	return
}
