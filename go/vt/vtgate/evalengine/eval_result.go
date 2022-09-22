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
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"
)

type flag uint16

const (
	// flagNull marks that this value is null; implies flagNullable
	flagNull flag = 1 << 0
	// flagNullable marks that this value CAN be null
	flagNullable flag = 1 << 1

	// flagIntegerUdf marks that this value is math.MinInt64, and will underflow if negated
	flagIntegerUdf flag = 1 << 5
	// flagIntegerCap marks that this value is (-math.MinInt64),
	// and should be promoted to flagIntegerUdf if negated
	flagIntegerCap flag = 1 << 6
	// flagIntegerOvf marks that this value will overflow if negated
	flagIntegerOvf flag = 1 << 7

	// flagHex marks that this value originated from a hex literal
	flagHex flag = 1 << 8
	// flagBit marks that this value originated from a bit literal
	flagBit flag = 1 << 9
	// flagExplicitCollation marks that this value has an explicit collation
	flagExplicitCollation flag = 1 << 10

	// flagIntegerRange are the flags that mark overflow/underflow in integers
	flagIntegerRange = flagIntegerOvf | flagIntegerCap | flagIntegerUdf
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
		type_  int16 //nolint
		flags_ flag  //nolint
		// collation_ is the collation of this result. It may be uninitialized.
		// Must not be accessed directly: call EvalResult.collation() instead.
		collation_ collations.TypedCollation //nolint
		// numeric_ is the numeric value of this result. It may be uninitialized.
		// Must not be accessed directly: call one of the numeric getters for EvalResult instead.
		numeric_ uint64 //nolint
		// bytes_ is the raw byte value this result. It may be uninitialized.
		// Must not be accessed directly: call EvalResult.bytes() instead.
		bytes_ []byte //nolint
		// tuple_ is the list of all results contained in this result, if the result is a tuple.
		// It may be uninitialized.
		// Must not be accessed directly: call EvalResult.tuple() instead.
		tuple_ *[]EvalResult //nolint
		// decimal_ is the numeric decimal for this result. It may be uninitialized.
		// Must not be accessed directly: call EvalResult.decimal() instead.
		decimal_ decimal.Decimal //nolint
		// length_ is the display length of this eval result; right now this only applies
		// to Decimal results, but in the future it may also work for CHAR
		length_ int32 //nolint
	}
)

// init initializes this EvalResult with the given expr. The actual value of this result will be
// calculated lazily when required, and will be the output of evaluating the expr.
func (er *EvalResult) init(env *ExpressionEnv, expr Expr) {
	er.expr = expr
	er.env = env

	var tt sqltypes.Type
	tt, er.flags_ = expr.typeof(env)
	er.type_ = int16(tt)
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
				panic(fmt.Sprintf("did not pre-compute the right type: %v before evaluation, %v after",
					sqltypes.Type(before).String(),
					sqltypes.Type(er.type_).String()))
			}
		} else {
			er.expr.eval(er.env, er)
		}
		er.expr = nil
	}
}

func (er *EvalResult) typeof() sqltypes.Type {
	if er.type_ < 0 {
		panic("er.type_ < 0")
	}
	return sqltypes.Type(er.type_)
}

func (er *EvalResult) hasFlag(f flag) bool {
	return (er.flags_ & f) != 0
}

func (er *EvalResult) clearFlags(f flag) {
	er.flags_ &= ^f
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

func (er *EvalResult) decimal() decimal.Decimal {
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
	if er.isNull() {
		return sqltypes.NULL
	}
	return sqltypes.MakeTrusted(er.typeof(), er.toRawBytes())
}

func (er *EvalResult) setNull() {
	er.flags_ |= flagNullable | flagNull
}

func (er *EvalResult) setBool(b bool) {
	er.collation_ = collationNumeric
	er.type_ = int16(sqltypes.Int64)
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

func (er *EvalResult) setRaw(typ sqltypes.Type, raw []byte, coll collations.TypedCollation) {
	er.type_ = int16(typ)
	er.bytes_ = raw
	er.collation_ = coll
}

func (er *EvalResult) setBinaryHex(raw []byte) {
	er.type_ = int16(sqltypes.VarBinary)
	er.bytes_ = raw
	er.collation_ = collationBinary
	er.flags_ = flagHex
}

func (er *EvalResult) setString(str string, coll collations.TypedCollation) {
	er.type_ = int16(sqltypes.VarChar)
	er.bytes_ = []byte(str)
	er.collation_ = coll
}

func (er *EvalResult) setRawNumeric(typ sqltypes.Type, u uint64) {
	er.type_ = int16(typ)
	er.numeric_ = u
	er.collation_ = collationNumeric
}

func (er *EvalResult) setInt64(i int64) {
	er.type_ = int16(sqltypes.Int64)
	er.numeric_ = uint64(i)
	er.collation_ = collationNumeric
	if i == math.MinInt64 {
		er.flags_ |= flagIntegerUdf
	}
}

func (er *EvalResult) setUint64(u uint64) {
	er.type_ = int16(sqltypes.Uint64)
	er.numeric_ = u
	er.collation_ = collationNumeric
	if u == math.MaxInt64+1 {
		er.flags_ |= flagIntegerCap
	}
	if u > math.MaxInt64+1 {
		er.flags_ |= flagIntegerOvf
	}
}

func (er *EvalResult) setFloat(f float64) {
	er.type_ = int16(sqltypes.Float64)
	er.numeric_ = math.Float64bits(f)
	er.collation_ = collationNumeric
}

func (er *EvalResult) setDecimal(dec decimal.Decimal, frac int32) {
	er.type_ = int16(sqltypes.Decimal)
	er.decimal_ = dec
	er.collation_ = collationNumeric
	er.length_ = frac
	er.clearFlags(flagIntegerRange)
}

func (er *EvalResult) setTuple(t []EvalResult) {
	er.type_ = int16(sqltypes.Tuple)
	er.tuple_ = &t
	er.collation_ = collations.TypedCollation{}
}

func (er *EvalResult) isHexLiteral() bool {
	return sqltypes.IsBinary(er.typeof()) && er.hasFlag(flagHex)
}

func (er *EvalResult) isNumeric() bool {
	return sqltypes.IsNumber(er.typeof())
}

func (er *EvalResult) isBitwiseBinaryString() bool {
	return sqltypes.IsBinary(er.typeof()) && !er.hasFlag(flagHex|flagBit)
}

func (er *EvalResult) isNull() bool {
	if !er.hasFlag(flagNullable) {
		return false
	}
	if er.hasFlag(flagNull) {
		return true
	}
	er.resolve()
	return er.hasFlag(flagNull)
}

func (er *EvalResult) isTextual() bool {
	tt := er.typeof()
	return sqltypes.IsText(tt) || sqltypes.IsBinary(tt)
}

func (er *EvalResult) isTruthy() boolean {
	if er.isNull() {
		return boolNULL
	}
	switch tt := er.typeof(); {
	case sqltypes.IsIntegral(tt):
		return makeboolean(er.uint64() != 0)
	case sqltypes.IsFloat(tt):
		return makeboolean(er.float64() != 0.0)
	case tt == sqltypes.Decimal:
		return makeboolean(!er.decimal().IsZero())
	case sqltypes.IsBinary(tt) || sqltypes.IsText(tt):
		return makeboolean(parseStringToFloat(er.string()) != 0.0)
	case tt == sqltypes.Tuple:
		panic("did not typecheck tuples")
	default:
		return boolTrue
	}
}

func (er *EvalResult) makeBinary() {
	er.resolve()
	er.bytes_ = er.toRawBytes()
	er.type_ = int16(sqltypes.VarBinary)
	er.collation_ = collationBinary
	er.clearFlags(flagBit | flagHex)
}

func (er *EvalResult) makeTextual(collation collations.ID) {
	er.resolve()
	er.bytes_ = er.toRawBytes()
	er.collation_.Collation = collation
	er.type_ = int16(sqltypes.VarChar)
}

func (er *EvalResult) makeTextualAndConvert(collation collations.ID) bool {
	er.resolve()
	er.bytes_ = er.toRawBytes()
	if er.collation_.Collation == collations.Unknown {
		er.collation_.Collation = collations.CollationBinaryID
	}
	if collation != collations.CollationBinaryID && collation != er.collation_.Collation {
		var err error
		environment := collations.Local()
		fromCollation := environment.LookupByID(er.collation_.Collation)
		toCollation := environment.LookupByID(collation)
		er.bytes_, err = collations.Convert(nil, toCollation, er.bytes_, fromCollation)
		if err != nil {
			er.setNull()
			return false
		}
	}
	er.collation_.Collation = collation
	er.type_ = int16(sqltypes.VarChar)
	return true
}

func (er *EvalResult) truncate(size int) {
	switch tt := er.typeof(); {
	case sqltypes.IsBinary(tt):
		if size > len(er.bytes_) {
			pad := make([]byte, size)
			copy(pad, er.bytes_)
			er.bytes_ = pad
		} else {
			er.bytes_ = er.bytes_[:size]
		}
	case sqltypes.IsText(tt):
		collation := collations.Local().LookupByID(er.collation().Collation)
		er.bytes_ = collations.Slice(collation, er.bytes_, 0, size)
	default:
		panic("called EvalResult.truncate on non-quoted")
	}
}

func (er *EvalResult) replaceCollation(collation collations.TypedCollation) {
	er.collation_ = collation
}

// debugString prints the entire EvalResult in a debug format
func (er *EvalResult) debugString() string {
	return fmt.Sprintf("(%s) 0x%08x %s", sqltypes.Type(er.type_).String(), er.numeric_, er.bytes_)
}

func (er *EvalResult) toRawBytes() []byte {
	if er.isNull() {
		return nil
	}
	switch tt := er.typeof(); {
	case sqltypes.IsSigned(tt):
		return strconv.AppendInt(nil, er.int64(), 10)
	case sqltypes.IsUnsigned(tt):
		return strconv.AppendUint(nil, er.uint64(), 10)
	case sqltypes.IsFloat(tt):
		return FormatFloat(sqltypes.Float64, er.float64())
	case tt == sqltypes.Decimal:
		return er.decimal().FormatMySQL(er.length_)
	default:
		return er.bytes()
	}
}

func (er *EvalResult) toSQLValue(resultType sqltypes.Type) sqltypes.Value {
	switch {
	case sqltypes.IsSigned(resultType):
		switch tt := er.typeof(); {
		case sqltypes.IsSigned(tt):
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, er.int64(), 10))
		case sqltypes.IsUnsigned(tt):
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, er.int64(), 10))
		case sqltypes.IsFloat(tt):
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(er.float64()), 10))
		}
	case sqltypes.IsUnsigned(resultType):
		switch tt := er.typeof(); {
		case sqltypes.IsIntegral(tt):
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, er.uint64(), 10))
		case sqltypes.IsFloat(tt):
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(er.float64()), 10))
		}
	case sqltypes.IsFloat(resultType) || resultType == sqltypes.Decimal:
		switch tt := er.typeof(); {
		case sqltypes.IsSigned(tt):
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, er.int64(), 10))
		case sqltypes.IsUnsigned(tt):
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, er.uint64(), 10))
		case sqltypes.IsFloat(tt):
			return sqltypes.MakeTrusted(resultType, FormatFloat(resultType, er.float64()))
		case tt == sqltypes.Decimal:
			dec := er.decimal()
			return sqltypes.MakeTrusted(resultType, dec.FormatMySQL(er.length_))
		}
	default:
		return sqltypes.MakeTrusted(resultType, er.bytes())
	}
	return sqltypes.NULL
}

// HashCode is a type alias to the code easier to read
type HashCode = uintptr

func (er *EvalResult) nullSafeHashcode() (HashCode, error) {
	er.resolve()

	switch {
	case er.isNull():
		return HashCode(math.MaxUint64), nil
	case er.isNumeric():
		return HashCode(er.uint64()), nil
	case er.isTextual():
		coll := collations.Local().LookupByID(er.collation().Collation)
		if coll == nil {
			return 0, UnsupportedCollationHashError
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

// NullsafeHashCodeInPlace behaves like NullsafeHashCode but the type coercion is performed
// in-place for performance reasons. Eventually this method will replace the old implementation.
func NullsafeHashCodeInPlace(v sqltypes.Value, collation collations.ID, typ sqltypes.Type) (HashCode, error) {
	switch {
	case typ == sqltypes.Null:
		return HashCode(math.MaxUint64), nil

	case sqltypes.IsFloat(typ):
		var f float64
		var err error

		switch {
		case v.IsSigned():
			var ival int64
			ival, err = v.ToInt64()
			f = float64(ival)
		case v.IsUnsigned():
			var uval uint64
			uval, err = v.ToUint64()
			f = float64(uval)
		case v.IsFloat() || v.Type() == sqltypes.Decimal:
			f, err = v.ToFloat64()
		case v.IsText() || v.IsBinary():
			f = parseStringToFloat(v.RawStr())
		default:
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a float: %v", v)
		}
		return HashCode(math.Float64bits(f)), err

	case sqltypes.IsSigned(typ):
		var i int64
		var err error

		switch {
		case v.IsSigned():
			i, err = v.ToInt64()
		case v.IsUnsigned():
			var uval uint64
			uval, err = v.ToUint64()
			i = int64(uval)
		default:
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a signed int: %v", v)
		}
		return HashCode(uint64(i)), err

	case sqltypes.IsUnsigned(typ):
		var u uint64
		var err error

		switch {
		case v.IsSigned():
			var ival int64
			ival, err = v.ToInt64()
			u = uint64(ival)
		case v.IsUnsigned():
			u, err = v.ToUint64()
		default:
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a unsigned int: %v", v)
		}

		return HashCode(u), err

	case sqltypes.IsBinary(typ):
		coll := collations.Local().LookupByID(collations.CollationBinaryID)
		return coll.Hash(v.Raw(), 0), nil

	case sqltypes.IsText(typ):
		coll := collations.Local().LookupByID(collation)
		if coll == nil {
			return 0, UnsupportedCollationHashError
		}
		return coll.Hash(v.Raw(), 0), nil

	default:
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported hash type: %v", v)
	}
}

func (er *EvalResult) setValueCast(v sqltypes.Value, typ sqltypes.Type) error {
	switch {
	case typ == sqltypes.Null:
		er.setNull()
		return nil

	case sqltypes.IsFloat(typ):
		switch {
		case v.IsSigned():
			ival, err := v.ToInt64()
			er.setFloat(float64(ival))
			return err
		case v.IsUnsigned():
			uval, err := v.ToUint64()
			er.setFloat(float64(uval))
			return err
		case v.IsFloat() || v.Type() == sqltypes.Decimal:
			fval, err := v.ToFloat64()
			er.setFloat(fval)
			return err
		case v.IsText() || v.IsBinary():
			er.setFloat(parseStringToFloat(v.RawStr()))
			return nil
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a float: %v", v)
		}

	case typ == sqltypes.Decimal:
		var dec decimal.Decimal
		switch {
		case v.IsIntegral() || v.Type() == sqltypes.Decimal:
			var err error
			dec, err = decimal.NewFromMySQL(v.Raw())
			if err != nil {
				return err
			}
		case v.IsFloat():
			fval, err := v.ToFloat64()
			if err != nil {
				return err
			}
			dec = decimal.NewFromFloat(fval)
		case v.IsText() || v.IsBinary():
			fval := parseStringToFloat(v.RawStr())
			dec = decimal.NewFromFloat(fval)
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a float: %v", v)
		}
		er.setDecimal(dec, -dec.Exponent())
		return nil

	case sqltypes.IsSigned(typ):
		switch {
		case v.IsSigned():
			ival, err := v.ToInt64()
			er.setInt64(ival)
			return err
		case v.IsUnsigned():
			uval, err := v.ToUint64()
			er.setInt64(int64(uval))
			return err
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a signed int: %v", v)
		}

	case sqltypes.IsUnsigned(typ):
		switch {
		case v.IsSigned():
			ival, err := v.ToInt64()
			er.setUint64(uint64(ival))
			return err
		case v.IsUnsigned():
			uval, err := v.ToUint64()
			er.setUint64(uval)
			return err
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

func (er *EvalResult) setValueIntegralNumeric(v sqltypes.Value) error {
	switch {
	case v.IsSigned():
		ival, err := v.ToInt64()
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		er.setInt64(ival)
		return nil
	case v.IsUnsigned():
		var uval uint64
		uval, err := v.ToUint64()
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		er.setUint64(uval)
		return nil
	default:
		uval, err := strconv.ParseUint(v.RawStr(), 10, 64)
		if err == nil {
			er.setUint64(uval)
			return nil
		}
		ival, err := strconv.ParseInt(v.RawStr(), 10, 64)
		if err == nil {
			er.setInt64(ival)
			return nil
		}
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: '%s'", v.RawStr())
	}
}

func (er *EvalResult) setValue(value sqltypes.Value, collation collations.TypedCollation) error {
	var err error
	switch tt := value.Type(); {
	case sqltypes.IsSigned(tt):
		var ival int64
		ival, err = value.ToInt64()
		er.setInt64(ival)
	case sqltypes.IsUnsigned(tt):
		var uval uint64
		uval, err = value.ToUint64()
		er.setUint64(uval)
	case sqltypes.IsFloat(tt):
		var fval float64
		fval, err = value.ToFloat64()
		er.setFloat(fval)
	case tt == sqltypes.Decimal:
		var dec decimal.Decimal
		dec, err = decimal.NewFromMySQL(value.Raw())
		er.setDecimal(dec, -dec.Exponent())
	case sqltypes.IsText(tt):
		if tt == sqltypes.HexNum {
			var raw []byte
			raw, err = parseHexNumber(value.Raw())
			er.setBinaryHex(raw)
		} else if tt == sqltypes.HexVal {
			var hex = value.Raw()
			var raw []byte
			raw, err = parseHexLiteral(hex[2 : len(hex)-1])
			er.setBinaryHex(raw)
		} else {
			er.setRaw(sqltypes.VarChar, value.Raw(), collation)
		}
	case sqltypes.IsBinary(tt):
		er.setRaw(sqltypes.VarBinary, value.Raw(), collationBinary)
	case sqltypes.IsDate(tt):
		er.setRaw(value.Type(), value.Raw(), collationNumeric)
	case sqltypes.IsNull(tt):
		er.setNull()
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Type is not supported: %q %s", value, value.Type())
	}
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}
	return nil
}

// CoerceTo takes two input types, and decides how they should be coerced before compared
func CoerceTo(v1, v2 sqltypes.Type) (sqltypes.Type, error) {
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
func NullsafeHashcode(v sqltypes.Value, collation collations.ID, coerceType sqltypes.Type) (HashCode, error) {
	var cast EvalResult
	if err := cast.setValueCast(v, coerceType); err != nil {
		return 0, err
	}
	cast.collation_.Collation = collation
	return cast.nullSafeHashcode()
}

func (er *EvalResult) makeFloat() {
	er.makeNumeric()
	switch tt := er.typeof(); {
	case sqltypes.IsFloat(tt):
	case tt == sqltypes.Decimal:
		f, _ := er.coerceDecimalToFloat()
		er.setFloat(f)
	case sqltypes.IsUnsigned(tt):
		er.setFloat(float64(er.uint64()))
	case sqltypes.IsSigned(tt):
		er.setFloat(float64(er.int64()))
	}
}

func (er *EvalResult) makeDecimal(m, d int32) {
	er.makeNumeric()

	var dec decimal.Decimal
	switch tt := er.typeof(); {
	case tt == sqltypes.Decimal:
		if m == 0 && d == 0 {
			return
		}
		dec = er.decimal()
	case sqltypes.IsFloat(tt):
		dec = decimal.NewFromFloatMySQL(er.float64())
	case sqltypes.IsSigned(tt):
		dec = decimal.NewFromInt(er.int64())
	case sqltypes.IsUnsigned(tt):
		dec = decimal.NewFromUint(er.uint64())
	}

	if m == 0 && d == 0 {
		er.setDecimal(dec, -dec.Exponent())
	} else {
		er.setDecimal(dec.Clamp(m-d, d), d)
	}
}

func (er *EvalResult) makeNumeric() {
	er.resolve()
	if er.isNumeric() {
		return
	}
	if er.isHexLiteral() {
		raw := er.bytes()
		if len(raw) > 8 {
			// overflow
			er.setFloat(0)
			return
		}

		var number [8]byte
		for i, b := range raw {
			number[8-len(raw)+i] = b
		}
		u := binary.BigEndian.Uint64(number[:])
		er.setUint64(u)
		return
	}
	er.setFloat(parseStringToFloat(er.string()))
}

func (er *EvalResult) upcastNumeric() {
	if !er.isNumeric() {
		panic("upcastNumeric on non-numeric")
	}
	switch tt := er.typeof(); {
	case sqltypes.IsSigned(tt):
		er.type_ = int16(sqltypes.Int64)
	case sqltypes.IsUnsigned(tt):
		er.type_ = int16(sqltypes.Uint64)
	case sqltypes.IsFloat(tt):
		er.type_ = int16(sqltypes.Float64)
	}
}

func (er *EvalResult) makeUnsignedIntegral() {
	er.makeNumeric()
	switch tt := er.typeof(); {
	case sqltypes.IsIntegral(tt):
		er.type_ = int16(sqltypes.Uint64)
	case sqltypes.IsFloat(tt):
		// We want to convert a float64 to its uint64 representation.
		// However, the cast `uint64(f)`, when f < 0, is actually implementation-defined
		// behavior in Go, so we cannot use it here.
		// The most noticeable example of this are M1 Macs with their ARM64 chipsets, where
		// the underflow is clamped at 0:
		//
		//		GOARCH=amd64 | uint64(-2.0) == 18446744073709551614
		// 		GOARCH=arm64 | uint64(-2.0) == 0
		//
		// The most obvious way to keep this well-defined is to do a two-step conversion:
		//		float64 -> int64 -> uint64
		// where every step of the conversion is well-defined. However, this conversion overflows
		// a range of floats, those larger than MaxInt64 but that would still fit in a 64-bit unsigned
		// integer. What's the right way to handle this overflow?
		//
		// Fortunately for us, the `uint64(f)` conversion for negative numbers is also undefined
		// behavior in C and C++, so MySQL is already handling this case! From running this
		// integration test, we can verify that MySQL is using a two-step conversion and it's clamping
		// the value to MaxInt64 on overflow:
		//
		//		mysql> SELECT CAST(18446744073709540000e0 AS UNSIGNED);
		//		+------------------------------------------+
		//		| CAST(18446744073709540000e0 AS UNSIGNED) |
		//		+------------------------------------------+
		//		|                      9223372036854775807 |
		//		+------------------------------------------+
		//
		f := math.Round(er.float64())
		i := uint64(int64(f))
		if i > math.MaxInt64 && !math.Signbit(f) {
			i = math.MaxInt64
		}
		er.setUint64(i)
	case tt == sqltypes.Decimal:
		dec := er.decimal().Round(0)
		if dec.Sign() < 0 {
			i, _ := dec.Int64()
			er.setUint64(uint64(i))
		} else {
			u, _ := dec.Uint64()
			er.setUint64(u)
		}
	default:
		panic("BUG: bad type from makeNumeric")
	}
}

func (er *EvalResult) makeSignedIntegral() {
	er.makeNumeric()
	switch tt := er.typeof(); {
	case sqltypes.IsIntegral(tt):
		er.type_ = int16(sqltypes.Int64)
	case sqltypes.IsFloat(tt):
		// the int64(f) conversion is always well-defined, but for float values larger than
		// MaxInt64, it returns a negative value. Check for underflow: if the sign of
		// our integral is negative but our float is not, clamp to MaxInt64 like MySQL does.
		f := math.Round(er.float64())
		i := int64(f)
		if i < 0 && !math.Signbit(f) {
			i = math.MaxInt64
		}
		er.setInt64(i)
	case tt == sqltypes.Decimal:
		dec := er.decimal().Round(0)
		i, _ := dec.Int64()
		er.setInt64(i)
	default:
		panic("BUG: bad type from makeNumeric")
	}
}

func (er *EvalResult) negateNumeric() {
	er.makeNumeric()
	switch tt := er.typeof(); {
	case sqltypes.IsSigned(tt):
		i := er.int64()
		if er.hasFlag(flagIntegerUdf) {
			dec := decimal.NewFromInt(i).NegInPlace()
			er.setDecimal(dec, 0)
		} else {
			er.setInt64(-i)
		}
	case sqltypes.IsUnsigned(tt):
		u := er.uint64()
		if er.hasFlag(flagHex) {
			er.setFloat(-float64(u))
		} else if er.hasFlag(flagIntegerOvf) {
			dec := decimal.NewFromUint(u).NegInPlace()
			er.setDecimal(dec, 0)
		} else {
			er.setInt64(-int64(u))
		}
	case sqltypes.IsFloat(tt):
		er.setFloat(-er.float64())
	case tt == sqltypes.Decimal:
		if !er.decimal_.IsZero() {
			er.decimal_ = er.decimal_.Neg()
		}
	}
}

func (er *EvalResult) coerceDecimalToFloat() (float64, bool) {
	return er.decimal().Float64()
}

func (er *EvalResult) coerceToFloat() (float64, error) {
	switch tt := er.typeof(); {
	case sqltypes.IsSigned(tt):
		return float64(er.int64()), nil
	case sqltypes.IsUnsigned(tt):
		return float64(er.uint64()), nil
	case tt == sqltypes.Decimal:
		if f, ok := er.coerceDecimalToFloat(); ok {
			return f, nil
		}
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DataOutOfRange, "DECIMAL value is out of range")
	default:
		return er.float64(), nil
	}
}

func (er *EvalResult) coerceToDecimal() decimal.Decimal {
	switch tt := er.typeof(); {
	case sqltypes.IsSigned(tt):
		return decimal.NewFromInt(er.int64())
	case sqltypes.IsUnsigned(tt):
		return decimal.NewFromUint(er.uint64())
	case sqltypes.IsFloat(tt):
		panic("should never coerce FLOAT64 to DECIMAL")
	case tt == sqltypes.Decimal:
		return er.decimal()
	default:
		panic("bad numeric type")
	}
}

func (er *EvalResult) coerce(typ sqltypes.Type, coll collations.ID) {
	if coll == collations.Unknown {
		panic("EvalResult.coerce with no collation")
	}
	if typ == sqltypes.VarChar || typ == sqltypes.Char {
		// if we have an explicit VARCHAR coercion, always force it so the collation is replaced in the target
		er.makeTextual(coll)
		return
	}
	if er.typeof() == typ || er.isNull() {
		// nothing to be done here
		return
	}
	er.resolve()
	switch typ {
	case sqltypes.Null:
		er.setNull()
	case sqltypes.Binary, sqltypes.VarBinary:
		er.makeBinary()
	case sqltypes.Char, sqltypes.VarChar:
		panic("unreacheable")
	case sqltypes.Decimal:
		er.makeDecimal(0, 0)
	case sqltypes.Float32, sqltypes.Float64:
		er.makeFloat()
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
		er.makeSignedIntegral()
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		er.makeUnsignedIntegral()
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Year, sqltypes.TypeJSON, sqltypes.Time, sqltypes.Bit:
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s", typ.String()))
	default:
		panic(fmt.Sprintf("BUG: emitted unknown type: %s", typ))
	}
}

func (er *EvalResult) String() string {
	return er.value().String()
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

func newEvalResultNumeric(v sqltypes.Value) (er EvalResult, err error) {
	err = er.setValueIntegralNumeric(v)
	return
}

func newEvalRaw(typ sqltypes.Type, raw []byte) (er EvalResult) {
	er.setRaw(typ, raw, collations.TypedCollation{})
	return
}

var evalResultPool = sync.Pool{
	New: func() any {
		return &EvalResult{}
	},
}

func borrowEvalResult() *EvalResult {
	return evalResultPool.Get().(*EvalResult)
}

func (er *EvalResult) unborrow() {
	er.flags_ = 0
	er.type_ = 0
	evalResultPool.Put(er)
}

// Value allows for retrieval of the value we expose for public consumption
func (er *EvalResult) Value() sqltypes.Value {
	if er.expr != nil {
		panic("did not resolve EvalResult after evaluation")
	}
	return er.value()
}

func (er *EvalResult) Collation() collations.ID {
	if er.expr != nil {
		panic("did not resolve EvalResult after evaluation")
	}
	if er.collation_.Collation == collations.Unknown {
		return collations.CollationBinaryID
	}
	return er.collation_.Collation
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

func (er *EvalResult) MustBoolean() bool {
	b, err := er.ToBooleanStrict()
	if err != nil {
		panic(err)
	}
	return b
}

// ToBooleanStrict is used when the casting to a boolean has to be minimally forgiving,
// such as when assigning to a system variable that is expected to be a boolean
func (er *EvalResult) ToBooleanStrict() (bool, error) {
	if er.expr != nil {
		panic("did not resolve EvalResult after evaluation")
	}

	switch tt := er.typeof(); {
	case sqltypes.IsIntegral(tt):
		switch er.uint64() {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%d is not a boolean", er.uint64())
		}
	case sqltypes.IsText(tt) || sqltypes.IsBinary(tt):
		switch strings.ToLower(er.string()) {
		case "on":
			return true, nil
		case "off":
			return false, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "'%s' is not a boolean", er.string())
		}
	}
	return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "'%s' is not a boolean", er.string())
}
