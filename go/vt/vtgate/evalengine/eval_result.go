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
	item struct {
		expr       Expr
		env        *ExpressionEnv
		res        *EvalResult
		cachedtype querypb.Type
	}

	EvalResult struct {
		typ2       querypb.Type
		collation2 collations.TypedCollation
		numval2    uint64
		bytes2     []byte
		tuple2     *[]item
		decimal2   *decimalResult
	}

	decimalResult struct {
		num  decimal.Big
		frac int
	}
)

func (env *ExpressionEnv) item(expr Expr) item {
	tt, err := expr.typeof(env)
	if err != nil {
		panic(err)
	}
	return item{
		expr:       expr,
		env:        env,
		res:        nil,
		cachedtype: tt,
	}
}

func (i *item) resolve() {
	if i.res == nil {
		res, err := i.expr.eval(i.env)
		if err != nil {
			throwEvalError(err)
		}
		i.res = &res
	}
}

func (i *item) typeof() querypb.Type {
	if i.res == nil {
		return i.cachedtype
	}
	return i.res.typ2
}

func (i *item) collation() collations.TypedCollation {
	i.resolve()
	return i.res.collation2
}

func (i *item) float64() float64 {
	i.resolve()
	return math.Float64frombits(i.res.numval2)
}

func (i *item) uint64() uint64 {
	i.resolve()
	return i.res.numval2
}

func (i *item) int64() int64 {
	i.resolve()
	return int64(i.res.numval2)
}

func (i *item) decimal() *decimalResult {
	i.resolve()
	return i.res.decimal2
}

func (i *item) tuple() []item {
	i.resolve()
	return *i.res.tuple2
}

func (i *item) bytes() []byte {
	i.resolve()
	return i.res.bytes2
}

func (i *item) string() string {
	i.resolve()
	return hack.String(i.res.bytes2)
}

func (i *item) value() sqltypes.Value {
	i.resolve()
	return i.res.Value()
}

func (i *item) textual() bool {
	i.resolve()
	return i.res.textual()
}

func (i *item) null() bool {
	i.resolve()
	return i.res.typ2 == sqltypes.Null
}

func (i *item) nonzero() boolean {
	i.resolve()
	return i.res.nonzero()
}

func (i *item) evalresult() EvalResult {
	i.resolve()
	return *i.res
}

func (i *item) nullSafeHashcode() (HashCode, error) {
	i.resolve()
	return i.res.nullSafeHashcode()
}

func resolved(er EvalResult) item {
	return item{res: &er, cachedtype: er.typ2}
}

var (
	resultNull = EvalResult{typ2: sqltypes.Null, collation2: collationNull}
)

// Value allows for retrieval of the value we expose for public consumption
func (e *EvalResult) Value() sqltypes.Value {
	return e.toSQLValue(e.typ2)
}

// TupleValues allows for retrieval of the value we expose for public consumption
func (e *EvalResult) TupleValues() []sqltypes.Value {
	if e.tuple2 == nil {
		return nil
	}

	values := *e.tuple2
	result := make([]sqltypes.Value, 0, len(values))
	for _, val := range values {
		result = append(result, val.value())
	}
	return result
}

// debugString prints the entire EvalResult in a debug format
func (e *EvalResult) debugString() string {
	return fmt.Sprintf("(%s) 0x%08x %s", querypb.Type_name[int32(e.typ2)], e.numval2, e.bytes2)
}

//ToBooleanStrict is used when the casting to a boolean has to be minimally forgiving,
//such as when assigning to a system variable that is expected to be a boolean
func (e *EvalResult) ToBooleanStrict() (bool, error) {
	intToBool := func(i int) (bool, error) {
		switch i {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%d is not a boolean", i)
		}
	}

	switch e.typ2 {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
		return intToBool(int(e.numval2))
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		return intToBool(int(e.numval2))
	case sqltypes.VarBinary:
		lower := strings.ToLower(string(e.bytes2))
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

func (e *EvalResult) textual() bool {
	return sqltypes.IsText(e.typ2) || sqltypes.IsBinary(e.typ2)
}

func (e *EvalResult) nonzero() boolean {
	switch e.typ2 {
	case sqltypes.Null:
		return boolNULL
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64, sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		return makeboolean(e.numval2 != 0)
	case sqltypes.Float64, sqltypes.Float32:
		return makeboolean(math.Float64frombits(e.numval2) != 0.0)
	case sqltypes.Decimal:
		return makeboolean(!e.decimal2.num.IsZero())
	case sqltypes.VarBinary:
		return makeboolean(parseStringToFloat(string(e.bytes2)) != 0.0)
	case querypb.Type_TUPLE:
		panic("did not typecheck tuples")
	default:
		return boolTrue
	}
}

func (v *EvalResult) toSQLValue(resultType querypb.Type) sqltypes.Value {
	switch {
	case sqltypes.IsSigned(resultType):
		switch v.typ2 {
		case sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.numval2), 10))
		case sqltypes.Uint64, sqltypes.Uint32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.numval2), 10))
		case sqltypes.Float64, sqltypes.Float32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(math.Float64frombits(v.numval2)), 10))
		}
	case sqltypes.IsUnsigned(resultType):
		switch v.typ2 {
		case sqltypes.Uint64, sqltypes.Uint32, sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.numval2), 10))
		case sqltypes.Float64, sqltypes.Float32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(math.Float64frombits(v.numval2)), 10))
		}
	case sqltypes.IsFloat(resultType) || resultType == sqltypes.Decimal:
		switch v.typ2 {
		case sqltypes.Int64, sqltypes.Int32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.numval2), 10))
		case sqltypes.Uint64, sqltypes.Uint32:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.numval2), 10))
		case sqltypes.Float64, sqltypes.Float32:
			format := byte('g')
			if resultType == sqltypes.Decimal {
				format = 'f'
			}
			return sqltypes.MakeTrusted(resultType, strconv.AppendFloat(nil, math.Float64frombits(v.numval2), format, -1, 64))
		case sqltypes.Decimal:
			return sqltypes.MakeTrusted(resultType, v.decimal2.num.FormatCustom(v.decimal2.frac, roundingModeFormat))
		}
	default:
		return sqltypes.MakeTrusted(resultType, v.bytes2)
	}
	return sqltypes.NULL
}

func numericalHashCode(v item) HashCode {
	return HashCode(v.uint64())
}

// newIntegralNumeric parses a value and produces an Int64 or Uint64.
func newIntegralNumeric(v sqltypes.Value) (EvalResult, error) {
	str := v.ToString()
	switch {
	case v.IsSigned():
		ival, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return newEvalInt64(ival), nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return newEvalUint64(uval), nil
	}

	// For other types, do best effort.
	if ival, err := strconv.ParseInt(str, 10, 64); err == nil {
		return newEvalInt64(ival), nil
	}
	if uval, err := strconv.ParseUint(str, 10, 64); err == nil {
		return newEvalUint64(uval), nil
	}
	return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: '%s'", str)
}

// newEvalResult parses a value and produces an EvalResult containing the value
func newEvalResult(v sqltypes.Value) (EvalResult, error) {
	switch {
	case v.IsBinary() || v.IsText():
		// TODO: collation
		return EvalResult{bytes2: v.Raw(), typ2: sqltypes.VarBinary}, nil
	case v.IsSigned():
		ival, err := strconv.ParseInt(v.RawStr(), 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return newEvalInt64(ival), nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(v.RawStr(), 10, 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return newEvalUint64(uval), nil
	case v.IsFloat():
		fval, err := strconv.ParseFloat(v.RawStr(), 64)
		if err != nil {
			return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return newEvalFloat(fval), nil
	case v.Type() == sqltypes.Decimal:
		dec, err := newDecimalString(v.RawStr())
		if err != nil {
			return EvalResult{}, err
		}
		return newEvalDecimal(dec), nil

	default:
		return EvalResult{typ2: v.Type(), bytes2: v.Raw()}, nil
	}
}

// HashCode is a type alias to the code easier to read
type HashCode = uintptr

func (er *EvalResult) nullSafeHashcode() (HashCode, error) {
	switch {
	case sqltypes.IsNull(er.typ2):
		return HashCode(math.MaxUint64), nil
	case sqltypes.IsNumber(er.typ2):
		return HashCode(er.numval2), nil
	case er.textual():
		coll := collations.Local().LookupByID(er.collation2.Collation)
		if coll == nil {
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "text type with an unknown/unsupported collation cannot be hashed")
		}
		return coll.Hash(er.bytes2, 0), nil
	case sqltypes.IsDate(er.typ2):
		time, err := parseDate(resolved(*er))
		if err != nil {
			return 0, err
		}
		return uintptr(time.UnixNano()), nil
	}
	return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v", er.typ2)
}

// NullsafeHashcode returns an int64 hashcode that is guaranteed to be the same
// for two values that are considered equal by `NullsafeCompare`.
func NullsafeHashcode(v sqltypes.Value, collation collations.ID, coerceType querypb.Type) (HashCode, error) {
	castValue, err := castTo(v, coerceType)
	if err != nil {
		return 0, err
	}
	castValue.collation2.Collation = collation
	return castValue.nullSafeHashcode()
}
