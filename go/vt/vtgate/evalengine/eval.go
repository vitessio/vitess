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
	"strconv"
	"time"
	"unicode/utf8"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/mysql/format"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

type typeFlag uint16

const (
	// flagNull marks that this value is null; implies flagNullable
	flagNull typeFlag = 1 << 0
	// flagNullable marks that this value CAN be null
	flagNullable typeFlag = 1 << 1
	// flagIsBoolean marks that this value should be interpreted as boolean
	flagIsBoolean typeFlag = 1 << 2

	// flagIntegerUdf marks that this value is math.MinInt64, and will underflow if negated
	flagIntegerUdf typeFlag = 1 << 5
	// flagIntegerCap marks that this value is (-math.MinInt64),
	// and should be promoted to flagIntegerUdf if negated
	flagIntegerCap typeFlag = 1 << 6
	// flagIntegerOvf marks that this value will overflow if negated
	flagIntegerOvf typeFlag = 1 << 7

	// flagHex marks that this value originated from a hex literal
	flagHex typeFlag = 1 << 8
	// flagBit marks that this value originated from a bit literal
	flagBit typeFlag = 1 << 9
	// flagExplicitCollation marks that this value has an explicit collation
	flagExplicitCollation typeFlag = 1 << 10

	// flagAmbiguousType marks that the type of this value depends on the value at runtime
	// and cannot be computed accurately
	flagAmbiguousType typeFlag = 1 << 11

	// flagIntegerRange are the flags that mark overflow/underflow in integers
	flagIntegerRange = flagIntegerOvf | flagIntegerCap | flagIntegerUdf
)

func (f typeFlag) Nullable() bool {
	return f&flagNullable != 0 || f&flagNull != 0
}

type eval interface {
	ToRawBytes() []byte
	SQLType() sqltypes.Type
}

type hashable interface {
	eval
	Hash(h *vthash.Hasher)
}

func evalToSQLValue(e eval) sqltypes.Value {
	if e == nil {
		return sqltypes.NULL
	}
	return sqltypes.MakeTrusted(e.SQLType(), e.ToRawBytes())
}

func evalToSQLValueWithType(e eval, resultType sqltypes.Type) sqltypes.Value {
	switch {
	case sqltypes.IsSigned(resultType):
		switch e := e.(type) {
		case *evalInt64:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, e.i, 10))
		case *evalUint64:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, e.u, 10))
		case *evalFloat:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, int64(e.f), 10))
		}
	case sqltypes.IsUnsigned(resultType):
		switch e := e.(type) {
		case *evalInt64:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(e.i), 10))
		case *evalUint64:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, e.u, 10))
		case *evalFloat:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, uint64(e.f), 10))
		}
	case sqltypes.IsFloat(resultType):
		switch e := e.(type) {
		case *evalInt64:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, e.i, 10))
		case *evalUint64:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, e.u, 10))
		case *evalFloat:
			return sqltypes.MakeTrusted(resultType, format.FormatFloat(e.f))
		case *evalDecimal:
			return sqltypes.MakeTrusted(resultType, e.dec.FormatMySQL(e.length))
		}
	case sqltypes.IsDecimal(resultType):
		switch e := e.(type) {
		case *evalInt64:
			return sqltypes.MakeTrusted(resultType, strconv.AppendInt(nil, e.i, 10))
		case *evalUint64:
			return sqltypes.MakeTrusted(resultType, strconv.AppendUint(nil, e.u, 10))
		case *evalFloat:
			return sqltypes.MakeTrusted(resultType, hack.StringBytes(strconv.FormatFloat(e.f, 'f', -1, 64)))
		case *evalDecimal:
			return sqltypes.MakeTrusted(resultType, e.dec.FormatMySQL(e.length))
		}
	case e != nil:
		return sqltypes.MakeTrusted(resultType, e.ToRawBytes())
	}
	return sqltypes.NULL
}

func evalIsTruthy(e eval) boolean {
	if e == nil {
		return boolNULL
	}
	switch e := e.(type) {
	case *evalInt64:
		return makeboolean(e.i != 0)
	case *evalUint64:
		return makeboolean(e.u != 0)
	case *evalFloat:
		return makeboolean(e.f != 0.0)
	case *evalDecimal:
		return makeboolean(!e.dec.IsZero())
	case *evalBytes:
		if e.isHexLiteral() {
			hex, ok := e.toNumericHex()
			if !ok {
				// overflow
				return makeboolean(true)
			}
			return makeboolean(hex.u != 0)
		}
		if e.isBitLiteral() {
			bit, ok := e.toNumericBit()
			if !ok {
				// overflow
				return makeboolean(true)
			}
			return makeboolean(bit.i != 0)
		}
		f, _ := fastparse.ParseFloat64(e.string())
		return makeboolean(f != 0.0)
	case *evalJSON:
		return makeboolean(e.ToBoolean())
	case *evalTemporal:
		return makeboolean(!e.isZero())
	default:
		panic("unhandled case: evalIsTruthy")
	}
}

func evalCoerce(e eval, typ sqltypes.Type, col collations.ID, now time.Time, allowZero bool) (eval, error) {
	if e == nil {
		return nil, nil
	}
	if col == collations.Unknown {
		panic("EvalResult.coerce with no collation")
	}
	if typ == sqltypes.VarChar || typ == sqltypes.Char {
		// if we have an explicit VARCHAR coercion, always force it so the collation is replaced in the target
		return evalToVarchar(e, col, false)
	}
	if e.SQLType() == typ {
		// nothing to be done here
		return e, nil
	}
	switch typ {
	case sqltypes.Null:
		return nil, nil
	case sqltypes.Binary, sqltypes.VarBinary:
		return evalToBinary(e), nil
	case sqltypes.Char, sqltypes.VarChar:
		panic("unreacheable")
	case sqltypes.Decimal:
		return evalToDecimal(e, 0, 0), nil
	case sqltypes.Float32, sqltypes.Float64:
		f, _ := evalToFloat(e)
		return f, nil
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
		return evalToInt64(e), nil
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		return evalToInt64(e).toUint64(), nil
	case sqltypes.Date:
		return evalToDate(e, now, allowZero), nil
	case sqltypes.Datetime, sqltypes.Timestamp:
		return evalToDateTime(e, -1, now, allowZero), nil
	case sqltypes.Time:
		return evalToTime(e, -1), nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s", typ.String())
	}
}

func valueToEvalCast(v sqltypes.Value, typ sqltypes.Type, collation collations.ID, sqlmode SQLMode) (eval, error) {
	switch {
	case typ == sqltypes.Null:
		return nil, nil

	case sqltypes.IsFloat(typ):
		switch {
		case v.IsSigned():
			ival, err := v.ToInt64()
			return newEvalFloat(float64(ival)), err
		case v.IsUnsigned():
			uval, err := v.ToUint64()
			return newEvalFloat(float64(uval)), err
		case v.IsFloat() || v.IsDecimal():
			fval, err := v.ToFloat64()
			return newEvalFloat(fval), err
		case v.IsText() || v.IsBinary():
			fval, _ := fastparse.ParseFloat64(v.RawStr())
			return newEvalFloat(fval), nil
		default:
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation))
			if err != nil {
				return nil, err
			}
			f, _ := evalToFloat(e)
			return f, nil
		}

	case sqltypes.IsDecimal(typ):
		var dec decimal.Decimal
		switch {
		case v.IsIntegral() || v.IsDecimal():
			var err error
			dec, err = decimal.NewFromMySQL(v.Raw())
			if err != nil {
				return nil, err
			}
		case v.IsFloat():
			fval, err := v.ToFloat64()
			if err != nil {
				return nil, err
			}
			dec = decimal.NewFromFloat(fval)
		case v.IsText() || v.IsBinary():
			fval, _ := fastparse.ParseFloat64(v.RawStr())
			dec = decimal.NewFromFloat(fval)
		default:
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation))
			if err != nil {
				return nil, err
			}
			return evalToDecimal(e, 0, 0), nil
		}
		return &evalDecimal{dec: dec, length: -dec.Exponent()}, nil

	case sqltypes.IsSigned(typ):
		switch {
		case v.IsSigned():
			ival, err := v.ToInt64()
			return newEvalInt64(ival), err
		case v.IsUnsigned():
			uval, err := v.ToUint64()
			return newEvalInt64(int64(uval)), err
		case v.IsText() || v.IsBinary():
			i, err := fastparse.ParseInt64(v.RawStr(), 10)
			return newEvalInt64(i), err
		default:
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation))
			if err != nil {
				return nil, err
			}
			return evalToInt64(e), nil
		}

	case sqltypes.IsUnsigned(typ):
		switch {
		case v.IsSigned():
			ival, err := v.ToInt64()
			return newEvalUint64(uint64(ival)), err
		case v.IsUnsigned():
			uval, err := v.ToUint64()
			return newEvalUint64(uval), err
		case v.IsText() || v.IsBinary():
			u, err := fastparse.ParseUint64(v.RawStr(), 10)
			return newEvalUint64(u), err
		default:
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation))
			if err != nil {
				return nil, err
			}
			i := evalToInt64(e)
			return newEvalUint64(uint64(i.i)), nil
		}

	case sqltypes.IsTextOrBinary(typ):
		switch {
		case v.IsText() || v.IsBinary():
			return newEvalRaw(v.Type(), v.Raw(), typedCoercionCollation(v.Type(), collation)), nil
		case sqltypes.IsText(typ):
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation))
			if err != nil {
				return nil, err
			}
			return evalToVarchar(e, collation, true)
		default:
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation))
			if err != nil {
				return nil, err
			}
			return evalToBinary(e), nil
		}

	case typ == sqltypes.TypeJSON:
		return json.NewFromSQL(v)
	case typ == sqltypes.Date:
		e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation))
		if err != nil {
			return nil, err
		}
		// Separate return here to avoid nil wrapped in interface type
		d := evalToDate(e, time.Now(), sqlmode.AllowZeroDate())
		if d == nil {
			return nil, nil
		}
		return d, nil
	case typ == sqltypes.Datetime || typ == sqltypes.Timestamp:
		e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation))
		if err != nil {
			return nil, err
		}
		// Separate return here to avoid nil wrapped in interface type
		dt := evalToDateTime(e, -1, time.Now(), sqlmode.AllowZeroDate())
		if dt == nil {
			return nil, nil
		}
		return dt, nil
	case typ == sqltypes.Time:
		e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation))
		if err != nil {
			return nil, err
		}
		// Separate return here to avoid nil wrapped in interface type
		t := evalToTime(e, -1)
		if t == nil {
			return nil, nil
		}
		return t, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value: %v", v)
}

func valueToEvalNumeric(v sqltypes.Value) (eval, error) {
	switch {
	case v.IsSigned():
		ival, err := v.ToInt64()
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return &evalInt64{i: ival}, nil
	case v.IsUnsigned():
		var uval uint64
		uval, err := v.ToUint64()
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
		return newEvalUint64(uval), nil
	default:
		uval, err := strconv.ParseUint(v.RawStr(), 10, 64)
		if err == nil {
			return newEvalUint64(uval), nil
		}
		ival, err := strconv.ParseInt(v.RawStr(), 10, 64)
		if err == nil {
			return &evalInt64{i: ival}, nil
		}
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse value: '%s'", v.RawStr())
	}
}

func valueToEval(value sqltypes.Value, collation collations.TypedCollation) (eval, error) {
	wrap := func(err error) error {
		if err == nil {
			return nil
		}
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}

	switch tt := value.Type(); {
	case sqltypes.IsSigned(tt):
		ival, err := value.ToInt64()
		return newEvalInt64(ival), wrap(err)
	case sqltypes.IsUnsigned(tt):
		uval, err := value.ToUint64()
		return newEvalUint64(uval), wrap(err)
	case sqltypes.IsFloat(tt):
		fval, err := value.ToFloat64()
		return newEvalFloat(fval), wrap(err)
	case tt == sqltypes.Decimal:
		dec, err := decimal.NewFromMySQL(value.Raw())
		return newEvalDecimal(dec, 0, 0), wrap(err)
	case sqltypes.IsText(tt):
		if tt == sqltypes.HexNum {
			raw, err := parseHexNumber(value.Raw())
			return newEvalBytesHex(raw), wrap(err)
		} else if tt == sqltypes.HexVal {
			hex := value.Raw()
			raw, err := parseHexLiteral(hex[2 : len(hex)-1])
			return newEvalBytesHex(raw), wrap(err)
		} else if tt == sqltypes.BitNum {
			raw, err := parseBitNum(value.Raw())
			return newEvalBytesBit(raw), wrap(err)
		} else {
			return newEvalText(value.Raw(), collation), nil
		}
	case sqltypes.IsBinary(tt):
		return newEvalBinary(value.Raw()), nil
	case tt == sqltypes.Date:
		return parseDate(value.Raw())
	case tt == sqltypes.Datetime || tt == sqltypes.Timestamp:
		return parseDateTime(value.Raw())
	case tt == sqltypes.Time:
		return parseTime(value.Raw())
	case sqltypes.IsNull(tt):
		return nil, nil
	case tt == sqltypes.TypeJSON:
		var p json.Parser
		j, err := p.ParseBytes(value.Raw())
		return j, wrap(err)
	case fallbackBinary(tt):
		return newEvalRaw(tt, value.Raw(), collation), nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Type is not supported: %q %s", value, value.Type())
	}
}

const hexchars = "0123456789ABCDEF"

func sanitizeErrorValue(s []byte) []byte {
	var buf []byte
	for width := 0; len(s) > 0; s = s[width:] {
		r := rune(s[0])
		width = 1
		if r >= utf8.RuneSelf {
			r, width = utf8.DecodeLastRune(s)
		}
		if width == 1 && r == utf8.RuneError {
			buf = append(buf, `\x`...)
			buf = append(buf, hexchars[s[0]>>4])
			buf = append(buf, hexchars[s[0]&0xF])
			continue
		}

		if strconv.IsPrint(r) {
			if r < utf8.RuneSelf {
				buf = append(buf, byte(r))
			} else {
				b := [utf8.UTFMax]byte{}
				n := utf8.EncodeRune(b[:], r)
				buf = append(buf, b[:n]...)
			}
			continue
		}

		buf = append(buf, `\x`...)
		buf = append(buf, hexchars[s[0]>>4])
		buf = append(buf, hexchars[s[0]&0xF])
	}
	return buf
}
