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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/datetime"
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

	// flagNumberFromBytes marks a string whose bytes are read as a number
	// in arithmetic, although it is not a hex or bit literal.
	//
	// IF, CASE, NULLIF and ELT return such a string when they select a hex
	// or bit literal: IF(1, 0x41, 'x') + 0 is 65 in MySQL.
	//
	// This flag and flagDecimalFromBytes are only set on an evalBytes
	// value, never on a ctype: which argument is selected is only known
	// when the expression runs.
	flagNumberFromBytes typeFlag = 1 << 12
	// flagDecimalFromBytes marks a string with flagNumberFromBytes whose
	// bytes are also read as a number in a conversion to a decimal.
	//
	// IF, CASE and NULLIF return such a string, and ELT does not:
	// CAST(IF(1, 0x3132, 'x') AS DECIMAL) is 12594, where
	// CAST(ELT(1, 0x3132) AS DECIMAL) parses the text and is 12.
	flagDecimalFromBytes typeFlag = 1 << 13

	// flagJSONBinary and flagJSONText mark the result of COALESCE when the
	// argument that it selected is a binary string and the result is not,
	// or the other way around. A conversion to JSON reads such a result
	// like the selected argument: with a text column sn = '12' and a
	// binary column vb, CAST(COALESCE(sn, vb) AS JSON) is 12, not a binary
	// value. Like flagNumberFromBytes, they are only set on an evalBytes
	// value.
	flagJSONBinary typeFlag = 1 << 14
	flagJSONText   typeFlag = 1 << 15

	// flagIntegerRange are the flags that mark overflow/underflow in integers
	flagIntegerRange = flagIntegerOvf | flagIntegerCap | flagIntegerUdf

	// flagNumericContext are the flags that make a string count as a number
	// in arithmetic. See (*evalBytes).makePlain.
	flagNumericContext = flagHex | flagBit | flagNumberFromBytes | flagDecimalFromBytes

	// flagSelected are the flags that a function result does not keep from
	// its argument. See (*evalBytes).makePlain.
	flagSelected = flagNumericContext | flagJSONBinary | flagJSONText
)

func (f typeFlag) Nullable() bool {
	return f&flagNullable != 0 || f&flagNull != 0
}

type eval interface {
	ToRawBytes() []byte
	SQLType() sqltypes.Type
	Size() int32
	Scale() int32
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

func evalToSQLValueWithType(e eval, resultType Type) sqltypes.Value {
	tt := resultType.Type()
	switch {
	case sqltypes.IsSigned(tt):
		switch e := e.(type) {
		case *evalInt64:
			return sqltypes.MakeTrusted(tt, strconv.AppendInt(nil, e.i, 10))
		case *evalUint64:
			return sqltypes.MakeTrusted(tt, strconv.AppendUint(nil, e.u, 10))
		case *evalFloat:
			return sqltypes.MakeTrusted(tt, strconv.AppendInt(nil, int64(e.f), 10))
		}
	case sqltypes.IsUnsigned(tt):
		switch e := e.(type) {
		case *evalInt64:
			return sqltypes.MakeTrusted(tt, strconv.AppendUint(nil, uint64(e.i), 10))
		case *evalUint64:
			return sqltypes.MakeTrusted(tt, strconv.AppendUint(nil, e.u, 10))
		case *evalFloat:
			return sqltypes.MakeTrusted(tt, strconv.AppendUint(nil, uint64(e.f), 10))
		}
	case sqltypes.IsFloat(tt):
		switch e := e.(type) {
		case *evalInt64:
			return sqltypes.MakeTrusted(tt, strconv.AppendInt(nil, e.i, 10))
		case *evalUint64:
			return sqltypes.MakeTrusted(tt, strconv.AppendUint(nil, e.u, 10))
		case *evalFloat:
			return sqltypes.MakeTrusted(tt, format.FormatFloat(e.f))
		case *evalDecimal:
			return sqltypes.MakeTrusted(tt, e.dec.FormatMySQL(e.length))
		}
	case sqltypes.IsDecimal(tt):
		if numeric, ok := e.(evalNumeric); ok {
			dec := numeric.toDecimal(resultType.size, resultType.scale)
			return sqltypes.MakeTrusted(tt, dec.dec.FormatMySQL(dec.length))
		}
	case e != nil:
		return sqltypes.MakeTrusted(tt, e.ToRawBytes())
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
		if e.flag&flagNumericContext != 0 {
			hex, ok := e.toNumericHex()
			if !ok {
				// The bytes do not fit 64 bits. MySQL reads such a value
				// as the number 0: IF(0xffffffffffffffffff, 1, 0) is 0.
				return makeboolean(false)
			}
			return makeboolean(hex.u != 0)
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

// evalPlain returns e as a regular value.
//
// The result of a function is never a boolean and never a hex or bit
// literal, even when the function returns its argument: ABS(TRUE) is the
// integer 1. Functions call evalPlain on such a result.
//
// evalPlain does not change e. It returns a copy if something has to
// change, so it is safe on a shared value such as the value of a Literal.
func evalPlain(e eval) eval {
	switch e := e.(type) {
	case *evalBytes:
		if e.flag&flagSelected != 0 {
			b := *e
			b.makePlain()
			return &b
		}
	case *evalInt64:
		if e.bitLiteral {
			return newEvalInt64(e.i)
		}
	case *evalUint64:
		if e.hexLiteral {
			return newEvalUint64(e.u)
		}
	}
	return evalNotBoolean(e)
}

// evalNotBoolean returns e, or the integer 1 or 0 if e is TRUE or FALSE.
//
// CASE and IF call it on their result: IF(1, TRUE, 2) is the integer 1.
func evalNotBoolean(e eval) eval {
	if e == evalBoolTrue || e == evalBoolFalse {
		return newEvalInt64(e.(*evalInt64).i)
	}
	return e
}

// evalKeepNumber is called by IF, CASE, NULLIF and ELT on their result.
// sel is the argument that they selected, and res is the result that they
// made of it.
//
// If sel counts as a number in arithmetic, like the hex literal 0x41, then
// res must too: IF(1, 0x41, 'x') + 0 is 65 in MySQL. In that case
// evalKeepNumber returns a copy of res with the flags in keep:
//
//   - flagNumberFromBytes | flagDecimalFromBytes for IF, CASE and NULLIF
//   - flagNumberFromBytes for ELT
//   - none for IFNULL, whose result is a regular string:
//     IFNULL(0x41, 'x') + 0 is 0
//
// The copy does not get flagDecimalFromBytes when sel is a string that ELT
// selected: CAST(IF(1, ELT(1, 0x3132), 'x') AS DECIMAL) is 12, as it is
// without the IF. In every other case evalKeepNumber returns res as it is.
func evalKeepNumber(sel, res eval, keep typeFlag) eval {
	s, ok := sel.(*evalBytes)
	if !ok || s.flag&flagNumericContext == 0 {
		return res
	}
	b, ok := res.(*evalBytes)
	if !ok {
		return res
	}
	r := *b
	r.makePlain()
	if keep != 0 {
		r.flag |= keep
		if s.flag&flagNumberFromBytes != 0 && s.flag&flagDecimalFromBytes == 0 {
			r.flag &^= flagDecimalFromBytes
		}
	}
	return &r
}

// evalWithCollation returns the string e with the coercibility of tc.
//
// The coercibility says how strongly a string holds on to its collation
// when it meets another string: the collation of a column wins over the one
// of a literal. A conversion such as evalCoerce gives its result the
// coercibility of a literal. The result of CASE or CAST needs the one of
// its declared type instead, which this function sets.
//
// It only acts when e already has the collation of tc. It does not change
// e: it returns a copy if something has to change.
func evalWithCollation(e eval, tc collations.TypedCollation) eval {
	b, ok := e.(*evalBytes)
	if !ok || b.col.Collation != tc.Collation || b.col == tc {
		return e
	}
	r := *b
	r.col = tc
	return &r
}

// evalCoerce converts e, the value that CASE or COALESCE selected, to the
// type typ that all their arguments combine to. size and scale are the
// largest of the arguments: the fractional digits of a temporal value and
// the digits after the point of a decimal. IFNULL(i, 1.5) is 5.0 when i is
// the integer 5.
func evalCoerce(e eval, typ sqltypes.Type, size, scale int32, col collations.ID, now time.Time, allowZero bool) (eval, error) {
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
	if e.SQLType() == typ && e.Size() == size && e.Scale() == scale {
		// nothing to be done here
		return evalNotJSONSelected(e), nil
	}
	switch typ {
	case sqltypes.Null:
		return nil, nil
	case sqltypes.Binary, sqltypes.VarBinary:
		return evalNotJSONSelected(evalToBinary(e)), nil
	case sqltypes.Blob, sqltypes.Text:
		// a BLOB with the collation of a character string is a TEXT
		var b *evalBytes
		if col == collations.CollationBinaryID {
			b = newEvalBinary(evalToBinary(e).bytes)
		} else {
			var err error
			if b, err = evalToVarchar(e, col, false); err != nil {
				return nil, err
			}
		}
		b.tt = int16(typ)
		return b, nil
	case sqltypes.Char, sqltypes.VarChar:
		panic("unreacheable")
	case sqltypes.Decimal:
		d := evalToDecimal(e, 0, 0)
		if d.length < scale {
			// IFNULL(i, 1.5) is 5.0
			d = newEvalDecimalWithPrec(d.dec, scale)
		}
		return d, nil
	case sqltypes.Float32, sqltypes.Float64:
		f, _ := evalToFloat(e)
		return f, nil
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
		return evalToInt64(e), nil
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		return evalToInt64(e).toUint64(), nil
	case sqltypes.Year:
		if y, ok := e.(*evalYear); ok {
			return y, nil
		}
		return newEvalYear(evalToInt64(e).i), nil
	case sqltypes.Date:
		return evalToDate(e, now, allowZero), nil
	case sqltypes.Datetime, sqltypes.Timestamp:
		return evalToDateTime(e, coercePrecision(e, size), now, allowZero), nil
	case sqltypes.Time:
		return evalToTime(e, coercePrecision(e, size)), nil
	case sqltypes.TypeJSON:
		return evalToJSON(e)
	case sqltypes.Bit, sqltypes.Geometry, sqltypes.Enum, sqltypes.Set:
		// these only combine with themselves
		return evalNotJSONSelected(e), nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Unsupported type conversion: %s", typ.String())
	}
}

// coercePrecision returns the fractional digits that evalCoerce gives e
// when it converts it to a temporal type with size fractional digits. It
// never removes digits that e has: IFNULL(ndtm, dtm) with a DATETIME(3)
// column dtm keeps its 3 digits, also when the declared size of dtm is not
// known and is 0. A size above 6 is not a number of fractional digits.
func coercePrecision(e eval, size int32) int {
	if size > datetime.DefaultPrecision {
		size = 0
	}
	if t, ok := e.(*evalTemporal); ok {
		return int(max(size, int32(t.prec)))
	}
	return int(size)
}

// evalNotJSONSelected returns e without flagJSONBinary and flagJSONText:
// the result of CASE has the character set of its type, also when it
// selects the result of COALESCE.
func evalNotJSONSelected(e eval) eval {
	if b, ok := e.(*evalBytes); ok && b.flag&(flagJSONBinary|flagJSONText) != 0 {
		r := *b
		r.flag &^= flagJSONBinary | flagJSONText
		return &r
	}
	return e
}

func valueToEvalCast(v sqltypes.Value, typ sqltypes.Type, collation collations.ID, values *EnumSetValues, sqlmode SQLMode) (eval, error) {
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
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation), values)
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
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation), values)
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
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation), values)
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
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation), values)
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
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation), values)
			if err != nil {
				return nil, err
			}
			return evalToVarchar(e, collation, true)
		default:
			e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation), values)
			if err != nil {
				return nil, err
			}
			return evalToBinary(e), nil
		}

	case typ == sqltypes.TypeJSON:
		return json.NewFromSQL(v)
	case typ == sqltypes.Date:
		e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation), values)
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
		e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation), values)
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
		e, err := valueToEval(v, typedCoercionCollation(v.Type(), collation), values)
		if err != nil {
			return nil, err
		}
		// Separate return here to avoid nil wrapped in interface type
		t := evalToTime(e, -1)
		if t == nil {
			return nil, nil
		}
		return t, nil
	case typ == sqltypes.Enum:
		return newEvalEnum(v.Raw(), values), nil
	case typ == sqltypes.Set:
		return newEvalSet(v.Raw(), values), nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value: %v", v)
}

func valueToEval(value sqltypes.Value, collation collations.TypedCollation, values *EnumSetValues) (eval, error) {
	wrap := func(err error) error {
		if err == nil {
			return nil
		}
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}

	switch tt := value.Type(); {
	case tt == sqltypes.Tuple:
		t := &evalTuple{}
		var memberErr error
		err := value.ForEachValue(func(bv sqltypes.Value) {
			if memberErr != nil {
				return
			}
			var e eval
			e, memberErr = valueToEval(bv, collation, values)
			if memberErr != nil {
				return
			}
			t.t = append(t.t, e)
		})
		if err != nil {
			return nil, wrap(err)
		}
		if memberErr != nil {
			return nil, memberErr
		}
		return t, nil
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
	case tt == sqltypes.Enum:
		return newEvalEnum(value.Raw(), values), nil
	case tt == sqltypes.Set:
		return newEvalSet(value.Raw(), values), nil
	case tt == sqltypes.Vector:
		return newEvalVector(value.Raw()), nil
	case sqltypes.IsText(tt):
		switch tt {
		case sqltypes.HexNum:
			raw, err := parseHexNumber(value.Raw())
			return newEvalBytesHex(raw), wrap(err)
		case sqltypes.HexVal:
			raw, err := parseHexValLiteral(value.Raw())
			return newEvalBytesHex(raw), wrap(err)
		case sqltypes.BitNum:
			raw, err := parseBitNum(value.Raw())
			return newEvalBytesBit(raw), wrap(err)
		default:
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
		j, err := p.ParseStored(value.Raw())
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
