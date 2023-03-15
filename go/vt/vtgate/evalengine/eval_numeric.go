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
	"math"
	"strconv"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/json"
	"vitess.io/vitess/go/vt/vthash"
)

type (
	evalNumeric interface {
		eval
		hashable
		toFloat() (*evalFloat, bool)
		toDecimal(m, d int32) *evalDecimal
		toInt64() *evalInt64
		toUint64() *evalUint64
		negate() evalNumeric
	}

	evalInt64 struct {
		i int64
	}

	evalUint64 struct {
		u          uint64
		hexLiteral bool
	}

	evalFloat struct {
		f float64
	}

	evalDecimal struct {
		dec    decimal.Decimal
		length int32
	}
)

var _ evalNumeric = (*evalInt64)(nil)
var _ evalNumeric = (*evalUint64)(nil)
var _ evalNumeric = (*evalFloat)(nil)
var _ evalNumeric = (*evalDecimal)(nil)

var evalBoolTrue = &evalInt64{1}
var evalBoolFalse = &evalInt64{0}

func newEvalUint64(u uint64) *evalUint64 {
	return &evalUint64{u: u}
}

func newEvalInt64(i int64) *evalInt64 {
	return &evalInt64{i: i}
}

func newEvalFloat(f float64) *evalFloat {
	return &evalFloat{f: f}
}

func newEvalDecimal(dec decimal.Decimal, m, d int32) *evalDecimal {
	if m == 0 && d == 0 {
		return newEvalDecimalWithPrec(dec, -dec.Exponent())
	}
	return newEvalDecimalWithPrec(dec.Clamp(m-d, d), d)
}

func newEvalDecimalWithPrec(dec decimal.Decimal, prec int32) *evalDecimal {
	return &evalDecimal{dec: dec, length: prec}
}

func newEvalBool(b bool) *evalInt64 {
	if b {
		return evalBoolTrue
	}
	return evalBoolFalse
}

func evalToNumeric(e eval) evalNumeric {
	switch e := e.(type) {
	case evalNumeric:
		return e
	case *evalBytes:
		if e.isHexLiteral {
			hex, ok := e.toNumericHex()
			if !ok {
				// overflow
				return newEvalFloat(0)
			}
			return hex
		}
		return &evalFloat{f: parseStringToFloat(e.string())}
	case *evalJSON:
		switch e.Type() {
		case json.TypeTrue:
			return newEvalBool(true)
		case json.TypeFalse:
			return newEvalBool(false)
		case json.TypeNumber, json.TypeString:
			return &evalFloat{f: parseStringToFloat(e.Raw())}
		default:
			return &evalFloat{f: 0}
		}
	default:
		panic("unsupported")
	}
}

func (e *evalInt64) Hash(h *vthash.Hasher) {
	if e.i < 0 {
		h.Write16(hashPrefixIntegralNegative)
	} else {
		h.Write16(hashPrefixIntegralPositive)
	}
	h.Write64(uint64(e.i))
}

func (e *evalInt64) SQLType() sqltypes.Type {
	return sqltypes.Int64
}

func (e *evalInt64) ToRawBytes() []byte {
	return strconv.AppendInt(nil, e.i, 10)
}

func (e *evalInt64) negate() evalNumeric {
	if e.i == math.MinInt64 {
		return newEvalDecimalWithPrec(decimal.NewFromInt(e.i).NegInPlace(), 0)
	}
	return newEvalInt64(-e.i)
}

func (e *evalInt64) toInt64() *evalInt64 {
	return e
}

func (e *evalInt64) toFloat0() float64 {
	return float64(e.i)
}

func (e *evalInt64) toFloat() (*evalFloat, bool) {
	return newEvalFloat(e.toFloat0()), true
}

func (e *evalInt64) toDecimal(m, d int32) *evalDecimal {
	return newEvalDecimal(decimal.NewFromInt(e.i), m, d)
}

func (e *evalInt64) toUint64() *evalUint64 {
	return newEvalUint64(uint64(e.i))
}

func (e *evalUint64) Hash(h *vthash.Hasher) {
	h.Write16(hashPrefixIntegralPositive)
	h.Write64(e.u)
}

func (e *evalUint64) SQLType() sqltypes.Type {
	return sqltypes.Uint64
}

func (e *evalUint64) ToRawBytes() []byte {
	return strconv.AppendUint(nil, e.u, 10)
}

func (e *evalUint64) negate() evalNumeric {
	if e.hexLiteral {
		return newEvalFloat(-float64(e.u))
	}
	if e.u > math.MaxInt64+1 {
		return newEvalDecimalWithPrec(decimal.NewFromUint(e.u).NegInPlace(), 0)
	}
	return newEvalInt64(-int64(e.u))
}

func (e *evalUint64) toInt64() *evalInt64 {
	return newEvalInt64(int64(e.u))
}

func (e *evalUint64) toFloat0() float64 {
	return float64(e.u)
}

func (e *evalUint64) toFloat() (*evalFloat, bool) {
	return newEvalFloat(e.toFloat0()), true
}

func (e *evalUint64) toDecimal(m, d int32) *evalDecimal {
	return newEvalDecimal(decimal.NewFromUint(e.u), m, d)
}

func (e *evalUint64) toUint64() *evalUint64 {
	return e
}

func (e *evalFloat) Hash(h *vthash.Hasher) {
	h.Write16(hashPrefixFloat)
	h.Write64(math.Float64bits(e.f))
}

func (e *evalFloat) SQLType() sqltypes.Type {
	return sqltypes.Float64
}

func (e *evalFloat) ToRawBytes() []byte {
	return FormatFloat(sqltypes.Float64, e.f)
}

func (e *evalFloat) negate() evalNumeric {
	return newEvalFloat(-e.f)
}

func (e *evalFloat) toInt64() *evalInt64 {
	// the int64(f) conversion is always well-defined, but for float values larger than
	// MaxInt64, it returns a negative value. Check for underflow: if the sign of
	// our integral is negative but our float is not, clamp to MaxInt64 like MySQL does.
	f := math.Round(e.f)
	i := int64(f)
	if i < 0 && !math.Signbit(f) {
		i = math.MaxInt64
	}
	return newEvalInt64(i)
}

func (e *evalFloat) toFloat() (*evalFloat, bool) {
	return e, true
}

func (e *evalFloat) toDecimal(m, d int32) *evalDecimal {
	return newEvalDecimal(decimal.NewFromFloatMySQL(e.f), m, d)
}

func (e *evalFloat) toUint64() *evalUint64 {
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
	f := math.Round(e.f)
	i := uint64(int64(f))
	if i > math.MaxInt64 && !math.Signbit(f) {
		i = math.MaxInt64
	}
	return newEvalUint64(i)
}

func (e *evalDecimal) Hash(h *vthash.Hasher) {
	h.Write16(hashPrefixDecimal)
	e.dec.Hash(h)
}

func (e *evalDecimal) SQLType() sqltypes.Type {
	return sqltypes.Decimal
}

func (e *evalDecimal) ToRawBytes() []byte {
	return e.dec.FormatMySQL(e.length)
}

func (e *evalDecimal) negate() evalNumeric {
	if e.dec.IsZero() {
		return e
	}
	return newEvalDecimalWithPrec(e.dec.Neg(), e.length)
}

func (e *evalDecimal) toInt64() *evalInt64 {
	dec := e.dec.Round(0)
	i, valid := dec.Int64()
	if !valid {
		if dec.Sign() < 0 {
			return newEvalInt64(math.MinInt64)
		}
		return newEvalInt64(math.MaxInt64)
	}
	return newEvalInt64(i)
}

func (e *evalDecimal) toFloat0() (float64, bool) {
	return e.dec.Float64()
}

func (e *evalDecimal) toFloat() (*evalFloat, bool) {
	f, exact := e.toFloat0()
	return newEvalFloat(f), exact
}

func (e *evalDecimal) toDecimal(m, d int32) *evalDecimal {
	if m == 0 && d == 0 {
		return e
	}
	return newEvalDecimal(e.dec, m, d)
}

func (e *evalDecimal) toUint64() *evalUint64 {
	dec := e.dec.Round(0)
	if dec.Sign() < 0 {
		i, _ := dec.Int64()
		return newEvalUint64(uint64(i))
	}

	u, valid := dec.Uint64()
	if !valid {
		return newEvalUint64(math.MaxUint64)
	}
	return newEvalUint64(u)
}
