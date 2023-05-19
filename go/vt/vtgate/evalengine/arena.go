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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/sqltypes"
)

// Arena is an arena memory allocator for eval types.
// It allocates the types from reusable slices to prevent heap allocations.
// After each evaluation execution, (*Arena).reset() should be called to reset the arena.
type Arena struct {
	aInt64   []evalInt64
	aUint64  []evalUint64
	aFloat64 []evalFloat
	aDecimal []evalDecimal
	aBytes   []evalBytes
}

func (a *Arena) reset() {
	a.aInt64 = a.aInt64[:0]
	a.aUint64 = a.aUint64[:0]
	a.aFloat64 = a.aFloat64[:0]
	a.aDecimal = a.aDecimal[:0]
	a.aBytes = a.aBytes[:0]
}

func (a *Arena) newEvalDecimalWithPrec(dec decimal.Decimal, prec int32) *evalDecimal {
	if cap(a.aDecimal) > len(a.aDecimal) {
		a.aDecimal = a.aDecimal[:len(a.aDecimal)+1]
	} else {
		a.aDecimal = append(a.aDecimal, evalDecimal{})
	}
	val := &a.aDecimal[len(a.aDecimal)-1]
	val.dec = dec
	val.length = prec
	return val
}

func (a *Arena) newEvalDecimal(dec decimal.Decimal, m, d int32) *evalDecimal {
	if m == 0 && d == 0 {
		return a.newEvalDecimalWithPrec(dec, -dec.Exponent())
	}
	return a.newEvalDecimalWithPrec(dec.Clamp(m-d, d), d)
}

func (a *Arena) newEvalBool(b bool) *evalInt64 {
	if b {
		return a.newEvalInt64(1)
	}
	return a.newEvalInt64(0)
}

func (a *Arena) newEvalInt64(i int64) *evalInt64 {
	if cap(a.aInt64) > len(a.aInt64) {
		a.aInt64 = a.aInt64[:len(a.aInt64)+1]
	} else {
		a.aInt64 = append(a.aInt64, evalInt64{})
	}
	val := &a.aInt64[len(a.aInt64)-1]
	val.i = i
	return val
}

func (a *Arena) newEvalUint64(u uint64) *evalUint64 {
	if cap(a.aUint64) > len(a.aUint64) {
		a.aUint64 = a.aUint64[:len(a.aUint64)+1]
	} else {
		a.aUint64 = append(a.aUint64, evalUint64{})
	}
	val := &a.aUint64[len(a.aUint64)-1]
	val.u = u
	val.hexLiteral = false
	return val
}

func (a *Arena) newEvalFloat(f float64) *evalFloat {
	if cap(a.aFloat64) > len(a.aFloat64) {
		a.aFloat64 = a.aFloat64[:len(a.aFloat64)+1]
	} else {
		a.aFloat64 = append(a.aFloat64, evalFloat{})
	}
	val := &a.aFloat64[len(a.aFloat64)-1]
	val.f = f
	return val
}

func (a *Arena) newEvalBytesEmpty() *evalBytes {
	if cap(a.aBytes) > len(a.aBytes) {
		a.aBytes = a.aBytes[:len(a.aBytes)+1]
	} else {
		a.aBytes = append(a.aBytes, evalBytes{})
	}
	return &a.aBytes[len(a.aBytes)-1]
}

func (a *Arena) newEvalBinary(raw []byte) *evalBytes {
	b := a.newEvalBytesEmpty()
	b.tt = int16(sqltypes.VarBinary)
	b.col = collationBinary
	b.bytes = raw
	return b
}

func (a *Arena) newEvalText(raw []byte, tc collations.TypedCollation) *evalBytes {
	b := a.newEvalBytesEmpty()
	b.tt = int16(sqltypes.VarChar)
	b.col = tc
	b.bytes = raw
	return b
}

func (a *Arena) newEvalRaw(raw []byte, tt sqltypes.Type, tc collations.TypedCollation) *evalBytes {
	b := a.newEvalBytesEmpty()
	b.tt = int16(tt)
	b.col = tc
	b.bytes = raw
	return b
}

func (a *Arena) newEvalTime(time datetime.Time, l int) *evalTemporal {
	// TODO: reuse evalTemporal
	return &evalTemporal{t: sqltypes.Time, dt: datetime.DateTime{Time: time.Round(l)}, prec: uint8(l)}
}

func (a *Arena) newEvalDateTime(dt datetime.DateTime, l int) *evalTemporal {
	// TODO: reuse evalTemporal
	return &evalTemporal{t: sqltypes.Datetime, dt: dt.Round(l), prec: uint8(l)}
}

func (a *Arena) newEvalDate(date datetime.Date) *evalTemporal {
	// TODO: reuse evalTemporal
	return &evalTemporal{t: sqltypes.Date, dt: datetime.DateTime{Date: date}}
}

func (a *Arena) newTemporal(t sqltypes.Type, dt datetime.DateTime, prec uint8) *evalTemporal {
	return &evalTemporal{t: t, dt: dt, prec: prec}
}
