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
	"encoding/binary"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

type evalBytes struct {
	tt    int16
	flag  typeFlag
	col   collations.TypedCollation
	bytes []byte
}

var (
	_ eval     = (*evalBytes)(nil)
	_ hashable = (*evalBytes)(nil)
)

func newEvalRaw(typ sqltypes.Type, raw []byte, col collations.TypedCollation) *evalBytes {
	return &evalBytes{tt: int16(typ), col: col, bytes: raw}
}

func newEvalBytesHex(raw []byte) eval {
	return &evalBytes{tt: int16(sqltypes.VarBinary), flag: flagHex, col: collationBinary, bytes: raw}
}

// newEvalBytesBit creates a new evalBytes for a bit literal.
// Turns out that a bit literal is not actually typed with
// sqltypes.Bit, but with sqltypes.VarBinary.
func newEvalBytesBit(raw []byte) eval {
	return &evalBytes{tt: int16(sqltypes.VarBinary), flag: flagBit, col: collationBinary, bytes: raw}
}

func newEvalBinary(raw []byte) *evalBytes {
	return newEvalRaw(sqltypes.VarBinary, raw, collationBinary)
}

func newEvalText(raw []byte, col collations.TypedCollation) *evalBytes {
	return newEvalRaw(sqltypes.VarChar, raw, col)
}

func newEvalVector(raw []byte) *evalBytes {
	return newEvalRaw(sqltypes.Vector, raw, collationBinary)
}

func evalToBinary(e eval) *evalBytes {
	if e, ok := e.(*evalBytes); ok && e.isBinary() && e.flag&flagSelected == 0 {
		return e
	}
	return newEvalBinary(e.ToRawBytes())
}

func evalToVarchar(e eval, col collations.ID, convert bool) (*evalBytes, error) {
	var bytes []byte
	var typedcol collations.TypedCollation

	if b, ok := e.(*evalBytes); ok && convert {
		if b.isVarChar() && b.col.Collation == col && b.flag&(flagIsNumber|flagJSONBinary|flagJSONText) == 0 {
			return b, nil
		}

		bytes = b.bytes
		typedcol = b.col
		typedcol.Collation = col

		if col != collations.CollationBinaryID {
			fromCollation := colldata.Lookup(b.col.Collation)
			toCollation := colldata.Lookup(col)

			var err error
			bytes, err = charset.Convert(nil, toCollation.Charset(), bytes, fromCollation.Charset())
			if err != nil {
				return nil, err
			}
		}
	} else {
		bytes = e.ToRawBytes()
		typedcol = collations.TypedCollation{
			Collation:    col,
			Coercibility: collations.CoerceCoercible,
			Repertoire:   collations.RepertoireASCII,
		}
	}
	return newEvalText(bytes, typedcol), nil
}

func (e *evalBytes) Hash(h *vthash.Hasher) {
	switch tt := e.SQLType(); tt {
	case sqltypes.VarBinary:
		h.Write16(hashPrefixBytes)
		_, _ = h.Write(e.bytes)
	default:
		h.Write16(hashPrefixBytes)
		col := colldata.Lookup(e.col.Collation)
		col.Hash(h, e.bytes, 0)
	}
}

func (e *evalBytes) isBinary() bool {
	return e.SQLType() == sqltypes.VarBinary || e.SQLType() == sqltypes.Binary || e.SQLType() == sqltypes.Blob
}

func (e *evalBytes) isHexLiteral() bool {
	return e.flag&flagHex != 0
}

func (e *evalBytes) isBitLiteral() bool {
	return e.flag&flagBit != 0
}

func (e *evalBytes) isHexOrBitLiteral() bool {
	return e.isHexLiteral() || e.isBitLiteral()
}

// makePlain marks e as a regular string.
//
// A hex or bit literal such as 0x41 is a string that counts as a number in
// arithmetic: 0x41 + 0 is 65. The result of a function never does, even if
// it has the same bytes: REVERSE(0x41) + 0 is 0. Functions call makePlain
// on their result to remove that special treatment.
//
// It also removes flagJSONBinary and flagJSONText: a function result has
// the character set of its own type.
//
// makePlain changes e itself, so do not call it on a shared value such as
// the value of a Literal. Use evalPlain there, which returns a copy.
func (e *evalBytes) makePlain() {
	e.flag &^= flagSelected
}

// withCharsetOf returns e, a string that a function made of a part of the
// string src, with the flagJSONBinary or flagJSONText of src.
//
// LEFT, RIGHT, SUBSTRING, TRIM, LOWER, UPPER, REVERSE, REPEAT, LPAD and
// RPAD keep the character set of the string that COALESCE selected: with a
// text column sn = '12' and a binary column vb, CAST(LEFT(COALESCE(sn, vb),
// 5) AS JSON) is 12. Other functions do not: CAST(CONCAT(COALESCE(sn, vb))
// AS JSON) is a binary value.
func (e *evalBytes) withCharsetOf(src *evalBytes) *evalBytes {
	e.flag |= src.flag & (flagJSONBinary | flagJSONText)
	return e
}

// makePlainKeepCharset is makePlain for the functions of withCharsetOf,
// which keep flagJSONBinary and flagJSONText.
func (e *evalBytes) makePlainKeepCharset() {
	e.flag &^= flagNumericContext
}

// jsonBinary reports whether a conversion to JSON reads e as a binary
// string, which becomes an opaque JSON value, and not as text.
//
// A string is binary when its collation is binary, whatever its type:
// CAST(IF(c, 0x41, 'x') AS JSON) is an opaque value, and the LONGTEXT
// IF(c, i, js) is parsed. COALESCE can mark its result otherwise, see
// flagJSONBinary.
func (e *evalBytes) jsonBinary() bool {
	switch {
	case e.flag&flagJSONBinary != 0:
		return true
	case e.flag&flagJSONText != 0:
		return false
	case e.col.Collation == collations.Unknown:
		return sqltypes.IsBinary(e.SQLType())
	}
	return isBinaryString(e.SQLType(), e.col.Collation) || isBinaryText(e.SQLType(), e.col)
}

// decimalFromBytes reports whether a conversion of e to a decimal reads the
// bytes of e as a number, instead of parsing its text.
//
// It does for a hex or bit literal, and for a string that IF, CASE or
// NULLIF selected from one: CAST(0x3132 AS DECIMAL) is 12594. It does not
// for a string that ELT selected: CAST(ELT(1, 0x3132) AS DECIMAL) is 12.
func (e *evalBytes) decimalFromBytes() bool {
	return e.flag&flagNumericContext != 0 && e.flag&flagNotDecimal == 0
}

func (e *evalBytes) isVarChar() bool {
	return e.SQLType() == sqltypes.VarChar
}

func (e *evalBytes) SQLType() sqltypes.Type {
	return sqltypes.Type(e.tt)
}

func (e *evalBytes) Size() int32 {
	return 0
}

func (e *evalBytes) Scale() int32 {
	return 0
}

func (e *evalBytes) ToRawBytes() []byte {
	return e.bytes
}

func (e *evalBytes) string() string {
	return hack.String(e.bytes)
}

func (e *evalBytes) withCollation(col collations.TypedCollation) *evalBytes {
	return newEvalRaw(e.SQLType(), e.bytes, col)
}

func (e *evalBytes) truncateInPlace(size int) {
	switch tt := e.SQLType(); {
	case sqltypes.IsBinary(tt):
		if size > len(e.bytes) {
			pad := make([]byte, size)
			copy(pad, e.bytes)
			e.bytes = pad
		} else {
			e.bytes = e.bytes[:size]
		}
	case sqltypes.IsText(tt):
		collation := colldata.Lookup(e.col.Collation)
		e.bytes = charset.Slice(collation.Charset(), e.bytes, 0, size)
	default:
		panic("called EvalResult.truncate on non-quoted")
	}
}

func (e *evalBytes) toDateBestEffort() datetime.DateTime {
	if t, _, _ := datetime.ParseDateTime(e.string(), -1); !t.IsZero() {
		return t
	}
	if t, _ := datetime.ParseDate(e.string()); !t.IsZero() {
		return datetime.DateTime{Date: t}
	}
	return datetime.DateTime{}
}

func (e *evalBytes) parseNumericBytes(number *[8]byte) bool {
	raw := e.bytes
	if l := len(raw); l > 8 {
		for _, b := range raw[:l-8] {
			if b != 0 {
				return false // overflow
			}
		}
		raw = raw[l-8:]
	}
	for i, b := range raw {
		number[8-len(raw)+i] = b
	}
	return true
}

func (e *evalBytes) toNumericHex() (*evalUint64, bool) {
	var number [8]byte
	if !e.parseNumericBytes(&number) {
		return nil, false
	}

	hex := newEvalUint64(binary.BigEndian.Uint64(number[:]))
	hex.hexLiteral = true
	return hex, true
}

func (e *evalBytes) toNumericBit() (*evalInt64, bool) {
	var number [8]byte
	if !e.parseNumericBytes(&number) {
		return nil, false
	}
	bit := newEvalInt64(int64(binary.BigEndian.Uint64(number[:])))
	bit.bitLiteral = true
	return bit, true
}
