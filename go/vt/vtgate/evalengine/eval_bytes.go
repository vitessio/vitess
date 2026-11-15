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
	"sync"

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

// rowCharset returns the character set that the text of rows and bind
// variables is in for env, and nil when it is in the character set of its
// own collation, which is the default: see
// collations.Environment.WithRowCharset.
func rowCharset(env *collations.Environment) charset.Charset {
	id := env.RowCharset()
	if id == collations.CollationBinaryID {
		return nil
	}
	coll := colldata.Lookup(id)
	if coll == nil {
		return nil
	}
	return coll.Charset()
}

// rowText returns the text of a row or of a bind variable, which is in the
// character set rows, in the character set of the collation that the
// evaluation gives it.
//
// With rows in utf8mb4, a latin1 column with "é" arrives as C3 A9, and a
// comparison in latin1_swedish_ci needs E9. Without the conversion the two
// bytes compare as the two latin1 characters "Ã©", and 'é' < 'd' is true where
// MySQL has it false.
//
// A character that the character set of the collation does not have becomes
// "?", as in a conversion in MySQL. utf8mb4 text for a utf8mb3 collation is
// not converted: text that came from a utf8mb3 column is valid utf8mb3
// already.
func rowText(raw []byte, rows charset.Charset, col collations.ID) []byte {
	if col == collations.Unknown || col == collations.CollationBinaryID {
		return raw
	}
	coll := colldata.Lookup(col)
	if coll == nil {
		return raw
	}
	converted, _ := appendRowText(nil, raw, rows, coll.Charset())
	return converted
}

// appendRowText is rowText for the character set cs of a collation. When the
// text has to be converted, it appends the converted text to dst and returns
// it with true; dst can be nil. Otherwise it returns raw itself and false.
func appendRowText(dst, raw []byte, rows, cs charset.Charset) ([]byte, bool) {
	if _, binary := cs.(charset.Charset_binary); binary || cs.IsSuperset(rows) || rows.IsSuperset(cs) {
		return raw, false
	}

	// ASCII has the same bytes in every character set but the ones with two
	// or four bytes for each character, and swe7, which has Ä at the byte of
	// "[" and eight more letters in place of ASCII punctuation.
	if asciiIdentical(rows) && asciiIdentical(cs) {
		ascii := true
		for _, b := range raw {
			if b >= 0x80 {
				ascii = false
				break
			}
		}
		if ascii {
			return raw, false
		}
	}

	// The error says that a character became "?".
	converted, _ := charset.Convert(dst, cs, raw, rows)
	return converted, true
}

// asciiIdentical reports whether ASCII text has the bytes of ASCII in cs.
func asciiIdentical(cs charset.Charset) bool {
	switch cs.(type) {
	case charset.Charset_ucs2, charset.Charset_utf16, charset.Charset_utf16le, charset.Charset_utf32:
		return false
	}
	return cs.Name() != "swe7"
}

// collateBuffers are the buffers that collateRowText converts the two sides
// of a comparison into.
type collateBuffers struct {
	left, right []byte
}

var collateBufferPool = sync.Pool{New: func() any {
	return &collateBuffers{left: make([]byte, 0, 64), right: make([]byte, 0, 64)}
}}

// maxCollateBuffer is the largest buffer that goes back to collateBufferPool.
const maxCollateBuffer = 64 * 1024

// collateRowText compares two texts that are in the character set rows in
// the collation coll, see rowText. A merge sort compares the same values many
// times, so the converted text goes into buffers that are used again: with
// rows in utf8mb4, comparing 'Müller' with 'Möller' in latin1_swedish_ci
// allocates nothing.
func collateRowText(coll colldata.Collation, rows charset.Charset, left, right []byte) int {
	cs := coll.Charset()
	bufs := collateBufferPool.Get().(*collateBuffers)
	l, converted := appendRowText(bufs.left[:0], left, rows, cs)
	if converted && cap(l) <= maxCollateBuffer {
		bufs.left = l
	}
	r, converted := appendRowText(bufs.right[:0], right, rows, cs)
	if converted && cap(r) <= maxCollateBuffer {
		bufs.right = r
	}
	result := coll.Collate(l, r, false)
	collateBufferPool.Put(bufs)
	return result
}

func newEvalVector(raw []byte) *evalBytes {
	return newEvalRaw(sqltypes.Vector, raw, collationBinary)
}

func evalToBinary(e eval) *evalBytes {
	if e, ok := e.(*evalBytes); ok && e.isBinary() && !e.isHexOrBitLiteral() {
		return e
	}
	return newEvalBinary(e.ToRawBytes())
}

func evalToVarchar(e eval, col collations.ID, convert bool) (*evalBytes, error) {
	var bytes []byte
	var typedcol collations.TypedCollation

	if b, ok := e.(*evalBytes); ok && convert {
		if b.isVarChar() && b.col.Collation == col {
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
