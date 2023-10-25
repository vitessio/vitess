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

var _ eval = (*evalBytes)(nil)
var _ hashable = (*evalBytes)(nil)

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
	switch tt := e.SQLType(); {
	case tt == sqltypes.VarBinary:
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
