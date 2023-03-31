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
	"time"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

type evalBytes struct {
	tt           int16
	isHexLiteral bool
	isBitLiteral bool
	col          collations.TypedCollation
	bytes        []byte
}

var _ eval = (*evalBytes)(nil)
var _ hashable = (*evalBytes)(nil)

func newEvalRaw(typ sqltypes.Type, raw []byte, col collations.TypedCollation) *evalBytes {
	return &evalBytes{tt: int16(typ), col: col, bytes: raw}
}

func newEvalBytesHex(raw []byte) eval {
	return &evalBytes{tt: int16(sqltypes.VarBinary), isHexLiteral: true, col: collationBinary, bytes: raw}
}

// newEvalBytesBit creates a new evalBytes for a bit literal.
// Turns out that a bit literal is not actually typed with
// sqltypes.Bit, but with sqltypes.VarBinary.
func newEvalBytesBit(raw []byte) eval {
	return &evalBytes{tt: int16(sqltypes.VarBinary), isBitLiteral: true, col: collationBinary, bytes: raw}
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
			fromCollation := b.col.Collation.Get()
			toCollation := col.Get()

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
	case sqltypes.IsDate(tt):
		t, err := e.parseDate()
		if err != nil {
			panic("parseDate() in evalBytes should never fail")
		}
		h.Write16(hashPrefixDate)
		h.Write64(uint64(t.UnixNano()))
	case tt == sqltypes.VarBinary:
		h.Write16(hashPrefixBytes)
		_, _ = h.Write(e.bytes)
	default:
		h.Write16(hashPrefixBytes)
		col := e.col.Collation.Get()
		col.Hash(h, e.bytes, 0)
	}
}

func (e *evalBytes) isBinary() bool {
	return e.SQLType() == sqltypes.VarBinary
}

func (e *evalBytes) isHexOrBitLiteral() bool {
	return e.isHexLiteral || e.isBitLiteral
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
		collation := e.col.Collation.Get()
		e.bytes = charset.Slice(collation.Charset(), e.bytes, 0, size)
	default:
		panic("called EvalResult.truncate on non-quoted")
	}
}

func (e *evalBytes) parseDate() (t time.Time, err error) {
	switch e.SQLType() {
	case sqltypes.Date:
		t, err = datetime.ParseDate(e.string())
	case sqltypes.Timestamp, sqltypes.Datetime:
		t, err = datetime.ParseDateTime(e.string())
	case sqltypes.Time:
		t, _, err = datetime.ParseTime(e.string())
	default:
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "type %v is not date-like", e.SQLType())
	}
	return
}

func (e *evalBytes) toNumericHex() (*evalUint64, bool) {
	raw := e.bytes
	if len(raw) > 8 {
		return nil, false // overflow
	}

	var number [8]byte
	for i, b := range raw {
		number[8-len(raw)+i] = b
	}
	hex := newEvalUint64(binary.BigEndian.Uint64(number[:]))
	hex.hexLiteral = true
	return hex, true
}
