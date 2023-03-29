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
	"encoding/hex"
	"math"
	"math/big"
	"strconv"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

// NullExpr is just what you are lead to believe
var NullExpr = &Literal{}

// NewLiteralIntegralFromBytes returns a literal expression.
// It tries to return an int64, but if the value is too large, it tries with an uint64
func NewLiteralIntegralFromBytes(val []byte) (*Literal, error) {
	if val[0] == '-' {
		panic("NewLiteralIntegralFromBytes: negative value")
	}

	uval, err := strconv.ParseUint(string(val), 10, 64)
	if err != nil {
		if numError, ok := err.(*strconv.NumError); ok && numError.Err == strconv.ErrRange {
			return NewLiteralDecimalFromBytes(val)
		}
		return nil, err
	}
	if uval <= math.MaxInt64 {
		return NewLiteralInt(int64(uval)), nil
	}
	return NewLiteralUint(uval), nil
}

// NewLiteralInt returns a literal expression
func NewLiteralInt(i int64) *Literal {
	return &Literal{newEvalInt64(i)}
}

func NewLiteralBool(b bool) *Literal {
	return &Literal{newEvalBool(b)}
}

// NewLiteralUint returns a literal expression
func NewLiteralUint(i uint64) *Literal {
	return &Literal{newEvalUint64(i)}
}

// NewLiteralFloat returns a literal expression
func NewLiteralFloat(val float64) *Literal {
	return &Literal{newEvalFloat(val)}
}

// NewLiteralFloatFromBytes returns a float literal expression from a slice of bytes
func NewLiteralFloatFromBytes(val []byte) (*Literal, error) {
	fval, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return nil, err
	}
	return &Literal{newEvalFloat(fval)}, nil
}

func NewLiteralDecimalFromBytes(val []byte) (*Literal, error) {
	dec, err := decimal.NewFromMySQL(val)
	if err != nil {
		return nil, err
	}
	return &Literal{newEvalDecimal(dec, 0, 0)}, nil
}

// NewLiteralString returns a literal expression
func NewLiteralString(val []byte, collation collations.TypedCollation) *Literal {
	collation.Repertoire = collations.RepertoireASCII
	for _, b := range val {
		if b >= utf8.RuneSelf {
			collation.Repertoire = collations.RepertoireUnicode
			break
		}
	}
	return &Literal{newEvalText(val, collation)}
}

// NewLiteralDateFromBytes returns a literal expression.
func NewLiteralDateFromBytes(val []byte) (*Literal, error) {
	_, err := sqlparser.ParseDate(string(val))
	if err != nil {
		return nil, err
	}
	return &Literal{newEvalRaw(querypb.Type_DATE, val, collationNumeric)}, nil
}

// NewLiteralTimeFromBytes returns a literal expression.
// it validates the time by parsing it and checking the error.
func NewLiteralTimeFromBytes(val []byte) (*Literal, error) {
	_, err := sqlparser.ParseTime(string(val))
	if err != nil {
		return nil, err
	}
	return &Literal{newEvalRaw(querypb.Type_TIME, val, collationNumeric)}, nil
}

// NewLiteralDatetimeFromBytes returns a literal expression.
// it validates the datetime by parsing it and checking the error.
func NewLiteralDatetimeFromBytes(val []byte) (*Literal, error) {
	_, err := sqlparser.ParseDateTime(string(val))
	if err != nil {
		return nil, err
	}
	return &Literal{newEvalRaw(querypb.Type_DATETIME, val, collationNumeric)}, nil
}

func parseHexLiteral(val []byte) ([]byte, error) {
	raw := make([]byte, hex.DecodedLen(len(val)))
	if _, err := hex.Decode(raw, val); err != nil {
		return nil, err
	}
	return raw, nil
}

func parseHexNumber(val []byte) ([]byte, error) {
	if val[0] != '0' || val[1] != 'x' {
		panic("malformed hex literal from parser")
	}
	if len(val)%2 == 0 {
		return parseHexLiteral(val[2:])
	}
	// If the hex literal doesn't have an even amount of hex digits, we need
	// to pad it with a '0' in the left. Instead of allocating a new slice
	// for padding pad in-place by replacing the 'x' in the original slice with
	// a '0', and clean it up after parsing.
	val[1] = '0'
	defer func() {
		val[1] = 'x'
	}()
	return parseHexLiteral(val[1:])
}

func parseBitLiteral(val []byte) ([]byte, error) {
	var i big.Int
	_, ok := i.SetString(string(val), 2)
	if !ok {
		panic("malformed bit literal from parser")
	}
	return i.Bytes(), nil
}

func NewLiteralBinary(val []byte) *Literal {
	return &Literal{newEvalBinary(val)}
}

func NewLiteralBinaryFromHex(val []byte) (*Literal, error) {
	raw, err := parseHexLiteral(val)
	if err != nil {
		return nil, err
	}
	return &Literal{newEvalBytesHex(raw)}, nil
}

func NewLiteralBinaryFromHexNum(val []byte) (*Literal, error) {
	raw, err := parseHexNumber(val)
	if err != nil {
		return nil, err
	}
	return &Literal{newEvalBytesHex(raw)}, nil
}

func NewLiteralBinaryFromBit(val []byte) (*Literal, error) {
	raw, err := parseBitLiteral(val)
	if err != nil {
		return nil, err
	}
	return &Literal{newEvalBytesBit(raw)}, nil
}

// NewBindVar returns a bind variable
func NewBindVar(key string) *BindVariable {
	return &BindVariable{
		Key: key,
		Collation: collations.TypedCollation{
			Collation:    collations.Unknown,
			Coercibility: collations.CoerceCoercible,
			Repertoire:   collations.RepertoireUnicode,
		},
	}
}

// NewBindVarTuple returns a bind variable containing a tuple
func NewBindVarTuple(key string) *BindVariable {
	return &BindVariable{
		Key:   key,
		Type:  sqltypes.Tuple,
		typed: true,
	}
}

// NewColumn returns a column expression
func NewColumn(offset int) *Column {
	return NewColumnWithCollation(offset, collations.TypedCollation{
		Collation:    collations.Unknown,
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireUnicode,
	})
}

func NewColumnWithCollation(offset int, coll collations.TypedCollation) *Column {
	return &Column{Offset: offset, Collation: coll}
}

// NewTupleExpr returns a tuple expression
func NewTupleExpr(exprs ...Expr) TupleExpr {
	tupleExpr := make(TupleExpr, 0, len(exprs))
	for _, f := range exprs {
		tupleExpr = append(tupleExpr, f)
	}
	return tupleExpr
}
