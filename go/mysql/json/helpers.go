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

package json

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/decimal"

	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

const hashPrefixJSON = 0xCCBB

// Hash writes a fingerprint of v into h. Callers use the fingerprint to decide
// equality, so two documents that compare unequal must not collide.
//
// This is deliberately not built on WeightString. Weight strings exist to order
// JSON values, and ordering takes shortcuts that equality cannot: it
// fingerprints arrays and objects by their cardinality alone, and it renders
// numbers through float64. Ordering and equality are separate jobs and want
// separate encodings. Every branch below fingerprints a value through the same
// representation that comparison compares it by, so that the two can only ever
// disagree by mistake.
//
// Recursion is bounded because parsing rejects documents deeper than MaxDepth.
func (v *Value) Hash(h *vthash.Hasher) {
	h.Write16(hashPrefixJSON)
	v.hash(h)
}

// hashFramed writes s length-prefixed. Variable-length payloads have to be
// framed so that a node's encoding cannot be mistaken for its neighbours':
// unframed, `["", "X\x02\x00\x04Y"]` and `["\x02\x00\x04X", "Y"]` produce the
// same byte stream, because a payload can spell out the tags around it.
// Fixed-width payloads need no frame, since the type tag already fixes how many
// bytes follow.
func hashFramed(h *vthash.Hasher, s string) {
	h.Write32(uint32(len(s)))
	_, _ = h.WriteString(s)
}

func (v *Value) hash(h *vthash.Hasher) {
	// Type() rather than v.t: it resolves a lazily unescaped raw string, which
	// two equal strings may or may not still be carrying.
	//
	// The type leads because comparison orders by type before it looks at any
	// value, which is also why Bit, Blob and Opaque are three tags and not one.
	typ := v.Type()
	h.Write16(uint16(typ))

	switch typ {
	case TypeNull:
		// Two nulls always compare equal, so the type tag says everything.
	case TypeArray:
		h.Write32(uint32(len(v.a)))
		for _, elem := range v.a {
			elem.hash(h)
		}
	case TypeObject:
		h.Write32(uint32(v.o.Len()))
		// Members are kept sorted by key, which is the order objects compare in.
		for _, kv := range v.o.kvs {
			hashFramed(h, kv.k)
			kv.v.hash(h)
		}
	case TypeNumber:
		v.hashNumber(h)
	case TypeString, TypeOpaque, TypeBit, TypeBlob:
		// Strings compare under utf8mb4_bin and the other three byte by byte,
		// so for all of them equality is equality of the unencoded bytes.
		hashFramed(h, v.s)
	case TypeBoolean:
		if v == ValueTrue {
			h.Write8(1)
		} else {
			h.Write8(0)
		}
	case TypeDate:
		// Comparison reads the parsed value and ignores a parse failure, so the
		// zero value it would compare is the zero value we fingerprint.
		d, _ := v.Date()
		d.Hash(h)
	case TypeDateTime:
		dt, _ := v.DateTime()
		dt.Hash(h)
	case TypeTime:
		t, _ := v.Time()
		t.Hash(h)
	default:
		panic(fmt.Errorf("BUG: cannot hash Value type: %d", typ))
	}
}

// Sign markers leading a number's fingerprint. Zero carries no sign, so that
// 0 and -0 fingerprint alike, and unrecognised text carries its own marker so
// that it can never be mistaken for a number that happens to spell the same.
const (
	numberHashZero = iota
	numberHashPositive
	numberHashNegative
	// The scaled markers carry an exponent after the digit count. Most numbers
	// are written with none, so the plain markers leave it off entirely.
	numberHashPositiveScaled
	numberHashNegativeScaled
	numberHashUnparsed
)

// NumericValue returns the value a number compares and fingerprints as.
//
// MySQL fixes the form of a number when the document is built, not when it is
// read: an integer that fits stays exact, and everything else becomes a double,
// losing precision there and then. Both comparison and hashing work from
// whatever was kept, which is why 9007199254740992.1 equals 9007199254740992
// (both are the same double) while 9007199254740993 does not (it stayed an
// integer). Reading the decimal straight off the text would keep a precision
// MySQL has already discarded.
func (v *Value) NumericValue() (decimal.Decimal, bool) {
	switch v.NumberType() {
	case NumberTypeSigned:
		i, ok := v.Int64()
		return decimal.NewFromInt(i), ok
	case NumberTypeUnsigned:
		u, ok := v.Uint64()
		return decimal.NewFromUint(u), ok
	case NumberTypeFloat:
		f, ok := v.Float64()
		return decimal.NewFromFloat(f), ok
	default:
		// A decimal carried over from a SQL value keeps the scale it was
		// written to, so it stands for itself.
		return v.Decimal()
	}
}

// hashNumber fingerprints a number by the form it is stored in, mirroring
// NumericValue so that two numbers fingerprint alike exactly when they compare
// equal. The text a number was written as is the wrong input: MySQL has already
// discarded any precision beyond that form.
func (v *Value) hashNumber(h *vthash.Hasher) {
	switch v.NumberType() {
	case NumberTypeSigned, NumberTypeUnsigned:
		// An integer is kept exactly, so its text is already the value.
		if hashDecimalText(h, v.s) {
			return
		}
	case NumberTypeFloat:
		if f, ok := v.Float64(); ok {
			// The shortest form that round-trips the double is the value it
			// stands for; hashDecimalText canonicalises whatever notation
			// AppendFloat picks.
			var buf [32]byte
			if hashDecimalText(h, hack.String(strconv.AppendFloat(buf[:0], f, 'g', -1, 64))) {
				return
			}
		}
	default:
		if dec, ok := v.Decimal(); ok {
			if hashDecimalText(h, dec.String()) {
				return
			}
		}
	}

	// A number with no value to fingerprint: too large for a double, so
	// comparison rejects it too. Its spelling is all there is to go on, and the
	// marker keeps it clear of anything that does have a value.
	h.Write8(numberHashUnparsed)
	hashFramed(h, v.s)
}

// hashDecimalText writes a canonical fingerprint of the decimal value spelled by
// num, and reports whether it recognised num as a number at all.
//
// Two spellings denote the same value exactly when they share a sign, a run of
// significant digits with no leading or trailing zeros, and the power of ten
// that run scales by, which is what gets written. Working from the text rather
// than from a parsed decimal keeps this allocation-free, which matters because
// every element of a JSON array of numbers passes through here.
func hashDecimalText(h *vthash.Hasher, num string) bool {
	rest := num

	var negative bool
	if strings.HasPrefix(rest, "-") {
		negative, rest = true, rest[1:]
	}

	integer := rest[:indexMantissaEnd(rest)]
	rest = rest[len(integer):]

	var fraction string
	if strings.HasPrefix(rest, ".") {
		rest = rest[1:]
		fraction = rest[:indexMantissaEnd(rest)]
		rest = rest[len(fraction):]
	}

	if integer == "" && fraction == "" || !allDigits(integer) || !allDigits(fraction) {
		return false
	}

	// The digits denote an integer scaled by ten to the exponent: the fraction
	// contributes one negative power of ten per digit, on top of any written
	// exponent.
	exponent := -int64(len(fraction))
	if rest != "" {
		written, ok := parseExponent(rest)
		if !ok {
			return false
		}
		exponent += written
	}

	// Leading zeros contribute nothing, and each trailing zero is worth one
	// power of ten, so dropping them scales the exponent up to compensate. The
	// digits are two slices rather than one, so the leading run can only reach
	// the fraction once the integer part is spent, and the trailing run can only
	// reach the integer part once the fraction is.
	head, tail := strings.TrimLeft(integer, "0"), fraction
	if head == "" {
		tail = strings.TrimLeft(tail, "0")
	}
	if trimmed := strings.TrimRight(tail, "0"); len(trimmed) != len(tail) {
		exponent += int64(len(tail) - len(trimmed))
		tail = trimmed
	}
	if tail == "" {
		if trimmed := strings.TrimRight(head, "0"); len(trimmed) != len(head) {
			exponent += int64(len(head) - len(trimmed))
			head = trimmed
		}
	}

	if head == "" && tail == "" {
		h.Write8(numberHashZero)
		return true
	}

	// The header goes in as one write: at array sizes the hasher call overhead
	// outweighs the work of assembling it.
	var header [13]byte
	switch {
	case exponent == 0 && !negative:
		header[0] = numberHashPositive
	case exponent == 0:
		header[0] = numberHashNegative
	case !negative:
		header[0] = numberHashPositiveScaled
	default:
		header[0] = numberHashNegativeScaled
	}
	binary.LittleEndian.PutUint32(header[1:], uint32(len(head)+len(tail)))
	written := 5
	if exponent != 0 {
		binary.LittleEndian.PutUint64(header[5:], uint64(exponent))
		written = 13
	}

	_, _ = h.Write(header[:written])
	_, _ = h.WriteString(head)
	if tail != "" {
		_, _ = h.WriteString(tail)
	}
	return true
}

// parseExponent reads the exponent suffix of a number, from the e onwards. It
// reports false for a suffix long enough to overflow, which is a value no
// decimal can hold anyway.
func parseExponent(suffix string) (int64, bool) {
	if suffix[0] != 'e' && suffix[0] != 'E' {
		return 0, false
	}

	rest := suffix[1:]
	var negative bool
	switch {
	case strings.HasPrefix(rest, "+"):
		rest = rest[1:]
	case strings.HasPrefix(rest, "-"):
		negative, rest = true, rest[1:]
	}

	if rest == "" || len(rest) > 9 || !allDigits(rest) {
		return 0, false
	}

	var exponent int64
	for i := range len(rest) {
		exponent = exponent*10 + int64(rest[i]-'0')
	}
	if negative {
		exponent = -exponent
	}
	return exponent, true
}

// indexMantissaEnd returns the offset of the first byte that ends a run of
// mantissa digits, or the length of s if the digits run to the end. This is
// strings.IndexAny over ".eE", spelled out because IndexAny decodes runes and
// this sits under every number in a JSON array.
func indexMantissaEnd(s string) int {
	for i := range len(s) {
		switch s[i] {
		case '.', 'e', 'E':
			return i
		}
	}
	return len(s)
}

func allDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func (v *Value) ToRawBytes() []byte {
	return v.MarshalTo(nil)
}

func (v *Value) ToUnencodedBytes() []byte {
	return []byte(v.s)
}

func (v *Value) SQLType() sqltypes.Type {
	return sqltypes.TypeJSON
}

func NewArray(vals []*Value) *Value {
	return &Value{a: vals, t: TypeArray}
}

func NewObject(obj Object) *Value {
	obj.sort()
	return &Value{o: obj, t: TypeObject}
}

func NewNumber(num string, n NumberType) *Value {
	return &Value{s: num, t: TypeNumber, n: n}
}

func NewString(raw string) *Value {
	return &Value{s: raw, t: TypeString}
}

func NewBlob(raw string) *Value {
	return &Value{s: raw, t: TypeBlob}
}

func NewBit(raw string) *Value {
	return &Value{s: raw, t: TypeBit}
}

func NewDate(raw string) *Value {
	return &Value{s: raw, t: TypeDate}
}

func NewDateTime(raw string) *Value {
	return &Value{s: raw, t: TypeDateTime}
}

func NewTime(raw string) *Value {
	return &Value{s: raw, t: TypeTime}
}

func NewOpaqueValue(raw string) *Value {
	return &Value{s: raw, t: TypeOpaque}
}

func NewFromSQL(v sqltypes.Value) (*Value, error) {
	switch {
	case v.Type() == sqltypes.TypeJSON:
		var p Parser
		return p.ParseBytes(v.Raw())
	case v.IsSigned():
		return NewNumber(v.RawStr(), NumberTypeSigned), nil
	case v.IsUnsigned():
		return NewNumber(v.RawStr(), NumberTypeUnsigned), nil
	case v.IsDecimal():
		return NewNumber(v.RawStr(), NumberTypeDecimal), nil
	case v.IsFloat():
		return NewNumber(v.RawStr(), NumberTypeFloat), nil
	case v.IsText():
		return NewString(v.RawStr()), nil
	case v.IsBinary():
		return NewBlob(v.RawStr()), nil
	case v.IsDateTime(), v.IsTimestamp():
		return NewDateTime(v.RawStr()), nil
	case v.IsDate():
		return NewDate(v.RawStr()), nil
	case v.IsTime():
		return NewTime(v.RawStr()), nil
	case v.IsEnum():
		return NewString(v.RawStr()), nil
	case v.IsSet():
		return NewString(v.RawStr()), nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "cannot coerce %v as a JSON type", v)
	}
}

func (v *Value) Depth() int {
	var depth int
	switch v.t {
	case TypeObject:
		for _, kv := range v.o.kvs {
			depth = max(kv.v.Depth(), depth)
		}
	case TypeArray:
		for _, a := range v.a {
			depth = max(a.Depth(), depth)
		}
	}
	return depth + 1
}

func (v *Value) Len() int {
	switch v.t {
	case TypeArray:
		return len(v.a)
	case TypeObject:
		return v.o.Len()
	default:
		return 1
	}
}
