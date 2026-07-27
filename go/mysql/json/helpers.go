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
	"fmt"
	"strings"

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

// hashNumber fingerprints the decimal value a number spells, so that every
// spelling of one value fingerprints alike while two values that differ only
// past the precision of a float64 do not.
func (v *Value) hashNumber(h *vthash.Hasher) {
	// Integral spellings are the common case and canonicalise without the
	// arbitrary-precision detour, byte for byte as Decimal.String would.
	if digits, negative, ok := integralDigits(v.s); ok {
		if negative {
			h.Write32(uint32(len(digits) + 1))
			h.Write8('-')
		} else {
			h.Write32(uint32(len(digits)))
		}
		_, _ = h.WriteString(digits)
		return
	}

	dec, ok := v.Decimal()
	if !ok {
		// A number that will not convert to a decimal cannot be compared
		// either, so its spelling is the best fingerprint left.
		hashFramed(h, v.s)
		return
	}
	hashFramed(h, dec.String())
}

// integralDigits canonicalises a number written without a fraction or an
// exponent: leading zeros go and zero is never signed, which is how
// Decimal.String renders the same value. Every other spelling reports false and
// takes the decimal conversion instead.
func integralDigits(num string) (digits string, negative bool, ok bool) {
	digits = num
	if strings.HasPrefix(digits, "-") {
		negative, digits = true, digits[1:]
	}
	if digits == "" {
		return "", false, false
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return "", false, false
		}
	}

	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return "0", false, true
	}
	return digits, negative, true
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
