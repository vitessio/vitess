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
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vthash"
)

const hashPrefixJSON = 0xCCBB

// Hash writes a fingerprint of v into h. Callers use the fingerprint to decide
// equality, so two documents that compare unequal must not collide. Unlike
// WeightString, which MySQL defines to fingerprint arrays and objects by their
// cardinality alone so that they sort by length, Hash descends into containers.
// Recursion is bounded because parsing rejects documents deeper than MaxDepth.
func (v *Value) Hash(h *vthash.Hasher) {
	h.Write16(hashPrefixJSON)
	var weights []byte
	v.hash(h, &weights)
}

func (v *Value) hash(h *vthash.Hasher, weights *[]byte) {
	// Type() rather than v.t: it resolves a lazily unescaped raw string, which
	// two equal strings may or may not still be carrying.
	//
	// The type leads: the scalar weight strings collapse Bit, Blob and Opaque
	// onto one tag, but JSON comparison orders those three types apart.
	typ := v.Type()
	h.Write16(uint16(typ))

	switch typ {
	case TypeArray:
		h.Write32(uint32(len(v.a)))
		for _, elem := range v.a {
			elem.hash(h, weights)
		}
	case TypeObject:
		h.Write32(uint32(v.o.Len()))
		// Members are kept sorted by key, which is the order objects compare in.
		for _, kv := range v.o.kvs {
			h.Write32(uint32(len(kv.k)))
			_, _ = h.WriteString(kv.k)
			kv.v.hash(h, weights)
		}
	default:
		*weights = v.WeightString((*weights)[:0])
		_, _ = h.Write(*weights)
	}
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
