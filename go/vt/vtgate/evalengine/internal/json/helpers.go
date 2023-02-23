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
	"vitess.io/vitess/go/vt/vthash"
)

const hashPrefixJSON = 0xCCBB

func (v *Value) Hash(h *vthash.Hasher) {
	h.Write16(hashPrefixJSON)
	_, _ = h.Write(v.ToRawBytes())
}

func (v *Value) ToRawBytes() []byte {
	return v.MarshalTo(nil)
}

func (v *Value) SQLType() sqltypes.Type {
	return sqltypes.TypeJSON
}

func NewArray(vals []*Value) *Value {
	return &Value{
		a: vals,
		t: TypeArray,
	}
}

func NewObject() *Value {
	return &Value{
		o: Object{},
		t: TypeObject,
	}
}

func NewNumber(num []byte) *Value {
	return &Value{
		s: string(num),
		t: TypeNumber,
	}
}

func NewString(raw []byte) *Value {
	return &Value{
		s: string(raw),
		t: TypeString,
	}
}

func (v *Value) Depth() int {
	max := func(a, b int) int {
		if a > b {
			return a
		}
		return b
	}

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
