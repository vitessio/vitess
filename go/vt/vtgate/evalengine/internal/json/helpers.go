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
