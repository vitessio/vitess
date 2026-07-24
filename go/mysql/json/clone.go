/*
Copyright 2026 The Vitess Authors.

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

import "strings"

// Clone returns a deep copy of v: arrays and objects are copied recursively,
// so in-place updates on one value never affect the other. Immutable leaf
// payloads are shared, except raw strings not yet unescaped: Type rewrites
// their backing bytes in place, so they are copied.
func (v *Value) Clone() *Value {
	if v == nil {
		return nil
	}
	switch v.t {
	case TypeObject:
		kvs := make([]kv, len(v.o.kvs))
		for i, item := range v.o.kvs {
			kvs[i] = kv{k: item.k, v: item.v.Clone()}
		}
		return &Value{o: Object{kvs: kvs}, t: TypeObject}
	case TypeArray:
		a := make([]*Value, len(v.a))
		for i, item := range v.a {
			a[i] = item.Clone()
		}
		return &Value{a: a, t: TypeArray}
	case TypeBoolean, TypeNull:
		// ValueTrue, ValueFalse and ValueNull are immutable singletons
		// whose pointer identity is meaningful.
		return v
	case typeRawString:
		return &Value{s: strings.Clone(v.s), t: typeRawString}
	default:
		return &Value{s: v.s, t: v.t, n: v.n}
	}
}
