/*
Copyright 2018 Aliaksandr Valialkin
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
	"strings"
)

// Del deletes the entry with the given key from o.
func (o *Object) Del(key string) {
	if o == nil {
		return
	}
	if !o.keysUnescaped && strings.IndexByte(key, '\\') < 0 {
		// Fast path - try searching for the key without object keys unescaping.
		for i, kv := range o.kvs {
			if kv.k == key {
				o.kvs = append(o.kvs[:i], o.kvs[i+1:]...)
				return
			}
		}
	}

	// Slow path - unescape object keys before item search.
	o.unescapeKeys()

	for i, kv := range o.kvs {
		if kv.k == key {
			o.kvs = append(o.kvs[:i], o.kvs[i+1:]...)
			return
		}
	}
}

// Set sets (key, value) entry in the o.
//
// The value must be unchanged during o lifetime.
func (o *Object) Set(key string, value *Value, t Transformation) {
	if o == nil {
		return
	}
	if value == nil {
		value = ValueNull
	}
	o.unescapeKeys()

	// Try substituting already existing entry with the given key.
	if t == Set || t == Replace {
		for i := range o.kvs {
			kv := &o.kvs[i]
			if kv.k == key {
				kv.v = value
				return
			}
		}
	}
	// Add new entry.
	if t == Set || t == Insert {
		kv := o.getKV()
		kv.k = key
		kv.v = value
	}
}

// SetArrayItem sets the value in the array v at idx position.
//
// The value must be unchanged during v lifetime.
func (v *Value) SetArrayItem(idx int, value *Value, t Transformation) {
	if v == nil || v.t != TypeArray || idx < 0 {
		return
	}
	switch t {
	case Insert:
		if idx < len(v.a) {
			return
		}
		fallthrough
	case Set:
		for idx >= len(v.a) {
			v.a = append(v.a, ValueNull)
		}
	}
	if idx < len(v.a) {
		v.a[idx] = value
	}
}

func (v *Value) DelArrayItem(n int) {
	if v == nil || v.t != TypeArray {
		return
	}
	if n < 0 || n >= len(v.a) {
		return
	}
	v.a = append(v.a[:n], v.a[n+1:]...)
}
