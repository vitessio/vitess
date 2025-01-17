/*
Copyright 2024 The Vitess Authors.

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

package bitset

// Mutable is a growable, in-place version that we can OR bits into
// and eventually turn into a stable (immutable) TableSet.
type Mutable struct {
	data []byte
}

// Or merges another TableSet into this Mutable, resizing if needed.
func (m *Mutable) Or(ts Bitset) {
	// If ts is longer than our current data, grow to accommodate it.
	if len(ts) > len(m.data) {
		oldData := m.data
		m.data = make([]byte, len(ts))
		copy(m.data, oldData)
	}
	// Merge in-place.
	for i := 0; i < len(ts); i++ {
		m.data[i] |= ts[i]
	}
}

// AsImmutable finalizes the Mutable into a TableSet, trimming trailing zeros.
func (m *Mutable) AsImmutable() Bitset {
	trim := len(m.data)
	for trim > 0 && m.data[trim-1] == 0 {
		trim--
	}
	return toBitset(m.data[:trim])
}
