/*
Copyright 2022 The Vitess Authors.

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

import (
	"math/bits"
	"unsafe"
)

// A Bitset is an immutable collection of bits. You can perform logical operations
// on it, but all mutable operations return a new Bitset.
// It is safe to compare directly using the comparison operator and to use as a map key.
type Bitset string

const bitsetWidth = 8

func bitsetWordSize(max int) int {
	return max/bitsetWidth + 1
}

// toBiset converts a slice of bytes into a Bitset without allocating memory.
// Bitset is actually a type alias for `string`, which is the only native type in Go that is dynamic _and_
// immutable, so it can be used as a key in maps or compared directly.
func toBitset(words []byte) Bitset {
	if len(words) == 0 {
		return ""
	}
	if words[len(words)-1] == 0 {
		panic("toBitset: did not truncate")
	}
	// to convert a byte slice into a bitset without cloning the slice, we use the same trick as
	// the Go standard library's `strings.Builder`. A slice header is [data, len, cap] while a
	// string header is [data, len], hence the first two words of a slice header can be reinterpreted
	// as a string header simply by casting into it.
	// This assumes that the `words` slice will never be written to after returning from this function.
	return *(*Bitset)(unsafe.Pointer(&words))
}

func minlen(a, b Bitset) int {
	if len(a) < len(b) {
		return len(a)
	}
	return len(b)
}

// Overlaps returns whether this Bitset and the input have any bits in common
func (bs Bitset) Overlaps(b2 Bitset) bool {
	min := minlen(bs, b2)
	for i := 0; i < min; i++ {
		if bs[i]&b2[i] != 0 {
			return true
		}
	}
	return false
}

// Or returns the logical OR of the two Bitsets as a new Bitset
func (bs Bitset) Or(b2 Bitset) Bitset {
	if len(bs) == 0 {
		return b2
	}
	if len(b2) == 0 {
		return bs
	}

	small, large := bs, b2
	if len(small) > len(large) {
		small, large = large, small
	}

	merged := make([]byte, len(large))
	m := 0

	for m < len(small) {
		merged[m] = small[m] | large[m]
		m++
	}
	for m < len(large) {
		merged[m] = large[m]
		m++
	}
	return toBitset(merged)
}

// AndNot returns the logical AND NOT of the two Bitsets as a new Bitset
func (bs Bitset) AndNot(b2 Bitset) Bitset {
	if len(b2) == 0 {
		return bs
	}

	merged := make([]byte, len(bs))
	m := 0

	for m = 0; m < len(bs); m++ {
		if m < len(b2) {
			merged[m] = bs[m] & ^b2[m]
		} else {
			merged[m] = bs[m]
		}
	}
	for ; m > 0; m-- {
		if merged[m-1] != 0 {
			break
		}
	}
	return toBitset(merged[:m])
}

// And returns the logical AND of the two bitsets as a new Bitset
func (bs Bitset) And(b2 Bitset) Bitset {
	if len(bs) == 0 || len(b2) == 0 {
		return ""
	}

	merged := make([]byte, minlen(bs, b2))
	m := 0

	for m = 0; m < len(merged); m++ {
		merged[m] = bs[m] & b2[m]
	}
	for ; m > 0; m-- {
		if merged[m-1] != 0 {
			break
		}
	}
	return toBitset(merged[:m])
}

// Set returns a copy of this Bitset where the bit at `offset` is set
func (bs Bitset) Set(offset int) Bitset {
	alloc := len(bs)
	if max := bitsetWordSize(offset); max > alloc {
		alloc = max
	}

	words := make([]byte, alloc)
	copy(words, bs)
	words[offset/bitsetWidth] |= 1 << (offset % bitsetWidth)
	return toBitset(words)
}

// SingleBit returns the position of the single bit that is set in this Bitset
// If the Bitset is empty, or contains more than one set bit, it returns -1
func (bs Bitset) SingleBit() int {
	offset := -1
	for i := 0; i < len(bs); i++ {
		t := bs[i]
		if t == 0 {
			continue
		}
		if offset >= 0 || bits.OnesCount8(t) != 1 {
			return -1
		}
		offset = i*bitsetWidth + bits.TrailingZeros8(t)
	}
	return offset
}

// IsContainedBy returns whether this Bitset is contained by the given Bitset
func (bs Bitset) IsContainedBy(b2 Bitset) bool {
	if len(bs) > len(b2) {
		return false
	}
	for i := 0; i < len(bs); i++ {
		left := bs[i]
		rigt := b2[i]
		if left&rigt != left {
			return false
		}
	}
	return true
}

// Popcount returns the number of bits that are set in this Bitset
func (bs Bitset) Popcount() (count int) {
	for i := 0; i < len(bs); i++ {
		count += bits.OnesCount8(bs[i])
	}
	return
}

// ForEach calls the given callback with the position of each bit set in this Bitset
func (bs Bitset) ForEach(yield func(int)) {
	// From Lemire, "Iterating over set bits quickly"
	// https://lemire.me/blog/2018/02/21/iterating-over-set-bits-quickly/
	for i := 0; i < len(bs); i++ {
		bitset := bs[i]
		for bitset != 0 {
			t := bitset & -bitset
			r := bits.TrailingZeros8(bitset)
			yield(i*bitsetWidth + r)
			bitset ^= t
		}
	}
}

// Build creates a new immutable Bitset where all the given bits are set
func Build(bits ...int) Bitset {
	if len(bits) == 0 {
		return ""
	}

	max := bits[0]
	for _, b := range bits[1:] {
		if b > max {
			max = b
		}
	}

	words := make([]byte, bitsetWordSize(max))
	for _, b := range bits {
		words[b/bitsetWidth] |= 1 << (b % bitsetWidth)
	}
	return toBitset(words)
}

const singleton = "\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x20\x00\x00\x00\x40\x00\x00\x00\x80"

// Single returns a new Bitset where only the given bit is set.
// If the given bit is less than 32, Single does not allocate to create a new Bitset.
func Single(bit int) Bitset {
	switch {
	case bit < 8:
		bit = (bit + 1) << 2
		return Bitset(singleton[bit-1 : bit])
	case bit < 16:
		bit = (bit + 1 - 8) << 2
		return Bitset(singleton[bit-2 : bit])
	case bit < 24:
		bit = (bit + 1 - 16) << 2
		return Bitset(singleton[bit-3 : bit])
	case bit < 32:
		bit = (bit + 1 - 24) << 2
		return Bitset(singleton[bit-4 : bit])
	default:
		words := make([]byte, bitsetWordSize(bit))
		words[bit/bitsetWidth] |= 1 << (bit % bitsetWidth)
		return toBitset(words)
	}
}
