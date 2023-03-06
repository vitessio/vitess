/*
Copyright 2021 The Vitess Authors.

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

package uca

import (
	"math/bits"
	"unicode/utf8"
	"unsafe"
)

type FastIterator900 struct {
	iterator900
	fastTable *[256]uint32
	unicode   int
}

func (it *FastIterator900) Done() {
	it.original = nil
	it.input = nil
	it.iterpool.Put(it)
}

func (it *FastIterator900) reset(input []byte) {
	it.fastTable = &fastweightTable_uca900_page000L0
	it.unicode = 0
	it.iterator900.reset(input)
}

func (it *FastIterator900) SkipLevel() int {
	it.codepoint.ce = 0
	it.level++
	it.resetForNextLevel()
	return it.level
}

const maxUnicodeBlocks = 3

// FastForward32 fast-forwards this iterator and the given it2 in parallel until
// there is a mismatch in their weights, and returns their difference.
// This function is similar to NextWeightBlock64 in that it only succeeds if the
// iterators are composed of (mostly) ASCII characters. See the docs for NextWeightBlock64
// for documentation on how these fast comparisons work.
func (it *FastIterator900) FastForward32(it2 *FastIterator900) int {
	// We use a heuristic to detect when we should stop using the FastForward32
	// iterator: every time we encounter a 4-byte block that is fully Unicode,
	// (i.e. without any ASCII characters), we increase the `it.unicode` counter.
	// Encountering a block that is fully ASCII decreases the counter. If the
	// counter ever gets to 4, further calls to FastForward32 are disabled.
	if it.unicode > maxUnicodeBlocks || it.codepoint.ce != 0 || it2.codepoint.ce != 0 {
		return 0
	}

	p1 := it.input
	p2 := it2.input
	var w1, w2 uint16

	for len(p1) >= 4 && len(p2) >= 4 {
		dword1 := *(*uint32)(unsafe.Pointer(&p1[0]))
		dword2 := *(*uint32)(unsafe.Pointer(&p2[0]))
		nonascii := (dword1 | dword2) & 0x80808080

		if nonascii == 0 {
			if dword1 != dword2 {
				// Use the weight string fast tables for quick weight comparisons;
				// see (*FastIterator900).NextWeightBlock64 for a description of
				// the table format
				table := it.fastTable
				if w1, w2 = uint16(table[p1[0]]), uint16(table[p2[0]]); w1 != w2 {
					goto mismatch
				}
				if w1, w2 = uint16(table[p1[1]]), uint16(table[p2[1]]); w1 != w2 {
					goto mismatch
				}
				if w1, w2 = uint16(table[p1[2]]), uint16(table[p2[2]]); w1 != w2 {
					goto mismatch
				}
				if w1, w2 = uint16(table[p1[3]]), uint16(table[p2[3]]); w1 != w2 {
					goto mismatch
				}
			}
			p1 = p1[4:]
			p2 = p2[4:]
			it.unicode--
			continue
		} else if bits.OnesCount32(nonascii) == 4 {
			it.unicode++
		}
		break
	}
	it.input = p1
	it2.input = p2
	return 0

mismatch:
	// If either of the weights was an ignorable, this is not really a mismatch;
	// return 0 so we fall back to the slow path and increase `it.unicode`. Although
	// these are _not_ unicode codepoints, if we find too many ignorable ASCII in
	// an iterator we want to skip further calls to FastForward32 because they
	// won't be able to optimize the comparisons at all
	if w1 == 0 || w2 == 0 {
		it.input = p1
		it2.input = p2
		it.unicode++
		return 0
	}
	// The weights must be byte-swapped before comparison because they're stored in big endian
	return int(bits.ReverseBytes16(w1)) - int(bits.ReverseBytes16(w2))
}

// NextWeightBlock64 takes a byte slice of 16 bytes and fills it with the next
// chunk of weights from this iterator. If the input slice is smaller than
// 16 bytes, the function will panic.
//
// The function returns the weights in Big Endian ordering: this is the
// same ordering that MySQL uses when generating weight strings, so the return
// of this function can be inserted directly into a weight string and the
// result will be compatible with MySQL. Likewise, the resulting slice
// can be compared byte-wise (bytes.Compare) to obtain a proper collation
// ordering against another string.
//
// Returns the number of bytes written to `dstbytes`. If 0, this iterator
// has been fully consumed.
//
// Implementation notes:
// This is a fast-path algorithm that can only work for UCA900 collations
// that do not have reorderings, contractions or any weight patches. The idea
// is detecting runs of 8 ASCII characters in a row, which are very frequent
// in most UTF8 code, particularly in English, and generating the weights
// for these 8 characters directly from an optimized table, instead of going
// through the whole Unicode Collation Algorithm. This is feasible because
// in UCA900, all characters in the ASCII range have either 0 or 1 weight
// triplets, so their weight can be calculated with a single lookup in a 128-entry
// table for each level (0, 1, 2).
func (it *FastIterator900) NextWeightBlock64(dstbytes []byte) int {
	// Ensure the destination slice has at least 16 bytes; this bounds check
	// removes all the other bound checks for the rest of the function.
	_ = dstbytes[15]

	// Unsafe cast the destination byte slice into a slice of uint16, so the
	// individual weights can be written directly to it.
	dst := (*[8]uint16)(unsafe.Pointer(&dstbytes[0]))
	p := it.input

	// The fast path works on 8-byte chunks from the original input.
	// If the underlying slow iterator is in the middle of processing the
	// weights for a codepoint, we cannot go through the fast path.
	if it.codepoint.ce == 0 && len(p) >= 8 {
		// Read 8 bytes from the input string. This would ideally be implemented
		// as a `binary.LittleEndian.Uint64` read, but the compiler doesn't seem to be
		// optimizing it properly, and it generates the individual byte shifts :(
		dword := *(*uint64)(unsafe.Pointer(&p[0]))

		// Check if all any of the  bytes in the next 8 bytes have their highest
		// bit set. If they're all clear, these are 8 ASCII bytes which can go through
		// the fast path
		if dword&0x8080808080808080 == 0 {
			// Load the fast table from the iterator. There are three fast tables hardcoded
			// for this implementation, for ASCII levels 0, 1 and 2. The slow iterator replaces
			// the table stored on `it.fastTable` every time we skip a level.
			// Note that the table has 256 entries although only the first 128 are used. We
			// want a 256 entry table because it forces the Go compiler to disable all bound
			// checks when accessing the table IF our index variable is a byte (since a byte
			// cannot be larger than 255).
			table := it.fastTable

			// All ASCII codepoints (0 >= cp >= 127) have either 0 or 1 weights to yield.
			// This is a problem for our fast path, because 0-weights must NOT appear in the
			// resulting weight string. The codepoints with 0 weights are, however, exceedingly
			// rare (they're mostly non-print codepoints), so based on this we can choose to
			// perform either an optimistic or pessimistic optimization, which is toggled at
			// compile time with this flag. For now, we're going with optimistic because it
			// provides better results in realistic benchmarks.
			const optimisticFastWrites = true

			if optimisticFastWrites {
				// For the optimistic optimization, we're going to assume that none of the
				// ASCII codepoints in this chunk have zero weights, and check only once at
				// the end of the chunk if that was the case. If we found a zero-weight (a
				// rare occurrence), we discard the whole chunk and fall back to the slow
				// iterator, which handles zero weights just fine.
				// To perform the check for zero weights efficiently, we've designed fast tables
				// where every entry is 32 bits, even though the actual weights are in the
				// bottom 16 bits. The upper 16 bits contain either 0x2, if this is a valid weight
				// or are zeroed out for 0-weights.
				// Because of this, we can lookup from the fast table and insert directly the lowest
				// 16 bits as the weight, and then we `and` the whole weight against a 32-bit mask
				// that starts as 0x20000. For any weights that are valid, that will leave the mask
				// with the same value, because only the higher bits will match, while any 0-weight
				// will fully clear the mask.
				// At the end of this block, we check if the mask has been cleared by any of the
				// writes, and if it has, we scrap this work (boo) and fall back to the slow iterator.
				var mask uint32 = 0x20000
				weight := table[p[0]]
				mask &= weight
				dst[0] = uint16(weight)

				weight = table[p[1]]
				mask &= weight
				dst[1] = uint16(weight)

				weight = table[p[2]]
				mask &= weight
				dst[2] = uint16(weight)

				weight = table[p[3]]
				mask &= weight
				dst[3] = uint16(weight)

				weight = table[p[4]]
				mask &= weight
				dst[4] = uint16(weight)

				weight = table[p[5]]
				mask &= weight
				dst[5] = uint16(weight)

				weight = table[p[6]]
				mask &= weight
				dst[6] = uint16(weight)

				weight = table[p[7]]
				mask &= weight
				dst[7] = uint16(weight)

				if mask != 0 {
					it.input = it.input[8:]
					return 16
				}
			} else {
				// For the pessimistic optimization, we're going to assume that any 8-byte chunk
				// can contain 0-weights (something rather rare in practice). We're writing
				// the lower 16 bits of the weight into the target buffer (just like in the optimistic
				// optimization), but then we're increasing our target buffer pointer by
				// the high 16 bits of the weight (weight >> 16). For valid weights, the high
				// bits will equal 0x2, which is exactly the offset we want to move our target
				// pointer so we can write the next weight afterwards, and for 0-weights, the
				// high bits will be 0x0, so the pointer will not advance and the next weight
				// we write will replace the 0-weight we've just written. This ensures that the
				// resulting byte output doesn't have any 0-weights in it, but it also causes
				// small stalls in the CPU because the writes are not necessarily linear and they
				// have an ordering dependency with the value we've loaded from the fast table.
				// Regardless of the stalls, this algorithm is obviously branch-less and very
				// efficient, it just happens that in real world scenarios, the optimistic
				// approach is even faster because 0-weights are very rare in practice.
				// For now, this algorithm is disabled.
				dstptr := (*uint16)(unsafe.Pointer(&dstbytes[0]))
				weight := table[p[0]]
				*dstptr = uint16(weight)
				dstptr = (*uint16)(unsafe.Add(unsafe.Pointer(dstptr), weight>>16))

				weight = table[p[1]]
				*dstptr = uint16(weight)
				dstptr = (*uint16)(unsafe.Add(unsafe.Pointer(dstptr), weight>>16))

				weight = table[p[2]]
				*dstptr = uint16(weight)
				dstptr = (*uint16)(unsafe.Add(unsafe.Pointer(dstptr), weight>>16))

				weight = table[p[3]]
				*dstptr = uint16(weight)
				dstptr = (*uint16)(unsafe.Add(unsafe.Pointer(dstptr), weight>>16))

				weight = table[p[4]]
				*dstptr = uint16(weight)
				dstptr = (*uint16)(unsafe.Add(unsafe.Pointer(dstptr), weight>>16))

				weight = table[p[5]]
				*dstptr = uint16(weight)
				dstptr = (*uint16)(unsafe.Add(unsafe.Pointer(dstptr), weight>>16))

				weight = table[p[6]]
				*dstptr = uint16(weight)
				dstptr = (*uint16)(unsafe.Add(unsafe.Pointer(dstptr), weight>>16))

				weight = table[p[7]]
				*dstptr = uint16(weight)
				dstptr = (*uint16)(unsafe.Add(unsafe.Pointer(dstptr), weight>>16))

				written := uintptr(unsafe.Pointer(dstptr)) - uintptr(unsafe.Pointer(&dstbytes[0]))
				if written != 0 {
					it.input = it.input[written>>1:]
					return int(written)
				}
			}
		}
	}

	// Slow path: just loop up to 8 times to fill the buffer and bail
	// early if we exhaust the iterator.
	for i := 0; i < 8; i++ {
		w, ok := it.Next()
		if !ok {
			return i * 2
		}
		dst[i] = bits.ReverseBytes16(w)
	}
	return 16
}

func (it *FastIterator900) resetForNextLevel() {
	it.input = it.original
	switch it.level {
	case 1:
		it.fastTable = &fastweightTable_uca900_page000L1
	case 2:
		it.fastTable = &fastweightTable_uca900_page000L2
	}
}

func (it *FastIterator900) Next() (uint16, bool) {
	for {
		if w, ok := it.codepoint.next(); ok {
			return w, true
		}

		cp, width := utf8.DecodeRune(it.input)
		if cp == utf8.RuneError && width < 3 {
			it.level++
			if it.level < it.maxLevel {
				it.resetForNextLevel()
				return 0, true
			}
			return 0, false
		}

		it.input = it.input[width:]
		it.codepoint.init(&it.iterator900, cp)
	}
}
