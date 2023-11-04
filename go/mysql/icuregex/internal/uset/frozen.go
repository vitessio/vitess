/*
© 2016 and later: Unicode, Inc. and others.
Copyright (C) 2004-2015, International Business Machines Corporation and others.
Copyright 2023 The Vitess Authors.

This file contains code derived from the Unicode Project's ICU library.
License & terms of use for the original code: http://www.unicode.org/copyright.html

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

package uset

type frozen struct {
	// One byte 0 or 1 per Latin-1 character.
	latin1Contains [0x100]byte

	// true if contains(U+FFFD)
	containsFFFD bool

	/*
	 * One bit per code point from U+0000..U+07FF.
	 * The bits are organized vertically; consecutive code points
	 * correspond to the same bit positions in consecutive table words.
	 * With code point parts
	 *   lead=c{10..6}
	 *   trail=c{5..0}
	 * it is set.contains(c)==(table7FF[trail] bit lead)
	 *
	 * Bits for 0..7F (non-shortest forms) are set to the result of contains(FFFD)
	 * for faster validity checking at runtime.
	 */
	table7FF [64]uint32

	/*
	 * One bit per 64 BMP code points.
	 * The bits are organized vertically; consecutive 64-code point blocks
	 * correspond to the same bit position in consecutive table words.
	 * With code point parts
	 *   lead=c{15..12}
	 *   t1=c{11..6}
	 * test bits (lead+16) and lead in bmpBlockBits[t1].
	 * If the upper bit is 0, then the lower bit indicates if contains(c)
	 * for all code points in the 64-block.
	 * If the upper bit is 1, then the block is mixed and set.contains(c)
	 * must be called.
	 *
	 * Bits for 0..7FF (non-shortest forms) and D800..DFFF are set to
	 * the result of contains(FFFD) for faster validity checking at runtime.
	 */
	bmpBlockBits [64]uint32

	/*
	 * Inversion list indexes for restricted binary searches in
	 * findCodePoint(), from
	 * findCodePoint(U+0800, U+1000, U+2000, .., U+F000, U+10000).
	 * U+0800 is the first 3-byte-UTF-8 code point. Code points below U+0800 are
	 * always looked up in the bit tables.
	 * The last pair of indexes is for finding supplementary code points.
	 */
	list4kStarts [18]int32
}

func freeze(list []rune) *frozen {
	f := &frozen{}

	listEnd := int32(len(list) - 1)

	f.list4kStarts[0] = f.findCodePoint(list, 0x800, 0, listEnd)
	for i := 1; i <= 0x10; i++ {
		f.list4kStarts[i] = f.findCodePoint(list, rune(i)<<12, f.list4kStarts[i-1], listEnd)
	}
	f.list4kStarts[0x11] = listEnd
	f.containsFFFD = f.containsSlow(list, 0xfffd, f.list4kStarts[0xf], f.list4kStarts[0x10])

	f.initBits(list)
	f.overrideIllegal()

	return f
}

func (f *frozen) containsSlow(list []rune, c rune, lo, hi int32) bool {
	return (f.findCodePoint(list, c, lo, hi) & 1) != 0
}

func (f *frozen) findCodePoint(list []rune, c rune, lo, hi int32) int32 {
	/* Examples:
	                                   findCodePoint(c)
	   set              list[]         c=0 1 3 4 7 8
	   ===              ==============   ===========
	   []               [110000]         0 0 0 0 0 0
	   [\u0000-\u0003]  [0, 4, 110000]   1 1 1 2 2 2
	   [\u0004-\u0007]  [4, 8, 110000]   0 0 0 1 1 2
	   [:Any:]          [0, 110000]      1 1 1 1 1 1
	*/

	// Return the smallest i such that c < list[i].  Assume
	// list[len - 1] == HIGH and that c is legal (0..HIGH-1).
	if c < list[lo] {
		return lo
	}
	// High runner test.  c is often after the last range, so an
	// initial check for this condition pays off.
	if lo >= hi || c >= list[hi-1] {
		return hi
	}
	// invariant: c >= list[lo]
	// invariant: c < list[hi]
	for {
		i := (lo + hi) >> 1
		if i == lo {
			break // Found!
		} else if c < list[i] {
			hi = i
		} else {
			lo = i
		}
	}
	return hi
}

func (f *frozen) set32x64bits(table *[64]uint32, start, limit int32) {
	lead := start >> 6    // Named for UTF-8 2-byte lead byte with upper 5 bits.
	trail := start & 0x3f // Named for UTF-8 2-byte trail byte with lower 6 bits.

	// Set one bit indicating an all-one block.
	bits := uint32(1) << lead
	if (start + 1) == limit { // Single-character shortcut.
		table[trail] |= bits
		return
	}

	limitLead := limit >> 6
	limitTrail := limit & 0x3f

	if lead == limitLead {
		// Partial vertical bit column.
		for trail < limitTrail {
			table[trail] |= bits
			trail++
		}
	} else {
		// Partial vertical bit column,
		// followed by a bit rectangle,
		// followed by another partial vertical bit column.
		if trail > 0 {
			for {
				table[trail] |= bits
				trail++
				if trail >= 64 {
					break
				}
			}
			lead++
		}
		if lead < limitLead {
			bits = ^((uint32(1) << lead) - 1)
			if limitLead < 0x20 {
				bits &= (uint32(1) << limitLead) - 1
			}
			for trail = 0; trail < 64; trail++ {
				table[trail] |= bits
			}
		}
		// limit<=0x800. If limit==0x800 then limitLead=32 and limitTrail=0.
		// In that case, bits=1<<limitLead is undefined but the bits value
		// is not used because trail<limitTrail is already false.
		if limitLead == 0x20 {
			bits = uint32(1) << (limitLead - 1)
		} else {
			bits = uint32(1) << limitLead
		}

		for trail = 0; trail < limitTrail; trail++ {
			table[trail] |= bits
		}
	}
}

func (f *frozen) overrideIllegal() {
	var bits, mask uint32
	var i int

	if f.containsFFFD {
		bits = 3 // Lead bytes 0xC0 and 0xC1.
		for i = 0; i < 64; i++ {
			f.table7FF[i] |= bits
		}

		bits = 1                 // Lead byte 0xE0.
		for i = 0; i < 32; i++ { // First half of 4k block.
			f.bmpBlockBits[i] |= bits
		}

		mask = ^(uint32(0x10001) << 0xd) // Lead byte 0xED.
		bits = 1 << 0xd
		for i = 32; i < 64; i++ { // Second half of 4k block.
			f.bmpBlockBits[i] = (f.bmpBlockBits[i] & mask) | bits
		}
	} else {
		mask = ^(uint32(0x10001) << 0xd) // Lead byte 0xED.
		for i = 32; i < 64; i++ {        // Second half of 4k block.
			f.bmpBlockBits[i] &= mask
		}
	}
}

func (f *frozen) initBits(list []rune) {
	var start, limit rune
	var listIndex int

	// Set latin1Contains[].
	for {
		start = list[listIndex]
		listIndex++

		if listIndex < len(list) {
			limit = list[listIndex]
			listIndex++
		} else {
			limit = 0x110000
		}
		if start >= 0x100 {
			break
		}
		for {
			f.latin1Contains[start] = 1
			start++
			if start >= limit || start >= 0x100 {
				break
			}
		}
		if limit > 0x100 {
			break
		}
	}

	// Find the first range overlapping with (or after) 80..FF again,
	// to include them in table7FF as well.
	listIndex = 0
	for {
		start = list[listIndex]
		listIndex++
		if listIndex < len(list) {
			limit = list[listIndex]
			listIndex++
		} else {
			limit = 0x110000
		}
		if limit > 0x80 {
			if start < 0x80 {
				start = 0x80
			}
			break
		}
	}

	// Set table7FF[].
	for start < 0x800 {
		var end rune
		if limit <= 0x800 {
			end = limit
		} else {
			end = 0x800
		}
		f.set32x64bits(&f.table7FF, start, end)
		if limit > 0x800 {
			start = 0x800
			break
		}

		start = list[listIndex]
		listIndex++
		if listIndex < len(list) {
			limit = list[listIndex]
			listIndex++
		} else {
			limit = 0x110000
		}
	}

	// Set bmpBlockBits[].
	minStart := rune(0x800)
	for start < 0x10000 {
		if limit > 0x10000 {
			limit = 0x10000
		}

		if start < minStart {
			start = minStart
		}
		if start < limit { // Else: Another range entirely in a known mixed-value block.
			if (start & 0x3f) != 0 {
				// Mixed-value block of 64 code points.
				start >>= 6
				f.bmpBlockBits[start&0x3f] |= 0x10001 << (start >> 6)
				start = (start + 1) << 6 // Round up to the next block boundary.
				minStart = start         // Ignore further ranges in this block.
			}
			if start < limit {
				if start < (limit &^ 0x3f) {
					// Multiple all-ones blocks of 64 code points each.
					f.set32x64bits(&f.bmpBlockBits, start>>6, limit>>6)
				}

				if (limit & 0x3f) != 0 {
					// Mixed-value block of 64 code points.
					limit >>= 6
					f.bmpBlockBits[limit&0x3f] |= 0x10001 << (limit >> 6)
					limit = (limit + 1) << 6 // Round up to the next block boundary.
					minStart = limit         // Ignore further ranges in this block.
				}
			}
		}

		if limit == 0x10000 {
			break
		}

		start = list[listIndex]
		listIndex++
		if listIndex < len(list) {
			limit = list[listIndex]
			listIndex++
		} else {
			limit = 0x110000
		}
	}
}
