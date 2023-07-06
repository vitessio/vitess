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

import (
	"fmt"

	"golang.org/x/exp/slices"
)

// HIGH_VALUE > all valid values. 110000 for codepoints
const unicodeSetHigh = 0x0110000

// LOW <= all valid values. ZERO for codepoints
const unicodeSetLow = 0x000000

const (
	/**
	 * Minimum value that can be stored in a UnicodeSet.
	 * @stable ICU 2.4
	 */
	MinValue = 0

	/**
	 * Maximum value that can be stored in a UnicodeSet.
	 * @stable ICU 2.4
	 */
	MaxValue = 0x10ffff
)

type UnicodeSet struct {
	list   []rune
	buffer []rune
	frozen *frozen
}

func New() *UnicodeSet {
	buf := make([]rune, 1, 25)
	buf[0] = unicodeSetHigh
	return &UnicodeSet{list: buf}
}

func FromRunes(list []rune) *UnicodeSet {
	return &UnicodeSet{list: list}
}

func (u *UnicodeSet) ensureBufferCapacity(c int) {
	if cap(u.buffer) < c {
		u.buffer = make([]rune, c)
		return
	}
	u.buffer = u.buffer[:cap(u.buffer)]
}

func (u *UnicodeSet) addbuffer(other []rune, polarity int8) {
	if u.frozen != nil {
		panic("UnicodeSet is frozen")
	}
	u.ensureBufferCapacity(len(u.list) + len(other))

	i := 1
	j := 1
	k := 0

	a := u.list[0]
	b := other[0]

	for {
		switch polarity {
		case 0:
			if a < b {
				if k > 0 && a <= u.buffer[k-1] {
					k--
					a = max(u.list[i], u.buffer[k])
				} else {
					u.buffer[k] = a
					k++
					a = u.list[i]
				}
				i++
				polarity ^= 1
			} else if b < a {
				if k > 0 && b <= u.buffer[k-1] {
					k--
					b = max(other[j], u.buffer[k])
				} else {
					u.buffer[k] = b
					k++
					b = other[j]
				}
				j++
				polarity ^= 2
			} else {
				if a == unicodeSetHigh {
					goto loopEnd
				}
				if k > 0 && a <= u.buffer[k-1] {
					k--
					a = max(u.list[i], u.buffer[k])
				} else {
					u.buffer[k] = a
					k++
					a = u.list[i]
				}
				i++
				polarity ^= 1
				b = other[j]
				j++
				polarity ^= 2
			}
		case 3:
			if b <= a {
				if a == unicodeSetHigh {
					goto loopEnd
				}
				u.buffer[k] = a
				k++
			} else {
				if b == unicodeSetHigh {
					goto loopEnd
				}
				u.buffer[k] = b
				k++
			}
			a = u.list[i]
			i++
			polarity ^= 1
			b = other[j]
			j++
			polarity ^= 2
		case 1:
			if a < b {
				u.buffer[k] = a
				k++
				a = u.list[i]
				i++
				polarity ^= 1
			} else if b < a {
				b = other[j]
				j++
				polarity ^= 2
			} else {
				if a == unicodeSetHigh {
					goto loopEnd
				}
				a = u.list[i]
				i++
				polarity ^= 1
				b = other[j]
				j++
				polarity ^= 2
			}
		case 2:
			if b < a {
				u.buffer[k] = b
				k++
				b = other[j]
				j++
				polarity ^= 2
			} else if a < b {
				a = u.list[i]
				i++
				polarity ^= 1
			} else {
				if a == unicodeSetHigh {
					goto loopEnd
				}
				a = u.list[i]
				i++
				polarity ^= 1
				b = other[j]
				j++
				polarity ^= 2
			}
		}
	}

loopEnd:
	u.buffer[k] = unicodeSetHigh
	k++

	u.list, u.buffer = u.buffer[:k], u.list
}

func max(a, b rune) rune {
	if a > b {
		return a
	}
	return b
}

func pinCodePoint(c *rune) rune {
	if *c < unicodeSetLow {
		*c = unicodeSetLow
	} else if *c > (unicodeSetHigh - 1) {
		*c = unicodeSetHigh - 1
	}
	return *c
}

func (u *UnicodeSet) AddRune(c rune) {
	if u.frozen != nil {
		panic("UnicodeSet is frozen")
	}

	// find smallest i such that c < list[i]
	// if odd, then it is IN the set
	// if even, then it is OUT of the set
	i := u.findCodePoint(pinCodePoint(&c))

	// already in set?
	if (i & 1) != 0 {
		return
	}

	// HIGH is 0x110000
	// assert(list[len-1] == HIGH);

	// empty = [HIGH]
	// [start_0, limit_0, start_1, limit_1, HIGH]

	// [..., start_k-1, limit_k-1, start_k, limit_k, ..., HIGH]
	//                             ^
	//                             list[i]

	// i == 0 means c is before the first range
	if c == u.list[i]-1 {
		// c is before start of next range
		u.list[i] = c
		// if we touched the HIGH mark, then add a new one
		if c == (unicodeSetHigh - 1) {
			u.list = append(u.list, unicodeSetHigh)
		}
		if i > 0 && c == u.list[i-1] {
			// collapse adjacent ranges

			// [..., start_k-1, c, c, limit_k, ..., HIGH]
			//                     ^
			//                     list[i]
			for k := i - 1; k < len(u.list)-2; k++ {
				u.list[k] = u.list[k+2]
			}
			u.list = u.list[:len(u.list)-2]
		}
	} else if i > 0 && c == u.list[i-1] {
		// c is after end of prior range
		u.list[i-1]++
		// no need to check for collapse here
	} else {
		// At this point we know the new char is not adjacent to
		// any existing ranges, and it is not 10FFFF.

		// [..., start_k-1, limit_k-1, start_k, limit_k, ..., HIGH]
		//                             ^
		//                             list[i]

		// [..., start_k-1, limit_k-1, c, c+1, start_k, limit_k, ..., HIGH]
		//                             ^
		//                             list[i]
		u.list = slices.Insert(u.list, i, c, c+1)
	}
}

func (u *UnicodeSet) AddRuneRange(start, end rune) {
	if pinCodePoint(&start) < pinCodePoint(&end) {
		limit := end + 1
		// Fast path for adding a new range after the last one.
		// Odd list length: [..., lastStart, lastLimit, HIGH]
		if (len(u.list) & 1) != 0 {
			// If the list is empty, set lastLimit low enough to not be adjacent to 0.
			var lastLimit rune
			if len(u.list) == 1 {
				lastLimit = -2
			} else {
				lastLimit = u.list[len(u.list)-2]
			}
			if lastLimit <= start {
				if lastLimit == start {
					// Extend the last range.
					u.list[len(u.list)-2] = limit
					if limit == unicodeSetHigh {
						u.list = u.list[:len(u.list)-1]
					}
				} else {
					u.list[len(u.list)-1] = start
					if limit < unicodeSetHigh {
						u.list = append(u.list, limit)
						u.list = append(u.list, unicodeSetHigh)
					} else { // limit == UNICODESET_HIGH
						u.list = append(u.list, unicodeSetHigh)
					}
				}
				return
			}
		}
		// This is slow. Could be much faster using findCodePoint(start)
		// and modifying the list, dealing with adjacent & overlapping ranges.
		addRange := [3]rune{start, limit, unicodeSetHigh}
		u.addbuffer(addRange[:], 0)
	} else if start == end {
		u.AddRune(start)
	}
}

func (u *UnicodeSet) AddAll(u2 *UnicodeSet) {
	if len(u2.list) > 0 {
		u.addbuffer(u2.list, 0)
	}
}

func (u *UnicodeSet) Complement() {
	if u.frozen != nil {
		panic("UnicodeSet is frozen")
	}
	if u.list[0] == unicodeSetLow {
		copy(u.list, u.list[1:])
		u.list = u.list[:len(u.list)-1]
	} else {
		u.list = slices.Insert(u.list, 0, unicodeSetLow)
	}
}

func (u *UnicodeSet) RemoveRuneRange(start, end rune) {
	if pinCodePoint(&start) < pinCodePoint(&end) {
		r := [3]rune{start, end + 1, unicodeSetHigh}
		u.retain(r[:], 2)
	}
}

func (u *UnicodeSet) RemoveAll(c *UnicodeSet) {
	u.retain(c.list, 2)
}

func (u *UnicodeSet) RetainAll(c *UnicodeSet) {
	u.retain(c.list, 0)
}

func (u *UnicodeSet) retain(other []rune, polarity int8) {
	if u.frozen != nil {
		panic("UnicodeSet is frozen")
	}

	u.ensureBufferCapacity(len(u.list) + len(other))

	i := 1
	j := 1
	k := 0

	a := u.list[0]
	b := other[0]

	// change from xor is that we have to check overlapping pairs
	// polarity bit 1 means a is second, bit 2 means b is.
	for {
		switch polarity {
		case 0: // both first; drop the smaller
			if a < b { // drop a
				a = u.list[i]
				i++
				polarity ^= 1
			} else if b < a { // drop b
				b = other[j]
				j++
				polarity ^= 2
			} else { // a == b, take one, drop other
				if a == unicodeSetHigh {
					goto loop_end
				}
				u.buffer[k] = a
				k++
				a = u.list[i]
				i++
				polarity ^= 1
				b = other[j]
				j++
				polarity ^= 2
			}
		case 3: // both second; take lower if unequal
			if a < b { // take a
				u.buffer[k] = a
				k++
				a = u.list[i]
				i++
				polarity ^= 1
			} else if b < a { // take b
				u.buffer[k] = b
				k++
				b = other[j]
				j++
				polarity ^= 2
			} else { // a == b, take one, drop other
				if a == unicodeSetHigh {
					goto loop_end
				}
				u.buffer[k] = a
				k++
				a = u.list[i]
				i++
				polarity ^= 1
				b = other[j]
				j++
				polarity ^= 2
			}
		case 1: // a second, b first;
			if a < b { // NO OVERLAP, drop a
				a = u.list[i]
				i++
				polarity ^= 1
			} else if b < a { // OVERLAP, take b
				u.buffer[k] = b
				k++
				b = other[j]
				j++
				polarity ^= 2
			} else { // a == b, drop both!
				if a == unicodeSetHigh {
					goto loop_end
				}
				a = u.list[i]
				i++
				polarity ^= 1
				b = other[j]
				j++
				polarity ^= 2
			}
		case 2: // a first, b second; if a < b, overlap
			if b < a { // no overlap, drop b
				b = other[j]
				j++
				polarity ^= 2
			} else if a < b { // OVERLAP, take a
				u.buffer[k] = a
				k++
				a = u.list[i]
				i++
				polarity ^= 1
			} else { // a == b, drop both!
				if a == unicodeSetHigh {
					goto loop_end
				}
				a = u.list[i]
				i++
				polarity ^= 1
				b = other[j]
				j++
				polarity ^= 2
			}
		}
	}

loop_end:
	u.buffer[k] = unicodeSetHigh // terminate
	k++
	u.list, u.buffer = u.buffer[:k], u.list
}

func (u *UnicodeSet) Clear() {
	if u.frozen != nil {
		panic("UnicodeSet is frozen")
	}
	u.list = u.list[:1]
	u.list[0] = unicodeSetHigh
}

func (u *UnicodeSet) Len() (n int) {
	count := u.RangeCount()
	for i := 0; i < count; i++ {
		n += int(u.RangeEnd(i)) - int(u.RangeStart(i)) + 1
	}
	return
}

func (u *UnicodeSet) RangeCount() int {
	return len(u.list) / 2
}

func (u *UnicodeSet) RangeStart(idx int) rune {
	return u.list[idx*2]
}

func (u *UnicodeSet) RangeEnd(idx int) rune {
	return u.list[idx*2+1] - 1
}

func (u *UnicodeSet) RuneAt(idx int) rune {
	if idx >= 0 {
		// len2 is the largest even integer <= len, that is, it is len
		// for even values and len-1 for odd values.  With odd values
		// the last entry is UNICODESET_HIGH.
		len2 := len(u.list)
		if (len2 & 0x1) != 0 {
			len2--
		}

		var i int
		for i < len2 {
			start := u.list[i]
			count := int(u.list[i+1] - start)
			i += 2
			if idx < count {
				return start + rune(idx)
			}
			idx -= count
		}
	}
	return -1
}

func (u *UnicodeSet) ContainsRune(c rune) bool {
	if f := u.frozen; f != nil {
		if c < 0 {
			return false
		} else if c <= 0xff {
			return f.latin1Contains[c] != 0
		} else if c <= 0x7ff {
			return (f.table7FF[c&0x3f] & (uint32(1) << (c >> 6))) != 0
		} else if c < 0xd800 || (c >= 0xe000 && c <= 0xffff) {
			lead := c >> 12
			twoBits := (f.bmpBlockBits[(c>>6)&0x3f] >> lead) & 0x10001
			if twoBits <= 1 {
				// All 64 code points with the same bits 15..6
				// are either in the set or not.
				return twoBits != 0
			}
			// Look up the code point in its 4k block of code points.
			return f.containsSlow(u.list, c, f.list4kStarts[lead], f.list4kStarts[lead+1])
		} else if c <= 0x10ffff {
			// surrogate or supplementary code point
			return f.containsSlow(u.list, c, f.list4kStarts[0xd], f.list4kStarts[0x11])
		}
		// Out-of-range code points get FALSE, consistent with long-standing
		// behavior of UnicodeSet::contains(c).
		return false
	}

	if c >= unicodeSetHigh {
		return false
	}
	i := u.findCodePoint(c)
	return (i & 1) != 0
}

func (u *UnicodeSet) ContainsRuneRange(from, to rune) bool {
	i := u.findCodePoint(from)
	return (i&1) != 0 && to < u.list[i]
}

func (u *UnicodeSet) findCodePoint(c rune) int {
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
	if c < u.list[0] {
		return 0
	}

	// High runner test.  c is often after the last range, so an
	// initial check for this condition pays off.
	lo := 0
	hi := len(u.list) - 1
	if lo >= hi || c >= u.list[hi-1] {
		return hi
	}

	// invariant: c >= list[lo]
	// invariant: c < list[hi]
	for {
		i := (lo + hi) >> 1
		if i == lo {
			break // Found!
		} else if c < u.list[i] {
			hi = i
		} else {
			lo = i
		}
	}
	return hi
}

func (u *UnicodeSet) AddString(chars string) {
	for _, c := range chars {
		u.AddRune(c)
	}
}

type Filter func(ch rune) bool

func (u *UnicodeSet) ApplyFilter(inclusions *UnicodeSet, filter Filter) {
	// Logically, walk through all Unicode characters, noting the start
	// and end of each range for which filter.contain(c) is
	// true.  Add each range to a set.
	//
	// To improve performance, use an inclusions set which
	// encodes information about character ranges that are known
	// to have identical properties.
	// inclusions contains the first characters of
	// same-value ranges for the given property.

	u.Clear()

	startHasProperty := rune(-1)
	limitRange := inclusions.RangeCount()

	for j := 0; j < limitRange; j++ {
		// get current range
		start := inclusions.RangeStart(j)
		end := inclusions.RangeEnd(j)

		// for all the code points in the range, process
		for ch := start; ch <= end; ch++ {
			// only add to this UnicodeSet on inflection points --
			// where the hasProperty value changes to false
			if filter(ch) {
				if startHasProperty < 0 {
					startHasProperty = ch
				}
			} else if startHasProperty >= 0 {
				u.AddRuneRange(startHasProperty, ch-1)
				startHasProperty = -1
			}
		}
	}
	if startHasProperty >= 0 {
		u.AddRuneRange(startHasProperty, 0x10FFFF)
	}
}

func (u *UnicodeSet) Clone() *UnicodeSet {
	return &UnicodeSet{list: slices.Clone(u.list)}
}

func (u *UnicodeSet) IsEmpty() bool {
	return len(u.list) == 1
}

func (u *UnicodeSet) CopyFrom(set *UnicodeSet) {
	if u.frozen != nil {
		panic("UnicodeSet is frozen")
	}
	u.list = slices.Clone(set.list)
}

func (u *UnicodeSet) Equals(other *UnicodeSet) bool {
	return slices.Equal(u.list, other.list)
}

func (u *UnicodeSet) Freeze() *UnicodeSet {
	u.frozen = freeze(u.list)
	return u
}

func (u *UnicodeSet) FreezeCheck_() error {
	if u == nil {
		return nil
	}
	if u.frozen == nil {
		return fmt.Errorf("UnicodeSet is not frozen")
	}
	for r := rune(0); r <= 0x10ffff; r++ {
		want := (u.findCodePoint(r) & 1) != 0
		got := u.ContainsRune(r)
		if want != got {
			return fmt.Errorf("rune '%c' (U+%04X) did not freeze", r, r)
		}
	}
	return nil
}
