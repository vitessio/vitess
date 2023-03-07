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

package collations

import (
	"bytes"
	"math"
	"math/bits"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

type Collation_unicode_general_ci struct {
	id      ID
	name    string
	unicase *UnicaseInfo
	charset charset.Charset
}

func (c *Collation_unicode_general_ci) ID() ID {
	return c.id
}

func (c *Collation_unicode_general_ci) Name() string {
	return c.name
}

func (c *Collation_unicode_general_ci) Charset() charset.Charset {
	return c.charset
}

func (c *Collation_unicode_general_ci) IsBinary() bool {
	return false
}

func (c *Collation_unicode_general_ci) Collate(left, right []byte, isPrefix bool) int {
	unicaseInfo := c.unicase
	cs := c.charset

	for len(left) > 0 && len(right) > 0 {
		l, lWidth := cs.DecodeRune(left)
		r, rWidth := cs.DecodeRune(right)

		if (l == charset.RuneError && lWidth < 3) || (r == charset.RuneError && rWidth < 3) {
			return bytes.Compare(left, right)
		}

		lRune := unicaseInfo.unicodeSort(l)
		rRune := unicaseInfo.unicodeSort(r)

		if lRune > rRune {
			return 1
		} else if lRune < rRune {
			return -1
		}

		left = left[lWidth:]
		right = right[rWidth:]
	}
	if isPrefix {
		return len(right)
	}
	return len(left) - len(right)
}

func (c *Collation_unicode_general_ci) WeightString(dst, src []byte, numCodepoints int) []byte {
	unicaseInfo := c.unicase
	cs := c.charset

	if numCodepoints == 0 || numCodepoints == PadToMax {
		for {
			r, width := cs.DecodeRune(src)
			if r == charset.RuneError && width < 3 {
				break
			}

			src = src[width:]
			sorted := unicaseInfo.unicodeSort(r)
			dst = append(dst, byte(sorted>>8), byte(sorted))
		}

		if numCodepoints == PadToMax {
			for len(dst)+1 < cap(dst) {
				dst = append(dst, 0x00, 0x20)
			}
			if len(dst) < cap(dst) {
				dst = append(dst, 0x00)
			}
		}
	} else {
		for numCodepoints > 0 {
			r, width := cs.DecodeRune(src)
			if r == charset.RuneError && width < 3 {
				break
			}

			src = src[width:]
			sorted := unicaseInfo.unicodeSort(r)
			dst = append(dst, byte(sorted>>8), byte(sorted))
			numCodepoints--
		}
		for numCodepoints > 0 {
			dst = append(dst, 0x00, 0x20)
			numCodepoints--
		}
	}

	return dst
}

func (c *Collation_unicode_general_ci) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	unicaseInfo := c.unicase
	cs := c.charset

	hasher.Write64(uint64(c.id))
	var left = numCodepoints
	if left == 0 {
		left = math.MaxInt32
	}

	for left > 0 {
		r, width := cs.DecodeRune(src)
		if r == charset.RuneError && width < 3 {
			break
		}
		src = src[width:]
		hasher.Write16(bits.ReverseBytes16(uint16(unicaseInfo.unicodeSort(r))))
		left--
	}

	if numCodepoints > 0 {
		for left > 0 {
			hasher.Write16(bits.ReverseBytes16(0x0020))
			left--
		}
	}
}

func (c *Collation_unicode_general_ci) WeightStringLen(numBytes int) int {
	return ((numBytes + 3) / 4) * 2
}

func (c *Collation_unicode_general_ci) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	var sort = c.unicase.unicodeSort
	var equals = func(a, b rune) bool {
		return sort(a) == sort(b)
	}
	return newUnicodeWildcardMatcher(c.charset, equals, c.Collate, pat, matchOne, matchMany, escape)
}

type Collation_unicode_bin struct {
	id      ID
	name    string
	charset charset.Charset
}

func (c *Collation_unicode_bin) ID() ID {
	return c.id
}

func (c *Collation_unicode_bin) Name() string {
	return c.name
}

func (c *Collation_unicode_bin) Charset() charset.Charset {
	return c.charset
}

func (c *Collation_unicode_bin) IsBinary() bool {
	return true
}

func (c *Collation_unicode_bin) Collate(left, right []byte, isPrefix bool) int {
	return collationBinary(left, right, isPrefix)
}

func (c *Collation_unicode_bin) WeightString(dst, src []byte, numCodepoints int) []byte {
	if c.charset.SupportsSupplementaryChars() {
		return c.weightStringUnicode(dst, src, numCodepoints)
	}
	return c.weightStringBMP(dst, src, numCodepoints)
}

func (c *Collation_unicode_bin) weightStringBMP(dst, src []byte, numCodepoints int) []byte {
	cs := c.charset
	if numCodepoints == 0 || numCodepoints == PadToMax {
		for {
			r, width := cs.DecodeRune(src)
			if r == charset.RuneError && width < 3 {
				break
			}
			src = src[width:]
			dst = append(dst, byte(r>>8), byte(r))
		}

		if numCodepoints == PadToMax {
			for len(dst)+1 < cap(dst) {
				dst = append(dst, 0x00, 0x20)
			}
			if len(dst) < cap(dst) {
				dst = append(dst, 0x00)
			}
		}
	} else {
		for numCodepoints > 0 {
			r, width := cs.DecodeRune(src)
			if r == charset.RuneError && width < 3 {
				break
			}
			src = src[width:]
			dst = append(dst, byte(r>>8), byte(r))
			numCodepoints--
		}
		for numCodepoints > 0 {
			dst = append(dst, 0x00, 0x20)
			numCodepoints--
		}
	}

	return dst
}

func (c *Collation_unicode_bin) weightStringUnicode(dst, src []byte, numCodepoints int) []byte {
	cs := c.charset
	if numCodepoints == 0 || numCodepoints == PadToMax {
		for {
			r, width := cs.DecodeRune(src)
			if r == charset.RuneError && width < 3 {
				break
			}

			src = src[width:]
			dst = append(dst, byte((r>>16)&0xFF), byte((r>>8)&0xFF), byte(r&0xFF))
		}

		if numCodepoints == PadToMax {
			for len(dst)+2 < cap(dst) {
				dst = append(dst, 0x00, 0x00, 0x20)
			}
			switch cap(dst) - len(dst) {
			case 0:
			case 1:
				dst = append(dst, 0x00)
			case 2:
				dst = append(dst, 0x00, 0x00)
			default:
				panic("unreachable")
			}
		}
	} else {
		for numCodepoints > 0 {
			r, width := cs.DecodeRune(src)
			if r == charset.RuneError && width < 3 {
				break
			}

			src = src[width:]
			dst = append(dst, byte((r>>16)&0xFF), byte((r>>8)&0xFF), byte(r&0xFF))
			numCodepoints--
		}
		for numCodepoints > 0 {
			dst = append(dst, 0x00, 0x00, 0x20)
			numCodepoints--
		}
	}

	return dst
}

func (c *Collation_unicode_bin) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	if c.charset.SupportsSupplementaryChars() {
		c.hashUnicode(hasher, src, numCodepoints)
	} else {
		c.hashBMP(hasher, src, numCodepoints)
	}
}

func (c *Collation_unicode_bin) hashUnicode(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	cs := c.charset

	hasher.Write64(uint64(c.id))
	var left = numCodepoints
	if left == 0 {
		left = math.MaxInt32
	}
	for left > 0 {
		r, width := cs.DecodeRune(src)
		if r == charset.RuneError && width < 3 {
			break
		}
		src = src[width:]
		hasher.Write32(bits.ReverseBytes32(uint32(r)))
		left--
	}
	if numCodepoints > 0 {
		for left > 0 {
			hasher.Write32(bits.ReverseBytes32(0x20))
			left--
		}
	}
}

func (c *Collation_unicode_bin) hashBMP(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	cs := c.charset

	hasher.Write64(uint64(c.id))
	var left = numCodepoints
	if left == 0 {
		left = math.MaxInt32
	}
	for left > 0 {
		r, width := cs.DecodeRune(src)
		if r == charset.RuneError && width < 3 {
			break
		}
		src = src[width:]
		hasher.Write16(bits.ReverseBytes16(uint16(r)))
		left--
	}
	if numCodepoints > 0 {
		for left > 0 {
			hasher.Write16(bits.ReverseBytes16(0x20))
			left--
		}
	}
}

func (c *Collation_unicode_bin) WeightStringLen(numBytes int) int {
	return ((numBytes + 3) / 4) * 3
}

func (c *Collation_unicode_bin) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	equals := func(a, b rune) bool {
		return a == b
	}
	return newUnicodeWildcardMatcher(c.charset, equals, c.Collate, pat, matchOne, matchMany, escape)
}

func collationBinary(left, right []byte, rightPrefix bool) int {
	minLen := minInt(len(left), len(right))
	if diff := bytes.Compare(left[:minLen], right[:minLen]); diff != 0 {
		return diff
	}
	if rightPrefix {
		left = left[:minLen]
	}
	return len(left) - len(right)
}
