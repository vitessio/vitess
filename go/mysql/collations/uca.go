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
	"math/bits"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/internal/uca"
	"vitess.io/vitess/go/vt/vthash"
)

type Collation_utf8mb4_uca_0900 struct {
	name string
	id   ID
	uca  *uca.Collation900
}

func (c *Collation_utf8mb4_uca_0900) Name() string {
	return c.name
}

func (c *Collation_utf8mb4_uca_0900) ID() ID {
	return c.id
}

func (c *Collation_utf8mb4_uca_0900) Charset() charset.Charset {
	return charset.Charset_utf8mb4{}
}

func (c *Collation_utf8mb4_uca_0900) IsBinary() bool {
	return false
}

func (c *Collation_utf8mb4_uca_0900) Collate(left, right []byte, rightIsPrefix bool) int {
	var (
		l, r            uint16
		lok, rok        bool
		level           int
		levelsToCompare = c.uca.MaxLevel()
		itleft          = c.uca.Iterator(left)
		itright         = c.uca.Iterator(right)

		fastleft, _  = itleft.(*uca.FastIterator900)
		fastright, _ = itright.(*uca.FastIterator900)
	)

	defer itleft.Done()
	defer itright.Done()

nextLevel:
	if fastleft != nil {
		for {
			if cmp := fastleft.FastForward32(fastright); cmp != 0 {
				return cmp
			}

			l, lok = fastleft.Next()
			r, rok = fastright.Next()

			if l != r || !lok || !rok {
				break
			}
			if fastleft.Level() != level || fastright.Level() != level {
				break
			}
		}
	} else {
		for {
			l, lok = itleft.Next()
			r, rok = itright.Next()

			if l != r || !lok || !rok {
				break
			}
			if itleft.Level() != level || itright.Level() != level {
				break
			}
		}
	}

	switch {
	case itleft.Level() == itright.Level():
		if l == r && lok && rok {
			level++
			if level < levelsToCompare {
				goto nextLevel
			}
		}
	case itleft.Level() > level:
		return -1
	case itright.Level() > level:
		if rightIsPrefix {
			level = itleft.SkipLevel()
			if level < levelsToCompare {
				goto nextLevel
			}
			return -int(r)
		}
		return 1
	}

	return int(l) - int(r)
}

func (c *Collation_utf8mb4_uca_0900) WeightString(dst, src []byte, numCodepoints int) []byte {
	it := c.uca.Iterator(src)
	defer it.Done()

	if fast, ok := it.(*uca.FastIterator900); ok {
		var chunk [16]byte
		for {
			for cap(dst)-len(dst) >= 16 {
				n := fast.NextWeightBlock64(dst[len(dst) : len(dst)+16])
				if n <= 0 {
					goto performPadding
				}
				dst = dst[:len(dst)+n]
			}
			n := fast.NextWeightBlock64(chunk[:16])
			if n <= 0 {
				goto performPadding
			}
			dst = append(dst, chunk[:n]...)
		}
	} else {
		for {
			w, ok := it.Next()
			if !ok {
				break
			}
			dst = append(dst, byte(w>>8), byte(w))
		}
	}

performPadding:
	if numCodepoints == PadToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, 0x00)
		}
	}

	return dst
}

func (c *Collation_utf8mb4_uca_0900) Hash(hasher *vthash.Hasher, src []byte, _ int) {
	hasher.Write64(uint64(c.id))

	it := c.uca.Iterator(src)
	defer it.Done()

	if fast, ok := it.(*uca.FastIterator900); ok {
		var chunk [16]byte
		var n int
		for {
			n = fast.NextWeightBlock64(chunk[:16])
			if n < 16 {
				break
			}
			hasher.Write(chunk[:16])
		}
		hasher.Write(chunk[:n])
		return
	}

	for {
		w, ok := it.Next()
		if !ok {
			break
		}
		hasher.Write16(bits.ReverseBytes16(w))
	}
}

func (c *Collation_utf8mb4_uca_0900) WeightStringLen(numBytes int) int {
	if numBytes%4 != 0 {
		panic("WeightStringLen called with non-MOD4 length")
	}
	levels := int(c.uca.MaxLevel())
	weights := (numBytes / 4) * uca.MaxCollationElementsPerCodepoint * levels
	weights += levels - 1 // one NULL byte as a separator between levels
	return weights * 2    // two bytes per weight
}

func (c *Collation_utf8mb4_uca_0900) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return newUnicodeWildcardMatcher(charset.Charset_utf8mb4{}, c.uca.WeightsEqual, c.Collate, pat, matchOne, matchMany, escape)
}

func (c *Collation_utf8mb4_uca_0900) ToLower(dst, src []byte) []byte {
	dst = append(dst, bytes.ToLower(src)...)
	return dst
}

func (c *Collation_utf8mb4_uca_0900) ToUpper(dst, src []byte) []byte {
	dst = append(dst, bytes.ToUpper(src)...)
	return dst
}

type Collation_utf8mb4_0900_bin struct{}

func (c *Collation_utf8mb4_0900_bin) ID() ID {
	return 309
}

func (c *Collation_utf8mb4_0900_bin) Name() string {
	return "utf8mb4_0900_bin"
}

func (c *Collation_utf8mb4_0900_bin) Charset() charset.Charset {
	return charset.Charset_utf8mb4{}
}

func (c *Collation_utf8mb4_0900_bin) IsBinary() bool {
	return true
}

func (c *Collation_utf8mb4_0900_bin) Collate(left, right []byte, isPrefix bool) int {
	return collationBinary(left, right, isPrefix)
}

func (c *Collation_utf8mb4_0900_bin) WeightString(dst, src []byte, numCodepoints int) []byte {
	dst = append(dst, src...)
	if numCodepoints == PadToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, 0x0)
		}
	}
	return dst
}

func (c *Collation_utf8mb4_0900_bin) Hash(hasher *vthash.Hasher, src []byte, _ int) {
	hasher.Write64(0xb900b900)
	hasher.Write(src)
}

func (c *Collation_utf8mb4_0900_bin) WeightStringLen(numBytes int) int {
	return numBytes
}

func (c *Collation_utf8mb4_0900_bin) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	equals := func(a, b rune) bool {
		return a == b
	}
	return newUnicodeWildcardMatcher(charset.Charset_utf8mb4{}, equals, c.Collate, pat, matchOne, matchMany, escape)
}

func (c *Collation_utf8mb4_0900_bin) ToLower(dst, src []byte) []byte {
	dst = append(dst, bytes.ToLower(src)...)
	return dst
}

func (c *Collation_utf8mb4_0900_bin) ToUpper(dst, src []byte) []byte {
	dst = append(dst, bytes.ToUpper(src)...)
	return dst
}

type Collation_uca_legacy struct {
	name string
	id   ID
	uca  *uca.CollationLegacy
}

func (c *Collation_uca_legacy) ID() ID {
	return c.id
}

func (c *Collation_uca_legacy) Name() string {
	return c.name
}

func (c *Collation_uca_legacy) Charset() charset.Charset {
	return c.uca.Charset()
}

func (c *Collation_uca_legacy) IsBinary() bool {
	return false
}

func (c *Collation_uca_legacy) Collate(left, right []byte, isPrefix bool) int {
	var (
		l, r     uint16
		lok, rok bool
		itleft   = c.uca.Iterator(left)
		itright  = c.uca.Iterator(right)
	)

	for {
		l, lok = itleft.Next()
		r, rok = itright.Next()

		if l == r && lok && rok {
			continue
		}
		if !rok && isPrefix {
			return 0
		}
		return int(l) - int(r)
	}
}

func (c *Collation_uca_legacy) WeightString(dst, src []byte, numCodepoints int) []byte {
	it := c.uca.Iterator(src)

	for {
		w, ok := it.Next()
		if !ok {
			break
		}
		dst = append(dst, byte(w>>8), byte(w))
	}

	if numCodepoints > 0 {
		weightForSpace := c.uca.WeightForSpace()
		w1, w2 := byte(weightForSpace>>8), byte(weightForSpace)

		if numCodepoints == PadToMax {
			for len(dst)+1 < cap(dst) {
				dst = append(dst, w1, w2)
			}
			if len(dst) < cap(dst) {
				dst = append(dst, w1)
			}
		} else {
			numCodepoints -= it.Length()
			for numCodepoints > 0 {
				dst = append(dst, w1, w2)
				numCodepoints--
			}
		}
	}

	return dst
}

func (c *Collation_uca_legacy) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	it := c.uca.Iterator(src)

	hasher.Write64(uint64(c.id))
	for {
		w, ok := it.Next()
		if !ok {
			break
		}
		hasher.Write16(bits.ReverseBytes16(w))
	}

	if numCodepoints > 0 {
		weightForSpace := bits.ReverseBytes16(c.uca.WeightForSpace())
		numCodepoints -= it.Length()
		for numCodepoints > 0 {
			hasher.Write16(weightForSpace)
			numCodepoints--
		}
	}
}

func (c *Collation_uca_legacy) WeightStringLen(numBytes int) int {
	// TODO: This is literally the worst case scenario. Improve on this.
	return numBytes * 8
}

func (c *Collation_uca_legacy) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return newUnicodeWildcardMatcher(c.uca.Charset(), c.uca.WeightsEqual, c.Collate, pat, matchOne, matchMany, escape)
}
