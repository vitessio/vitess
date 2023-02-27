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
	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/vt/vthash"
)

var sortOrderIdentity [256]byte

func init() {
	for i := range sortOrderIdentity {
		sortOrderIdentity[i] = byte(i)
	}

	register(&Collation_binary{})
}

type simpletables struct {
	// By default we're not building in the tables for lower/upper-casing and
	// character classes, because we're not using them for collation and they
	// take up a lot of binary space.
	// Uncomment these fields and pass `-full8bit` to `makemysqldata` to generate
	// these tables.
	tolower *[256]byte
	toupper *[256]byte
	ctype   *[256]byte
	sort    *[256]byte
}

type Collation_8bit_bin struct {
	id   ID
	name string
	simpletables
	charset charset.Charset
}

func (c *Collation_8bit_bin) Init() {}

func (c *Collation_8bit_bin) Name() string {
	return c.name
}

func (c *Collation_8bit_bin) ID() ID {
	return c.id
}

func (c *Collation_8bit_bin) Charset() charset.Charset {
	return c.charset
}

func (c *Collation_8bit_bin) IsBinary() bool {
	return true
}

func (c *Collation_8bit_bin) Collate(left, right []byte, rightIsPrefix bool) int {
	return collationBinary(left, right, rightIsPrefix)
}

func (c *Collation_8bit_bin) WeightString(dst, src []byte, numCodepoints int) []byte {
	copyCodepoints := len(src)

	var padToMax bool
	switch numCodepoints {
	case 0:
		numCodepoints = copyCodepoints
	case PadToMax:
		padToMax = true
	default:
		copyCodepoints = minInt(copyCodepoints, numCodepoints)
	}

	dst = append(dst, src[:copyCodepoints]...)
	return weightStringPadingSimple(' ', dst, numCodepoints-copyCodepoints, padToMax)
}

func (c *Collation_8bit_bin) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	hasher.Write64(0x8b8b0000 | uint64(c.id))
	if numCodepoints == 0 {
		hasher.Write(src)
		return
	}

	tocopy := minInt(len(src), numCodepoints)
	hasher.Write(src[:tocopy])

	numCodepoints -= tocopy
	for numCodepoints > 0 {
		hasher.Write8(' ')
		numCodepoints--
	}
}

func (c *Collation_8bit_bin) WeightStringLen(numBytes int) int {
	return numBytes
}

func (c *Collation_8bit_bin) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return newEightbitWildcardMatcher(&sortOrderIdentity, c.Collate, pat, matchOne, matchMany, escape)
}

func (c *Collation_8bit_bin) ToLower(dst, src []byte) []byte {
	lowerTable := c.simpletables.tolower

	for _, c := range src {
		dst = append(dst, lowerTable[c])
	}
	return dst
}

func (c *Collation_8bit_bin) ToUpper(dst, src []byte) []byte {
	upperTable := c.simpletables.toupper

	for _, c := range src {
		dst = append(dst, upperTable[c])
	}
	return dst
}

type Collation_8bit_simple_ci struct {
	id   ID
	name string
	simpletables
	charset charset.Charset
}

func (c *Collation_8bit_simple_ci) Init() {
	if c.sort == nil {
		panic("8bit_simple_ci collation without sort table")
	}
}

func (c *Collation_8bit_simple_ci) Name() string {
	return c.name
}

func (c *Collation_8bit_simple_ci) ID() ID {
	return c.id
}

func (c *Collation_8bit_simple_ci) Charset() charset.Charset {
	return c.charset
}

func (c *Collation_8bit_simple_ci) IsBinary() bool {
	return false
}

func (c *Collation_8bit_simple_ci) Collate(left, right []byte, rightIsPrefix bool) int {
	sortOrder := c.sort
	cmpLen := minInt(len(left), len(right))

	for i := 0; i < cmpLen; i++ {
		sortL, sortR := sortOrder[left[i]], sortOrder[right[i]]
		if sortL != sortR {
			return int(sortL) - int(sortR)
		}
	}
	if rightIsPrefix {
		left = left[:cmpLen]
	}
	return len(left) - len(right)
}

func (c *Collation_8bit_simple_ci) WeightString(dst, src []byte, numCodepoints int) []byte {
	padToMax := false
	sortOrder := c.sort
	copyCodepoints := len(src)

	switch numCodepoints {
	case 0:
		numCodepoints = copyCodepoints
	case PadToMax:
		padToMax = true
	default:
		copyCodepoints = minInt(copyCodepoints, numCodepoints)
	}

	for _, ch := range src[:copyCodepoints] {
		dst = append(dst, sortOrder[ch])
	}
	return weightStringPadingSimple(' ', dst, numCodepoints-copyCodepoints, padToMax)
}

func (c *Collation_8bit_simple_ci) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	sortOrder := c.sort

	var tocopy = len(src)
	if numCodepoints > 0 {
		tocopy = minInt(tocopy, numCodepoints)
	}

	hasher.Write64(uint64(c.id))
	for _, ch := range src[:tocopy] {
		hasher.Write8(sortOrder[ch])
	}

	if numCodepoints > 0 {
		numCodepoints -= tocopy
		for numCodepoints > 0 {
			hasher.Write8(' ')
			numCodepoints--
		}
	}
}

func (c *Collation_8bit_simple_ci) WeightStringLen(numBytes int) int {
	return numBytes
}

func (c *Collation_8bit_simple_ci) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return newEightbitWildcardMatcher(c.sort, c.Collate, pat, matchOne, matchMany, escape)
}

func weightStringPadingSimple(padChar byte, dst []byte, numCodepoints int, padToMax bool) []byte {
	if padToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, padChar)
		}
	} else {
		for numCodepoints > 0 {
			dst = append(dst, padChar)
			numCodepoints--
		}
	}
	return dst
}

func (c *Collation_8bit_simple_ci) ToLower(dst, src []byte) []byte {
	lowerTable := c.simpletables.tolower

	for _, c := range src {
		dst = append(dst, lowerTable[c])
	}
	return dst
}

func (c *Collation_8bit_simple_ci) ToUpper(dst, src []byte) []byte {
	upperTable := c.simpletables.toupper

	for _, c := range src {
		dst = append(dst, upperTable[c])
	}
	return dst
}

type Collation_binary struct{}

func (c *Collation_binary) Init() {}

func (c *Collation_binary) ID() ID {
	return CollationBinaryID
}

func (c *Collation_binary) Name() string {
	return "binary"
}

func (c *Collation_binary) Charset() charset.Charset {
	return charset.Charset_binary{}
}

func (c *Collation_binary) IsBinary() bool {
	return true
}

func (c *Collation_binary) Collate(left, right []byte, isPrefix bool) int {
	return collationBinary(left, right, isPrefix)
}

func (c *Collation_binary) WeightString(dst, src []byte, numCodepoints int) []byte {
	padToMax := false
	copyCodepoints := len(src)

	switch numCodepoints {
	case 0: // no-op
	case PadToMax:
		padToMax = true
	default:
		copyCodepoints = minInt(copyCodepoints, numCodepoints)
	}

	dst = append(dst, src[:copyCodepoints]...)
	if padToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, 0x0)
		}
	}
	return dst
}

func (c *Collation_binary) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	if numCodepoints > 0 {
		src = src[:numCodepoints]
	}
	hasher.Write64(0xBBBBBBBB)
	hasher.Write(src)
}

func (c *Collation_binary) WeightStringLen(numBytes int) int {
	return numBytes
}

func (c *Collation_binary) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return newEightbitWildcardMatcher(&sortOrderIdentity, c.Collate, pat, matchOne, matchMany, escape)
}

func (c *Collation_binary) ToLower(dst, raw []byte) []byte {
	dst = append(dst, raw...)
	return dst
}

func (c *Collation_binary) ToUpper(dst, raw []byte) []byte {
	dst = append(dst, raw...)
	return dst
}
