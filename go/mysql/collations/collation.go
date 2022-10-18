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
	"math"

	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/vt/vthash"
)

//go:generate go run ./tools/makecolldata/ -embed=true

// CaseAwareCollation implements lowercase and uppercase conventions for collations.
type CaseAwareCollation interface {
	Collation
	ToUpper(dst []byte, src []byte) []byte
	ToLower(dst []byte, src []byte) []byte
}

// ID is a numeric identifier for a collation. These identifiers are defined by MySQL, not by Vitess.
type ID uint16

// Unknown is the default ID for an unknown collation.
const Unknown ID = 0

// Collation implements a MySQL-compatible collation. It defines how to compare
// for sorting order and equality two strings with the same encoding.
type Collation interface {
	// Init initializes the internal state for the collation the first time it is used
	Init()

	// ID returns the numerical identifier for this collation. This is the same
	// value that is returned by MySQL in a query's headers to identify the collation
	// for a given column
	ID() ID

	// Name is the full name of this collation, in the form of "ENCODING_LANG_SENSITIVITY"
	Name() string

	// Collate compares two strings using this collation. `left` and `right` must be the
	// two strings encoded in the proper encoding for this collation. If `isPrefix` is true,
	// the function instead behaves equivalently to `strings.HasPrefix(left, right)`, but
	// being collation-aware.
	// It returns a numeric value like a normal comparison function: <0 if left < right,
	// 0 if left == right, >0 if left > right
	Collate(left, right []byte, isPrefix bool) int

	// WeightString returns a weight string for the given `src` string. A weight string
	// is a binary representation of the weights for the given string, that can be
	// compared byte-wise to return identical results to collating this string.
	//
	// This means:
	//		bytes.Compare(WeightString(left), WeightString(right)) == Collate(left, right)
	//
	// The semantics of this API have been carefully designed to match MySQL's behavior
	// in its `strnxfrm` API. Most notably, the `numCodepoints` argument implies different
	// behaviors depending on the collation's padding mode:
	//
	// - For collations that pad WITH SPACE (this is, all legacy collations in MySQL except
	//	for the newly introduced UCA v9.0.0 utf8mb4 collations in MySQL 8.0), `numCodepoints`
	// 	can have the following values:
	//
	//		- if `numCodepoints` is any integer greater than zero, this treats the `src` string
	//		as if it were in a `CHAR(numCodepoints)` column in MySQL, meaning that the resulting
	//		weight string will be padded with the weight for the SPACE character until it becomes
	//		wide enough to fill the `CHAR` column. This is necessary to perform weight comparisons
	//		in fixed-`CHAR` columns. If `numCodepoints` is smaller than the actual amount of
	//		codepoints stored in `src`, the result is unspecified.
	//
	//		- if `numCodepoints` is zero, this is equivalent to `numCodepoints = RuneCount(src)`,
	//		meaning that the resulting weight string will have no padding at the end: it'll only have
	//		the weight values for the exact amount of codepoints contained in `src`. This is the
	//		behavior required to sort `VARCHAR` columns.
	//
	//		- if `numCodepoints` is the special constant PadToMax, then the `dst` slice must be
	//		pre-allocated to a zero-length slice with enough capacity to hold the complete weight
	//		string, and any remaining capacity in `dst` will be filled by the weights for the
	//		padding character, repeatedly. This is a special flag used by MySQL when performing
	//		filesorts, where all the sorting keys must have identical sizes, even for `VARCHAR`
	//		columns.
	//
	//	- For collations that have NO PAD (this is, the newly introduced UCA v9.0.0 utf8mb4 collations
	//	in MySQL 8.0), `numCodepoints` can only have the special constant `PadToMax`, which will make
	//	the weight string padding equivalent to a PAD SPACE collation (as explained in the previous
	//	section). All other values for `numCodepoints` are ignored, because NO PAD collations always
	//	return the weights for the codepoints in their strings, with no further padding at the end.
	//
	// The resulting weight string is written to `dst`, which can be pre-allocated to
	// WeightStringLen() bytes to prevent growing the slice. `dst` can also be nil, in which
	// case it will grow dynamically. If `numCodepoints` has the special PadToMax value explained
	// earlier, `dst` MUST be pre-allocated to the target size or the function will return an
	// empty slice.
	WeightString(dst, src []byte, numCodepoints int) []byte

	// WeightStringLen returns a size (in bytes) that would fit any weight strings for a string
	// with `numCodepoints` using this collation. Note that this is a higher bound for the size
	// of the string, and in practice weight strings can be significantly smaller than the
	// returned value.
	WeightStringLen(numCodepoints int) int

	// Hash returns a 32 or 64 bit identifier (depending on the platform) that uniquely identifies
	// the given string based on this collation. It is functionally equivalent to calling WeightString
	// and then hashing the result.
	//
	// Consequently, if the hashes for two strings are different, then the two strings are considered
	// different according to this collation. If the hashes for two strings are equal, the two strings
	// may or may not be considered equal according to this collation, because hashes can collide unlike
	// weight strings.
	//
	// The numCodepoints argument has the same behavior as in WeightString: if this collation uses PAD SPACE,
	// the hash will interpret the source string as if it were stored in a `CHAR(n)` column. If the value of
	// numCodepoints is 0, this is equivalent to setting `numCodepoints = RuneCount(src)`.
	// For collations with NO PAD, the numCodepoint argument is ignored.
	Hash(hasher *vthash.Hasher, src []byte, numCodepoints int)

	// Wildcard returns a matcher for the given wildcard pattern. The matcher can be used to repeatedly
	// test different strings to check if they match the pattern. The pattern must be a traditional wildcard
	// pattern, which may contain the provided special characters for matching one character or several characters.
	// The provided `escape` character will be used as an escape sequence in front of the other special characters.
	//
	// This method is fully collation aware; the matching will be performed according to the underlying collation.
	// I.e. if this is a case-insensitive collation, matching will be case-insensitive.
	//
	// The returned WildcardPattern is always valid, but if the provided special characters do not exist in this
	// collation's repertoire, the returned pattern will not match any strings. Likewise, if the provided pattern
	// has invalid syntax, the returned pattern will not match any strings.
	//
	// If the provided special characters are 0, the defaults to parse an SQL 'LIKE' statement will be used.
	// This is, '_' for matching one character, '%' for matching many and '\\' for escape.
	//
	// This method can also be used for Shell-like matching with '?', '*' and '\\' as their respective special
	// characters.
	Wildcard(pat []byte, matchOne, matchMany, escape rune) WildcardPattern

	// Charset returns the Charset with which this collation is encoded
	Charset() charset.Charset

	// IsBinary returns whether this collation is a binary collation
	IsBinary() bool
}

// WildcardPattern is a matcher for a wildcard pattern, constructed from a given collation
type WildcardPattern interface {
	// Match returns whether the given string matches this pattern
	Match(in []byte) bool
}

const PadToMax = math.MaxInt32

func minInt(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

var globalAllCollations = make(map[ID]Collation)

func register(c Collation) {
	if _, found := globalAllCollations[c.ID()]; found {
		panic("duplicated collation registered")
	}
	globalAllCollations[c.ID()] = c
}

// Slice returns the substring in `input[from:to]`, where `from` and `to`
// are collation-aware character indices instead of bytes.
func Slice(collation Collation, input []byte, from, to int) []byte {
	return charset.Slice(collation.Charset(), input, from, to)
}

// Validate returns whether the given `input` is properly encoded with the
// character set for the given collation.
func Validate(collation Collation, input []byte) bool {
	return charset.Validate(collation.Charset(), input)
}

// Convert converts the bytes in `src`, which are encoded in `srcCollation`'s charset,
// into a byte slice encoded in `dstCollation`'s charset. The resulting byte slice is
// appended to `dst` and returned.
func Convert(dst []byte, dstCollation Collation, src []byte, srcCollation Collation) ([]byte, error) {
	return charset.Convert(dst, dstCollation.Charset(), src, srcCollation.Charset())
}

// Length returns the number of codepoints in the input based on the given collation
func Length(collation Collation, input []byte) int {
	return charset.Length(collation.Charset(), input)
}

// ConvertForJSON behaves like Convert, but the output string is always utf8mb4 encoded,
// as it is required for JSON strings in MySQL.
func ConvertForJSON(dst []byte, src []byte, srcCollation Collation) ([]byte, error) {
	return charset.Convert(dst, charset.Charset_utf8mb4{}, src, srcCollation.Charset())
}
