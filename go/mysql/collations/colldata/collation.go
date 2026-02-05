/*
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

package colldata

import (
	"bytes"
	"fmt"
	"math"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

type Charset = charset.Charset

// Collation implements a MySQL-compatible collation. It defines how to compare
// for sorting order and equality two strings with the same encoding.
type Collation interface {
	// ID returns the numerical identifier for this collation. This is the same
	// value that is returned by MySQL in a query's headers to identify the collation
	// for a given column
	ID() collations.ID

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
	Charset() Charset

	// IsBinary returns whether this collation is a binary collation
	IsBinary() bool
}

// WildcardPattern is a matcher for a wildcard pattern, constructed from a given collation
type WildcardPattern interface {
	// Match returns whether the given string matches this pattern
	Match(in []byte) bool
}

const PadToMax = math.MaxInt32

// CaseAwareCollation implements lowercase and uppercase conventions for collations.
type CaseAwareCollation interface {
	Collation
	ToUpper(dst []byte, src []byte) []byte
	ToLower(dst []byte, src []byte) []byte
}

// TinyWeightCollation implements the TinyWeightString API for collations.
type TinyWeightCollation interface {
	Collation
	// TinyWeightString returns a 32-bit weight string for a source string based on this collation.
	// This is usually the 4-byte prefix of the full weight string, calculated more efficiently.
	TinyWeightString(src []byte) uint32
}

func Lookup(id collations.ID) Collation {
	if int(id) >= len(collationsById) {
		return nil
	}
	return collationsById[id]
}

// All returns a slice with all known collations in Vitess.
func All(env *collations.Environment) []Collation {
	allCols := env.AllCollationIDs()
	all := make([]Collation, 0, len(allCols))
	for _, col := range allCols {
		all = append(all, collationsById[col])
	}
	return all
}

func checkCompatibleCollations(
	left Collation, leftCoercibility collations.Coercibility, leftRepertoire collations.Repertoire,
	right Collation, rightCoercibility collations.Coercibility, rightRepertoire collations.Repertoire,
) bool {
	leftCS := left.Charset()
	rightCS := right.Charset()

	switch leftCS.(type) {
	case charset.Charset_utf8mb4:
		if leftCoercibility <= rightCoercibility {
			return true
		}

	case charset.Charset_utf32:
		switch {
		case leftCoercibility < rightCoercibility:
			return true
		case leftCoercibility == rightCoercibility:
			if !charset.IsUnicode(rightCS) {
				return true
			}
			if !left.IsBinary() {
				return true
			}
		}

	case charset.Charset_utf8mb3, charset.Charset_ucs2, charset.Charset_utf16, charset.Charset_utf16le:
		switch {
		case leftCoercibility < rightCoercibility:
			return true
		case leftCoercibility == rightCoercibility:
			if !charset.IsUnicode(rightCS) {
				return true
			}
		}
	}

	if rightRepertoire == collations.RepertoireASCII {
		switch {
		case leftCoercibility < rightCoercibility:
			return true
		case leftCoercibility == rightCoercibility:
			if leftRepertoire == collations.RepertoireUnicode {
				return true
			}
		}
	}

	return false
}

// CoercionOptions is used to configure how aggressive the algorithm can be
// when merging two different collations by transcoding them.
type CoercionOptions struct {
	// ConvertToSuperset allows merging two different collations as long
	// as the charset of one of them is a strict superset of the other. In
	// order to operate on the two expressions, one of them will need to
	// be transcoded. This transcoding will always be safe because the string
	// with the smallest repertoire will be transcoded to its superset, which
	// cannot fail.
	ConvertToSuperset bool

	// ConvertWithCoercion allows merging two different collations by forcing
	// a coercion as long as the coercibility of the two sides is lax enough.
	// This will force a transcoding of one of the expressions even if their
	// respective charsets are not a strict superset, so the resulting transcoding
	// CAN fail depending on the content of their strings.
	ConvertWithCoercion bool
}

// Coercion is a function that will transform either the given argument
// arguments of the function into a specific character set. The `dst` argument
// will be used as the destination of the coerced argument, but it can be nil.
type Coercion func(dst, in []byte) ([]byte, error)

// Merge returns a Coercion function for a pair of TypedCollation based
// on their coercibility.
//
// The function takes the typed collations for the two sides of a text operation
// (namely, a comparison or concatenation of two textual expressions). These typed
// collations includes the actual collation for the expression on each size, their
// coercibility values (see: Coercibility) and their respective repertoires,
// and returns the target collation (i.e. the collation into which the two expressions
// must be coerced, and a Coercion function. The Coercion function can be called repeatedly
// with the different values for the two expressions and will transcode either
// the left-hand or right-hand value to the appropriate charset so it can be
// collated against the other value.
//
// If the collations for both sides of the expressions are the same, the returned
// Coercion function will be a no-op. Likewise, if the two collations are not the same,
// but they are compatible and have the same charset, the Coercion function will also
// be a no-op.
//
// If the collations for both sides of the expression are not compatible, an error
// will be returned and the returned TypedCollation and Coercion will be nil.
func Merge(env *collations.Environment, left, right collations.TypedCollation, opt CoercionOptions) (collations.TypedCollation, Coercion, Coercion, error) {
	leftColl := Lookup(left.Collation)
	rightColl := Lookup(right.Collation)
	if leftColl == nil || rightColl == nil {
		return collations.TypedCollation{}, nil, nil, fmt.Errorf("unsupported TypeCollationID: %v / %v", left.Collation, right.Collation)
	}

	leftCS := leftColl.Charset()
	rightCS := rightColl.Charset()

	if left.Coercibility == collations.CoerceExplicit && right.Coercibility == collations.CoerceExplicit {
		if left.Collation != right.Collation {
			goto cannotCoerce
		}
	}

	if leftCS.Name() == rightCS.Name() {
		switch {
		case left.Coercibility < right.Coercibility:
			left.Repertoire |= right.Repertoire
			return left, nil, nil, nil

		case left.Coercibility > right.Coercibility:
			right.Repertoire |= left.Repertoire
			return right, nil, nil, nil

		case left.Collation == right.Collation:
			left.Repertoire |= right.Repertoire
			return left, nil, nil, nil
		}

		if left.Coercibility == collations.CoerceExplicit {
			goto cannotCoerce
		}

		leftCsBin := leftColl.IsBinary()
		rightCsBin := rightColl.IsBinary()

		switch {
		case leftCsBin && rightCsBin:
			left.Coercibility = collations.CoerceNone
			return left, nil, nil, nil

		case leftCsBin:
			return left, nil, nil, nil

		case rightCsBin:
			return right, nil, nil, nil
		}

		defaults := env.LookupByCharset(leftCS.Name())
		return collations.TypedCollation{
			Collation:    defaults.Binary,
			Coercibility: collations.CoerceNone,
			Repertoire:   left.Repertoire | right.Repertoire,
		}, nil, nil, nil
	}

	if _, leftIsBinary := leftColl.(*Collation_binary); leftIsBinary {
		if left.Coercibility <= right.Coercibility {
			return left, nil, nil, nil
		}
		goto coerceToRight
	}
	if _, rightIsBinary := rightColl.(*Collation_binary); rightIsBinary {
		if left.Coercibility >= right.Coercibility {
			return right, nil, nil, nil
		}
		goto coerceToLeft
	}

	if opt.ConvertToSuperset {
		if checkCompatibleCollations(leftColl, left.Coercibility, left.Repertoire, rightColl, right.Coercibility, right.Repertoire) {
			goto coerceToLeft
		}
		if checkCompatibleCollations(rightColl, right.Coercibility, right.Repertoire, leftColl, left.Coercibility, left.Repertoire) {
			goto coerceToRight
		}
	}

	if opt.ConvertWithCoercion {
		if left.Coercibility < right.Coercibility && right.Coercibility > collations.CoerceImplicit {
			goto coerceToLeft
		}
		if right.Coercibility < left.Coercibility && left.Coercibility > collations.CoerceImplicit {
			goto coerceToRight
		}
	}

cannotCoerce:
	return collations.TypedCollation{}, nil, nil, fmt.Errorf("Illegal mix of collations (%s,%s) and (%s,%s)",
		leftColl.Name(), left.Coercibility, rightColl.Name(), right.Coercibility)

coerceToLeft:
	return left, nil,
		func(dst, in []byte) ([]byte, error) {
			return charset.Convert(dst, leftCS, in, rightCS)
		}, nil

coerceToRight:
	return right,
		func(dst, in []byte) ([]byte, error) {
			return charset.Convert(dst, rightCS, in, leftCS)
		}, nil, nil
}

func Index(col Collation, str, sub []byte, offset int) int {
	cs := col.Charset()
	if offset > 0 {
		l := charset.Length(cs, str)
		if offset > l {
			return -1
		}
		str = charset.Slice(cs, str, offset, len(str))
	}

	pos := instr(col, str, sub)
	if pos < 0 {
		return -1
	}
	return offset + pos
}

func instr(col Collation, str, sub []byte) int {
	if len(sub) == 0 {
		return 0
	}

	if len(str) == 0 {
		return -1
	}

	if col.IsBinary() && col.Charset().MaxWidth() == 1 {
		return bytes.Index(str, sub)
	}

	var pos int
	cs := col.Charset()
	for len(str) > 0 {
		if col.Collate(str, sub, true) == 0 {
			return pos
		}
		_, size := cs.DecodeRune(str)
		str = str[size:]
		pos++
	}
	return -1
}
