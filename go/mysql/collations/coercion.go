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
	"fmt"
	"unsafe"

	"vitess.io/vitess/go/mysql/collations/internal/charset"
)

func init() {
	if unsafe.Sizeof(TypedCollation{}) != 4 {
		panic("TypedCollation should fit in an int32")
	}
}

// Coercibility is a numeric value that represents the precedence of a collation
// when applied to a SQL expression. When trying to coerce the collations
// of two different expressions so that they can be compared, the expression
// with the lowest coercibility value will win and its collation will be forced
// upon the other expression.
//
// The rules for assigning a Coercibility value to an expression are as follows:
//
//   - An explicit COLLATE clause has a coercibility of 0 (not coercible at all).
//   - The concatenation of two strings with different collations has a coercibility of 1.
//   - The collation of a column or a stored routine parameter or local variable has a coercibility of 2.
//   - A “system constant” (the string returned by functions such as USER() or VERSION()) has a coercibility of 3.
//   - The collation of a literal has a coercibility of 4.
//   - The collation of a numeric or temporal value has a coercibility of 5.
//   - NULL or an expression that is derived from NULL has a coercibility of 6.
//
// According to the MySQL documentation, Coercibility is an actual word of the English
// language, although the Vitess maintainers disagree with this assessment.
//
// See: https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html
type Coercibility byte

const (
	CoerceExplicit Coercibility = iota
	CoerceNone
	CoerceImplicit
	CoerceSysconst
	CoerceCoercible
	CoerceNumeric
	CoerceIgnorable
)

func (ci Coercibility) String() string {
	switch ci {
	case 0:
		return "EXPLICIT"
	case 1:
		return "NONE"
	case 2:
		return "IMPLICIT"
	case 3:
		return "SYSCONST"
	case 4:
		return "COERCIBLE"
	case 5:
		return "NUMERIC"
	case 6:
		return "IGNORABLE"
	default:
		panic("invalid Coercibility value")
	}
}

// Repertoire is a constant that defines the collection of characters in an expression.
// MySQL only distinguishes between an ASCII repertoire (i.e. an expression where all
// the contained codepoints are < 128), or an Unicode repertoire (an expression that
// can contain any possible codepoint).
//
// See: https://dev.mysql.com/doc/refman/8.0/en/charset-repertoire.html
type Repertoire byte

const (
	RepertoireASCII Repertoire = iota
	RepertoireUnicode
)

// Coercion is a function that will transform either the given argument
// arguments of the function into a specific character set. The `dst` argument
// will be used as the destination of the coerced argument, but it can be nil.
type Coercion func(dst, in []byte) ([]byte, error)

// TypedCollation is the Collation of a SQL expression, including its coercibility
// and repertoire.
type TypedCollation struct {
	Collation    ID
	Coercibility Coercibility
	Repertoire   Repertoire
}

func (tc TypedCollation) Valid() bool {
	return tc.Collation != Unknown
}

func checkCompatibleCollations(
	left Collation, leftCoercibility Coercibility, leftRepertoire Repertoire,
	right Collation, rightCoercibility Coercibility, rightRepertoire Repertoire,
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

	if rightRepertoire == RepertoireASCII {
		switch {
		case leftCoercibility < rightCoercibility:
			return true
		case leftCoercibility == rightCoercibility:
			if leftRepertoire == RepertoireUnicode {
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

// MergeCollations returns a Coercion function for a pair of TypedCollation based
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
func (env *Environment) MergeCollations(left, right TypedCollation, opt CoercionOptions) (TypedCollation, Coercion, Coercion, error) {
	leftColl := env.LookupByID(left.Collation)
	rightColl := env.LookupByID(right.Collation)
	if leftColl == nil || rightColl == nil {
		return TypedCollation{}, nil, nil, fmt.Errorf("unsupported TypeCollationID: %v / %v", left.Collation, right.Collation)
	}
	leftCS := leftColl.Charset()
	rightCS := rightColl.Charset()

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

		if left.Coercibility == CoerceExplicit {
			goto cannotCoerce
		}

		leftCsBin := leftColl.IsBinary()
		rightCsBin := rightColl.IsBinary()

		switch {
		case leftCsBin && rightCsBin:
			left.Coercibility = CoerceNone
			return left, nil, nil, nil

		case leftCsBin:
			return left, nil, nil, nil

		case rightCsBin:
			return right, nil, nil, nil
		}

		defaults := env.byCharset[leftCS.Name()]
		return TypedCollation{
			Collation:    defaults.Binary.ID(),
			Coercibility: CoerceNone,
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
		if left.Coercibility < right.Coercibility && right.Coercibility > CoerceImplicit {
			goto coerceToLeft
		}
		if right.Coercibility < left.Coercibility && left.Coercibility > CoerceImplicit {
			goto coerceToRight
		}
	}

cannotCoerce:
	return TypedCollation{}, nil, nil, fmt.Errorf("Illegal mix of collations (%s,%s) and (%s,%s)",
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

func (env *Environment) EnsureCollate(fromID, toID ID) error {
	// these two lookups should never fail
	from := env.LookupByID(fromID)
	to := env.LookupByID(toID)
	if from.Charset().Name() != to.Charset().Name() {
		return fmt.Errorf("COLLATION '%s' is not valid for CHARACTER SET '%s'", to.Name(), from.Charset().Name())
	}
	return nil
}
