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

func (env *Environment) EnsureCollate(fromID, toID ID) error {
	// these two lookups should never fail
	fromCharsetName := env.LookupCharsetName(fromID)
	toCharsetName := env.LookupCharsetName(toID)
	if fromCharsetName != toCharsetName {
		toCollName := env.LookupName(toID)
		return fmt.Errorf("COLLATION '%s' is not valid for CHARACTER SET '%s'", toCollName, fromCharsetName)
	}
	return nil
}
