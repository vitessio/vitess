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
)

// Generate mysqldata.go from the JSON information dumped from MySQL
//go:generate go run ./tools/makemysqldata/

// Collation implements a MySQL-compatible collation. It defines how to compare
// for sorting order and equality two strings with the same encoding.
type Collation interface {
	// init initializes the internal state for the collation the first time it is used
	init()

	// Id returns the numerical identifier for this collation. This is the same
	// value that is returned by MySQL in a query's headers to identify the collation
	// for a given column
	Id() uint

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
	// This means:
	//		bytes.Compare(WeightString(left), WeightString(right)) == Collate(left, right)
	// The resulting weight string is written to `dst`, which can be pre-allocated to
	// WeightStringLen() bytes to prevent growing the slice. `dst` can also be nil, in which
	// case it will grow dynamically. The final slice with the exact length for the weight
	// string is returned by the function.
	WeightString(dst []byte, src []byte) []byte

	// WeightStringPad returns a weight string for a source string with the same semantics
	// as MySQL's `strnxfrm` method in the MySQL Public API. It works similarly to WeightString,
	// with the following differences:
	//
	// - `dst` is never grown: it must be a zero-size slice allocated by the caller
	// 	with enough capacity to hold the whole weight string. If `dst` is not large enough,
	// 	the resulting weight string will be truncated.
	// - `numCodepoints` treats the `src` string as if it were in a `CHAR(numCodepoints)` column.
	// 	This is only relevant for pad collations, where strings with fewer than numCodepoints
	// 	codepoints will have the weights of the 'space' character for their encoding appended
	// 	at the end of the weight string, as to ensure that all the weight strings for the column
	// 	have an equal size.
	// - if `padToMax` is true, the `numCodepoints` behavior is overriden and instead the resulting
	// 	weight string is paded with zeroes until the full capacity of the `dst` buffer.
	//
	// Compatibility notes:
	// This is a best-effort implementation of the original `strxfrm` implementation in MySQL. It's
	// not clear what are the use cases for this API that cannot be handled by the simpler WeightString,
	// but from perusing the documentation, it appears that this padding behavior must be used when
	// collating strings from `CHAR(n)` columns with space-padded collations, and all other cases
	// (VARCHAR columns, non-space padded collations) can be handled by WeightString.
	WeightStringPad(dst []byte, numCodepoints int, src []byte, padToMax bool) []byte

	// WeightStringLen returns a size (in bytes) that would fit any weight strings for a string
	// with `numCodepoints` using this collation. Note that this is a higher bound for the size
	// of the string, and in practice weight strings can be significantly smaller than the
	// returned value.
	WeightStringLen(numCodepoints int) int
}

func minInt(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

var collationsByName = make(map[string]Collation)
var collationsById = make(map[uint]Collation)

func register(c Collation) {
	duplicatedCharset := func(old Collation) {
		panic(fmt.Sprintf("duplicated collation: %s[%d] (existing collation is %s[%d])",
			c.Name(), c.Id(), old.Name(), old.Id(),
		))
	}
	if old, found := collationsByName[c.Name()]; found {
		duplicatedCharset(old)
	}
	if old, found := collationsById[c.Id()]; found {
		duplicatedCharset(old)
	}
	collationsByName[c.Name()] = c
	collationsById[c.Id()] = c
}

// LookupByName returns the collation with the given name. The collation
// is initialized if it's the first time being accessed.
func LookupByName(name string) Collation {
	csi := collationsByName[name]
	if csi != nil {
		csi.init()
	}
	return csi
}

// LookupById returns the collation with the given numerical identifier. The collation
// is initialized if it's the first time being accessed.
func LookupById(id uint) Collation {
	csi := collationsById[id]
	if csi != nil {
		csi.init()
	}
	return csi
}

// All returns a slice with all known collations in Vitess. This is an expensive call because
// it will initialize the internal state of all the collations before returning them.
// Used for testing/debugging.
func All() (all []Collation) {
	all = make([]Collation, 0, len(collationsById))
	for _, col := range collationsById {
		col.init()
		all = append(all, col)
	}
	return
}
