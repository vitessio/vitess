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
	"math"
)

// Generate mysqldata.go from the JSON information dumped from MySQL
//go:generate go run ./tools/makemysqldata/

type ID uint16

const Unknown ID = 0

// Collation implements a MySQL-compatible collation. It defines how to compare
// for sorting order and equality two strings with the same encoding.
type Collation interface {
	// init initializes the internal state for the collation the first time it is used
	init()

	// Id returns the numerical identifier for this collation. This is the same
	// value that is returned by MySQL in a query's headers to identify the collation
	// for a given column
	Id() ID

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
}

const PadToMax = math.MaxInt32

func minInt(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

var collationsByName = make(map[string]Collation)
var collationsById = make(map[ID]Collation)

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

// LookupIDByName returns the collation ID for the given name
func LookupIDByName(name string) ID {
	csi := collationsByName[name]
	if csi == nil {
		return Unknown
	}

	return csi.Id()
}

// LookupById returns the collation with the given numerical identifier. The collation
// is initialized if it's the first time being accessed.
func LookupById(id ID) Collation {
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
