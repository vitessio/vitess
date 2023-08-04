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

import "vitess.io/vitess/go/mysql/icuregex/internal/ucase"

type USet uint32

const (
	/**
	 * Ignore white space within patterns unless quoted or escaped.
	 * @stable ICU 2.4
	 */
	IgnoreSpace USet = 1

	/**
	 * Enable case insensitive matching.  E.g., "[ab]" with this flag
	 * will match 'a', 'A', 'b', and 'B'.  "[^ab]" with this flag will
	 * match all except 'a', 'A', 'b', and 'B'. This performs a full
	 * closure over case mappings, e.g. U+017F for s.
	 *
	 * The resulting set is a superset of the input for the code points but
	 * not for the strings.
	 * It performs a case mapping closure of the code points and adds
	 * full case folding strings for the code points, and reduces strings of
	 * the original set to their full case folding equivalents.
	 *
	 * This is designed for case-insensitive matches, for example
	 * in regular expressions. The full code point case closure allows checking of
	 * an input character directly against the closure set.
	 * Strings are matched by comparing the case-folded form from the closure
	 * set with an incremental case folding of the string in question.
	 *
	 * The closure set will also contain single code points if the original
	 * set contained case-equivalent strings (like U+00DF for "ss" or "Ss" etc.).
	 * This is not necessary (that is, redundant) for the above matching method
	 * but results in the same closure sets regardless of whether the original
	 * set contained the code point or a string.
	 *
	 * @stable ICU 2.4
	 */
	CaseInsensitive USet = 2

	/**
	 * Enable case insensitive matching.  E.g., "[ab]" with this flag
	 * will match 'a', 'A', 'b', and 'B'.  "[^ab]" with this flag will
	 * match all except 'a', 'A', 'b', and 'B'. This adds the lower-,
	 * title-, and uppercase mappings as well as the case folding
	 * of each existing element in the set.
	 * @stable ICU 3.2
	 */
	AddCaseMappings USet = 4
)

func (u *UnicodeSet) CloseOver(attribute USet) {
	if attribute&AddCaseMappings != 0 {
		panic("USET_ADD_CASE_MAPPINGS is unsupported")
	}
	if (attribute & CaseInsensitive) == 0 {
		return
	}

	foldSet := u.Clone()
	n := u.RangeCount()

	for i := 0; i < n; i++ {
		start := u.RangeStart(i)
		end := u.RangeEnd(i)

		// full case closure
		for cp := start; cp <= end; cp++ {
			ucase.AddCaseClosure(cp, foldSet)
		}
	}

	*u = *foldSet
}
