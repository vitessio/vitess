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

import (
	"strings"

	"vitess.io/vitess/go/mysql/icuregex/internal/pattern"
)

func (u *UnicodeSet) String() string {
	var buf strings.Builder
	u.ToPattern(&buf, true)
	return buf.String()
}

func (u *UnicodeSet) ToPattern(w *strings.Builder, escapeUnprintable bool) {
	w.WriteByte('[')

	//  // Check against the predefined categories.  We implicitly build
	//  // up ALL category sets the first time toPattern() is called.
	//  for (int8_t cat=0; cat<Unicode::GENERAL_TYPES_COUNT; ++cat) {
	//      if (*this == getCategorySet(cat)) {
	//          result.append(u':');
	//          result.append(CATEGORY_NAMES, cat*2, 2);
	//          return result.append(CATEGORY_CLOSE);
	//      }
	//  }

	count := u.RangeCount()

	// If the set contains at least 2 intervals and includes both
	// MIN_VALUE and MAX_VALUE, then the inverse representation will
	// be more economical.
	if count > 1 && u.RangeStart(0) == MinValue && u.RangeEnd(count-1) == MaxValue {

		// Emit the inverse
		w.WriteByte('^')

		for i := 1; i < count; i++ {
			start := u.RangeEnd(i-1) + 1
			end := u.RangeStart(i) - 1
			u.appendToPattern(w, start, escapeUnprintable)
			if start != end {
				if (start + 1) != end {
					w.WriteByte('-')
				}
				u.appendToPattern(w, end, escapeUnprintable)
			}
		}
	} else {
		// Default; emit the ranges as pairs
		for i := 0; i < count; i++ {
			start := u.RangeStart(i)
			end := u.RangeEnd(i)
			u.appendToPattern(w, start, escapeUnprintable)
			if start != end {
				if (start + 1) != end {
					w.WriteByte('-')
				}
				u.appendToPattern(w, end, escapeUnprintable)
			}
		}
	}

	w.WriteByte(']')
}

func (u *UnicodeSet) appendToPattern(w *strings.Builder, c rune, escapeUnprintable bool) {
	if escapeUnprintable && pattern.IsUnprintable(c) {
		// Use hex escape notation (\uxxxx or \Uxxxxxxxx) for anything
		// unprintable
		pattern.EscapeUnprintable(w, c)
		return
	}

	// Okay to let ':' pass through
	switch c {
	case '[', ']', '-', '^', '&', '\\', '{', '}', ':', '$':
		w.WriteByte('\\')
	default:
		// Escape whitespace
		if pattern.IsWhitespace(c) {
			w.WriteByte('\\')
		}
	}
	w.WriteRune(c)
}
