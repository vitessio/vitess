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

package utf16

import "unicode/utf16"

func IsLead(c rune) bool {
	return (uint32(c) & 0xfffffc00) == 0xd800
}

func IsTrail(c rune) bool {
	return (uint32(c) & 0xfffffc00) == 0xdc00
}

/**
 * Is this code point a surrogate (U+d800..U+dfff)?
 * @param c 32-bit code point
 * @return true or false
 * @stable ICU 2.4
 */
func IsSurrogate(c rune) bool {
	return (uint32(c) & 0xfffff800) == 0xd800
}

/**
 * Assuming c is a surrogate code point (U_IS_SURROGATE(c)),
 * is it a lead surrogate?
 * @param c 32-bit code point
 * @return true or false
 * @stable ICU 2.4
 */
func IsSurrogateLead(c rune) bool {
	return (uint32(c) & 0x400) == 0
}

func DecodeRune(a, b rune) rune {
	return utf16.DecodeRune(a, b)
}

func NextUnsafe(s []uint16) (rune, []uint16) {
	c := rune(s[0])
	if !IsLead(c) {
		return c, s[1:]
	}
	return DecodeRune(c, rune(s[1])), s[2:]
}
