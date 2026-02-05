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

package pattern

import (
	"strings"
	"unicode/utf8"
)

var patternPropsLatin1 = [256]uint8{
	// WS: 9..D
	0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 5, 5, 5, 5, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	// WS: 20  Syntax: 21..2F
	5, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	// Syntax: 3A..40
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 3, 3, 3, 3,
	3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	// Syntax: 5B..5E
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 3, 3, 0,
	// Syntax: 60
	3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	// Syntax: 7B..7E
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 3, 3, 0,
	// WS: 85
	0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	// Syntax: A1..A7, A9, AB, AC, AE
	0, 3, 3, 3, 3, 3, 3, 3, 0, 3, 0, 3, 3, 0, 3, 0,
	// Syntax: B0, B1, B6, BB, BF
	3, 3, 0, 0, 0, 0, 3, 0, 0, 0, 0, 3, 0, 0, 0, 3,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	// Syntax: D7
	0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	// Syntax: F7
	0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0,
}

func IsWhitespace(c rune) bool {
	if c < 0 {
		return false
	} else if c <= 0xff {
		return (patternPropsLatin1[c]>>2)&1 != 0
	} else if 0x200e <= c && c <= 0x2029 {
		return c <= 0x200f || 0x2028 <= c
	} else {
		return false
	}
}

func SkipWhitespace(str string) string {
	for {
		r, w := utf8.DecodeRuneInString(str)
		if r == utf8.RuneError && (w == 0 || w == 1) {
			return str[w:]
		}
		if !IsWhitespace(r) {
			return str
		}
		str = str[w:]
	}
}

func IsUnprintable(c rune) bool {
	return !(c >= 0x20 && c <= 0x7E)
}

// "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
var digits = [...]byte{
	48, 49, 50, 51, 52, 53, 54, 55, 56, 57,
	65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
	75, 76, 77, 78, 79, 80, 81, 82, 83, 84,
	85, 86, 87, 88, 89, 90,
}

func EscapeUnprintable(w *strings.Builder, c rune) {
	w.WriteByte('\\')
	if (c & ^0xFFFF) != 0 {
		w.WriteByte('U')
		w.WriteByte(digits[0xF&(c>>28)])
		w.WriteByte(digits[0xF&(c>>24)])
		w.WriteByte(digits[0xF&(c>>20)])
		w.WriteByte(digits[0xF&(c>>16)])
	} else {
		w.WriteByte('u')
	}
	w.WriteByte(digits[0xF&(c>>12)])
	w.WriteByte(digits[0xF&(c>>8)])
	w.WriteByte(digits[0xF&(c>>4)])
	w.WriteByte(digits[0xF&c])
}
