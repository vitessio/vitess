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

package unames

import (
	"testing"
)

func TestCharForName(t *testing.T) {
	var TestNames = []struct {
		code                   rune
		name, oldName, extName string
	}{
		{0x0061, "LATIN SMALL LETTER A", "", "LATIN SMALL LETTER A"},
		{0x01a2, "LATIN CAPITAL LETTER OI", "", "LATIN CAPITAL LETTER OI"},
		{0x0284, "LATIN SMALL LETTER DOTLESS J WITH STROKE AND HOOK", "", "LATIN SMALL LETTER DOTLESS J WITH STROKE AND HOOK"},
		{0x0fd0, "TIBETAN MARK BSKA- SHOG GI MGO RGYAN", "", "TIBETAN MARK BSKA- SHOG GI MGO RGYAN"},
		{0x3401, "CJK UNIFIED IDEOGRAPH-3401", "", "CJK UNIFIED IDEOGRAPH-3401"},
		{0x7fed, "CJK UNIFIED IDEOGRAPH-7FED", "", "CJK UNIFIED IDEOGRAPH-7FED"},
		{0xac00, "HANGUL SYLLABLE GA", "", "HANGUL SYLLABLE GA"},
		{0xd7a3, "HANGUL SYLLABLE HIH", "", "HANGUL SYLLABLE HIH"},
		{0xd800, "", "", "<lead surrogate-D800>"},
		{0xdc00, "", "", "<trail surrogate-DC00>"},
		{0xff08, "FULLWIDTH LEFT PARENTHESIS", "", "FULLWIDTH LEFT PARENTHESIS"},
		{0xffe5, "FULLWIDTH YEN SIGN", "", "FULLWIDTH YEN SIGN"},
		{0xffff, "", "", "<noncharacter-FFFF>"},
		{0x1d0c5, "BYZANTINE MUSICAL SYMBOL FHTORA SKLIRON CHROMA VASIS", "", "BYZANTINE MUSICAL SYMBOL FHTORA SKLIRON CHROMA VASIS"},
		{0x23456, "CJK UNIFIED IDEOGRAPH-23456", "", "CJK UNIFIED IDEOGRAPH-23456"},
	}

	for _, tn := range TestNames {
		if tn.name != "" {
			r := CharForName(UnicodeCharName, tn.name)
			if r != tn.code {
				t.Errorf("CharFromName(U_UNICODE_CHAR_NAME, %q) = '%c' (U+%d), expected %c (U+%d)", tn.name, r, r, tn.code, tn.code)
			}
		}
		if tn.extName != "" {
			r := CharForName(ExtendedCharName, tn.extName)
			if r != tn.code {
				t.Errorf("CharFromName(U_EXTENDED_CHAR_NAME, %q) = '%c' (U+%d), expected %c (U+%d)", tn.extName, r, r, tn.code, tn.code)
			}
		}
	}
}
