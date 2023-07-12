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

package ucase

import (
	"math/bits"

	"vitess.io/vitess/go/mysql/icuregex/internal/utf16"
)

func FoldRunes(str []rune) []rune {
	out := make([]rune, 0, len(str))
	for _, c := range str {
		r, exp := FullFolding(c)
		if exp == nil {
			out = append(out, r)
			continue
		}

		for len(exp) > 0 {
			r, exp = utf16.NextUnsafe(exp)
			out = append(out, r)
		}
	}
	return out
}

/*
  - Case folding is similar to lowercasing.
  - The result may be a simple mapping, i.e., a single code point, or
  - a full mapping, i.e., a string.
  - If the case folding for a code point is the same as its simple (1:1) lowercase mapping,
  - then only the lowercase mapping is stored.
    *
  - Some special cases are hardcoded because their conditions cannot be
  - parsed and processed from CaseFolding.txt.
    *
  - Unicode 3.2 CaseFolding.txt specifies for its status field:

# C: common case folding, common mappings shared by both simple and full mappings.
# F: full case folding, mappings that cause strings to grow in length. Multiple characters are separated by spaces.
# S: simple case folding, mappings to single characters where different from F.
# T: special case for uppercase I and dotted uppercase I
#    - For non-Turkic languages, this mapping is normally not used.
#    - For Turkic languages (tr, az), this mapping can be used instead of the normal mapping for these characters.
#
# Usage:
#  A. To do a simple case folding, use the mappings with status C + S.
#  B. To do a full case folding, use the mappings with status C + F.
#
#    The mappings with status T can be used or omitted depending on the desired case-folding
#    behavior. (The default option is to exclude them.)

  - Unicode 3.2 has 'T' mappings as follows:

0049; T; 0131; # LATIN CAPITAL LETTER I
0130; T; 0069; # LATIN CAPITAL LETTER I WITH DOT ABOVE

  - while the default mappings for these code points are:

0049; C; 0069; # LATIN CAPITAL LETTER I
0130; F; 0069 0307; # LATIN CAPITAL LETTER I WITH DOT ABOVE

  - U+0130 has no simple case folding (simple-case-folds to itself).
*/
func Fold(c rune) rune {
	props := ucase.trie.Get16(c)
	if !hasException(props) {
		if isUpperOrTitle(props) {
			c += getDelta(props)
		}
	} else {
		pe := getExceptions(props)
		excWord := pe[0]
		pe = pe[1:]
		if (excWord & excConditionalFold) != 0 {
			/* special case folding mappings, hardcoded */
			/* default mappings */
			if c == 0x49 {
				/* 0049; C; 0069; # LATIN CAPITAL LETTER I */
				return 0x69
			} else if c == 0x130 {
				/* no simple case folding for U+0130 */
				return c
			}
		}
		if (excWord & excNoSimpleCaseFolding) != 0 {
			return c
		}
		if hasSlot(excWord, excDelta) && isUpperOrTitle(props) {
			var delta int32
			delta, _ = getSlotValue(excWord, excDelta, pe)
			if excWord&excDeltaIsNegative == 0 {
				return c + delta
			}
			return c - delta
		}

		var idx int32
		if hasSlot(excWord, excFold) {
			idx = excFold
		} else if hasSlot(excWord, excLower) {
			idx = excLower
		} else {
			return c
		}
		c, _ = getSlotValue(excWord, idx, pe)
	}
	return c
}

func FullFolding(c rune) (rune, []uint16) {
	result := c
	props := ucase.trie.Get16(c)

	if !hasException(props) {
		if isUpperOrTitle(props) {
			result = c + getDelta(props)
		}
		return result, nil
	}

	pe := getExceptions(props)
	excWord := pe[0]
	pe = pe[1:]
	var idx int32

	if excWord&excConditionalFold != 0 {
		/* use hardcoded conditions and mappings */
		/* default mappings */
		if c == 0x49 {
			/* 0049; C; 0069; # LATIN CAPITAL LETTER I */
			return 0x69, nil
		} else if c == 0x130 {
			/* 0130; F; 0069 0307; # LATIN CAPITAL LETTER I WITH DOT ABOVE */
			return -1, []uint16{0x69, 0x307}
		}
	} else if hasSlot(excWord, excFullMappings) {
		full, pe := getSlotValue(excWord, excFullMappings, pe)

		/* start of full case mapping strings */
		pe = pe[1:]

		/* skip the lowercase result string */
		pe = pe[full&fullLower:]
		full = (full >> 4) & 0xf

		if full != 0 {
			/* set the output pointer to the result string */
			return -1, pe[:full]
		}
	}

	if excWord&excNoSimpleCaseFolding != 0 {
		return result, nil
	}
	if hasSlot(excWord, excDelta) && isUpperOrTitle(props) {
		delta, _ := getSlotValue(excWord, excDelta, pe)
		if excWord&excDeltaIsNegative == 0 {
			return c + delta, nil
		}
		return c - delta, nil
	}
	if hasSlot(excWord, excFold) {
		idx = excFold
	} else if hasSlot(excWord, excLower) {
		idx = excLower
	} else {
		return c, nil
	}
	result, _ = getSlotValue(excWord, idx, pe)
	return result, nil
}

const (
	excLower = iota
	excFold
	excUpper
	excTitle
	excDelta
	exc5 /* reserved */
	excClosure
	excFullMappings
)

const (
	/* complex/conditional mappings */
	excConditionalSpecial  = 0x4000
	excConditionalFold     = 0x8000
	excNoSimpleCaseFolding = 0x200
	excDeltaIsNegative     = 0x400
	excSensitive           = 0x800

	excDoubleSlots = 0x100
)

func isUpperOrTitle(props uint16) bool {
	return props&2 != 0
}

func getDelta(props uint16) rune {
	return rune(int16(props) >> 7)
}

func getExceptions(props uint16) []uint16 {
	return ucase.exceptions[props>>4:]
}

func hasSlot(flags uint16, idx int32) bool {
	return (flags & (1 << idx)) != 0
}

func slotOffset(flags uint16, idx int32) int {
	return bits.OnesCount8(uint8(flags & ((1 << idx) - 1)))
}

func getSlotValue(excWord uint16, idx int32, pExc16 []uint16) (int32, []uint16) {
	if excWord&excDoubleSlots == 0 {
		pExc16 = pExc16[slotOffset(excWord, idx):]
		return int32(pExc16[0]), pExc16
	}
	pExc16 = pExc16[2*slotOffset(excWord, idx):]
	return (int32(pExc16[0]) << 16) | int32(pExc16[1]), pExc16[1:]
}
