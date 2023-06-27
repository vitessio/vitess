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
	"fmt"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utf16"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var ucase struct {
	trie       *utrie.UTrie2
	exceptions []uint16
	unfold     []uint16
}

func readData(bytes *udata.Bytes) error {
	const (
		IX_INDEX_TOP       = 0
		IX_LENGTH          = 1
		IX_TRIE_SIZE       = 2
		IX_EXC_LENGTH      = 3
		IX_UNFOLD_LENGTH   = 4
		IX_MAX_FULL_LENGTH = 15
		IX_TOP             = 16
	)

	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.FormatVersion[0] == 4
	})
	if err != nil {
		return err
	}

	count := int32(bytes.Uint32())
	if count < IX_TOP {
		return fmt.Errorf("indexes[0] too small in ucase.icu")
	}

	indexes := make([]int32, count)
	indexes[0] = count

	for i := int32(1); i < count; i++ {
		indexes[i] = int32(bytes.Uint32())
	}

	ucase.trie, err = utrie.UTrie2FromBytes(bytes)
	if err != nil {
		return err
	}

	expectedTrieLength := indexes[IX_TRIE_SIZE]
	trieLength := ucase.trie.SerializedLength()

	if trieLength > expectedTrieLength {
		return fmt.Errorf("ucase.icu: not enough bytes for the trie")
	}

	bytes.Skip(expectedTrieLength - trieLength)

	if n := indexes[IX_EXC_LENGTH]; n > 0 {
		ucase.exceptions = bytes.Uint16Slice(n)
	}
	if n := indexes[IX_UNFOLD_LENGTH]; n > 0 {
		ucase.unfold = bytes.Uint16Slice(n)
	}

	return nil
}

func init() {
	b := udata.NewBytes(icudata.UCase)
	if err := readData(b); err != nil {
		panic(err)
	}
}

type PropertySet interface {
	AddRune(ch rune)
}

func AddPropertyStarts(sa PropertySet) {
	/* add the start code point of each same-value range of the trie */
	ucase.trie.Enum(nil, func(start, _ rune, _ uint32) bool {
		sa.AddRune(start)
		return true
	})

	/* add code points with hardcoded properties, plus the ones following them */

	/* (none right now, see comment below) */

	/*
	 * Omit code points with hardcoded specialcasing properties
	 * because we do not build property UnicodeSets for them right now.
	 */
}

const (
	UCASE_FULL_MAPPINGS_MAX_LENGTH = (4 * 0xf)
	UCASE_CLOSURE_MAX_LENGTH       = 0xf

	UCASE_FULL_LOWER   = 0xf
	UCASE_FULL_FOLDING = 0xf0
	UCASE_FULL_UPPER   = 0xf00
	UCASE_FULL_TITLE   = 0xf000
)

func AddCaseClosure(c rune, sa PropertySet) {
	/*
	 * Hardcode the case closure of i and its relatives and ignore the
	 * data file data for these characters.
	 * The Turkic dotless i and dotted I with their case mapping conditions
	 * and case folding option make the related characters behave specially.
	 * This code matches their closure behavior to their case folding behavior.
	 */

	switch c {
	case 0x49:
		/* regular i and I are in one equivalence class */
		sa.AddRune(0x69)
		return
	case 0x69:
		sa.AddRune(0x49)
		return
	case 0x130:
		/* dotted I is in a class with <0069 0307> (for canonical equivalence with <0049 0307>) */
		// the Regex engine calls removeAllStrings() on all UnicodeSets, so we don't need to insert them
		// sa->addString(sa->set, iDot, 2);
		return
	case 0x131:
		/* dotless i is in a class by itself */
		return
	default:
		/* otherwise use the data file data */
		break
	}

	props := ucase.trie.Get16(c)
	if !hasException(props) {
		if getPropsType(props) != UCASE_NONE {
			/* add the one simple case mapping, no matter what type it is */
			delta := getDelta(props)
			if delta != 0 {
				sa.AddRune(c + delta)
			}
		}
	} else {
		/*
		 * c has exceptions, so there may be multiple simple and/or
		 * full case mappings. Add them all.
		 */
		pe := getExceptions(props)
		excWord := pe[0]
		pe = pe[1:]
		var idx int32
		var closure []uint16

		/* add all simple case mappings */
		for idx = UCASE_EXC_LOWER; idx <= UCASE_EXC_TITLE; idx++ {
			if hasSlot(excWord, idx) {
				c, _ = getSlotValue(excWord, idx, pe)
				sa.AddRune(c)
			}
		}
		if hasSlot(excWord, UCASE_EXC_DELTA) {
			delta, _ := getSlotValue(excWord, UCASE_EXC_DELTA, pe)
			if excWord&UCASE_EXC_DELTA_IS_NEGATIVE == 0 {
				sa.AddRune(c + delta)
			} else {
				sa.AddRune(c - delta)
			}
		}

		/* get the closure string pointer & length */
		if hasSlot(excWord, UCASE_EXC_CLOSURE) {
			closureLength, pe1 := getSlotValue(excWord, UCASE_EXC_CLOSURE, pe)
			closureLength &= UCASE_CLOSURE_MAX_LENGTH /* higher bits are reserved */
			closure = pe1[1 : 1+closureLength]        /* behind this slot, unless there are full case mappings */
		}

		/* add the full case folding */
		if hasSlot(excWord, UCASE_EXC_FULL_MAPPINGS) {
			fullLength, pe1 := getSlotValue(excWord, UCASE_EXC_FULL_MAPPINGS, pe)

			/* start of full case mapping strings */
			pe1 = pe1[1:]

			fullLength &= 0xffff /* bits 16 and higher are reserved */

			/* skip the lowercase result string */
			pe1 = pe1[fullLength&UCASE_FULL_LOWER:]
			fullLength >>= 4

			/* skip adding the case folding strings */
			length := fullLength & 0xf
			pe1 = pe1[length:]

			/* skip the uppercase and titlecase strings */
			fullLength >>= 4
			pe1 = pe1[fullLength&0xf:]
			fullLength >>= 4
			pe1 = pe1[fullLength:]

			closure = pe1[:len(closure)]
		}

		/* add each code point in the closure string */
		for len(closure) > 0 {
			c, closure = utf16.NextUnsafe(closure)
			sa.AddRune(c)
		}
	}
}

const UCASE_DOT_MASK = 0x60

const (
	UCASE_NO_DOT       = 0    /* normal characters with cc=0 */
	UCASE_SOFT_DOTTED  = 0x20 /* soft-dotted characters with cc=0 */
	UCASE_ABOVE        = 0x40 /* "above" accents with cc=230 */
	UCASE_OTHER_ACCENT = 0x60 /* other accent character (0<cc!=230) */
)

const (
	UCASE_IGNORABLE = 4
	UCASE_EXCEPTION = 8
	UCASE_SENSITIVE = 0x10
)

/* UCASE_EXC_DOT_MASK=UCASE_DOT_MASK<<UCASE_EXC_DOT_SHIFT */
const UCASE_EXC_DOT_SHIFT = 7

func hasException(props uint16) bool {
	return (props & UCASE_EXCEPTION) != 0
}

func IsSoftDotted(c rune) bool {
	return getDotType(c) == UCASE_SOFT_DOTTED
}

/** @return UCASE_NO_DOT, UCASE_SOFT_DOTTED, UCASE_ABOVE, UCASE_OTHER_ACCENT */
func getDotType(c rune) int32 {
	props := ucase.trie.Get16(c)
	if !hasException(props) {
		return int32(props & UCASE_DOT_MASK)
	}
	pe := getExceptions(props)
	return int32((pe[0] >> UCASE_EXC_DOT_SHIFT) & UCASE_DOT_MASK)
}

func IsCaseSensitive(c rune) bool {
	props := ucase.trie.Get16(c)
	if !hasException(props) {
		return (props & UCASE_SENSITIVE) != 0
	} else {
		pe := getExceptions(props)
		return (pe[0] & UCASE_EXC_SENSITIVE) != 0
	}
}

func ToFullLower(c rune) rune {
	// The sign of the result has meaning, input must be non-negative so that it can be returned as is.
	result := c
	props := ucase.trie.Get16(c)
	if !hasException(props) {
		if isUpperOrTitle(props) {
			result = c + getDelta(props)
		}
	} else {
		pe := getExceptions(props)
		excWord := pe[0]
		pe = pe[1:]

		if excWord&UCASE_EXC_CONDITIONAL_SPECIAL != 0 {
			/* use hardcoded conditions and mappings */
			if c == 0x130 {
				return 2
			}
			/* no known conditional special case mapping, use a normal mapping */
		} else if hasSlot(excWord, UCASE_EXC_FULL_MAPPINGS) {
			full, _ := getSlotValue(excWord, UCASE_EXC_FULL_MAPPINGS, pe)
			full = full & UCASE_FULL_LOWER
			if full != 0 {
				/* return the string length */
				return full
			}
		}

		if hasSlot(excWord, UCASE_EXC_DELTA) && isUpperOrTitle(props) {
			delta, _ := getSlotValue(excWord, UCASE_EXC_DELTA, pe)
			if (excWord & UCASE_EXC_DELTA_IS_NEGATIVE) == 0 {
				return c + delta
			}
			return c - delta
		}
		if hasSlot(excWord, UCASE_EXC_LOWER) {
			result, _ = getSlotValue(excWord, UCASE_EXC_LOWER, pe)
		}
	}

	if result == c {
		return ^result
	}
	return result
}

func ToFullUpper(c rune) rune {
	return toUpperOrTitle(c, true)
}

func ToFullTitle(c rune) rune {
	return toUpperOrTitle(c, false)
}

func toUpperOrTitle(c rune, upperNotTitle bool) rune {
	result := c
	props := ucase.trie.Get16(c)
	if !hasException(props) {
		if getPropsType(props) == UCASE_LOWER {
			result = c + getDelta(props)
		}
	} else {
		pe := getExceptions(props)
		excWord := pe[0]
		pe = pe[1:]

		if excWord&UCASE_EXC_CONDITIONAL_SPECIAL != 0 {
			if c == 0x0587 {
				return 2
			}
			/* no known conditional special case mapping, use a normal mapping */
		} else if hasSlot(excWord, UCASE_EXC_FULL_MAPPINGS) {
			full, _ := getSlotValue(excWord, UCASE_EXC_FULL_MAPPINGS, pe)

			/* skip the lowercase and case-folding result strings */
			full >>= 8

			if upperNotTitle {
				full &= 0xf
			} else {
				/* skip the uppercase result string */
				full = (full >> 4) & 0xf
			}

			if full != 0 {
				/* return the string length */
				return full
			}
		}

		if hasSlot(excWord, UCASE_EXC_DELTA) && getPropsType(props) == UCASE_LOWER {
			delta, _ := getSlotValue(excWord, UCASE_EXC_DELTA, pe)
			if (excWord & UCASE_EXC_DELTA_IS_NEGATIVE) == 0 {
				return c + delta
			}
			return c - delta
		}
		var idx int32
		if !upperNotTitle && hasSlot(excWord, UCASE_EXC_TITLE) {
			idx = UCASE_EXC_TITLE
		} else if hasSlot(excWord, UCASE_EXC_UPPER) {
			/* here, titlecase is same as uppercase */
			idx = UCASE_EXC_UPPER
		} else {
			return ^c
		}
		result, _ = getSlotValue(excWord, idx, pe)
	}

	if result == c {
		return ^result
	}
	return result
}

func GetTypeOrIgnorable(c rune) int32 {
	props := ucase.trie.Get16(c)
	return int32(props & 7)
}

type UCaseType int32

const (
	UCASE_NONE UCaseType = iota
	UCASE_LOWER
	UCASE_UPPER
	UCASE_TITLE
)

const UCASE_TYPE_MASK = 3

func GetType(c rune) UCaseType {
	props := ucase.trie.Get16(c)
	return getPropsType(props)
}

func getPropsType(props uint16) UCaseType {
	return UCaseType(props & UCASE_TYPE_MASK)
}
