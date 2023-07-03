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
	"errors"

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

const (
	ixIndexTop      = 0
	ixLength        = 1
	ixTrieSize      = 2
	ixExcLength     = 3
	ixUnfoldLength  = 4
	ixMaxFullLength = 15
	ixTop           = 16
)

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.DataFormat[0] == 0x63 &&
			info.DataFormat[1] == 0x41 &&
			info.DataFormat[2] == 0x53 &&
			info.DataFormat[3] == 0x45 &&
			info.FormatVersion[0] == 4
	})
	if err != nil {
		return err
	}

	count := int32(bytes.Uint32())
	if count < ixTop {
		return errors.New("indexes[0] too small in ucase.icu")
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

	expectedTrieLength := indexes[ixTrieSize]
	trieLength := ucase.trie.SerializedLength()

	if trieLength > expectedTrieLength {
		return errors.New("ucase.icu: not enough bytes for the trie")
	}

	bytes.Skip(expectedTrieLength - trieLength)

	if n := indexes[ixExcLength]; n > 0 {
		ucase.exceptions = bytes.Uint16Slice(n)
	}
	if n := indexes[ixUnfoldLength]; n > 0 {
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

type propertySet interface {
	AddRune(ch rune)
}

func AddPropertyStarts(sa propertySet) {
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
	fullMappingsMaxLength = (4 * 0xf)
	closureMaxLength      = 0xf

	fullLower   = 0xf
	fullFolding = 0xf0
	fullUpper   = 0xf00
	fullTitle   = 0xf000
)

func AddCaseClosure(c rune, sa propertySet) {
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
		if getPropsType(props) != None {
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
		for idx = excLower; idx <= excTitle; idx++ {
			if hasSlot(excWord, idx) {
				c, _ = getSlotValue(excWord, idx, pe)
				sa.AddRune(c)
			}
		}
		if hasSlot(excWord, excDelta) {
			delta, _ := getSlotValue(excWord, excDelta, pe)
			if excWord&excDeltaIsNegative == 0 {
				sa.AddRune(c + delta)
			} else {
				sa.AddRune(c - delta)
			}
		}

		/* get the closure string pointer & length */
		if hasSlot(excWord, excClosure) {
			closureLength, pe1 := getSlotValue(excWord, excClosure, pe)
			closureLength &= closureMaxLength  /* higher bits are reserved */
			closure = pe1[1 : 1+closureLength] /* behind this slot, unless there are full case mappings */
		}

		/* add the full case folding */
		if hasSlot(excWord, excFullMappings) {
			fullLength, pe1 := getSlotValue(excWord, excFullMappings, pe)

			/* start of full case mapping strings */
			pe1 = pe1[1:]

			fullLength &= 0xffff /* bits 16 and higher are reserved */

			/* skip the lowercase result string */
			pe1 = pe1[fullLength&fullLower:]
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

const dotMask = 0x60

const (
	noDot       = 0    /* normal characters with cc=0 */
	softDotted  = 0x20 /* soft-dotted characters with cc=0 */
	above       = 0x40 /* "above" accents with cc=230 */
	otherAccent = 0x60 /* other accent character (0<cc!=230) */
)

const (
	ignorable = 4
	exception = 8
	sensitive = 0x10
)

/* UCASE_EXC_DOT_MASK=UCASE_DOT_MASK<<excDotShift */
const excDotShift = 7

func hasException(props uint16) bool {
	return (props & exception) != 0
}

func IsSoftDotted(c rune) bool {
	return getDotType(c) == softDotted
}

/** @return UCASE_NO_DOT, UCASE_SOFT_DOTTED, UCASE_ABOVE, UCASE_OTHER_ACCENT */
func getDotType(c rune) int32 {
	props := ucase.trie.Get16(c)
	if !hasException(props) {
		return int32(props & dotMask)
	}
	pe := getExceptions(props)
	return int32((pe[0] >> excDotShift) & dotMask)
}

func IsCaseSensitive(c rune) bool {
	props := ucase.trie.Get16(c)
	if !hasException(props) {
		return (props & sensitive) != 0
	}
	pe := getExceptions(props)
	return (pe[0] & excSensitive) != 0
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

		if excWord&excConditionalSpecial != 0 {
			/* use hardcoded conditions and mappings */
			if c == 0x130 {
				return 2
			}
			/* no known conditional special case mapping, use a normal mapping */
		} else if hasSlot(excWord, excFullMappings) {
			full, _ := getSlotValue(excWord, excFullMappings, pe)
			full = full & fullLower
			if full != 0 {
				/* return the string length */
				return full
			}
		}

		if hasSlot(excWord, excDelta) && isUpperOrTitle(props) {
			delta, _ := getSlotValue(excWord, excDelta, pe)
			if (excWord & excDeltaIsNegative) == 0 {
				return c + delta
			}
			return c - delta
		}
		if hasSlot(excWord, excLower) {
			result, _ = getSlotValue(excWord, excLower, pe)
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
		if getPropsType(props) == Lower {
			result = c + getDelta(props)
		}
	} else {
		pe := getExceptions(props)
		excWord := pe[0]
		pe = pe[1:]

		if excWord&excConditionalSpecial != 0 {
			if c == 0x0587 {
				return 2
			}
			/* no known conditional special case mapping, use a normal mapping */
		} else if hasSlot(excWord, excFullMappings) {
			full, _ := getSlotValue(excWord, excFullMappings, pe)

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

		if hasSlot(excWord, excDelta) && getPropsType(props) == Lower {
			delta, _ := getSlotValue(excWord, excDelta, pe)
			if (excWord & excDeltaIsNegative) == 0 {
				return c + delta
			}
			return c - delta
		}
		var idx int32
		if !upperNotTitle && hasSlot(excWord, excTitle) {
			idx = excTitle
		} else if hasSlot(excWord, excUpper) {
			/* here, titlecase is same as uppercase */
			idx = excUpper
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

type Type int32

const (
	None Type = iota
	Lower
	Upper
	Title
)

const typeMask = 3

func GetType(c rune) Type {
	props := ucase.trie.Get16(c)
	return getPropsType(props)
}

func getPropsType(props uint16) Type {
	return Type(props & typeMask)
}
