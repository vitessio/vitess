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

package ubidi

import (
	"fmt"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

const (
	UBIDI_IX_INDEX_TOP = iota
	UBIDI_IX_LENGTH
	UBIDI_IX_TRIE_SIZE
	UBIDI_IX_MIRROR_LENGTH

	UBIDI_IX_JG_START
	UBIDI_IX_JG_LIMIT
	UBIDI_IX_JG_START2 /* new in format version 2.2, ICU 54 */
	UBIDI_IX_JG_LIMIT2

	UBIDI_MAX_VALUES_INDEX
	UBIDI_IX_TOP
)

var ubidi struct {
	indexes []int32
	trie    *utrie.UTrie2
	mirrors []uint32
	jg      []uint8
	jg2     []uint8
}

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.FormatVersion[0] == 2
	})
	if err != nil {
		return err
	}

	count := int32(bytes.Uint32())
	if count < UBIDI_IX_TOP {
		return fmt.Errorf("indexes[0] too small in ucase.icu")
	}

	ubidi.indexes = make([]int32, count)
	ubidi.indexes[0] = count

	for i := int32(1); i < count; i++ {
		ubidi.indexes[i] = int32(bytes.Uint32())
	}

	ubidi.trie, err = utrie.UTrie2FromBytes(bytes)
	if err != nil {
		return err
	}

	expectedTrieLength := ubidi.indexes[UBIDI_IX_TRIE_SIZE]
	trieLength := ubidi.trie.SerializedLength()

	if trieLength > expectedTrieLength {
		return fmt.Errorf("ucase.icu: not enough bytes for the trie")
	}

	bytes.Skip(expectedTrieLength - trieLength)

	if n := ubidi.indexes[UBIDI_IX_MIRROR_LENGTH]; n > 0 {
		ubidi.mirrors = bytes.Uint32Slice(n)
	}
	if n := ubidi.indexes[UBIDI_IX_JG_LIMIT] - ubidi.indexes[UBIDI_IX_JG_START]; n > 0 {
		ubidi.jg = bytes.Uint8Slice(n)
	}
	if n := ubidi.indexes[UBIDI_IX_JG_LIMIT2] - ubidi.indexes[UBIDI_IX_JG_START2]; n > 0 {
		ubidi.jg2 = bytes.Uint8Slice(n)
	}

	return nil
}

func init() {
	b := udata.NewBytes(icudata.UBidi)
	if err := readData(b); err != nil {
		panic(err)
	}
}

const (
	/* UBIDI_CLASS_SHIFT=0, */ /* bidi class: 5 bits (4..0) */
	UBIDI_JT_SHIFT             = 5 /* joining type: 3 bits (7..5) */

	UBIDI_BPT_SHIFT = 8 /* Bidi_Paired_Bracket_Type(bpt): 2 bits (9..8) */

	UBIDI_JOIN_CONTROL_SHIFT = 10
	UBIDI_BIDI_CONTROL_SHIFT = 11

	UBIDI_IS_MIRRORED_SHIFT  = 12 /* 'is mirrored' */
	UBIDI_MIRROR_DELTA_SHIFT = 13 /* bidi mirroring delta: 3 bits (15..13) */

	UBIDI_MAX_JG_SHIFT = 16 /* max JG value in indexes[UBIDI_MAX_VALUES_INDEX] bits 23..16 */
)

/**
 * Bidi Paired Bracket Type constants.
 *
 * @see UCHAR_BIDI_PAIRED_BRACKET_TYPE
 * @stable ICU 52
 */
type UBidiPairedBracketType int32

/*
 * Note: UBidiPairedBracketType constants are parsed by preparseucd.py.
 * It matches lines like
 *     U_BPT_<Unicode Bidi_Paired_Bracket_Type value name>
 */
const (
	/** Not a paired bracket. @stable ICU 52 */
	U_BPT_NONE = iota
	/** Open paired bracket. @stable ICU 52 */
	U_BPT_OPEN
	/** Close paired bracket. @stable ICU 52 */
	U_BPT_CLOSE
)

const UBIDI_CLASS_MASK = 0x0000001f
const UBIDI_JT_MASK = 0x000000e0
const UBIDI_BPT_MASK = 0x00000300

/**
 * Joining Type constants.
 *
 * @see UCHAR_JOINING_TYPE
 * @stable ICU 2.2
 */
type UJoiningType int32

/*
 * Note: UJoiningType constants are parsed by preparseucd.py.
 * It matches lines like
 *     U_JT_<Unicode Joining_Type value name>
 */
const (
	U_JT_NON_JOINING   UJoiningType = iota /*[U]*/
	U_JT_JOIN_CAUSING                      /*[C]*/
	U_JT_DUAL_JOINING                      /*[D]*/
	U_JT_LEFT_JOINING                      /*[L]*/
	U_JT_RIGHT_JOINING                     /*[R]*/
	U_JT_TRANSPARENT                       /*[T]*/
)

/**
 * Joining Group constants.
 *
 * @see UCHAR_JOINING_GROUP
 * @stable ICU 2.2
 */
type UJoiningGroup int32

/*
 * Note: UJoiningGroup constants are parsed by preparseucd.py.
 * It matches lines like
 *     U_JG_<Unicode Joining_Group value name>
 */
const (
	U_JG_NO_JOINING_GROUP UJoiningGroup = iota
	U_JG_AIN
	U_JG_ALAPH
	U_JG_ALEF
	U_JG_BEH
	U_JG_BETH
	U_JG_DAL
	U_JG_DALATH_RISH
	U_JG_E
	U_JG_FEH
	U_JG_FINAL_SEMKATH
	U_JG_GAF
	U_JG_GAMAL
	U_JG_HAH
	U_JG_TEH_MARBUTA_GOAL /**< @stable ICU 4.6 */
	U_JG_HE
	U_JG_HEH
	U_JG_HEH_GOAL
	U_JG_HETH
	U_JG_KAF
	U_JG_KAPH
	U_JG_KNOTTED_HEH
	U_JG_LAM
	U_JG_LAMADH
	U_JG_MEEM
	U_JG_MIM
	U_JG_NOON
	U_JG_NUN
	U_JG_PE
	U_JG_QAF
	U_JG_QAPH
	U_JG_REH
	U_JG_REVERSED_PE
	U_JG_SAD
	U_JG_SADHE
	U_JG_SEEN
	U_JG_SEMKATH
	U_JG_SHIN
	U_JG_SWASH_KAF
	U_JG_SYRIAC_WAW
	U_JG_TAH
	U_JG_TAW
	U_JG_TEH_MARBUTA
	U_JG_TETH
	U_JG_WAW
	U_JG_YEH
	U_JG_YEH_BARREE
	U_JG_YEH_WITH_TAIL
	U_JG_YUDH
	U_JG_YUDH_HE
	U_JG_ZAIN
	U_JG_FE                    /**< @stable ICU 2.6 */
	U_JG_KHAPH                 /**< @stable ICU 2.6 */
	U_JG_ZHAIN                 /**< @stable ICU 2.6 */
	U_JG_BURUSHASKI_YEH_BARREE /**< @stable ICU 4.0 */
	U_JG_FARSI_YEH             /**< @stable ICU 4.4 */
	U_JG_NYA                   /**< @stable ICU 4.4 */
	U_JG_ROHINGYA_YEH          /**< @stable ICU 49 */
	U_JG_MANICHAEAN_ALEPH      /**< @stable ICU 54 */
	U_JG_MANICHAEAN_AYIN       /**< @stable ICU 54 */
	U_JG_MANICHAEAN_BETH       /**< @stable ICU 54 */
	U_JG_MANICHAEAN_DALETH     /**< @stable ICU 54 */
	U_JG_MANICHAEAN_DHAMEDH    /**< @stable ICU 54 */
	U_JG_MANICHAEAN_FIVE       /**< @stable ICU 54 */
	U_JG_MANICHAEAN_GIMEL      /**< @stable ICU 54 */
	U_JG_MANICHAEAN_HETH       /**< @stable ICU 54 */
	U_JG_MANICHAEAN_HUNDRED    /**< @stable ICU 54 */
	U_JG_MANICHAEAN_KAPH       /**< @stable ICU 54 */
	U_JG_MANICHAEAN_LAMEDH     /**< @stable ICU 54 */
	U_JG_MANICHAEAN_MEM        /**< @stable ICU 54 */
	U_JG_MANICHAEAN_NUN        /**< @stable ICU 54 */
	U_JG_MANICHAEAN_ONE        /**< @stable ICU 54 */
	U_JG_MANICHAEAN_PE         /**< @stable ICU 54 */
	U_JG_MANICHAEAN_QOPH       /**< @stable ICU 54 */
	U_JG_MANICHAEAN_RESH       /**< @stable ICU 54 */
	U_JG_MANICHAEAN_SADHE      /**< @stable ICU 54 */
	U_JG_MANICHAEAN_SAMEKH     /**< @stable ICU 54 */
	U_JG_MANICHAEAN_TAW        /**< @stable ICU 54 */
	U_JG_MANICHAEAN_TEN        /**< @stable ICU 54 */
	U_JG_MANICHAEAN_TETH       /**< @stable ICU 54 */
	U_JG_MANICHAEAN_THAMEDH    /**< @stable ICU 54 */
	U_JG_MANICHAEAN_TWENTY     /**< @stable ICU 54 */
	U_JG_MANICHAEAN_WAW        /**< @stable ICU 54 */
	U_JG_MANICHAEAN_YODH       /**< @stable ICU 54 */
	U_JG_MANICHAEAN_ZAYIN      /**< @stable ICU 54 */
	U_JG_STRAIGHT_WAW          /**< @stable ICU 54 */
	U_JG_AFRICAN_FEH           /**< @stable ICU 58 */
	U_JG_AFRICAN_NOON          /**< @stable ICU 58 */
	U_JG_AFRICAN_QAF           /**< @stable ICU 58 */

	U_JG_MALAYALAM_BHA  /**< @stable ICU 60 */
	U_JG_MALAYALAM_JA   /**< @stable ICU 60 */
	U_JG_MALAYALAM_LLA  /**< @stable ICU 60 */
	U_JG_MALAYALAM_LLLA /**< @stable ICU 60 */
	U_JG_MALAYALAM_NGA  /**< @stable ICU 60 */
	U_JG_MALAYALAM_NNA  /**< @stable ICU 60 */
	U_JG_MALAYALAM_NNNA /**< @stable ICU 60 */
	U_JG_MALAYALAM_NYA  /**< @stable ICU 60 */
	U_JG_MALAYALAM_RA   /**< @stable ICU 60 */
	U_JG_MALAYALAM_SSA  /**< @stable ICU 60 */
	U_JG_MALAYALAM_TTA  /**< @stable ICU 60 */

	U_JG_HANIFI_ROHINGYA_KINNA_YA /**< @stable ICU 62 */
	U_JG_HANIFI_ROHINGYA_PA       /**< @stable ICU 62 */

	U_JG_THIN_YEH      /**< @stable ICU 70 */
	U_JG_VERTICAL_TAIL /**< @stable ICU 70 */
)

/**
 * This specifies the language directional property of a character set.
 * @stable ICU 2.0
 */
type UCharDirection int32

/*
 * Note: UCharDirection constants and their API comments are parsed by preparseucd.py.
 * It matches pairs of lines like
 *     / ** <Unicode 1..3-letter Bidi_Class value> comment... * /
 *     U_<[A-Z_]+> = <integer>,
 */

const (
	/** L @stable ICU 2.0 */
	U_LEFT_TO_RIGHT UCharDirection = 0
	/** R @stable ICU 2.0 */
	U_RIGHT_TO_LEFT UCharDirection = 1
	/** EN @stable ICU 2.0 */
	U_EUROPEAN_NUMBER UCharDirection = 2
	/** ES @stable ICU 2.0 */
	U_EUROPEAN_NUMBER_SEPARATOR UCharDirection = 3
	/** ET @stable ICU 2.0 */
	U_EUROPEAN_NUMBER_TERMINATOR UCharDirection = 4
	/** AN @stable ICU 2.0 */
	U_ARABIC_NUMBER UCharDirection = 5
	/** CS @stable ICU 2.0 */
	U_COMMON_NUMBER_SEPARATOR UCharDirection = 6
	/** B @stable ICU 2.0 */
	U_BLOCK_SEPARATOR UCharDirection = 7
	/** S @stable ICU 2.0 */
	U_SEGMENT_SEPARATOR UCharDirection = 8
	/** WS @stable ICU 2.0 */
	U_WHITE_SPACE_NEUTRAL UCharDirection = 9
	/** ON @stable ICU 2.0 */
	U_OTHER_NEUTRAL UCharDirection = 10
	/** LRE @stable ICU 2.0 */
	U_LEFT_TO_RIGHT_EMBEDDING UCharDirection = 11
	/** LRO @stable ICU 2.0 */
	U_LEFT_TO_RIGHT_OVERRIDE UCharDirection = 12
	/** AL @stable ICU 2.0 */
	U_RIGHT_TO_LEFT_ARABIC UCharDirection = 13
	/** RLE @stable ICU 2.0 */
	U_RIGHT_TO_LEFT_EMBEDDING UCharDirection = 14
	/** RLO @stable ICU 2.0 */
	U_RIGHT_TO_LEFT_OVERRIDE UCharDirection = 15
	/** PDF @stable ICU 2.0 */
	U_POP_DIRECTIONAL_FORMAT UCharDirection = 16
	/** NSM @stable ICU 2.0 */
	U_DIR_NON_SPACING_MARK UCharDirection = 17
	/** BN @stable ICU 2.0 */
	U_BOUNDARY_NEUTRAL UCharDirection = 18
	/** FSI @stable ICU 52 */
	U_FIRST_STRONG_ISOLATE UCharDirection = 19
	/** LRI @stable ICU 52 */
	U_LEFT_TO_RIGHT_ISOLATE UCharDirection = 20
	/** RLI @stable ICU 52 */
	U_RIGHT_TO_LEFT_ISOLATE UCharDirection = 21
	/** PDI @stable ICU 52 */
	U_POP_DIRECTIONAL_ISOLATE UCharDirection = 22
)

type PropertySet interface {
	AddRune(ch rune)
	AddRuneRange(from rune, to rune)
}

func AddPropertyStarts(sa PropertySet) {
	/* add the start code point of each same-value range of the trie */
	ubidi.trie.Enum(nil, func(start, _ rune, _ uint32) bool {
		sa.AddRune(start)
		return true
	})

	/* add the code points from the bidi mirroring table */
	length := ubidi.indexes[UBIDI_IX_MIRROR_LENGTH]
	for i := int32(0); i < length; i++ {
		c := mirrorCodePoint(rune(ubidi.mirrors[i]))
		sa.AddRuneRange(c, c+1)
	}

	/* add the code points from the Joining_Group array where the value changes */
	start := ubidi.indexes[UBIDI_IX_JG_START]
	limit := ubidi.indexes[UBIDI_IX_JG_LIMIT]
	jgArray := ubidi.jg[:]
	for {
		prev := uint8(0)
		for start < limit {
			jg := jgArray[0]
			jgArray = jgArray[1:]
			if jg != prev {
				sa.AddRune(start)
				prev = jg
			}
			start++
		}
		if prev != 0 {
			/* add the limit code point if the last value was not 0 (it is now start==limit) */
			sa.AddRune(limit)
		}
		if limit == ubidi.indexes[UBIDI_IX_JG_LIMIT] {
			/* switch to the second Joining_Group range */
			start = ubidi.indexes[UBIDI_IX_JG_START2]
			limit = ubidi.indexes[UBIDI_IX_JG_LIMIT2]
			jgArray = ubidi.jg2[:]
		} else {
			break
		}
	}

	/* add code points with hardcoded properties, plus the ones following them */

	/* (none right now) */
}

func HasFlag(props uint16, shift int) bool {
	return ((props >> shift) & 1) != 0
}

func mirrorCodePoint(m rune) rune {
	return m & 0x1fffff
}

func IsJoinControl(c rune) bool {
	props := ubidi.trie.Get16(c)
	return HasFlag(props, UBIDI_JOIN_CONTROL_SHIFT)
}

func JoiningType(c rune) UJoiningType {
	props := ubidi.trie.Get16(c)
	return UJoiningType((props & UBIDI_JT_MASK) >> UBIDI_JT_SHIFT)
}

func JoiningGroup(c rune) UJoiningGroup {
	start := ubidi.indexes[UBIDI_IX_JG_START]
	limit := ubidi.indexes[UBIDI_IX_JG_LIMIT]
	if start <= c && c < limit {
		return UJoiningGroup(ubidi.jg[c-start])
	}
	start = ubidi.indexes[UBIDI_IX_JG_START2]
	limit = ubidi.indexes[UBIDI_IX_JG_LIMIT2]
	if start <= c && c < limit {
		return UJoiningGroup(ubidi.jg2[c-start])
	}
	return U_JG_NO_JOINING_GROUP
}

func IsMirrored(c rune) bool {
	props := ubidi.trie.Get16(c)
	return HasFlag(props, UBIDI_IS_MIRRORED_SHIFT)
}

func IsBidiControl(c rune) bool {
	props := ubidi.trie.Get16(c)
	return HasFlag(props, UBIDI_BIDI_CONTROL_SHIFT)
}

func PairedBracketType(c rune) UBidiPairedBracketType {
	props := ubidi.trie.Get16(c)
	return UBidiPairedBracketType((props & UBIDI_BPT_MASK) >> UBIDI_BPT_SHIFT)
}

func Class(c rune) UCharDirection {
	props := ubidi.trie.Get16(c)
	return UCharDirection(props & UBIDI_CLASS_MASK)
}
