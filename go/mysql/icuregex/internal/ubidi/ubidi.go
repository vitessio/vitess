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

const (
	ixIndexTop = iota
	ixLength
	ixTrieSize
	ixMirrorLength

	ixJgStart
	ixJgLimit
	ixJgStart2 /* new in format version 2.2, ICU 54 */
	ixJgLimit2

	maxValuesIndex
	ixTop
)

const (
	/* UBIDI_CLASS_SHIFT=0, */ /* bidi class: 5 bits (4..0) */
	jtShift                    = 5 /* joining type: 3 bits (7..5) */

	bptShift = 8 /* Bidi_Paired_Bracket_Type(bpt): 2 bits (9..8) */

	joinControlShift = 10
	bidiControlShift = 11

	isMirroredShift = 12 /* 'is mirrored' */
)

/**
 * Bidi Paired Bracket Type constants.
 *
 * @see UCHAR_BIDI_PAIRED_BRACKET_TYPE
 * @stable ICU 52
 */
type UPairedBracketType int32

/*
 * Note: UBidiPairedBracketType constants are parsed by preparseucd.py.
 * It matches lines like
 *     U_BPT_<Unicode Bidi_Paired_Bracket_Type value name>
 */
const (
	/** Not a paired bracket. @stable ICU 52 */
	BptNone UPairedBracketType = iota
	/** Open paired bracket. @stable ICU 52 */
	BptOpen
	/** Close paired bracket. @stable ICU 52 */
	BptClose
)

const classMask = 0x0000001f
const jtMask = 0x000000e0
const bptMask = 0x00000300

/**
 * Joining Type constants.
 *
 * @see UCHAR_JOINING_TYPE
 * @stable ICU 2.2
 */
type JoiningType int32

/*
 * Note: UJoiningType constants are parsed by preparseucd.py.
 * It matches lines like
 *     U_JT_<Unicode Joining_Type value name>
 */
const (
	JtNonJoining   JoiningType = iota /*[U]*/
	JtJoinCausing                     /*[C]*/
	JtDualJoining                     /*[D]*/
	JtLeftJoining                     /*[L]*/
	JtRightJoining                    /*[R]*/
	JtTransparent                     /*[T]*/
)

/**
 * Joining Group constants.
 *
 * @see UCHAR_JOINING_GROUP
 * @stable ICU 2.2
 */
type JoiningGroup int32

/*
 * Note: UJoiningGroup constants are parsed by preparseucd.py.
 * It matches lines like
 *     U_JG_<Unicode Joining_Group value name>
 */
const (
	JgNoJoiningGroup JoiningGroup = iota
	JgAin
	JgAlaph
	JgAlef
	JgBeh
	JgBeth
	JgDal
	JgDalathRish
	JgE
	JgFeh
	JgFinalSemkath
	JgGaf
	JgGamal
	JgHah
	JgTehMarbutaGoal /**< @stable ICU 4.6 */
	JgHe
	JgHeh
	JgHehGoal
	JgHeth
	JgKaf
	JgKaph
	JgKnottedHeh
	JgLam
	JgLamadh
	JgMeem
	JgMim
	JgNoon
	JgNun
	JgPe
	JgQaf
	JgQaph
	JgReh
	JgReversedPe
	JgSad
	JgSadhe
	JgSeen
	JgSemkath
	JgShin
	JgSwashKaf
	JgSyriacWaw
	JgTah
	JgTaw
	JgTehMarbuta
	JgTeth
	JgWaw
	JgYeh
	JgYehBarree
	JgYehWithTail
	JgYudh
	JgYudhHe
	JgZain
	JgFe                   /**< @stable ICU 2.6 */
	JgKhaph                /**< @stable ICU 2.6 */
	JgZhain                /**< @stable ICU 2.6 */
	JgBurushashkiYehBarree /**< @stable ICU 4.0 */
	JgFarsiYeh             /**< @stable ICU 4.4 */
	JgNya                  /**< @stable ICU 4.4 */
	JgRohingyaYeh          /**< @stable ICU 49 */
	JgManichaeanAleph      /**< @stable ICU 54 */
	JgManichaeanAyin       /**< @stable ICU 54 */
	JgManichaeanBeth       /**< @stable ICU 54 */
	JgManichaeanDaleth     /**< @stable ICU 54 */
	JgManichaeanDhamedh    /**< @stable ICU 54 */
	JgManichaeanFive       /**< @stable ICU 54 */
	JgManichaeanGimel      /**< @stable ICU 54 */
	JgManichaeanHeth       /**< @stable ICU 54 */
	JgManichaeanHundred    /**< @stable ICU 54 */
	JgManichaeanKaph       /**< @stable ICU 54 */
	JgManichaeanLamedh     /**< @stable ICU 54 */
	JgManichaeanMem        /**< @stable ICU 54 */
	JgManichaeanNun        /**< @stable ICU 54 */
	JgManichaeanOne        /**< @stable ICU 54 */
	JgManichaeanPe         /**< @stable ICU 54 */
	JgManichaeanQoph       /**< @stable ICU 54 */
	JgManichaeanResh       /**< @stable ICU 54 */
	JgManichaeanSadhe      /**< @stable ICU 54 */
	JgManichaeanSamekh     /**< @stable ICU 54 */
	JgManichaeanTaw        /**< @stable ICU 54 */
	JgManichaeanTen        /**< @stable ICU 54 */
	JgManichaeanTeth       /**< @stable ICU 54 */
	JgManichaeanThamedh    /**< @stable ICU 54 */
	JgManichaeanTwenty     /**< @stable ICU 54 */
	JgManichaeanWaw        /**< @stable ICU 54 */
	JgManichaeanYodh       /**< @stable ICU 54 */
	JgManichaeanZayin      /**< @stable ICU 54 */
	JgStraightWaw          /**< @stable ICU 54 */
	JgAfricanFeh           /**< @stable ICU 58 */
	JgAfricanNoon          /**< @stable ICU 58 */
	JgAfricanQaf           /**< @stable ICU 58 */

	JgMalayalamBha  /**< @stable ICU 60 */
	JgMalayalamJa   /**< @stable ICU 60 */
	JgMalayalamLla  /**< @stable ICU 60 */
	JgMalayalamLlla /**< @stable ICU 60 */
	JgMalayalamNga  /**< @stable ICU 60 */
	JgMalayalamNna  /**< @stable ICU 60 */
	JgMalayalamNnna /**< @stable ICU 60 */
	JgMalayalamNya  /**< @stable ICU 60 */
	JgMalayalamRa   /**< @stable ICU 60 */
	JgMalayalamSsa  /**< @stable ICU 60 */
	JgMalayalamTta  /**< @stable ICU 60 */

	JgHanafiRohingyaKinnaYa /**< @stable ICU 62 */
	JgHanafiRohingyaPa      /**< @stable ICU 62 */

	JgThinYeh      /**< @stable ICU 70 */
	JgVerticalTail /**< @stable ICU 70 */
)

/**
 * This specifies the language directional property of a character set.
 * @stable ICU 2.0
 */
type CharDirection int32

/*
 * Note: UCharDirection constants and their API comments are parsed by preparseucd.py.
 * It matches pairs of lines like
 *     / ** <Unicode 1..3-letter Bidi_Class value> comment... * /
 *     U_<[A-Z_]+> = <integer>,
 */

const (
	/** L @stable ICU 2.0 */
	LeftToRight CharDirection = 0
	/** R @stable ICU 2.0 */
	RightToLeft CharDirection = 1
	/** EN @stable ICU 2.0 */
	EuropeanNumber CharDirection = 2
	/** ES @stable ICU 2.0 */
	EuropeanNumberSeparator CharDirection = 3
	/** ET @stable ICU 2.0 */
	EuropeanNumberTerminator CharDirection = 4
	/** AN @stable ICU 2.0 */
	ArabicNumber CharDirection = 5
	/** CS @stable ICU 2.0 */
	CommonNumberSeparator CharDirection = 6
	/** B @stable ICU 2.0 */
	BlockSeparator CharDirection = 7
	/** S @stable ICU 2.0 */
	SegmentSeparator CharDirection = 8
	/** WS @stable ICU 2.0 */
	WhiteSpaceNeutral CharDirection = 9
	/** ON @stable ICU 2.0 */
	OtherNeutral CharDirection = 10
	/** LRE @stable ICU 2.0 */
	LeftToRightEmbedding CharDirection = 11
	/** LRO @stable ICU 2.0 */
	LeftToRightOverride CharDirection = 12
	/** AL @stable ICU 2.0 */
	RightToLeftArabic CharDirection = 13
	/** RLE @stable ICU 2.0 */
	RightToLeftEmbedding CharDirection = 14
	/** RLO @stable ICU 2.0 */
	RightToLeftOverride CharDirection = 15
	/** PDF @stable ICU 2.0 */
	PopDirectionalFormat CharDirection = 16
	/** NSM @stable ICU 2.0 */
	DirNonSpacingMark CharDirection = 17
	/** BN @stable ICU 2.0 */
	BoundaryNeutral CharDirection = 18
	/** FSI @stable ICU 52 */
	StrongIsolate CharDirection = 19
	/** LRI @stable ICU 52 */
	LeftToRightIsolate CharDirection = 20
	/** RLI @stable ICU 52 */
	RightToLeftIsolate CharDirection = 21
	/** PDI @stable ICU 52 */
	PopDirectionalIsolate CharDirection = 22
)

type propertySet interface {
	AddRune(ch rune)
	AddRuneRange(from rune, to rune)
}

func AddPropertyStarts(sa propertySet) {
	/* add the start code point of each same-value range of the trie */
	trie().Enum(nil, func(start, _ rune, _ uint32) bool {
		sa.AddRune(start)
		return true
	})

	idxs := indexes()
	mrs := mirrors()
	/* add the code points from the bidi mirroring table */
	length := idxs[ixMirrorLength]
	for i := int32(0); i < length; i++ {
		c := mirrorCodePoint(rune(mrs[i]))
		sa.AddRuneRange(c, c+1)
	}

	/* add the code points from the Joining_Group array where the value changes */
	start := idxs[ixJgStart]
	limit := idxs[ixJgLimit]
	jgArray := jg()
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
		if limit == idxs[ixJgLimit] {
			/* switch to the second Joining_Group range */
			start = idxs[ixJgStart2]
			limit = idxs[ixJgLimit2]
			jgArray = jg2()
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
	props := trie().Get16(c)
	return HasFlag(props, joinControlShift)
}

func JoinType(c rune) JoiningType {
	props := trie().Get16(c)
	return JoiningType((props & jtMask) >> jtShift)
}

func JoinGroup(c rune) JoiningGroup {
	idxs := indexes()
	start := idxs[ixJgStart]
	limit := idxs[ixJgLimit]
	if start <= c && c < limit {
		return JoiningGroup(jg()[c-start])
	}
	start = idxs[ixJgStart2]
	limit = idxs[ixJgLimit2]
	if start <= c && c < limit {
		return JoiningGroup(jg2()[c-start])
	}
	return JgNoJoiningGroup
}

func IsMirrored(c rune) bool {
	props := trie().Get16(c)
	return HasFlag(props, isMirroredShift)
}

func IsBidiControl(c rune) bool {
	props := trie().Get16(c)
	return HasFlag(props, bidiControlShift)
}

func PairedBracketType(c rune) UPairedBracketType {
	props := trie().Get16(c)
	return UPairedBracketType((props & bptMask) >> bptShift)
}

func Class(c rune) CharDirection {
	props := trie().Get16(c)
	return CharDirection(props & classMask)
}
