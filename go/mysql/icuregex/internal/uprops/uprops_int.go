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

package uprops

import (
	"vitess.io/vitess/go/mysql/icuregex/internal/normalizer"
	"vitess.io/vitess/go/mysql/icuregex/internal/ubidi"
	"vitess.io/vitess/go/mysql/icuregex/internal/uchar"
	"vitess.io/vitess/go/mysql/icuregex/internal/ulayout"
)

type intPropertyGetValue func(prop *intProperty, c rune, which Property) int32

type intProperty struct {
	column   propertySource
	mask     uint32
	shift    int32
	getValue intPropertyGetValue
}

const (
	blockMask  = 0x0001ff00
	blockShift = 8

	eaMask  = 0x000e0000
	eaShift = 17

	lbMask  = 0x03f00000
	lbShift = 20

	sbMask  = 0x000f8000
	sbShift = 15

	wbMask  = 0x00007c00
	wbShift = 10

	gcbMask  = 0x000003e0
	gcbShift = 5

	dtMask = 0x0000001f
)

type numericType int32

/**
 * Numeric Type constants.
 *
 * @see UCHAR_NUMERIC_TYPE
 * @stable ICU 2.2
 */
const (
	/*
	 * Note: UNumericType constants are parsed by preparseucd.py.
	 * It matches lines like
	 *     U_NT_<Unicode Numeric_Type value name>
	 */

	ntNone    numericType = iota /*[None]*/
	ntDecimal                    /*[de]*/
	ntDigit                      /*[di]*/
	ntNumeric                    /*[nu]*/
	/**
	 * One more than the highest normal UNumericType value.
	 * The highest value is available via u_getIntPropertyMaxValue(UCHAR_NUMERIC_TYPE).
	 *
	 * @deprecated ICU 58 The numeric value may change over time, see ICU ticket #12420.
	 */
	ntCount
)

/**
 * Hangul Syllable Type constants.
 *
 * @see UCHAR_HANGUL_SYLLABLE_TYPE
 * @stable ICU 2.6
 */

type hangunSyllableType int32

const (
	/*
	 * Note: UHangulSyllableType constants are parsed by preparseucd.py.
	 * It matches lines like
	 *     U_HST_<Unicode Hangul_Syllable_Type value name>
	 */

	hstNotApplicable hangunSyllableType = iota /*[NA]*/
	hstLeadingJamo                             /*[L]*/
	hstVowelJamo                               /*[V]*/
	hstTrailingJamo                            /*[T]*/
	hstLvSyllable                              /*[LV]*/
	hstLvtSyllable                             /*[LVT]*/
	/**
	 * One more than the highest normal UHangulSyllableType value.
	 * The highest value is available via u_getIntPropertyMaxValue(UCHAR_HANGUL_SYLLABLE_TYPE).
	 *
	 * @deprecated ICU 58 The numeric value may change over time, see ICU ticket #12420.
	 */
	hstCount
)

var intProps = [uCharIntLimit - UCharIntStart]*intProperty{
	/*
	 * column, mask and shift values for int-value properties from u_getUnicodeProperties().
	 * Must be in order of corresponding UProperty,
	 * and there must be exactly one entry per int UProperty.
	 *
	 * Properties with mask==0 are handled in code.
	 * For them, column is the UPropertySource value.
	 */
	{srcBidi, 0, 0, getBiDiClass},
	{0, blockMask, blockShift, defaultGetValue},
	{srcNfc, 0, 0xff, getCombiningClass},
	{2, dtMask, 0, defaultGetValue},
	{0, eaMask, eaShift, defaultGetValue},
	{srcChar, 0, int32(uchar.CharCategoryCount - 1), getGeneralCategory},
	{srcBidi, 0, 0, getJoiningGroup},
	{srcBidi, 0, 0, getJoiningType},
	{2, lbMask, lbShift, defaultGetValue},
	{srcChar, 0, int32(ntCount - 1), getNumericType},
	{srcPropsvec, 0, 0, getScript},
	{srcPropsvec, 0, int32(hstCount - 1), getHangulSyllableType},
	// UCHAR_NFD_QUICK_CHECK: max=1=YES -- never "maybe", only "no" or "yes"
	{srcNfc, 0, int32(normalizer.Yes), getNormQuickCheck},
	// UCHAR_NFKD_QUICK_CHECK: max=1=YES -- never "maybe", only "no" or "yes"
	{srcNfkc, 0, int32(normalizer.Yes), getNormQuickCheck},
	// UCHAR_NFC_QUICK_CHECK: max=2=MAYBE
	{srcNfc, 0, int32(normalizer.Maybe), getNormQuickCheck},
	// UCHAR_NFKC_QUICK_CHECK: max=2=MAYBE
	{srcNfkc, 0, int32(normalizer.Maybe), getNormQuickCheck},
	{srcNfc, 0, 0xff, getLeadCombiningClass},
	{srcNfc, 0, 0xff, getTrailCombiningClass},
	{2, gcbMask, gcbShift, defaultGetValue},
	{2, sbMask, sbShift, defaultGetValue},
	{2, wbMask, wbShift, defaultGetValue},
	{srcBidi, 0, 0, getBiDiPairedBracketType},
	{srcInpc, 0, 0, getInPC},
	{srcInsc, 0, 0, getInSC},
	{srcVo, 0, 0, getVo},
}

func getVo(_ *intProperty, c rune, _ Property) int32 {
	return int32(ulayout.VoTrie().Get(c))
}

func getInSC(_ *intProperty, c rune, _ Property) int32 {
	return int32(ulayout.InscTrie().Get(c))
}

func getInPC(_ *intProperty, c rune, _ Property) int32 {
	return int32(ulayout.InpcTrie().Get(c))
}

func getBiDiPairedBracketType(_ *intProperty, c rune, _ Property) int32 {
	return int32(ubidi.PairedBracketType(c))
}

func getTrailCombiningClass(_ *intProperty, c rune, _ Property) int32 {
	return int32(normalizer.Nfc().GetFCD16(c) & 0xff)
}

func getLeadCombiningClass(_ *intProperty, c rune, _ Property) int32 {
	val := int32(normalizer.Nfc().GetFCD16(c) >> 8)
	return val
}

func getNormQuickCheck(_ *intProperty, c rune, which Property) int32 {
	return int32(normalizer.QuickCheck(c, normalizer.Mode(int32(which)-int32(UCharNfdQuickCheck)+int32(normalizer.NormNfd))))
}

/*
 * Map some of the Grapheme Cluster Break values to Hangul Syllable Types.
 * Hangul_Syllable_Type is fully redundant with a subset of Grapheme_Cluster_Break.
 */
var gcbToHst = []hangunSyllableType{
	hstNotApplicable, /* U_GCB_OTHER */
	hstNotApplicable, /* U_GCB_CONTROL */
	hstNotApplicable, /* U_GCB_CR */
	hstNotApplicable, /* U_GCB_EXTEND */
	hstLeadingJamo,   /* U_GCB_L */
	hstNotApplicable, /* U_GCB_LF */
	hstLvSyllable,    /* U_GCB_LV */
	hstLvtSyllable,   /* U_GCB_LVT */
	hstTrailingJamo,  /* U_GCB_T */
	hstVowelJamo,     /* U_GCB_V */
	/*
	 * Omit GCB values beyond what we need for hst.
	 * The code below checks for the array length.
	 */
}

func getHangulSyllableType(_ *intProperty, c rune, _ Property) int32 {
	/* see comments on gcbToHst[] above */
	gcb := (int32(uchar.GetUnicodeProperties(c, 2)) & gcbMask) >> gcbShift

	if gcb < int32(len(gcbToHst)) {
		return int32(gcbToHst[gcb])
	}
	return int32(hstNotApplicable)
}

func getScript(_ *intProperty, c rune, _ Property) int32 {
	return script(c)
}

func getNumericType(_ *intProperty, c rune, _ Property) int32 {
	ntv := uchar.NumericTypeValue(c)
	return int32(ntvGetType(ntv))
}

func getJoiningType(_ *intProperty, c rune, _ Property) int32 {
	return int32(ubidi.JoinType(c))
}

func getJoiningGroup(_ *intProperty, c rune, _ Property) int32 {
	return int32(ubidi.JoinGroup(c))
}

func getGeneralCategory(_ *intProperty, c rune, _ Property) int32 {
	return int32(uchar.CharType(c))
}

func getCombiningClass(_ *intProperty, c rune, _ Property) int32 {
	return int32(normalizer.Nfc().CombiningClass(c))
}

func defaultGetValue(prop *intProperty, c rune, _ Property) int32 {
	return int32(uchar.GetUnicodeProperties(c, int(prop.column))&prop.mask) >> prop.shift
}

func getBiDiClass(_ *intProperty, c rune, _ Property) int32 {
	return int32(ubidi.Class(c))
}

func ntvGetType(ntv uint16) numericType {
	switch {
	case ntv == uchar.UPropsNtvNone:
		return ntNone
	case ntv < uchar.UPropsNtvDigitStart:
		return ntDecimal
	case ntv < uchar.UPropsNtvNumericStart:
		return ntDigit
	default:
		return ntNumeric
	}
}
