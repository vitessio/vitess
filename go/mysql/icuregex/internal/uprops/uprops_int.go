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
	"vitess.io/vitess/go/mysql/icuregex/internal/ubidi"
	uchar2 "vitess.io/vitess/go/mysql/icuregex/internal/uchar"
	"vitess.io/vitess/go/mysql/icuregex/internal/ulayout"
)

type IntPropertyGetValue func(prop *IntProperty, c rune, which Property) int32

type IntProperty struct {
	column   PropertySource
	mask     uint32
	shift    int32
	getValue IntPropertyGetValue
}

const (
	UPROPS_BLOCK_MASK  = 0x0001ff00
	UPROPS_BLOCK_SHIFT = 8

	UPROPS_EA_MASK  = 0x000e0000
	UPROPS_EA_SHIFT = 17

	UPROPS_LB_MASK  = 0x03f00000
	UPROPS_LB_SHIFT = 20

	UPROPS_SB_MASK  = 0x000f8000
	UPROPS_SB_SHIFT = 15

	UPROPS_WB_MASK  = 0x00007c00
	UPROPS_WB_SHIFT = 10

	UPROPS_GCB_MASK  = 0x000003e0
	UPROPS_GCB_SHIFT = 5

	UPROPS_DT_MASK = 0x0000001f
)

type NormalizationCheckResult int32

const (
	/**
	 * The input string is not in the normalization form.
	 * @stable ICU 2.0
	 */
	UNORM_NO NormalizationCheckResult = iota
	/**
	 * The input string is in the normalization form.
	 * @stable ICU 2.0
	 */
	UNORM_YES
	/**
	 * The input string may or may not be in the normalization form.
	 * This value is only returned for composition forms like NFC and FCC,
	 * when a backward-combining character is found for which the surrounding text
	 * would have to be analyzed further.
	 * @stable ICU 2.0
	 */
	UNORM_MAYBE
)

type NumericType int32

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

	U_NT_NONE    NumericType = iota /*[None]*/
	U_NT_DECIMAL                    /*[de]*/
	U_NT_DIGIT                      /*[di]*/
	U_NT_NUMERIC                    /*[nu]*/
	/**
	 * One more than the highest normal UNumericType value.
	 * The highest value is available via u_getIntPropertyMaxValue(UCHAR_NUMERIC_TYPE).
	 *
	 * @deprecated ICU 58 The numeric value may change over time, see ICU ticket #12420.
	 */
	U_NT_COUNT
)

/**
 * Hangul Syllable Type constants.
 *
 * @see UCHAR_HANGUL_SYLLABLE_TYPE
 * @stable ICU 2.6
 */

type HangunSyllableType int32

const (
	/*
	 * Note: UHangulSyllableType constants are parsed by preparseucd.py.
	 * It matches lines like
	 *     U_HST_<Unicode Hangul_Syllable_Type value name>
	 */

	U_HST_NOT_APPLICABLE HangunSyllableType = iota /*[NA]*/
	U_HST_LEADING_JAMO                             /*[L]*/
	U_HST_VOWEL_JAMO                               /*[V]*/
	U_HST_TRAILING_JAMO                            /*[T]*/
	U_HST_LV_SYLLABLE                              /*[LV]*/
	U_HST_LVT_SYLLABLE                             /*[LVT]*/
	/**
	 * One more than the highest normal UHangulSyllableType value.
	 * The highest value is available via u_getIntPropertyMaxValue(UCHAR_HANGUL_SYLLABLE_TYPE).
	 *
	 * @deprecated ICU 58 The numeric value may change over time, see ICU ticket #12420.
	 */
	U_HST_COUNT
)

var intProps = [UCHAR_INT_LIMIT - UCHAR_INT_START]*IntProperty{
	/*
	 * column, mask and shift values for int-value properties from u_getUnicodeProperties().
	 * Must be in order of corresponding UProperty,
	 * and there must be exactly one entry per int UProperty.
	 *
	 * Properties with mask==0 are handled in code.
	 * For them, column is the UPropertySource value.
	 */
	{UPROPS_SRC_BIDI, 0, 0, getBiDiClass},
	{0, UPROPS_BLOCK_MASK, UPROPS_BLOCK_SHIFT, defaultGetValue},
	{UPROPS_SRC_NFC, 0, 0xff, getCombiningClass},
	{2, UPROPS_DT_MASK, 0, defaultGetValue},
	{0, UPROPS_EA_MASK, UPROPS_EA_SHIFT, defaultGetValue},
	{UPROPS_SRC_CHAR, 0, uchar2.U_CHAR_CATEGORY_COUNT - 1, getGeneralCategory},
	{UPROPS_SRC_BIDI, 0, 0, getJoiningGroup},
	{UPROPS_SRC_BIDI, 0, 0, getJoiningType},
	{2, UPROPS_LB_MASK, UPROPS_LB_SHIFT, defaultGetValue},
	{UPROPS_SRC_CHAR, 0, int32(U_NT_COUNT - 1), getNumericType},
	{UPROPS_SRC_PROPSVEC, 0, 0, getScript},
	{UPROPS_SRC_PROPSVEC, 0, int32(U_HST_COUNT - 1), getHangulSyllableType},
	// UCHAR_NFD_QUICK_CHECK: max=1=YES -- never "maybe", only "no" or "yes"
	{UPROPS_SRC_NFC, 0, int32(UNORM_YES), getNormQuickCheck},
	// UCHAR_NFKD_QUICK_CHECK: max=1=YES -- never "maybe", only "no" or "yes"
	{UPROPS_SRC_NFKC, 0, int32(UNORM_YES), getNormQuickCheck},
	// UCHAR_NFC_QUICK_CHECK: max=2=MAYBE
	{UPROPS_SRC_NFC, 0, int32(UNORM_MAYBE), getNormQuickCheck},
	// UCHAR_NFKC_QUICK_CHECK: max=2=MAYBE
	{UPROPS_SRC_NFKC, 0, int32(UNORM_MAYBE), getNormQuickCheck},
	{UPROPS_SRC_NFC, 0, 0xff, getLeadCombiningClass},
	{UPROPS_SRC_NFC, 0, 0xff, getTrailCombiningClass},
	{2, UPROPS_GCB_MASK, UPROPS_GCB_SHIFT, defaultGetValue},
	{2, UPROPS_SB_MASK, UPROPS_SB_SHIFT, defaultGetValue},
	{2, UPROPS_WB_MASK, UPROPS_WB_SHIFT, defaultGetValue},
	{UPROPS_SRC_BIDI, 0, 0, getBiDiPairedBracketType},
	{UPROPS_SRC_INPC, 0, 0, getInPC},
	{UPROPS_SRC_INSC, 0, 0, getInSC},
	{UPROPS_SRC_VO, 0, 0, getVo},
}

func getVo(prop *IntProperty, c rune, which Property) int32 {
	return int32(ulayout.VoTrie().Get(c))
}

func getInSC(prop *IntProperty, c rune, which Property) int32 {
	return int32(ulayout.InscTrie().Get(c))
}

func getInPC(prop *IntProperty, c rune, which Property) int32 {
	return int32(ulayout.InpcTrie().Get(c))
}

func getBiDiPairedBracketType(prop *IntProperty, c rune, which Property) int32 {
	return int32(ubidi.PairedBracketType(c))
}

func getTrailCombiningClass(prop *IntProperty, c rune, which Property) int32 {
	panic("TODO")
}

func getLeadCombiningClass(prop *IntProperty, c rune, which Property) int32 {
	panic("TODO")
}

func getNormQuickCheck(prop *IntProperty, c rune, which Property) int32 {
	panic("TODO")
}

/*
 * Map some of the Grapheme Cluster Break values to Hangul Syllable Types.
 * Hangul_Syllable_Type is fully redundant with a subset of Grapheme_Cluster_Break.
 */
var gcbToHst = []HangunSyllableType{
	U_HST_NOT_APPLICABLE, /* U_GCB_OTHER */
	U_HST_NOT_APPLICABLE, /* U_GCB_CONTROL */
	U_HST_NOT_APPLICABLE, /* U_GCB_CR */
	U_HST_NOT_APPLICABLE, /* U_GCB_EXTEND */
	U_HST_LEADING_JAMO,   /* U_GCB_L */
	U_HST_NOT_APPLICABLE, /* U_GCB_LF */
	U_HST_LV_SYLLABLE,    /* U_GCB_LV */
	U_HST_LVT_SYLLABLE,   /* U_GCB_LVT */
	U_HST_TRAILING_JAMO,  /* U_GCB_T */
	U_HST_VOWEL_JAMO,     /* U_GCB_V */
	/*
	 * Omit GCB values beyond what we need for hst.
	 * The code below checks for the array length.
	 */
}

func getHangulSyllableType(prop *IntProperty, c rune, which Property) int32 {
	/* see comments on gcbToHst[] above */
	gcb := (int32(uchar2.GetUnicodeProperties(c, 2)) & UPROPS_GCB_MASK) >> UPROPS_GCB_SHIFT

	if gcb < int32(len(gcbToHst)) {
		return int32(gcbToHst[gcb])
	} else {
		return int32(U_HST_NOT_APPLICABLE)
	}
}

func getScript(_ *IntProperty, c rune, _ Property) int32 {
	return GetScript(c)
}

func getNumericType(prop *IntProperty, c rune, which Property) int32 {
	ntv := uchar2.NumericTypeValue(c)
	return int32(ntvGetType(ntv))
}

func getJoiningType(prop *IntProperty, c rune, which Property) int32 {
	return int32(ubidi.JoiningType(c))
}

func getJoiningGroup(prop *IntProperty, c rune, which Property) int32 {
	return int32(ubidi.JoiningGroup(c))
}

func getGeneralCategory(prop *IntProperty, c rune, which Property) int32 {
	return int32(uchar2.CharType(c))
}

func getCombiningClass(prop *IntProperty, c rune, which Property) int32 {
	panic("TODO")
}

func defaultGetValue(prop *IntProperty, c rune, which Property) int32 {
	return int32(uchar2.GetUnicodeProperties(c, int(prop.column))&prop.mask) >> prop.shift
}

func getBiDiClass(prop *IntProperty, c rune, which Property) int32 {
	return int32(ubidi.Class(c))
}

func ntvGetType(ntv uint16) NumericType {
	switch {
	case ntv == uchar2.UPROPS_NTV_NONE:
		return U_NT_NONE
	case ntv < uchar2.UPROPS_NTV_DIGIT_START:
		return U_NT_DECIMAL
	case ntv < uchar2.UPROPS_NTV_NUMERIC_START:
		return U_NT_DIGIT
	default:
		return U_NT_NUMERIC
	}
}
