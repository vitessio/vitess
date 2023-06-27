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

package uchar

import "golang.org/x/exp/constraints"

func U_MASK[T constraints.Integer](x T) uint32 {
	return 1 << x
}

const (
	/*
	 * Note: UCharCategory constants and their API comments are parsed by preparseucd.py.
	 * It matches pairs of lines like
	 *     / ** <Unicode 2-letter General_Category value> comment... * /
	 *     U_<[A-Z_]+> = <integer>,
	 */

	/** Non-category for unassigned and non-character code points. @stable ICU 2.0 */
	U_UNASSIGNED = 0
	/** Cn "Other, Not Assigned (no characters in [UnicodeData.txt] have this property)" (same as U_UNASSIGNED!) @stable ICU 2.0 */
	U_GENERAL_OTHER_TYPES = 0
	/** Lu @stable ICU 2.0 */
	U_UPPERCASE_LETTER = 1
	/** Ll @stable ICU 2.0 */
	U_LOWERCASE_LETTER = 2
	/** Lt @stable ICU 2.0 */
	U_TITLECASE_LETTER = 3
	/** Lm @stable ICU 2.0 */
	U_MODIFIER_LETTER = 4
	/** Lo @stable ICU 2.0 */
	U_OTHER_LETTER = 5
	/** Mn @stable ICU 2.0 */
	U_NON_SPACING_MARK = 6
	/** Me @stable ICU 2.0 */
	U_ENCLOSING_MARK = 7
	/** Mc @stable ICU 2.0 */
	U_COMBINING_SPACING_MARK = 8
	/** Nd @stable ICU 2.0 */
	U_DECIMAL_DIGIT_NUMBER = 9
	/** Nl @stable ICU 2.0 */
	U_LETTER_NUMBER = 10
	/** No @stable ICU 2.0 */
	U_OTHER_NUMBER = 11
	/** Zs @stable ICU 2.0 */
	U_SPACE_SEPARATOR = 12
	/** Zl @stable ICU 2.0 */
	U_LINE_SEPARATOR = 13
	/** Zp @stable ICU 2.0 */
	U_PARAGRAPH_SEPARATOR = 14
	/** Cc @stable ICU 2.0 */
	U_CONTROL_CHAR = 15
	/** Cf @stable ICU 2.0 */
	U_FORMAT_CHAR = 16
	/** Co @stable ICU 2.0 */
	U_PRIVATE_USE_CHAR = 17
	/** Cs @stable ICU 2.0 */
	U_SURROGATE = 18
	/** Pd @stable ICU 2.0 */
	U_DASH_PUNCTUATION = 19
	/** Ps @stable ICU 2.0 */
	U_START_PUNCTUATION = 20
	/** Pe @stable ICU 2.0 */
	U_END_PUNCTUATION = 21
	/** Pc @stable ICU 2.0 */
	U_CONNECTOR_PUNCTUATION = 22
	/** Po @stable ICU 2.0 */
	U_OTHER_PUNCTUATION = 23
	/** Sm @stable ICU 2.0 */
	U_MATH_SYMBOL = 24
	/** Sc @stable ICU 2.0 */
	U_CURRENCY_SYMBOL = 25
	/** Sk @stable ICU 2.0 */
	U_MODIFIER_SYMBOL = 26
	/** So @stable ICU 2.0 */
	U_OTHER_SYMBOL = 27
	/** Pi @stable ICU 2.0 */
	U_INITIAL_PUNCTUATION = 28
	/** Pf @stable ICU 2.0 */
	U_FINAL_PUNCTUATION = 29
	/**
	 * One higher than the last enum UCharCategory constant.
	 * This numeric value is stable (will not change), see
	 * http://www.unicode.org/policies/stability_policy.html#Property_Value
	 *
	 * @stable ICU 2.0
	 */
	U_CHAR_CATEGORY_COUNT = 30
)

var (
	U_GC_CN_MASK = U_MASK(U_GENERAL_OTHER_TYPES)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_LU_MASK = U_MASK(U_UPPERCASE_LETTER)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_LL_MASK = U_MASK(U_LOWERCASE_LETTER)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_LT_MASK = U_MASK(U_TITLECASE_LETTER)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_LM_MASK = U_MASK(U_MODIFIER_LETTER)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_LO_MASK = U_MASK(U_OTHER_LETTER)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_MN_MASK = U_MASK(U_NON_SPACING_MARK)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_ME_MASK = U_MASK(U_ENCLOSING_MARK)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_MC_MASK = U_MASK(U_COMBINING_SPACING_MARK)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_ND_MASK = U_MASK(U_DECIMAL_DIGIT_NUMBER)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_NL_MASK = U_MASK(U_LETTER_NUMBER)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_NO_MASK = U_MASK(U_OTHER_NUMBER)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_ZS_MASK = U_MASK(U_SPACE_SEPARATOR)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_ZL_MASK = U_MASK(U_LINE_SEPARATOR)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_ZP_MASK = U_MASK(U_PARAGRAPH_SEPARATOR)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_CC_MASK = U_MASK(U_CONTROL_CHAR)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_CF_MASK = U_MASK(U_FORMAT_CHAR)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_CO_MASK = U_MASK(U_PRIVATE_USE_CHAR)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_CS_MASK = U_MASK(U_SURROGATE)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_PD_MASK = U_MASK(U_DASH_PUNCTUATION)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_PS_MASK = U_MASK(U_START_PUNCTUATION)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_PE_MASK = U_MASK(U_END_PUNCTUATION)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_PC_MASK = U_MASK(U_CONNECTOR_PUNCTUATION)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_PO_MASK = U_MASK(U_OTHER_PUNCTUATION)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_SM_MASK = U_MASK(U_MATH_SYMBOL)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_SC_MASK = U_MASK(U_CURRENCY_SYMBOL)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_SK_MASK = U_MASK(U_MODIFIER_SYMBOL)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	U_GC_SO_MASK = U_MASK(U_OTHER_SYMBOL)

	/** Mask constant for multiple UCharCategory bits (L Letters). @stable ICU 2.1 */
	U_GC_L_MASK = (U_GC_LU_MASK | U_GC_LL_MASK | U_GC_LT_MASK | U_GC_LM_MASK | U_GC_LO_MASK)

	/** Mask constant for multiple UCharCategory bits (LC Cased Letters). @stable ICU 2.1 */
	U_GC_LC_MASK = (U_GC_LU_MASK | U_GC_LL_MASK | U_GC_LT_MASK)

	/** Mask constant for multiple UCharCategory bits (M Marks). @stable ICU 2.1 */
	U_GC_M_MASK = (U_GC_MN_MASK | U_GC_ME_MASK | U_GC_MC_MASK)

	/** Mask constant for multiple UCharCategory bits (N Numbers). @stable ICU 2.1 */
	U_GC_N_MASK = (U_GC_ND_MASK | U_GC_NL_MASK | U_GC_NO_MASK)

	/** Mask constant for multiple UCharCategory bits (Z Separators). @stable ICU 2.1 */
	U_GC_Z_MASK = (U_GC_ZS_MASK | U_GC_ZL_MASK | U_GC_ZP_MASK)
)

const UPROPS_AGE_SHIFT = 24
const U_MAX_VERSION_LENGTH = 4
const U_VERSION_DELIMITER = '.'

type UVersionInfo [U_MAX_VERSION_LENGTH]uint8

const (
	/** No numeric value. */
	UPROPS_NTV_NONE = 0
	/** Decimal digits: nv=0..9 */
	UPROPS_NTV_DECIMAL_START = 1
	/** Other digits: nv=0..9 */
	UPROPS_NTV_DIGIT_START = 11
	/** Small integers: nv=0..154 */
	UPROPS_NTV_NUMERIC_START = 21
	/** Fractions: ((ntv>>4)-12) / ((ntv&0xf)+1) = -1..17 / 1..16 */
	UPROPS_NTV_FRACTION_START = 0xb0
	/**
	 * Large integers:
	 * ((ntv>>5)-14) * 10^((ntv&0x1f)+2) = (1..9)*(10^2..10^33)
	 * (only one significant decimal digit)
	 */
	UPROPS_NTV_LARGE_START = 0x1e0
	/**
	 * Sexagesimal numbers:
	 * ((ntv>>2)-0xbf) * 60^((ntv&3)+1) = (1..9)*(60^1..60^4)
	 */
	UPROPS_NTV_BASE60_START = 0x300
	/**
	 * Fraction-20 values:
	 * frac20 = ntv-0x324 = 0..0x17 -> 1|3|5|7 / 20|40|80|160|320|640
	 * numerator: num = 2*(frac20&3)+1
	 * denominator: den = 20<<(frac20>>2)
	 */
	UPROPS_NTV_FRACTION20_START = UPROPS_NTV_BASE60_START + 36 // 0x300+9*4=0x324
	/**
	 * Fraction-32 values:
	 * frac32 = ntv-0x34c = 0..15 -> 1|3|5|7 / 32|64|128|256
	 * numerator: num = 2*(frac32&3)+1
	 * denominator: den = 32<<(frac32>>2)
	 */
	UPROPS_NTV_FRACTION32_START = UPROPS_NTV_FRACTION20_START + 24 // 0x324+6*4=0x34c
	/** No numeric value (yet). */
	UPROPS_NTV_RESERVED_START = UPROPS_NTV_FRACTION32_START + 16 // 0x34c+4*4=0x35c

	UPROPS_NTV_MAX_SMALL_INT = UPROPS_NTV_FRACTION_START - UPROPS_NTV_NUMERIC_START - 1
)

const U_NO_NUMERIC_VALUE = -123456789.0
