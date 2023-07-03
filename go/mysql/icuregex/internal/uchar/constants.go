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

func uMask[T constraints.Integer](x T) uint32 {
	return 1 << x
}

type Category int8

const (
	/*
	 * Note: UCharCategory constants and their API comments are parsed by preparseucd.py.
	 * It matches pairs of lines like
	 *     / ** <Unicode 2-letter General_Category value> comment... * /
	 *     U_<[A-Z_]+> = <integer>,
	 */

	/** Non-category for unassigned and non-character code points. @stable ICU 2.0 */
	Unassigned Category = 0
	/** Cn "Other, Not Assigned (no characters in [UnicodeData.txt] have this property)" (same as U_UNASSIGNED!) @stable ICU 2.0 */
	GeneralOtherTypes Category = iota - 1
	/** Lu @stable ICU 2.0 */
	UppercaseLetter
	/** Ll @stable ICU 2.0 */
	LowercaseLetter
	/** Lt @stable ICU 2.0 */
	TitlecaseLetter
	/** Lm @stable ICU 2.0 */
	ModifierLetter
	/** Lo @stable ICU 2.0 */
	OtherLetter
	/** Mn @stable ICU 2.0 */
	NonSpacingMask
	/** Me @stable ICU 2.0 */
	EnclosingMark
	/** Mc @stable ICU 2.0 */
	CombiningSpacingMask
	/** Nd @stable ICU 2.0 */
	DecimalDigitNumber
	/** Nl @stable ICU 2.0 */
	LetterNumber
	/** No @stable ICU 2.0 */
	OtherNumber
	/** Zs @stable ICU 2.0 */
	SpaceSeparator
	/** Zl @stable ICU 2.0 */
	LineSeparator
	/** Zp @stable ICU 2.0 */
	ParagraphSeparator
	/** Cc @stable ICU 2.0 */
	ControlChar
	/** Cf @stable ICU 2.0 */
	FormatChar
	/** Co @stable ICU 2.0 */
	PrivateUseChar
	/** Cs @stable ICU 2.0 */
	Surrogate
	/** Pd @stable ICU 2.0 */
	DashPunctuation
	/** Ps @stable ICU 2.0 */
	StartPunctuation
	/** Pe @stable ICU 2.0 */
	EndPunctuation
	/** Pc @stable ICU 2.0 */
	ConnectorPunctuation
	/** Po @stable ICU 2.0 */
	OtherPunctuation
	/** Sm @stable ICU 2.0 */
	MathSymbol
	/** Sc @stable ICU 2.0 */
	CurrencySymbol
	/** Sk @stable ICU 2.0 */
	ModifierSymbol
	/** So @stable ICU 2.0 */
	OtherSymbol
	/** Pi @stable ICU 2.0 */
	InitialPunctuation
	/** Pf @stable ICU 2.0 */
	FinalPunctuation
	/**
	 * One higher than the last enum UCharCategory constant.
	 * This numeric value is stable (will not change), see
	 * http://www.unicode.org/policies/stability_policy.html#Property_Value
	 *
	 * @stable ICU 2.0
	 */
	CharCategoryCount
)

var (
	GcCnMask = uMask(GeneralOtherTypes)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcLuMask = uMask(UppercaseLetter)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcLlMask = uMask(LowercaseLetter)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcLtMask = uMask(TitlecaseLetter)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcLmMask = uMask(ModifierLetter)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcLoMask = uMask(OtherLetter)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcMnMask = uMask(NonSpacingMask)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcMeMask = uMask(EnclosingMark)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcMcMask = uMask(CombiningSpacingMask)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcNdMask = uMask(DecimalDigitNumber)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcNlMask = uMask(LetterNumber)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcNoMask = uMask(OtherNumber)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcZsMask = uMask(SpaceSeparator)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcZlMask = uMask(LineSeparator)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcZpMask = uMask(ParagraphSeparator)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcCcMask = uMask(ControlChar)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcCfMask = uMask(FormatChar)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcCoMask = uMask(PrivateUseChar)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcCsMask = uMask(Surrogate)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcPdMask = uMask(DashPunctuation)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcPsMask = uMask(StartPunctuation)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcPeMask = uMask(EndPunctuation)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcPcMask = uMask(ConnectorPunctuation)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcPoMask = uMask(OtherPunctuation)

	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcSmMask = uMask(MathSymbol)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcScMask = uMask(CurrencySymbol)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcSkMask = uMask(ModifierSymbol)
	/** Mask constant for a UCharCategory. @stable ICU 2.1 */
	GcSoMask = uMask(OtherSymbol)

	/** Mask constant for multiple UCharCategory bits (L Letters). @stable ICU 2.1 */
	GcLMask = (GcLuMask | GcLlMask | GcLtMask | GcLmMask | GcLoMask)

	/** Mask constant for multiple UCharCategory bits (LC Cased Letters). @stable ICU 2.1 */
	GcLcMask = (GcLuMask | GcLlMask | GcLtMask)

	/** Mask constant for multiple UCharCategory bits (M Marks). @stable ICU 2.1 */
	GcMMask = (GcMnMask | GcMeMask | GcMcMask)

	/** Mask constant for multiple UCharCategory bits (N Numbers). @stable ICU 2.1 */
	GcNMask = (GcNdMask | GcNlMask | GcNoMask)

	/** Mask constant for multiple UCharCategory bits (Z Separators). @stable ICU 2.1 */
	GcZMask = (GcZsMask | GcZlMask | GcZpMask)
)

const upropsAgeShift = 24
const maxVersionLength = 4
const versionDelimiter = '.'

type UVersionInfo [maxVersionLength]uint8

const (
	/** No numeric value. */
	UPropsNtvNone = 0
	/** Decimal digits: nv=0..9 */
	UPropsNtvDecimalStart = 1
	/** Other digits: nv=0..9 */
	UPropsNtvDigitStart = 11
	/** Small integers: nv=0..154 */
	UPropsNtvNumericStart = 21
	/** Fractions: ((ntv>>4)-12) / ((ntv&0xf)+1) = -1..17 / 1..16 */
	UPropsNtvFractionStart = 0xb0
	/**
	 * Large integers:
	 * ((ntv>>5)-14) * 10^((ntv&0x1f)+2) = (1..9)*(10^2..10^33)
	 * (only one significant decimal digit)
	 */
	UPropsNtvLargeStart = 0x1e0
	/**
	 * Sexagesimal numbers:
	 * ((ntv>>2)-0xbf) * 60^((ntv&3)+1) = (1..9)*(60^1..60^4)
	 */
	UPropsNtvBase60Start = 0x300
	/**
	 * Fraction-20 values:
	 * frac20 = ntv-0x324 = 0..0x17 -> 1|3|5|7 / 20|40|80|160|320|640
	 * numerator: num = 2*(frac20&3)+1
	 * denominator: den = 20<<(frac20>>2)
	 */
	UPropsNtvFraction20Start = UPropsNtvBase60Start + 36 // 0x300+9*4=0x324
	/**
	 * Fraction-32 values:
	 * frac32 = ntv-0x34c = 0..15 -> 1|3|5|7 / 32|64|128|256
	 * numerator: num = 2*(frac32&3)+1
	 * denominator: den = 32<<(frac32>>2)
	 */
	UPropsNtvFraction32Start = UPropsNtvFraction20Start + 24 // 0x324+6*4=0x34c
	/** No numeric value (yet). */
	UPropsNtvReservedStart = UPropsNtvFraction32Start + 16 // 0x34c+4*4=0x35c

	UPropsNtvMaxSmallInt = UPropsNtvFractionStart - UPropsNtvNumericStart - 1
)

const noNumericValue = -123456789.0
