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
	"golang.org/x/exp/constraints"

	"vitess.io/vitess/go/mysql/icuregex/internal/ubidi"
	"vitess.io/vitess/go/mysql/icuregex/internal/ucase"
	"vitess.io/vitess/go/mysql/icuregex/internal/uchar"
)

type BinaryProperty struct {
	column   PropertySource
	mask     uint32
	contains func(prop *BinaryProperty, c rune, which Property) bool
}

func U_MASK[T constraints.Integer](x T) uint32 {
	return 1 << x
}

func defaultContains(prop *BinaryProperty, c rune, _ Property) bool {
	return (uchar.GetUnicodeProperties(c, int(prop.column)) & prop.mask) != 0
}

var binProps = [UCHAR_BINARY_LIMIT]*BinaryProperty{
	/*
	 * column and mask values for binary properties from u_getUnicodeProperties().
	 * Must be in order of corresponding UProperty,
	 * and there must be exactly one entry per binary UProperty.
	 *
	 * Properties with mask==0 are handled in code.
	 * For them, column is the UPropertySource value.
	 */
	{1, U_MASK(UPROPS_ALPHABETIC), defaultContains},
	{1, U_MASK(UPROPS_ASCII_HEX_DIGIT), defaultContains},
	{UPROPS_SRC_BIDI, 0, isBidiControl},
	{UPROPS_SRC_BIDI, 0, isMirrored},
	{1, U_MASK(UPROPS_DASH), defaultContains},
	{1, U_MASK(UPROPS_DEFAULT_IGNORABLE_CODE_POINT), defaultContains},
	{1, U_MASK(UPROPS_DEPRECATED), defaultContains},
	{1, U_MASK(UPROPS_DIACRITIC), defaultContains},
	{1, U_MASK(UPROPS_EXTENDER), defaultContains},
	{UPROPS_SRC_NFC, 0, hasFullCompositionExclusion},
	{1, U_MASK(UPROPS_GRAPHEME_BASE), defaultContains},
	{1, U_MASK(UPROPS_GRAPHEME_EXTEND), defaultContains},
	{1, U_MASK(UPROPS_GRAPHEME_LINK), defaultContains},
	{1, U_MASK(UPROPS_HEX_DIGIT), defaultContains},
	{1, U_MASK(UPROPS_HYPHEN), defaultContains},
	{1, U_MASK(UPROPS_ID_CONTINUE), defaultContains},
	{1, U_MASK(UPROPS_ID_START), defaultContains},
	{1, U_MASK(UPROPS_IDEOGRAPHIC), defaultContains},
	{1, U_MASK(UPROPS_IDS_BINARY_OPERATOR), defaultContains},
	{1, U_MASK(UPROPS_IDS_TRINARY_OPERATOR), defaultContains},
	{UPROPS_SRC_BIDI, 0, isJoinControl},
	{1, U_MASK(UPROPS_LOGICAL_ORDER_EXCEPTION), defaultContains},
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_LOWERCASE
	{1, U_MASK(UPROPS_MATH), defaultContains},
	{1, U_MASK(UPROPS_NONCHARACTER_CODE_POINT), defaultContains},
	{1, U_MASK(UPROPS_QUOTATION_MARK), defaultContains},
	{1, U_MASK(UPROPS_RADICAL), defaultContains},
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_SOFT_DOTTED
	{1, U_MASK(UPROPS_TERMINAL_PUNCTUATION), defaultContains},
	{1, U_MASK(UPROPS_UNIFIED_IDEOGRAPH), defaultContains},
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_UPPERCASE
	{1, U_MASK(UPROPS_WHITE_SPACE), defaultContains},
	{1, U_MASK(UPROPS_XID_CONTINUE), defaultContains},
	{1, U_MASK(UPROPS_XID_START), defaultContains},
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_CASE_SENSITIVE
	{1, U_MASK(UPROPS_S_TERM), defaultContains},
	{1, U_MASK(UPROPS_VARIATION_SELECTOR), defaultContains},
	{UPROPS_SRC_NFC, 0, isNormInert},  // UCHAR_NFD_INERT
	{UPROPS_SRC_NFKC, 0, isNormInert}, // UCHAR_NFKD_INERT
	{UPROPS_SRC_NFC, 0, isNormInert},  // UCHAR_NFC_INERT
	{UPROPS_SRC_NFKC, 0, isNormInert}, // UCHAR_NFKC_INERT
	{UPROPS_SRC_NFC_CANON_ITER, 0, isCanonSegmentStarter},
	{1, U_MASK(UPROPS_PATTERN_SYNTAX), defaultContains},
	{1, U_MASK(UPROPS_PATTERN_WHITE_SPACE), defaultContains},
	{UPROPS_SRC_CHAR_AND_PROPSVEC, 0, isPOSIX_alnum},
	{UPROPS_SRC_CHAR, 0, isPOSIX_blank},
	{UPROPS_SRC_CHAR, 0, isPOSIX_graph},
	{UPROPS_SRC_CHAR, 0, isPOSIX_print},
	{UPROPS_SRC_CHAR, 0, isPOSIX_xdigit},
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_CASED
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_CASE_IGNORABLE
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_CHANGES_WHEN_LOWERCASED
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_CHANGES_WHEN_UPPERCASED
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_CHANGES_WHEN_TITLECASED
	{UPROPS_SRC_CASE_AND_NORM, 0, changesWhenCasefolded},
	{UPROPS_SRC_CASE, 0, caseBinaryPropertyContains}, // UCHAR_CHANGES_WHEN_CASEMAPPED
	{UPROPS_SRC_NFKC_CF, 0, changesWhenNFKC_Casefolded},
	{2, U_MASK(UPROPS_2_EMOJI), defaultContains},
	{2, U_MASK(UPROPS_2_EMOJI_PRESENTATION), defaultContains},
	{2, U_MASK(UPROPS_2_EMOJI_MODIFIER), defaultContains},
	{2, U_MASK(UPROPS_2_EMOJI_MODIFIER_BASE), defaultContains},
	{2, U_MASK(UPROPS_2_EMOJI_COMPONENT), defaultContains},
	{2, 0, isRegionalIndicator},
	{1, U_MASK(UPROPS_PREPENDED_CONCATENATION_MARK), defaultContains},
	{2, U_MASK(UPROPS_2_EXTENDED_PICTOGRAPHIC), defaultContains},
}

func isBidiControl(prop *BinaryProperty, c rune, which Property) bool {
	return ubidi.IsBidiControl(c)
}

func isMirrored(prop *BinaryProperty, c rune, which Property) bool {
	return ubidi.IsMirrored(c)
}

func isRegionalIndicator(prop *BinaryProperty, c rune, which Property) bool {
	return 0x1F1E6 <= c && c <= 0x1F1FF
}

func changesWhenNFKC_Casefolded(prop *BinaryProperty, c rune, which Property) bool {
	panic("TODO")
}

func changesWhenCasefolded(prop *BinaryProperty, c rune, which Property) bool {
	panic("TODO")
}

func isPOSIX_xdigit(prop *BinaryProperty, c rune, which Property) bool {
	return uchar.IsXDigit(c)
}

func isPOSIX_print(prop *BinaryProperty, c rune, which Property) bool {
	return uchar.IsPOSIXPrint(c)
}

func isPOSIX_graph(prop *BinaryProperty, c rune, which Property) bool {
	return uchar.IsGraphPOSIX(c)
}

func isPOSIX_blank(prop *BinaryProperty, c rune, which Property) bool {
	return uchar.IsBlank(c)
}

func isPOSIX_alnum(prop *BinaryProperty, c rune, which Property) bool {
	return (uchar.GetUnicodeProperties(c, 1)&U_MASK(UPROPS_ALPHABETIC)) != 0 || uchar.IsDigit(c)
}

func isCanonSegmentStarter(prop *BinaryProperty, c rune, which Property) bool {
	panic("TODO")
}

func isJoinControl(prop *BinaryProperty, c rune, which Property) bool {
	return ubidi.IsJoinControl(c)
}

func hasFullCompositionExclusion(prop *BinaryProperty, c rune, which Property) bool {
	panic("TODO")
}

func caseBinaryPropertyContains(prop *BinaryProperty, c rune, which Property) bool {
	return HasBinaryPropertyUcase(c, which)
}

func HasBinaryPropertyUcase(c rune, which Property) bool {
	/* case mapping properties */
	switch which {
	case UCHAR_LOWERCASE:
		return ucase.UCASE_LOWER == ucase.GetType(c)
	case UCHAR_UPPERCASE:
		return ucase.UCASE_UPPER == ucase.GetType(c)
	case UCHAR_SOFT_DOTTED:
		return ucase.IsSoftDotted(c)
	case UCHAR_CASE_SENSITIVE:
		return ucase.IsCaseSensitive(c)
	case UCHAR_CASED:
		return ucase.UCASE_NONE != ucase.GetType(c)
	case UCHAR_CASE_IGNORABLE:
		return (ucase.GetTypeOrIgnorable(c) >> 2) != 0
	/*
	 * Note: The following Changes_When_Xyz are defined as testing whether
	 * the NFD form of the input changes when Xyz-case-mapped.
	 * However, this simpler implementation of these properties,
	 * ignoring NFD, passes the tests.
	 * The implementation needs to be changed if the tests start failing.
	 * When that happens, optimizations should be used to work with the
	 * per-single-code point ucase_toFullXyz() functions unless
	 * the NFD form has more than one code point,
	 * and the property starts set needs to be the union of the
	 * start sets for normalization and case mappings.
	 */
	case UCHAR_CHANGES_WHEN_LOWERCASED:
		return ucase.ToFullLower(c) >= 0
	case UCHAR_CHANGES_WHEN_UPPERCASED:
		return ucase.ToFullUpper(c) >= 0
	case UCHAR_CHANGES_WHEN_TITLECASED:
		return ucase.ToFullTitle(c) >= 0
	/* case UCHAR_CHANGES_WHEN_CASEFOLDED: -- in uprops.c */
	case UCHAR_CHANGES_WHEN_CASEMAPPED:
		return ucase.ToFullLower(c) >= 0 || ucase.ToFullUpper(c) >= 0 || ucase.ToFullTitle(c) >= 0
	default:
		return false
	}
}

func isNormInert(prop *BinaryProperty, c rune, which Property) bool {
	panic("TODO")
}

func HasBinaryProperty(c rune, which Property) bool {
	if which < UCHAR_BINARY_START || UCHAR_BINARY_LIMIT <= which {
		return false
	}
	prop := binProps[which]
	return prop.contains(prop, c, which)
}
