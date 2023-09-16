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
	"slices"

	"vitess.io/vitess/go/mysql/icuregex/internal/normalizer"
	"vitess.io/vitess/go/mysql/icuregex/internal/ubidi"
	"vitess.io/vitess/go/mysql/icuregex/internal/ucase"
	"vitess.io/vitess/go/mysql/icuregex/internal/uchar"
	"vitess.io/vitess/go/mysql/icuregex/internal/uemoji"
)

type binaryProperty struct {
	column   propertySource
	mask     uint32
	contains func(prop *binaryProperty, c rune, which Property) bool
}

func defaultContains(prop *binaryProperty, c rune, _ Property) bool {
	return (uchar.GetUnicodeProperties(c, int(prop.column)) & prop.mask) != 0
}

var binProps = [uCharBinaryLimit]*binaryProperty{
	/*
	 * column and mask values for binary properties from u_getUnicodeProperties().
	 * Must be in order of corresponding UProperty,
	 * and there must be exactly one entry per binary UProperty.
	 *
	 * Properties with mask==0 are handled in code.
	 * For them, column is the UPropertySource value.
	 *
	 * See also https://unicode-org.github.io/icu/userguide/strings/properties.html
	 */
	{1, uchar.Mask(pAlphabetic), defaultContains},
	{1, uchar.Mask(pASCIIHexDigit), defaultContains},
	{srcBidi, 0, isBidiControl},
	{srcBidi, 0, isMirrored},
	{1, uchar.Mask(pDash), defaultContains},
	{1, uchar.Mask(pDefaultIgnorableCodePoint), defaultContains},
	{1, uchar.Mask(pDeprecated), defaultContains},
	{1, uchar.Mask(pDiacritic), defaultContains},
	{1, uchar.Mask(pExtender), defaultContains},
	{srcNfc, 0, hasFullCompositionExclusion},
	{1, uchar.Mask(pGraphemeBase), defaultContains},
	{1, uchar.Mask(pGraphemeExtend), defaultContains},
	{1, uchar.Mask(pGraphemeLink), defaultContains},
	{1, uchar.Mask(pHexDigit), defaultContains},
	{1, uchar.Mask(pHyphen), defaultContains},
	{1, uchar.Mask(pIDContinue), defaultContains},
	{1, uchar.Mask(pIDStart), defaultContains},
	{1, uchar.Mask(pIdeographic), defaultContains},
	{1, uchar.Mask(pIdsBinaryOperator), defaultContains},
	{1, uchar.Mask(pIdsTrinaryOperator), defaultContains},
	{srcBidi, 0, isJoinControl},
	{1, uchar.Mask(pLogicalOrderException), defaultContains},
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_LOWERCASE
	{1, uchar.Mask(pMath), defaultContains},
	{1, uchar.Mask(pNoncharacterCodePoint), defaultContains},
	{1, uchar.Mask(pQuotationMark), defaultContains},
	{1, uchar.Mask(pRadical), defaultContains},
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_SOFT_DOTTED
	{1, uchar.Mask(pTerminalPunctuation), defaultContains},
	{1, uchar.Mask(pUnifiedIdeograph), defaultContains},
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_UPPERCASE
	{1, uchar.Mask(pWhiteSpace), defaultContains},
	{1, uchar.Mask(pXidContinue), defaultContains},
	{1, uchar.Mask(pXidStart), defaultContains},
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_CASE_SENSITIVE
	{1, uchar.Mask(pSTerm), defaultContains},
	{1, uchar.Mask(pVariationSelector), defaultContains},
	{srcNfc, 0, isNormInert},  // UCHAR_NFD_INERT
	{srcNfkc, 0, isNormInert}, // UCHAR_NFKD_INERT
	{srcNfc, 0, isNormInert},  // UCHAR_NFC_INERT
	{srcNfkc, 0, isNormInert}, // UCHAR_NFKC_INERT
	{srcNfcCanonIter, 0, nil}, // Segment_Starter is currently unsupported
	{1, uchar.Mask(pPatternSyntax), defaultContains},
	{1, uchar.Mask(pPatternWhiteSpace), defaultContains},
	{srcCharAndPropsvec, 0, isPOSIXAlnum},
	{srcChar, 0, isPOSIXBlank},
	{srcChar, 0, isPOSIXGraph},
	{srcChar, 0, isPOSIXPrint},
	{srcChar, 0, isPOSIXXdigit},
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_CASED
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_CASE_IGNORABLE
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_CHANGES_WHEN_LOWERCASED
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_CHANGES_WHEN_UPPERCASED
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_CHANGES_WHEN_TITLECASED
	{srcCaseAndNorm, 0, changesWhenCasefolded},
	{srcCase, 0, caseBinaryPropertyContains}, // UCHAR_CHANGES_WHEN_CASEMAPPED
	{srcNfkcCf, 0, nil},                      // Changes_When_NFKC_Casefolded is currently unsupported
	{srcEmoji, 0, hasEmojiProperty},          // UCHAR_EMOJI
	{srcEmoji, 0, hasEmojiProperty},          // UCHAR_EMOJI_PRESENTATION
	{srcEmoji, 0, hasEmojiProperty},          // UCHAR_EMOJI_MODIFIER
	{srcEmoji, 0, hasEmojiProperty},          // UCHAR_EMOJI_MODIFIER_BASE
	{srcEmoji, 0, hasEmojiProperty},          // UCHAR_EMOJI_COMPONENT
	{2, 0, isRegionalIndicator},
	{1, uchar.Mask(pPrependedConcatenationMark), defaultContains},
	{srcEmoji, 0, hasEmojiProperty}, // UCHAR_EXTENDED_PICTOGRAPHIC
	{srcEmoji, 0, hasEmojiProperty}, // UCHAR_BASIC_EMOJI
	{srcEmoji, 0, hasEmojiProperty}, // UCHAR_EMOJI_KEYCAP_SEQUENCE
	{srcEmoji, 0, hasEmojiProperty}, // UCHAR_RGI_EMOJI_MODIFIER_SEQUENCE
	{srcEmoji, 0, hasEmojiProperty}, // UCHAR_RGI_EMOJI_FLAG_SEQUENCE
	{srcEmoji, 0, hasEmojiProperty}, // UCHAR_RGI_EMOJI_TAG_SEQUENCE
	{srcEmoji, 0, hasEmojiProperty}, // UCHAR_RGI_EMOJI_ZWJ_SEQUENCE
	{srcEmoji, 0, hasEmojiProperty}, // UCHAR_RGI_EMOJI
}

func isBidiControl(_ *binaryProperty, c rune, _ Property) bool {
	return ubidi.IsBidiControl(c)
}

func isMirrored(_ *binaryProperty, c rune, _ Property) bool {
	return ubidi.IsMirrored(c)
}

func isRegionalIndicator(_ *binaryProperty, c rune, _ Property) bool {
	return 0x1F1E6 <= c && c <= 0x1F1FF
}

func changesWhenCasefolded(_ *binaryProperty, c rune, _ Property) bool {
	if c < 0 {
		return false
	}

	nfd := normalizer.Nfc().Decompose(c)
	if nfd == nil {
		nfd = []rune{c}
	}
	folded := ucase.FoldRunes(nfd)
	return !slices.Equal(nfd, folded)
}

func isPOSIXXdigit(_ *binaryProperty, c rune, _ Property) bool {
	return uchar.IsXDigit(c)
}

func isPOSIXPrint(_ *binaryProperty, c rune, _ Property) bool {
	return uchar.IsPOSIXPrint(c)
}

func isPOSIXGraph(_ *binaryProperty, c rune, _ Property) bool {
	return uchar.IsGraphPOSIX(c)
}

func isPOSIXBlank(_ *binaryProperty, c rune, _ Property) bool {
	return uchar.IsBlank(c)
}

func isPOSIXAlnum(_ *binaryProperty, c rune, _ Property) bool {
	return (uchar.GetUnicodeProperties(c, 1)&uchar.Mask(pAlphabetic)) != 0 || uchar.IsDigit(c)
}

func isJoinControl(_ *binaryProperty, c rune, _ Property) bool {
	return ubidi.IsJoinControl(c)
}

func hasFullCompositionExclusion(_ *binaryProperty, c rune, _ Property) bool {
	impl := normalizer.Nfc()
	return impl.IsCompNo(c)
}

func caseBinaryPropertyContains(_ *binaryProperty, c rune, which Property) bool {
	return HasBinaryPropertyUcase(c, which)
}

func HasBinaryPropertyUcase(c rune, which Property) bool {
	/* case mapping properties */
	switch which {
	case UCharLowercase:
		return ucase.Lower == ucase.GetType(c)
	case UCharUppercase:
		return ucase.Upper == ucase.GetType(c)
	case UCharSoftDotted:
		return ucase.IsSoftDotted(c)
	case UCharCaseSensitive:
		return ucase.IsCaseSensitive(c)
	case UCharCased:
		return ucase.None != ucase.GetType(c)
	case UCharCaseIgnorable:
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
	case UCharChangesWhenLowercased:
		return ucase.ToFullLower(c) >= 0
	case UCharChangesWhenUppercased:
		return ucase.ToFullUpper(c) >= 0
	case UCharChangesWhenTitlecased:
		return ucase.ToFullTitle(c) >= 0
	/* case UCHAR_CHANGES_WHEN_CASEFOLDED: -- in uprops.c */
	case UCharChangesWhenCasemapped:
		return ucase.ToFullLower(c) >= 0 || ucase.ToFullUpper(c) >= 0 || ucase.ToFullTitle(c) >= 0
	default:
		return false
	}
}

func isNormInert(_ *binaryProperty, c rune, which Property) bool {
	mode := normalizer.Mode(int32(which) - int32(UCharNfdInert) + int32(normalizer.NormNfd))
	return normalizer.IsInert(c, mode)
}

func HasBinaryProperty(c rune, which Property) bool {
	if which < UCharBinaryStart || uCharBinaryLimit <= which {
		return false
	}
	prop := binProps[which]
	if prop.contains == nil {
		return false
	}
	return prop.contains(prop, c, which)
}

func hasEmojiProperty(_ *binaryProperty, c rune, which Property) bool {
	if which < UCharEmoji || UCharRgiEmoji < which {
		return false
	}
	return uemoji.HasBinaryProperty(c, int(which-UCharEmoji))
}
