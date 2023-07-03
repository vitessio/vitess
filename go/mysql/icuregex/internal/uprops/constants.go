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

type Property int32

const (
	/*
	 * Note: UProperty constants are parsed by preparseucd.py.
	 * It matches lines like
	 *     UCHAR_<Unicode property name>=<integer>,
	 */

	/*  Note: Place UCHAR_ALPHABETIC before UCHAR_BINARY_START so that
	    debuggers display UCHAR_ALPHABETIC as the symbolic name for 0,
	    rather than UCHAR_BINARY_START.  Likewise for other *_START
	    identifiers. */

	/** Binary property Alphabetic. Same as u_isUAlphabetic, different from u_isalpha.
	  Lu+Ll+Lt+Lm+Lo+Nl+Other_Alphabetic @stable ICU 2.1 */
	UCharAlphabetic Property = 0
	/** First constant for binary Unicode properties. @stable ICU 2.1 */
	UCharBinaryStart = UCharAlphabetic
	/** Binary property ASCII_Hex_Digit. 0-9 A-F a-f @stable ICU 2.1 */
	UCharASCIIHexDigit Property = 1
	/** Binary property Bidi_Control.
	  Format controls which have specific functions
	  in the Bidi Algorithm. @stable ICU 2.1 */
	UCharBidiControl Property = 2
	/** Binary property Bidi_Mirrored.
	  Characters that may change display in RTL text.
	  Same as u_isMirrored.
	  See Bidi Algorithm, UTR 9. @stable ICU 2.1 */
	UCharBidiMirrored Property = 3
	/** Binary property Dash. Variations of dashes. @stable ICU 2.1 */
	UCharDash Property = 4
	/** Binary property Default_Ignorable_Code_Point (new in Unicode 3.2).
	  Ignorable in most processing.
	  <2060..206F, FFF0..FFFB, E0000..E0FFF>+Other_Default_Ignorable_Code_Point+(Cf+Cc+Cs-White_Space) @stable ICU 2.1 */
	UCharDefaultIgnorableCodePoint Property = 5
	/** Binary property Deprecated (new in Unicode 3.2).
	  The usage of deprecated characters is strongly discouraged. @stable ICU 2.1 */
	UCharDeprecated Property = 6
	/** Binary property Diacritic. Characters that linguistically modify
	  the meaning of another character to which they apply. @stable ICU 2.1 */
	UCharDiacritic Property = 7
	/** Binary property Extender.
	  Extend the value or shape of a preceding alphabetic character,
	  e.g., length and iteration marks. @stable ICU 2.1 */
	UCharExtender Property = 8
	/** Binary property Full_Composition_Exclusion.
	  CompositionExclusions.txt+Singleton Decompositions+
	  Non-Starter Decompositions. @stable ICU 2.1 */
	UCharFullCompositionExclusion Property = 9
	/** Binary property Grapheme_Base (new in Unicode 3.2).
	  For programmatic determination of grapheme cluster boundaries.
	  [0..10FFFF]-Cc-Cf-Cs-Co-Cn-Zl-Zp-Grapheme_Link-Grapheme_Extend-CGJ @stable ICU 2.1 */
	UCharGraphemeBase Property = 10
	/** Binary property Grapheme_Extend (new in Unicode 3.2).
	  For programmatic determination of grapheme cluster boundaries.
	  Me+Mn+Mc+Other_Grapheme_Extend-Grapheme_Link-CGJ @stable ICU 2.1 */
	UCharGraphemeExtend Property = 11
	/** Binary property Grapheme_Link (new in Unicode 3.2).
	  For programmatic determination of grapheme cluster boundaries. @stable ICU 2.1 */
	UCharGraphemeLink Property = 12
	/** Binary property Hex_Digit.
	  Characters commonly used for hexadecimal numbers. @stable ICU 2.1 */
	UCharHexDigit Property = 13
	/** Binary property Hyphen. Dashes used to mark connections
	  between pieces of words, plus the Katakana middle dot. @stable ICU 2.1 */
	UCharHyphen Property = 14
	/** Binary property ID_Continue.
	  Characters that can continue an identifier.
	  DerivedCoreProperties.txt also says "NOTE: Cf characters should be filtered out."
	  ID_Start+Mn+Mc+Nd+Pc @stable ICU 2.1 */
	UCharIDContinue Property = 15
	/** Binary property ID_Start.
	  Characters that can start an identifier.
	  Lu+Ll+Lt+Lm+Lo+Nl @stable ICU 2.1 */
	UCharIDStart Property = 16
	/** Binary property Ideographic.
	  CJKV ideographs. @stable ICU 2.1 */
	UCharIdeographic Property = 17
	/** Binary property IDS_Binary_Operator (new in Unicode 3.2).
	  For programmatic determination of
	  Ideographic Description Sequences. @stable ICU 2.1 */
	UCharIdsBinaryOperator Property = 18
	/** Binary property IDS_Trinary_Operator (new in Unicode 3.2).
	  For programmatic determination of
	  Ideographic Description Sequences. @stable ICU 2.1 */
	UCharIdsTrinaryOperator Property = 19
	/** Binary property Join_Control.
	  Format controls for cursive joining and ligation. @stable ICU 2.1 */
	UCharJoinControl Property = 20
	/** Binary property Logical_Order_Exception (new in Unicode 3.2).
	  Characters that do not use logical order and
	  require special handling in most processing. @stable ICU 2.1 */
	UCharLogicalOrderException Property = 21
	/** Binary property Lowercase. Same as u_isULowercase, different from u_islower.
	  Ll+Other_Lowercase @stable ICU 2.1 */
	UCharLowercase Property = 22
	/** Binary property Math. Sm+Other_Math @stable ICU 2.1 */
	UCharMath Property = 23
	/** Binary property Noncharacter_Code_Point.
	  Code points that are explicitly defined as illegal
	  for the encoding of characters. @stable ICU 2.1 */
	UCharNoncharacterCodePoint Property = 24
	/** Binary property Quotation_Mark. @stable ICU 2.1 */
	UCharQuotationMark Property = 25
	/** Binary property Radical (new in Unicode 3.2).
	  For programmatic determination of
	  Ideographic Description Sequences. @stable ICU 2.1 */
	UCharRadical Property = 26
	/** Binary property Soft_Dotted (new in Unicode 3.2).
	  Characters with a "soft dot", like i or j.
	  An accent placed on these characters causes
	  the dot to disappear. @stable ICU 2.1 */
	UCharSoftDotted Property = 27
	/** Binary property Terminal_Punctuation.
	  Punctuation characters that generally mark
	  the end of textual units. @stable ICU 2.1 */
	UCharTerminalPunctuation Property = 28
	/** Binary property Unified_Ideograph (new in Unicode 3.2).
	  For programmatic determination of
	  Ideographic Description Sequences. @stable ICU 2.1 */
	UCharUnifiedIdeograph Property = 29
	/** Binary property Uppercase. Same as u_isUUppercase, different from u_isupper.
	  Lu+Other_Uppercase @stable ICU 2.1 */
	UCharUppercase Property = 30
	/** Binary property White_Space.
	  Same as u_isUWhiteSpace, different from u_isspace and u_isWhitespace.
	  Space characters+TAB+CR+LF-ZWSP-ZWNBSP @stable ICU 2.1 */
	UCharWhiteSpace Property = 31
	/** Binary property XID_Continue.
	  ID_Continue modified to allow closure under
	  normalization forms NFKC and NFKD. @stable ICU 2.1 */
	UCharXidContinue Property = 32
	/** Binary property XID_Start. ID_Start modified to allow
	  closure under normalization forms NFKC and NFKD. @stable ICU 2.1 */
	UCharXidStart Property = 33
	/** Binary property Case_Sensitive. Either the source of a case
	  mapping or _in_ the target of a case mapping. Not the same as
	  the general category Cased_Letter. @stable ICU 2.6 */
	UCharCaseSensitive Property = 34
	/** Binary property STerm (new in Unicode 4.0.1).
	  Sentence Terminal. Used in UAX #29: Text Boundaries
	  (http://www.unicode.org/reports/tr29/)
	  @stable ICU 3.0 */
	UCharSTerm Property = 35
	/** Binary property Variation_Selector (new in Unicode 4.0.1).
	  Indicates all those characters that qualify as Variation Selectors.
	  For details on the behavior of these characters,
	  see StandardizedVariants.html and 15.6 Variation Selectors.
	  @stable ICU 3.0 */
	UCharVariationSelector Property = 36
	/** Binary property NFD_Inert.
	  ICU-specific property for characters that are inert under NFD,
	  i.e., they do not interact with adjacent characters.
	  See the documentation for the Normalizer2 class and the
	  Normalizer2::isInert() method.
	  @stable ICU 3.0 */
	UCharNfdInert Property = 37
	/** Binary property NFKD_Inert.
	  ICU-specific property for characters that are inert under NFKD,
	  i.e., they do not interact with adjacent characters.
	  See the documentation for the Normalizer2 class and the
	  Normalizer2::isInert() method.
	  @stable ICU 3.0 */
	UCharNfkdInert Property = 38
	/** Binary property NFC_Inert.
	  ICU-specific property for characters that are inert under NFC,
	  i.e., they do not interact with adjacent characters.
	  See the documentation for the Normalizer2 class and the
	  Normalizer2::isInert() method.
	  @stable ICU 3.0 */
	UCharNfcInert Property = 39
	/** Binary property NFKC_Inert.
	  ICU-specific property for characters that are inert under NFKC,
	  i.e., they do not interact with adjacent characters.
	  See the documentation for the Normalizer2 class and the
	  Normalizer2::isInert() method.
	  @stable ICU 3.0 */
	UCharNfkcInert Property = 40
	/** Binary Property Segment_Starter.
	  ICU-specific property for characters that are starters in terms of
	  Unicode normalization and combining character sequences.
	  They have ccc=0 and do not occur in non-initial position of the
	  canonical decomposition of any character
	  (like a-umlaut in NFD and a Jamo T in an NFD(Hangul LVT)).
	  ICU uses this property for segmenting a string for generating a set of
	  canonically equivalent strings, e.g. for canonical closure while
	  processing collation tailoring rules.
	  @stable ICU 3.0 */
	UCharSegmentStarter Property = 41
	/** Binary property Pattern_Syntax (new in Unicode 4.1).
	  See UAX #31 Identifier and Pattern Syntax
	  (http://www.unicode.org/reports/tr31/)
	  @stable ICU 3.4 */
	UCharPatternSyntax Property = 42
	/** Binary property Pattern_White_Space (new in Unicode 4.1).
	  See UAX #31 Identifier and Pattern Syntax
	  (http://www.unicode.org/reports/tr31/)
	  @stable ICU 3.4 */
	UCharPatternWhiteSpace Property = 43
	/** Binary property alnum (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCharPosixAlnum Property = 44
	/** Binary property blank (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCharPosixBlank Property = 45
	/** Binary property graph (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCharPosixGraph Property = 46
	/** Binary property print (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCharPosixPrint Property = 47
	/** Binary property xdigit (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCharPosixXdigit Property = 48
	/** Binary property Cased. For Lowercase, Uppercase and Titlecase characters. @stable ICU 4.4 */
	UCharCased Property = 49
	/** Binary property Case_Ignorable. Used in context-sensitive case mappings. @stable ICU 4.4 */
	UCharCaseIgnorable Property = 50
	/** Binary property Changes_When_Lowercased. @stable ICU 4.4 */
	UCharChangesWhenLowercased Property = 51
	/** Binary property Changes_When_Uppercased. @stable ICU 4.4 */
	UCharChangesWhenUppercased Property = 52
	/** Binary property Changes_When_Titlecased. @stable ICU 4.4 */
	UCharChangesWhenTitlecased Property = 53
	/** Binary property Changes_When_Casefolded. @stable ICU 4.4 */
	UCharChangesWhenCasefolded Property = 54
	/** Binary property Changes_When_Casemapped. @stable ICU 4.4 */
	UCharChangesWhenCasemapped Property = 55
	/** Binary property Changes_When_NFKC_Casefolded. @stable ICU 4.4 */
	UCharChangesWhenNfkcCasefolded Property = 56
	/**
	 * Binary property Emoji.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 57
	 */
	UCharEmoji Property = 57
	/**
	 * Binary property Emoji_Presentation.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 57
	 */
	UCharEmojiPresentation Property = 58
	/**
	 * Binary property Emoji_Modifier.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 57
	 */
	UCharEmojiModifier Property = 59
	/**
	 * Binary property Emoji_Modifier_Base.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 57
	 */
	UCharEmojiModifierBase Property = 60
	/**
	 * Binary property Emoji_Component.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 60
	 */
	UCharEmojiComponent Property = 61
	/**
	 * Binary property Regional_Indicator.
	 * @stable ICU 60
	 */
	UCharRegionalIndicator Property = 62
	/**
	 * Binary property Prepended_Concatenation_Mark.
	 * @stable ICU 60
	 */
	UCharPrependedConcatenationMark Property = 63
	/**
	 * Binary property Extended_Pictographic.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 62
	 */
	UCharExtendedPictographic Property = 64

	/** Enumerated property Bidi_Class.
	  Same as u_charDirection, returns UCharDirection values. @stable ICU 2.2 */
	UCharBidiClass Property = 0x1000
	/** First constant for enumerated/integer Unicode properties. @stable ICU 2.2 */
	UCharIntStart = UCharBidiClass
	/** Enumerated property Block.
	  Same as ublock_getCode, returns UBlockCode values. @stable ICU 2.2 */
	UCharBlock Property = 0x1001
	/** Enumerated property Canonical_Combining_Class.
	  Same as u_getCombiningClass, returns 8-bit numeric values. @stable ICU 2.2 */
	UCharCanonicalCombiningClass Property = 0x1002
	/** Enumerated property Decomposition_Type.
	  Returns UDecompositionType values. @stable ICU 2.2 */
	UCharDecompositionType Property = 0x1003
	/** Enumerated property East_Asian_Width.
	  See http://www.unicode.org/reports/tr11/
	  Returns UEastAsianWidth values. @stable ICU 2.2 */
	UCharEastAsianWidth Property = 0x1004
	/** Enumerated property General_Category.
	  Same as u_charType, returns UCharCategory values. @stable ICU 2.2 */
	UCharGeneralCategory Property = 0x1005
	/** Enumerated property Joining_Group.
	  Returns UJoiningGroup values. @stable ICU 2.2 */
	UCharJoiningGroup Property = 0x1006
	/** Enumerated property Joining_Type.
	  Returns UJoiningType values. @stable ICU 2.2 */
	UCharJoiningType Property = 0x1007
	/** Enumerated property Line_Break.
	  Returns ULineBreak values. @stable ICU 2.2 */
	UCharLineBreak Property = 0x1008
	/** Enumerated property Numeric_Type.
	  Returns UNumericType values. @stable ICU 2.2 */
	UCharNumericType Property = 0x1009
	/** Enumerated property Script.
	  Same as uscript_getScript, returns UScriptCode values. @stable ICU 2.2 */
	UCharScript Property = 0x100A
	/** Enumerated property Hangul_Syllable_Type, new in Unicode 4.
	  Returns UHangulSyllableType values. @stable ICU 2.6 */
	UCharHangulSyllableType Property = 0x100B
	/** Enumerated property NFD_Quick_Check.
	  Returns UNormalizationCheckResult values. @stable ICU 3.0 */
	UCharNfdQuickCheck Property = 0x100C
	/** Enumerated property NFKD_Quick_Check.
	  Returns UNormalizationCheckResult values. @stable ICU 3.0 */
	UCharNfkdQuickCheck Property = 0x100D
	/** Enumerated property NFC_Quick_Check.
	  Returns UNormalizationCheckResult values. @stable ICU 3.0 */
	UCharNfcQuickCheck Property = 0x100E
	/** Enumerated property NFKC_Quick_Check.
	  Returns UNormalizationCheckResult values. @stable ICU 3.0 */
	UCharNfkcQuickCheck Property = 0x100F
	/** Enumerated property Lead_Canonical_Combining_Class.
	  ICU-specific property for the ccc of the first code point
	  of the decomposition, or lccc(c)=ccc(NFD(c)[0]).
	  Useful for checking for canonically ordered text;
	  see UNORM_FCD and http://www.unicode.org/notes/tn5/#FCD .
	  Returns 8-bit numeric values like UCHAR_CANONICAL_COMBINING_CLASS. @stable ICU 3.0 */
	UCharLeadCanonicalCombiningClass Property = 0x1010
	/** Enumerated property Trail_Canonical_Combining_Class.
	  ICU-specific property for the ccc of the last code point
	  of the decomposition, or tccc(c)=ccc(NFD(c)[last]).
	  Useful for checking for canonically ordered text;
	  see UNORM_FCD and http://www.unicode.org/notes/tn5/#FCD .
	  Returns 8-bit numeric values like UCHAR_CANONICAL_COMBINING_CLASS. @stable ICU 3.0 */
	UCharTrailCanonicalCombiningClass Property = 0x1011
	/** Enumerated property Grapheme_Cluster_Break (new in Unicode 4.1).
	  Used in UAX #29: Text Boundaries
	  (http://www.unicode.org/reports/tr29/)
	  Returns UGraphemeClusterBreak values. @stable ICU 3.4 */
	UCharGraphemeClusterBreak Property = 0x1012
	/** Enumerated property Sentence_Break (new in Unicode 4.1).
	  Used in UAX #29: Text Boundaries
	  (http://www.unicode.org/reports/tr29/)
	  Returns USentenceBreak values. @stable ICU 3.4 */
	UCharSentenceBreak Property = 0x1013
	/** Enumerated property Word_Break (new in Unicode 4.1).
	  Used in UAX #29: Text Boundaries
	  (http://www.unicode.org/reports/tr29/)
	  Returns UWordBreakValues values. @stable ICU 3.4 */
	UCharWordBreak Property = 0x1014
	/** Enumerated property Bidi_Paired_Bracket_Type (new in Unicode 6.3).
	  Used in UAX #9: Unicode Bidirectional Algorithm
	  (http://www.unicode.org/reports/tr9/)
	  Returns UBidiPairedBracketType values. @stable ICU 52 */
	UCharBidiPairedBracketType Property = 0x1015
	/**
	 * Enumerated property Indic_Positional_Category.
	 * New in Unicode 6.0 as provisional property Indic_Matra_Category;
	 * renamed and changed to informative in Unicode 8.0.
	 * See http://www.unicode.org/reports/tr44/#IndicPositionalCategory.txt
	 * @stable ICU 63
	 */
	UCharIndicPositionalCategory Property = 0x1016
	/**
	 * Enumerated property Indic_Syllabic_Category.
	 * New in Unicode 6.0 as provisional; informative since Unicode 8.0.
	 * See http://www.unicode.org/reports/tr44/#IndicSyllabicCategory.txt
	 * @stable ICU 63
	 */
	UCharIndicSyllableCategory Property = 0x1017
	/**
	 * Enumerated property Vertical_Orientation.
	 * Used for UAX #50 Unicode Vertical Text Layout (https://www.unicode.org/reports/tr50/).
	 * New as a UCD property in Unicode 10.0.
	 * @stable ICU 63
	 */
	UCharVerticalOrientation Property = 0x1018

	/** Bitmask property General_Category_Mask.
	  This is the General_Category property returned as a bit mask.
	  When used in u_getIntPropertyValue(c), same as U_MASK(u_charType(c)),
	  returns bit masks for UCharCategory values where exactly one bit is set.
	  When used with u_getPropertyValueName() and u_getPropertyValueEnum(),
	  a multi-bit mask is used for sets of categories like "Letters".
	  Mask values should be cast to uint32_t.
	  @stable ICU 2.4 */
	UCharGeneralCategoryMask Property = 0x2000
	/** First constant for bit-mask Unicode properties. @stable ICU 2.4 */
	UCharMaskStart = UCharGeneralCategoryMask
	/** Double property Numeric_Value.
	  Corresponds to u_getNumericValue. @stable ICU 2.4 */
	UCharNumericValue Property = 0x3000
	/** First constant for double Unicode properties. @stable ICU 2.4 */
	UCharDoubleStart = UCharNumericValue
	/** String property Age.
	  Corresponds to u_charAge. @stable ICU 2.4 */
	UCharAge Property = 0x4000
	/** First constant for string Unicode properties. @stable ICU 2.4 */
	UCharStringStart = UCharAge
	/** String property Bidi_Mirroring_Glyph.
	  Corresponds to u_charMirror. @stable ICU 2.4 */
	UCharBidiMirroringGlyph Property = 0x4001
	/** String property Case_Folding.
	  Corresponds to u_strFoldCase in ustring.h. @stable ICU 2.4 */
	UCharCaseFolding Property = 0x4002
	/** String property Lowercase_Mapping.
	  Corresponds to u_strToLower in ustring.h. @stable ICU 2.4 */
	UCharLowercaseMapping Property = 0x4004
	/** String property Name.
	  Corresponds to u_charName. @stable ICU 2.4 */
	UCharName Property = 0x4005
	/** String property Simple_Case_Folding.
	  Corresponds to u_foldCase. @stable ICU 2.4 */
	UCharSimpleCaseFolding Property = 0x4006
	/** String property Simple_Lowercase_Mapping.
	  Corresponds to u_tolower. @stable ICU 2.4 */
	UCharSimpleLowercaseMapping Property = 0x4007
	/** String property Simple_Titlecase_Mapping.
	  Corresponds to u_totitle. @stable ICU 2.4 */
	UcharSimpleTitlecaseMapping Property = 0x4008
	/** String property Simple_Uppercase_Mapping.
	  Corresponds to u_toupper. @stable ICU 2.4 */
	UCharSimpleUppercaseMapping Property = 0x4009
	/** String property Titlecase_Mapping.
	  Corresponds to u_strToTitle in ustring.h. @stable ICU 2.4 */
	UCharTitlecaseMapping Property = 0x400A
	/** String property Uppercase_Mapping.
	  Corresponds to u_strToUpper in ustring.h. @stable ICU 2.4 */
	UCharUppercaseMapping Property = 0x400C
	/** String property Bidi_Paired_Bracket (new in Unicode 6.3).
	  Corresponds to u_getBidiPairedBracket. @stable ICU 52 */
	UCharBidiPairedBracket Property = 0x400D

	/** Miscellaneous property Script_Extensions (new in Unicode 6.0).
	  Some characters are commonly used in multiple scripts.
	  For more information, see UAX #24: http://www.unicode.org/reports/tr24/.
	  Corresponds to uscript_hasScript and uscript_getScriptExtensions in uscript.h.
	  @stable ICU 4.6 */
	UCharScriptExtensions Property = 0x7000
	/** First constant for Unicode properties with unusual value types. @stable ICU 4.6 */
	UCharOtherPropertyStart = UCharScriptExtensions

	/** Represents a nonexistent or invalid property or property value. @stable ICU 2.4 */
	UCharInvalidCode Property = -1
)

const (
	uCharBinaryLimit = 65
	uCharIntLimit    = 0x1019
	uCharMaskLimit   = 0x2001
	uCharStringLimit = 0x400E
)

/*
 * Properties in vector word 1
 * Each bit encodes one binary property.
 * The following constants represent the bit number, use 1<<UPROPS_XYZ.
 * pBinary1Top<=32!
 *
 * Keep this list of property enums in sync with
 * propListNames[] in icu/source/tools/genprops/props2.c!
 *
 * ICU 2.6/uprops format version 3.2 stores full properties instead of "Other_".
 */
const (
	pWhiteSpace = iota
	pDash
	pHyphen
	pQuotationMark
	pTerminalPunctuation
	pMath
	pHexDigit
	pASCIIHexDigit
	pAlphabetic
	pIdeographic
	pDiacritic
	pExtender
	pNoncharacterCodePoint
	pGraphemeExtend
	pGraphemeLink
	pIdsBinaryOperator
	pIdsTrinaryOperator
	pRadical
	pUnifiedIdeograph
	pDefaultIgnorableCodePoint
	pDeprecated
	pLogicalOrderException
	pXidStart
	pXidContinue
	pIDStart
	pIDContinue
	pGraphemeBase
	pSTerm
	pVariationSelector
	pPatternSyntax
	pPatternWhiteSpace
	pPrependedConcatenationMark
	pBinary1Top
)

/*
 * Properties in vector word 2
 * Bits
 * 31..26   http://www.unicode.org/reports/tr51/#Emoji_Properties
 * 25..20   Line Break
 * 19..15   Sentence Break
 * 14..10   Word Break
 *  9.. 5   Grapheme Cluster Break
 *  4.. 0   Decomposition Type
 */
const (
	p2ExtendedPictographic = 26 + iota
	p2EmojiComponent
	p2Emoji
	p2EmojiPresentation
	p2EmojiModifier
	p2EmojiModifierBase
)

type propertySource int32

const (
	/** No source, not a supported property. */
	srcNone propertySource = iota
	/** From uchar.c/uprops.icu main trie */
	srcChar
	/** From uchar.c/uprops.icu properties vectors trie */
	srcPropsvec
	/** From unames.c/unames.icu */
	srcNames
	/** From ucase.c/ucase.icu */
	srcCase
	/** From ubidi_props.c/ubidi.icu */
	srcBidi
	/** From uchar.c/uprops.icu main trie as well as properties vectors trie */
	srcCharAndPropsvec
	/** From ucase.c/ucase.icu as well as unorm.cpp/unorm.icu */
	srcCaseAndNorm
	/** From normalizer2impl.cpp/nfc.nrm */
	srcNfc
	/** From normalizer2impl.cpp/nfkc.nrm */
	srcNfkc
	/** From normalizer2impl.cpp/nfkc_cf.nrm */
	srcNfkcCf
	/** From normalizer2impl.cpp/nfc.nrm canonical iterator data */
	srcNfcCanonIter
	// Text layout properties.
	srcInpc
	srcInsc
	srcVo
)

const (
	scriptXMask  = 0x00f000ff
	scriptXShift = 22

	scriptHighMask  = 0x00300000
	scriptHighShift = 12
	maxScript       = 0x3ff

	scriptLowMask = 0x000000ff

	scriptXWithCommon    = 0x400000
	scriptXWithInherited = 0x800000
	scriptXWithOther     = 0xc00000
)
