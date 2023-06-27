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

const (
	UPROPS_PROPS32_INDEX = iota
	UPROPS_EXCEPTIONS_INDEX
	UPROPS_EXCEPTIONS_TOP_INDEX

	UPROPS_ADDITIONAL_TRIE_INDEX
	UPROPS_ADDITIONAL_VECTORS_INDEX
	UPROPS_ADDITIONAL_VECTORS_COLUMNS_INDEX

	UPROPS_SCRIPT_EXTENSIONS_INDEX

	UPROPS_RESERVED_INDEX_7
	UPROPS_RESERVED_INDEX_8

	/* size of the data file (number of 32-bit units after the header) */
	UPROPS_DATA_TOP_INDEX

	/* maximum values for code values in vector word 0 */
	UPROPS_MAX_VALUES_INDEX = 10
	/* maximum values for code values in vector word 2 */
	UPROPS_MAX_VALUES_2_INDEX = 11

	UPROPS_INDEX_COUNT = 16
)

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
	UCHAR_ALPHABETIC Property = 0
	/** First constant for binary Unicode properties. @stable ICU 2.1 */
	UCHAR_BINARY_START = UCHAR_ALPHABETIC
	/** Binary property ASCII_Hex_Digit. 0-9 A-F a-f @stable ICU 2.1 */
	UCHAR_ASCII_HEX_DIGIT Property = 1
	/** Binary property Bidi_Control.
	  Format controls which have specific functions
	  in the Bidi Algorithm. @stable ICU 2.1 */
	UCHAR_BIDI_CONTROL Property = 2
	/** Binary property Bidi_Mirrored.
	  Characters that may change display in RTL text.
	  Same as u_isMirrored.
	  See Bidi Algorithm, UTR 9. @stable ICU 2.1 */
	UCHAR_BIDI_MIRRORED Property = 3
	/** Binary property Dash. Variations of dashes. @stable ICU 2.1 */
	UCHAR_DASH Property = 4
	/** Binary property Default_Ignorable_Code_Point (new in Unicode 3.2).
	  Ignorable in most processing.
	  <2060..206F, FFF0..FFFB, E0000..E0FFF>+Other_Default_Ignorable_Code_Point+(Cf+Cc+Cs-White_Space) @stable ICU 2.1 */
	UCHAR_DEFAULT_IGNORABLE_CODE_POINT Property = 5
	/** Binary property Deprecated (new in Unicode 3.2).
	  The usage of deprecated characters is strongly discouraged. @stable ICU 2.1 */
	UCHAR_DEPRECATED Property = 6
	/** Binary property Diacritic. Characters that linguistically modify
	  the meaning of another character to which they apply. @stable ICU 2.1 */
	UCHAR_DIACRITIC Property = 7
	/** Binary property Extender.
	  Extend the value or shape of a preceding alphabetic character,
	  e.g., length and iteration marks. @stable ICU 2.1 */
	UCHAR_EXTENDER Property = 8
	/** Binary property Full_Composition_Exclusion.
	  CompositionExclusions.txt+Singleton Decompositions+
	  Non-Starter Decompositions. @stable ICU 2.1 */
	UCHAR_FULL_COMPOSITION_EXCLUSION Property = 9
	/** Binary property Grapheme_Base (new in Unicode 3.2).
	  For programmatic determination of grapheme cluster boundaries.
	  [0..10FFFF]-Cc-Cf-Cs-Co-Cn-Zl-Zp-Grapheme_Link-Grapheme_Extend-CGJ @stable ICU 2.1 */
	UCHAR_GRAPHEME_BASE Property = 10
	/** Binary property Grapheme_Extend (new in Unicode 3.2).
	  For programmatic determination of grapheme cluster boundaries.
	  Me+Mn+Mc+Other_Grapheme_Extend-Grapheme_Link-CGJ @stable ICU 2.1 */
	UCHAR_GRAPHEME_EXTEND Property = 11
	/** Binary property Grapheme_Link (new in Unicode 3.2).
	  For programmatic determination of grapheme cluster boundaries. @stable ICU 2.1 */
	UCHAR_GRAPHEME_LINK Property = 12
	/** Binary property Hex_Digit.
	  Characters commonly used for hexadecimal numbers. @stable ICU 2.1 */
	UCHAR_HEX_DIGIT Property = 13
	/** Binary property Hyphen. Dashes used to mark connections
	  between pieces of words, plus the Katakana middle dot. @stable ICU 2.1 */
	UCHAR_HYPHEN Property = 14
	/** Binary property ID_Continue.
	  Characters that can continue an identifier.
	  DerivedCoreProperties.txt also says "NOTE: Cf characters should be filtered out."
	  ID_Start+Mn+Mc+Nd+Pc @stable ICU 2.1 */
	UCHAR_ID_CONTINUE Property = 15
	/** Binary property ID_Start.
	  Characters that can start an identifier.
	  Lu+Ll+Lt+Lm+Lo+Nl @stable ICU 2.1 */
	UCHAR_ID_START Property = 16
	/** Binary property Ideographic.
	  CJKV ideographs. @stable ICU 2.1 */
	UCHAR_IDEOGRAPHIC Property = 17
	/** Binary property IDS_Binary_Operator (new in Unicode 3.2).
	  For programmatic determination of
	  Ideographic Description Sequences. @stable ICU 2.1 */
	UCHAR_IDS_BINARY_OPERATOR Property = 18
	/** Binary property IDS_Trinary_Operator (new in Unicode 3.2).
	  For programmatic determination of
	  Ideographic Description Sequences. @stable ICU 2.1 */
	UCHAR_IDS_TRINARY_OPERATOR Property = 19
	/** Binary property Join_Control.
	  Format controls for cursive joining and ligation. @stable ICU 2.1 */
	UCHAR_JOIN_CONTROL Property = 20
	/** Binary property Logical_Order_Exception (new in Unicode 3.2).
	  Characters that do not use logical order and
	  require special handling in most processing. @stable ICU 2.1 */
	UCHAR_LOGICAL_ORDER_EXCEPTION Property = 21
	/** Binary property Lowercase. Same as u_isULowercase, different from u_islower.
	  Ll+Other_Lowercase @stable ICU 2.1 */
	UCHAR_LOWERCASE Property = 22
	/** Binary property Math. Sm+Other_Math @stable ICU 2.1 */
	UCHAR_MATH Property = 23
	/** Binary property Noncharacter_Code_Point.
	  Code points that are explicitly defined as illegal
	  for the encoding of characters. @stable ICU 2.1 */
	UCHAR_NONCHARACTER_CODE_POINT Property = 24
	/** Binary property Quotation_Mark. @stable ICU 2.1 */
	UCHAR_QUOTATION_MARK Property = 25
	/** Binary property Radical (new in Unicode 3.2).
	  For programmatic determination of
	  Ideographic Description Sequences. @stable ICU 2.1 */
	UCHAR_RADICAL Property = 26
	/** Binary property Soft_Dotted (new in Unicode 3.2).
	  Characters with a "soft dot", like i or j.
	  An accent placed on these characters causes
	  the dot to disappear. @stable ICU 2.1 */
	UCHAR_SOFT_DOTTED Property = 27
	/** Binary property Terminal_Punctuation.
	  Punctuation characters that generally mark
	  the end of textual units. @stable ICU 2.1 */
	UCHAR_TERMINAL_PUNCTUATION Property = 28
	/** Binary property Unified_Ideograph (new in Unicode 3.2).
	  For programmatic determination of
	  Ideographic Description Sequences. @stable ICU 2.1 */
	UCHAR_UNIFIED_IDEOGRAPH Property = 29
	/** Binary property Uppercase. Same as u_isUUppercase, different from u_isupper.
	  Lu+Other_Uppercase @stable ICU 2.1 */
	UCHAR_UPPERCASE Property = 30
	/** Binary property White_Space.
	  Same as u_isUWhiteSpace, different from u_isspace and u_isWhitespace.
	  Space characters+TAB+CR+LF-ZWSP-ZWNBSP @stable ICU 2.1 */
	UCHAR_WHITE_SPACE Property = 31
	/** Binary property XID_Continue.
	  ID_Continue modified to allow closure under
	  normalization forms NFKC and NFKD. @stable ICU 2.1 */
	UCHAR_XID_CONTINUE Property = 32
	/** Binary property XID_Start. ID_Start modified to allow
	  closure under normalization forms NFKC and NFKD. @stable ICU 2.1 */
	UCHAR_XID_START Property = 33
	/** Binary property Case_Sensitive. Either the source of a case
	  mapping or _in_ the target of a case mapping. Not the same as
	  the general category Cased_Letter. @stable ICU 2.6 */
	UCHAR_CASE_SENSITIVE Property = 34
	/** Binary property STerm (new in Unicode 4.0.1).
	  Sentence Terminal. Used in UAX #29: Text Boundaries
	  (http://www.unicode.org/reports/tr29/)
	  @stable ICU 3.0 */
	UCHAR_S_TERM Property = 35
	/** Binary property Variation_Selector (new in Unicode 4.0.1).
	  Indicates all those characters that qualify as Variation Selectors.
	  For details on the behavior of these characters,
	  see StandardizedVariants.html and 15.6 Variation Selectors.
	  @stable ICU 3.0 */
	UCHAR_VARIATION_SELECTOR Property = 36
	/** Binary property NFD_Inert.
	  ICU-specific property for characters that are inert under NFD,
	  i.e., they do not interact with adjacent characters.
	  See the documentation for the Normalizer2 class and the
	  Normalizer2::isInert() method.
	  @stable ICU 3.0 */
	UCHAR_NFD_INERT Property = 37
	/** Binary property NFKD_Inert.
	  ICU-specific property for characters that are inert under NFKD,
	  i.e., they do not interact with adjacent characters.
	  See the documentation for the Normalizer2 class and the
	  Normalizer2::isInert() method.
	  @stable ICU 3.0 */
	UCHAR_NFKD_INERT Property = 38
	/** Binary property NFC_Inert.
	  ICU-specific property for characters that are inert under NFC,
	  i.e., they do not interact with adjacent characters.
	  See the documentation for the Normalizer2 class and the
	  Normalizer2::isInert() method.
	  @stable ICU 3.0 */
	UCHAR_NFC_INERT Property = 39
	/** Binary property NFKC_Inert.
	  ICU-specific property for characters that are inert under NFKC,
	  i.e., they do not interact with adjacent characters.
	  See the documentation for the Normalizer2 class and the
	  Normalizer2::isInert() method.
	  @stable ICU 3.0 */
	UCHAR_NFKC_INERT Property = 40
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
	UCHAR_SEGMENT_STARTER Property = 41
	/** Binary property Pattern_Syntax (new in Unicode 4.1).
	  See UAX #31 Identifier and Pattern Syntax
	  (http://www.unicode.org/reports/tr31/)
	  @stable ICU 3.4 */
	UCHAR_PATTERN_SYNTAX Property = 42
	/** Binary property Pattern_White_Space (new in Unicode 4.1).
	  See UAX #31 Identifier and Pattern Syntax
	  (http://www.unicode.org/reports/tr31/)
	  @stable ICU 3.4 */
	UCHAR_PATTERN_WHITE_SPACE Property = 43
	/** Binary property alnum (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCHAR_POSIX_ALNUM Property = 44
	/** Binary property blank (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCHAR_POSIX_BLANK Property = 45
	/** Binary property graph (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCHAR_POSIX_GRAPH Property = 46
	/** Binary property print (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCHAR_POSIX_PRINT Property = 47
	/** Binary property xdigit (a C/POSIX character class).
	  Implemented according to the UTS #18 Annex C Standard Recommendation.
	  See the uchar.h file documentation.
	  @stable ICU 3.4 */
	UCHAR_POSIX_XDIGIT Property = 48
	/** Binary property Cased. For Lowercase, Uppercase and Titlecase characters. @stable ICU 4.4 */
	UCHAR_CASED Property = 49
	/** Binary property Case_Ignorable. Used in context-sensitive case mappings. @stable ICU 4.4 */
	UCHAR_CASE_IGNORABLE Property = 50
	/** Binary property Changes_When_Lowercased. @stable ICU 4.4 */
	UCHAR_CHANGES_WHEN_LOWERCASED Property = 51
	/** Binary property Changes_When_Uppercased. @stable ICU 4.4 */
	UCHAR_CHANGES_WHEN_UPPERCASED Property = 52
	/** Binary property Changes_When_Titlecased. @stable ICU 4.4 */
	UCHAR_CHANGES_WHEN_TITLECASED Property = 53
	/** Binary property Changes_When_Casefolded. @stable ICU 4.4 */
	UCHAR_CHANGES_WHEN_CASEFOLDED Property = 54
	/** Binary property Changes_When_Casemapped. @stable ICU 4.4 */
	UCHAR_CHANGES_WHEN_CASEMAPPED Property = 55
	/** Binary property Changes_When_NFKC_Casefolded. @stable ICU 4.4 */
	UCHAR_CHANGES_WHEN_NFKC_CASEFOLDED Property = 56
	/**
	 * Binary property Emoji.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 57
	 */
	UCHAR_EMOJI Property = 57
	/**
	 * Binary property Emoji_Presentation.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 57
	 */
	UCHAR_EMOJI_PRESENTATION Property = 58
	/**
	 * Binary property Emoji_Modifier.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 57
	 */
	UCHAR_EMOJI_MODIFIER Property = 59
	/**
	 * Binary property Emoji_Modifier_Base.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 57
	 */
	UCHAR_EMOJI_MODIFIER_BASE Property = 60
	/**
	 * Binary property Emoji_Component.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 60
	 */
	UCHAR_EMOJI_COMPONENT Property = 61
	/**
	 * Binary property Regional_Indicator.
	 * @stable ICU 60
	 */
	UCHAR_REGIONAL_INDICATOR Property = 62
	/**
	 * Binary property Prepended_Concatenation_Mark.
	 * @stable ICU 60
	 */
	UCHAR_PREPENDED_CONCATENATION_MARK Property = 63
	/**
	 * Binary property Extended_Pictographic.
	 * See http://www.unicode.org/reports/tr51/#Emoji_Properties
	 *
	 * @stable ICU 62
	 */
	UCHAR_EXTENDED_PICTOGRAPHIC Property = 64

	/** Enumerated property Bidi_Class.
	  Same as u_charDirection, returns UCharDirection values. @stable ICU 2.2 */
	UCHAR_BIDI_CLASS Property = 0x1000
	/** First constant for enumerated/integer Unicode properties. @stable ICU 2.2 */
	UCHAR_INT_START = UCHAR_BIDI_CLASS
	/** Enumerated property Block.
	  Same as ublock_getCode, returns UBlockCode values. @stable ICU 2.2 */
	UCHAR_BLOCK Property = 0x1001
	/** Enumerated property Canonical_Combining_Class.
	  Same as u_getCombiningClass, returns 8-bit numeric values. @stable ICU 2.2 */
	UCHAR_CANONICAL_COMBINING_CLASS Property = 0x1002
	/** Enumerated property Decomposition_Type.
	  Returns UDecompositionType values. @stable ICU 2.2 */
	UCHAR_DECOMPOSITION_TYPE Property = 0x1003
	/** Enumerated property East_Asian_Width.
	  See http://www.unicode.org/reports/tr11/
	  Returns UEastAsianWidth values. @stable ICU 2.2 */
	UCHAR_EAST_ASIAN_WIDTH Property = 0x1004
	/** Enumerated property General_Category.
	  Same as u_charType, returns UCharCategory values. @stable ICU 2.2 */
	UCHAR_GENERAL_CATEGORY Property = 0x1005
	/** Enumerated property Joining_Group.
	  Returns UJoiningGroup values. @stable ICU 2.2 */
	UCHAR_JOINING_GROUP Property = 0x1006
	/** Enumerated property Joining_Type.
	  Returns UJoiningType values. @stable ICU 2.2 */
	UCHAR_JOINING_TYPE Property = 0x1007
	/** Enumerated property Line_Break.
	  Returns ULineBreak values. @stable ICU 2.2 */
	UCHAR_LINE_BREAK Property = 0x1008
	/** Enumerated property Numeric_Type.
	  Returns UNumericType values. @stable ICU 2.2 */
	UCHAR_NUMERIC_TYPE Property = 0x1009
	/** Enumerated property Script.
	  Same as uscript_getScript, returns UScriptCode values. @stable ICU 2.2 */
	UCHAR_SCRIPT Property = 0x100A
	/** Enumerated property Hangul_Syllable_Type, new in Unicode 4.
	  Returns UHangulSyllableType values. @stable ICU 2.6 */
	UCHAR_HANGUL_SYLLABLE_TYPE Property = 0x100B
	/** Enumerated property NFD_Quick_Check.
	  Returns UNormalizationCheckResult values. @stable ICU 3.0 */
	UCHAR_NFD_QUICK_CHECK Property = 0x100C
	/** Enumerated property NFKD_Quick_Check.
	  Returns UNormalizationCheckResult values. @stable ICU 3.0 */
	UCHAR_NFKD_QUICK_CHECK Property = 0x100D
	/** Enumerated property NFC_Quick_Check.
	  Returns UNormalizationCheckResult values. @stable ICU 3.0 */
	UCHAR_NFC_QUICK_CHECK Property = 0x100E
	/** Enumerated property NFKC_Quick_Check.
	  Returns UNormalizationCheckResult values. @stable ICU 3.0 */
	UCHAR_NFKC_QUICK_CHECK Property = 0x100F
	/** Enumerated property Lead_Canonical_Combining_Class.
	  ICU-specific property for the ccc of the first code point
	  of the decomposition, or lccc(c)=ccc(NFD(c)[0]).
	  Useful for checking for canonically ordered text;
	  see UNORM_FCD and http://www.unicode.org/notes/tn5/#FCD .
	  Returns 8-bit numeric values like UCHAR_CANONICAL_COMBINING_CLASS. @stable ICU 3.0 */
	UCHAR_LEAD_CANONICAL_COMBINING_CLASS Property = 0x1010
	/** Enumerated property Trail_Canonical_Combining_Class.
	  ICU-specific property for the ccc of the last code point
	  of the decomposition, or tccc(c)=ccc(NFD(c)[last]).
	  Useful for checking for canonically ordered text;
	  see UNORM_FCD and http://www.unicode.org/notes/tn5/#FCD .
	  Returns 8-bit numeric values like UCHAR_CANONICAL_COMBINING_CLASS. @stable ICU 3.0 */
	UCHAR_TRAIL_CANONICAL_COMBINING_CLASS Property = 0x1011
	/** Enumerated property Grapheme_Cluster_Break (new in Unicode 4.1).
	  Used in UAX #29: Text Boundaries
	  (http://www.unicode.org/reports/tr29/)
	  Returns UGraphemeClusterBreak values. @stable ICU 3.4 */
	UCHAR_GRAPHEME_CLUSTER_BREAK Property = 0x1012
	/** Enumerated property Sentence_Break (new in Unicode 4.1).
	  Used in UAX #29: Text Boundaries
	  (http://www.unicode.org/reports/tr29/)
	  Returns USentenceBreak values. @stable ICU 3.4 */
	UCHAR_SENTENCE_BREAK Property = 0x1013
	/** Enumerated property Word_Break (new in Unicode 4.1).
	  Used in UAX #29: Text Boundaries
	  (http://www.unicode.org/reports/tr29/)
	  Returns UWordBreakValues values. @stable ICU 3.4 */
	UCHAR_WORD_BREAK Property = 0x1014
	/** Enumerated property Bidi_Paired_Bracket_Type (new in Unicode 6.3).
	  Used in UAX #9: Unicode Bidirectional Algorithm
	  (http://www.unicode.org/reports/tr9/)
	  Returns UBidiPairedBracketType values. @stable ICU 52 */
	UCHAR_BIDI_PAIRED_BRACKET_TYPE Property = 0x1015
	/**
	 * Enumerated property Indic_Positional_Category.
	 * New in Unicode 6.0 as provisional property Indic_Matra_Category;
	 * renamed and changed to informative in Unicode 8.0.
	 * See http://www.unicode.org/reports/tr44/#IndicPositionalCategory.txt
	 * @stable ICU 63
	 */
	UCHAR_INDIC_POSITIONAL_CATEGORY Property = 0x1016
	/**
	 * Enumerated property Indic_Syllabic_Category.
	 * New in Unicode 6.0 as provisional; informative since Unicode 8.0.
	 * See http://www.unicode.org/reports/tr44/#IndicSyllabicCategory.txt
	 * @stable ICU 63
	 */
	UCHAR_INDIC_SYLLABIC_CATEGORY Property = 0x1017
	/**
	 * Enumerated property Vertical_Orientation.
	 * Used for UAX #50 Unicode Vertical Text Layout (https://www.unicode.org/reports/tr50/).
	 * New as a UCD property in Unicode 10.0.
	 * @stable ICU 63
	 */
	UCHAR_VERTICAL_ORIENTATION Property = 0x1018

	/** Bitmask property General_Category_Mask.
	  This is the General_Category property returned as a bit mask.
	  When used in u_getIntPropertyValue(c), same as U_MASK(u_charType(c)),
	  returns bit masks for UCharCategory values where exactly one bit is set.
	  When used with u_getPropertyValueName() and u_getPropertyValueEnum(),
	  a multi-bit mask is used for sets of categories like "Letters".
	  Mask values should be cast to uint32_t.
	  @stable ICU 2.4 */
	UCHAR_GENERAL_CATEGORY_MASK Property = 0x2000
	/** First constant for bit-mask Unicode properties. @stable ICU 2.4 */
	UCHAR_MASK_START = UCHAR_GENERAL_CATEGORY_MASK
	/** Double property Numeric_Value.
	  Corresponds to u_getNumericValue. @stable ICU 2.4 */
	UCHAR_NUMERIC_VALUE Property = 0x3000
	/** First constant for double Unicode properties. @stable ICU 2.4 */
	UCHAR_DOUBLE_START = UCHAR_NUMERIC_VALUE
	/** String property Age.
	  Corresponds to u_charAge. @stable ICU 2.4 */
	UCHAR_AGE Property = 0x4000
	/** First constant for string Unicode properties. @stable ICU 2.4 */
	UCHAR_STRING_START = UCHAR_AGE
	/** String property Bidi_Mirroring_Glyph.
	  Corresponds to u_charMirror. @stable ICU 2.4 */
	UCHAR_BIDI_MIRRORING_GLYPH Property = 0x4001
	/** String property Case_Folding.
	  Corresponds to u_strFoldCase in ustring.h. @stable ICU 2.4 */
	UCHAR_CASE_FOLDING Property = 0x4002
	/** String property Lowercase_Mapping.
	  Corresponds to u_strToLower in ustring.h. @stable ICU 2.4 */
	UCHAR_LOWERCASE_MAPPING Property = 0x4004
	/** String property Name.
	  Corresponds to u_charName. @stable ICU 2.4 */
	UCHAR_NAME Property = 0x4005
	/** String property Simple_Case_Folding.
	  Corresponds to u_foldCase. @stable ICU 2.4 */
	UCHAR_SIMPLE_CASE_FOLDING Property = 0x4006
	/** String property Simple_Lowercase_Mapping.
	  Corresponds to u_tolower. @stable ICU 2.4 */
	UCHAR_SIMPLE_LOWERCASE_MAPPING Property = 0x4007
	/** String property Simple_Titlecase_Mapping.
	  Corresponds to u_totitle. @stable ICU 2.4 */
	UCHAR_SIMPLE_TITLECASE_MAPPING Property = 0x4008
	/** String property Simple_Uppercase_Mapping.
	  Corresponds to u_toupper. @stable ICU 2.4 */
	UCHAR_SIMPLE_UPPERCASE_MAPPING Property = 0x4009
	/** String property Titlecase_Mapping.
	  Corresponds to u_strToTitle in ustring.h. @stable ICU 2.4 */
	UCHAR_TITLECASE_MAPPING Property = 0x400A
	/** String property Uppercase_Mapping.
	  Corresponds to u_strToUpper in ustring.h. @stable ICU 2.4 */
	UCHAR_UPPERCASE_MAPPING Property = 0x400C
	/** String property Bidi_Paired_Bracket (new in Unicode 6.3).
	  Corresponds to u_getBidiPairedBracket. @stable ICU 52 */
	UCHAR_BIDI_PAIRED_BRACKET Property = 0x400D

	/** Miscellaneous property Script_Extensions (new in Unicode 6.0).
	  Some characters are commonly used in multiple scripts.
	  For more information, see UAX #24: http://www.unicode.org/reports/tr24/.
	  Corresponds to uscript_hasScript and uscript_getScriptExtensions in uscript.h.
	  @stable ICU 4.6 */
	UCHAR_SCRIPT_EXTENSIONS Property = 0x7000
	/** First constant for Unicode properties with unusual value types. @stable ICU 4.6 */
	UCHAR_OTHER_PROPERTY_START = UCHAR_SCRIPT_EXTENSIONS

	/** Represents a nonexistent or invalid property or property value. @stable ICU 2.4 */
	UCHAR_INVALID_CODE Property = -1
)

const (
	UCHAR_BINARY_LIMIT = 65
	UCHAR_INT_LIMIT    = 0x1019
	UCHAR_MASK_LIMIT   = 0x2001
	UCHAR_STRING_LIMIT = 0x400E
)

/*
 * Properties in vector word 1
 * Each bit encodes one binary property.
 * The following constants represent the bit number, use 1<<UPROPS_XYZ.
 * UPROPS_BINARY_1_TOP<=32!
 *
 * Keep this list of property enums in sync with
 * propListNames[] in icu/source/tools/genprops/props2.c!
 *
 * ICU 2.6/uprops format version 3.2 stores full properties instead of "Other_".
 */
const (
	UPROPS_WHITE_SPACE = iota
	UPROPS_DASH
	UPROPS_HYPHEN
	UPROPS_QUOTATION_MARK
	UPROPS_TERMINAL_PUNCTUATION
	UPROPS_MATH
	UPROPS_HEX_DIGIT
	UPROPS_ASCII_HEX_DIGIT
	UPROPS_ALPHABETIC
	UPROPS_IDEOGRAPHIC
	UPROPS_DIACRITIC
	UPROPS_EXTENDER
	UPROPS_NONCHARACTER_CODE_POINT
	UPROPS_GRAPHEME_EXTEND
	UPROPS_GRAPHEME_LINK
	UPROPS_IDS_BINARY_OPERATOR
	UPROPS_IDS_TRINARY_OPERATOR
	UPROPS_RADICAL
	UPROPS_UNIFIED_IDEOGRAPH
	UPROPS_DEFAULT_IGNORABLE_CODE_POINT
	UPROPS_DEPRECATED
	UPROPS_LOGICAL_ORDER_EXCEPTION
	UPROPS_XID_START
	UPROPS_XID_CONTINUE
	UPROPS_ID_START
	UPROPS_ID_CONTINUE
	UPROPS_GRAPHEME_BASE
	UPROPS_S_TERM
	UPROPS_VARIATION_SELECTOR
	UPROPS_PATTERN_SYNTAX
	UPROPS_PATTERN_WHITE_SPACE
	UPROPS_PREPENDED_CONCATENATION_MARK
	UPROPS_BINARY_1_TOP
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
	UPROPS_2_EXTENDED_PICTOGRAPHIC = 26 + iota
	UPROPS_2_EMOJI_COMPONENT
	UPROPS_2_EMOJI
	UPROPS_2_EMOJI_PRESENTATION
	UPROPS_2_EMOJI_MODIFIER
	UPROPS_2_EMOJI_MODIFIER_BASE
)

type PropertySource int32

const (
	/** No source, not a supported property. */
	UPROPS_SRC_NONE PropertySource = iota
	/** From uchar.c/uprops.icu main trie */
	UPROPS_SRC_CHAR
	/** From uchar.c/uprops.icu properties vectors trie */
	UPROPS_SRC_PROPSVEC
	/** From unames.c/unames.icu */
	UPROPS_SRC_NAMES
	/** From ucase.c/ucase.icu */
	UPROPS_SRC_CASE
	/** From ubidi_props.c/ubidi.icu */
	UPROPS_SRC_BIDI
	/** From uchar.c/uprops.icu main trie as well as properties vectors trie */
	UPROPS_SRC_CHAR_AND_PROPSVEC
	/** From ucase.c/ucase.icu as well as unorm.cpp/unorm.icu */
	UPROPS_SRC_CASE_AND_NORM
	/** From normalizer2impl.cpp/nfc.nrm */
	UPROPS_SRC_NFC
	/** From normalizer2impl.cpp/nfkc.nrm */
	UPROPS_SRC_NFKC
	/** From normalizer2impl.cpp/nfkc_cf.nrm */
	UPROPS_SRC_NFKC_CF
	/** From normalizer2impl.cpp/nfc.nrm canonical iterator data */
	UPROPS_SRC_NFC_CANON_ITER
	// Text layout properties.
	UPROPS_SRC_INPC
	UPROPS_SRC_INSC
	UPROPS_SRC_VO
	/** One more than the highest UPropertySource (UPROPS_SRC_) constant. */
	UPROPS_SRC_COUNT
)
