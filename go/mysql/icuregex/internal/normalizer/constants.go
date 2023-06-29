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

package normalizer

const (
	// Fixed norm16 values.
	MIN_YES_YES_WITH_CC  = 0xfe02
	JAMO_VT              = 0xfe00
	MIN_NORMAL_MAYBE_YES = 0xfc00
	JAMO_L               = 2 // offset=1 hasCompBoundaryAfter=false
	INERT                = 1 // offset=0 hasCompBoundaryAfter=true

	// norm16 bit 0 is comp-boundary-after.
	HAS_COMP_BOUNDARY_AFTER = 1
	OFFSET_SHIFT            = 1

	// For algorithmic one-way mappings, norm16 bits 2..1 indicate the
	// tccc (0, 1, >1) for quick FCC boundary-after tests.
	DELTA_TCCC_0    = 0
	DELTA_TCCC_1    = 2
	DELTA_TCCC_GT_1 = 4
	DELTA_TCCC_MASK = 6
	DELTA_SHIFT     = 3

	MAX_DELTA = 0x40
)

const (
	JAMO_L_BASE rune = 0x1100 /* "lead" jamo */
	JAMO_L_END  rune = 0x1112
	JAMO_V_BASE rune = 0x1161 /* "vowel" jamo */
	JAMO_V_END  rune = 0x1175
	JAMO_T_BASE rune = 0x11a7 /* "trail" jamo */
	JAMO_T_END  rune = 0x11c2

	HANGUL_BASE rune = 0xac00
	HANGUL_END  rune = 0xd7a3

	JAMO_L_COUNT rune = 19
	JAMO_V_COUNT rune = 21
	JAMO_T_COUNT rune = 28

	JAMO_VT_COUNT = JAMO_V_COUNT * JAMO_T_COUNT

	HANGUL_COUNT = JAMO_L_COUNT * JAMO_V_COUNT * JAMO_T_COUNT
	HANGUL_LIMIT = HANGUL_BASE + HANGUL_COUNT
)

const (
	MAPPING_HAS_CCC_LCCC_WORD = 0x80
	MAPPING_HAS_RAW_MAPPING   = 0x40
	// unused bit 0x20,
	MAPPING_LENGTH_MASK = 0x1f
)

/**
 * Constants for normalization modes.
 * @deprecated ICU 56 Use unorm2.h instead.
 */
type UNormalizationMode int32

const (
	/** No decomposition/composition. @deprecated ICU 56 Use unorm2.h instead. */
	UNORM_NONE UNormalizationMode = 1
	/** Canonical decomposition. @deprecated ICU 56 Use unorm2.h instead. */
	UNORM_NFD UNormalizationMode = 2
	/** Compatibility decomposition. @deprecated ICU 56 Use unorm2.h instead. */
	UNORM_NFKD UNormalizationMode = 3
	/** Canonical decomposition followed by canonical composition. @deprecated ICU 56 Use unorm2.h instead. */
	UNORM_NFC UNormalizationMode = 4
	/** Default normalization. @deprecated ICU 56 Use unorm2.h instead. */
	UNORM_DEFAULT UNormalizationMode = UNORM_NFC
	/** Compatibility decomposition followed by canonical composition. @deprecated ICU 56 Use unorm2.h instead. */
	UNORM_NFKC UNormalizationMode = 5
	/** "Fast C or D" form. @deprecated ICU 56 Use unorm2.h instead. */
	UNORM_FCD UNormalizationMode = 6
)

/**
 * Result values for normalization quick check functions.
 * For details see http://www.unicode.org/reports/tr15/#Detecting_Normalization_Forms
 * @stable ICU 2.0
 */
type UNormalizationCheckResult int

const (
	/**
	 * The input string is not in the normalization form.
	 * @stable ICU 2.0
	 */
	UNORM_NO UNormalizationCheckResult = iota
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
