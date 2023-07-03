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
	minYesYesWithCC   = 0xfe02
	jamoVt            = 0xfe00
	minNormalMaybeYes = 0xfc00
	jamoL             = 2 // offset=1 hasCompBoundaryAfter=false
	inert             = 1 // offset=0 hasCompBoundaryAfter=true

	// norm16 bit 0 is comp-boundary-after.
	hasCompBoundaryAfter = 1
	offsetShift          = 1

	// For algorithmic one-way mappings, norm16 bits 2..1 indicate the
	// tccc (0, 1, >1) for quick FCC boundary-after tests.
	deltaTccc0    = 0
	deltaTccc1    = 2
	deltaTcccGt1  = 4
	deltaTcccMask = 6
	deltaShift    = 3

	maxDelta = 0x40
)

const (
	jamoLBase rune = 0x1100 /* "lead" jamo */
	jamoLEnd  rune = 0x1112
	jamoVBase rune = 0x1161 /* "vowel" jamo */
	jamoVEnd  rune = 0x1175
	jamoTBase rune = 0x11a7 /* "trail" jamo */
	jamoTEnd  rune = 0x11c2

	hangulBase rune = 0xac00
	hangulEnd  rune = 0xd7a3

	jamoLCount rune = 19
	jamoVCount rune = 21
	jamoTCount rune = 28

	hangulCount = jamoLCount * jamoVCount * jamoTCount
	hangulLimit = hangulBase + hangulCount
)

const (
	mappingHasCccLcccWord = 0x80
	mappingHasRawMapping  = 0x40
	// unused bit 0x20,
	mappingLengthMask = 0x1f
)

/**
 * Constants for normalization modes.
 * @deprecated ICU 56 Use unorm2.h instead.
 */
type Mode int32

const (
	/** No decomposition/composition. @deprecated ICU 56 Use unorm2.h instead. */
	NormNone Mode = 1
	/** Canonical decomposition. @deprecated ICU 56 Use unorm2.h instead. */
	NormNfd Mode = 2
	/** Compatibility decomposition. @deprecated ICU 56 Use unorm2.h instead. */
	NormNfkd Mode = 3
	/** Canonical decomposition followed by canonical composition. @deprecated ICU 56 Use unorm2.h instead. */
	NormNfc Mode = 4
	/** Default normalization. @deprecated ICU 56 Use unorm2.h instead. */
	NormDefault Mode = NormNfc
	/** Compatibility decomposition followed by canonical composition. @deprecated ICU 56 Use unorm2.h instead. */
	NormNfkc Mode = 5
	/** "Fast C or D" form. @deprecated ICU 56 Use unorm2.h instead. */
	NormFcd Mode = 6
)

/**
 * Result values for normalization quick check functions.
 * For details see http://www.unicode.org/reports/tr15/#Detecting_Normalization_Forms
 * @stable ICU 2.0
 */
type CheckResult int

const (
	/**
	 * The input string is not in the normalization form.
	 * @stable ICU 2.0
	 */
	No CheckResult = iota
	/**
	 * The input string is in the normalization form.
	 * @stable ICU 2.0
	 */
	Yes
	/**
	 * The input string may or may not be in the normalization form.
	 * This value is only returned for composition forms like NFC and FCC,
	 * when a backward-combining character is found for which the surrounding text
	 * would have to be analyzed further.
	 * @stable ICU 2.0
	 */
	Maybe
)
