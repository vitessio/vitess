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

import (
	"fmt"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/uset"
	"vitess.io/vitess/go/mysql/icuregex/internal/utf16"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

type normalizer struct {
	minDecompNoCP    rune
	minCompNoMaybeCP rune
	minLcccCP        rune

	// Norm16 value thresholds for quick check combinations and types of extra data.
	minYesNo                  uint16
	minYesNoMappingsOnly      uint16
	minNoNo                   uint16
	minNoNoCompBoundaryBefore uint16
	minNoNoCompNoMaybeCC      uint16
	minNoNoEmpty              uint16
	limitNoNo                 uint16
	centerNoNoDelta           uint16
	minMaybeYes               uint16

	normTrie *utrie.UcpTrie

	maybeYesCompositions []uint16
	extraData            []uint16 // mappings and/or compositions for yesYes, yesNo & noNo characters
	smallFCD             []uint8  // [0x100] one bit per 32 BMP code points, set if any FCD!=0
}

var nfc *normalizer
var nfkc *normalizer

var normalizerOnce sync.Once

func loadNormalizer() {
	normalizerOnce.Do(func() {
		nfc = &normalizer{}
		if err := nfc.load(icudata.NFC); err != nil {
			panic(err)
		}

		nfkc = &normalizer{}
		if err := nfkc.load(icudata.NFKC); err != nil {
			panic(err)
		}
	})
}

const IX_NORM_TRIE_OFFSET = 0
const IX_EXTRA_DATA_OFFSET = 1
const IX_SMALL_FCD_OFFSET = 2
const IX_RESERVED3_OFFSET = 3
const IX_TOTAL_SIZE = 7

const IX_MIN_DECOMP_NO_CP = 8
const IX_MIN_COMP_NO_MAYBE_CP = 9

/** Mappings & compositions in [minYesNo..minYesNoMappingsOnly[. */
const IX_MIN_YES_NO = 10

/** Mappings are comp-normalized. */
const IX_MIN_NO_NO = 11
const IX_LIMIT_NO_NO = 12
const IX_MIN_MAYBE_YES = 13

/** Mappings only in [minYesNoMappingsOnly..minNoNo[. */
const IX_MIN_YES_NO_MAPPINGS_ONLY = 14

/** Mappings are not comp-normalized but have a comp boundary before. */
const IX_MIN_NO_NO_COMP_BOUNDARY_BEFORE = 15

/** Mappings do not have a comp boundary before. */
const IX_MIN_NO_NO_COMP_NO_MAYBE_CC = 16

/** Mappings to the empty string. */
const IX_MIN_NO_NO_EMPTY = 17

const IX_MIN_LCCC_CP = 18
const IX_COUNT = 20

func (n *normalizer) load(data []byte) error {
	bytes := udata.NewBytes(data)

	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.Size >= 20 &&
			info.IsBigEndian == 0 &&
			info.CharsetFamily == 0 &&
			info.DataFormat[0] == 0x4e && /* dataFormat="unam" */
			info.DataFormat[1] == 0x72 &&
			info.DataFormat[2] == 0x6d &&
			info.DataFormat[3] == 0x32 &&
			info.FormatVersion[0] == 4
	})
	if err != nil {
		return err
	}

	indexesLength := int32(bytes.Uint32()) / 4
	if indexesLength <= IX_MIN_LCCC_CP {
		return fmt.Errorf("normalizer2 data: not enough indexes")
	}
	indexes := make([]int32, indexesLength)
	indexes[0] = indexesLength * 4
	for i := int32(1); i < indexesLength; i++ {
		indexes[i] = bytes.Int32()
	}

	n.minDecompNoCP = indexes[IX_MIN_DECOMP_NO_CP]
	n.minCompNoMaybeCP = indexes[IX_MIN_COMP_NO_MAYBE_CP]
	n.minLcccCP = indexes[IX_MIN_LCCC_CP]

	n.minYesNo = uint16(indexes[IX_MIN_YES_NO])
	n.minYesNoMappingsOnly = uint16(indexes[IX_MIN_YES_NO_MAPPINGS_ONLY])
	n.minNoNo = uint16(indexes[IX_MIN_NO_NO])
	n.minNoNoCompBoundaryBefore = uint16(indexes[IX_MIN_NO_NO_COMP_BOUNDARY_BEFORE])
	n.minNoNoCompNoMaybeCC = uint16(indexes[IX_MIN_NO_NO_COMP_NO_MAYBE_CC])
	n.minNoNoEmpty = uint16(indexes[IX_MIN_NO_NO_EMPTY])
	n.limitNoNo = uint16(indexes[IX_LIMIT_NO_NO])
	n.minMaybeYes = uint16(indexes[IX_MIN_MAYBE_YES])

	n.centerNoNoDelta = uint16(indexes[IX_MIN_MAYBE_YES]>>DELTA_SHIFT) - MAX_DELTA - 1

	offset := indexes[IX_NORM_TRIE_OFFSET]
	nextOffset := indexes[IX_EXTRA_DATA_OFFSET]
	triePosition := bytes.Position()

	n.normTrie, err = utrie.UcpTrieFromBytes(bytes)
	if err != nil {
		return err
	}

	trieLength := bytes.Position() - triePosition
	if trieLength > nextOffset-offset {
		return fmt.Errorf("normalizer2 data: not enough bytes for normTrie")
	}
	bytes.Skip((nextOffset - offset) - trieLength) // skip padding after trie bytes

	// Read the composition and mapping data.
	offset = nextOffset
	nextOffset = indexes[IX_SMALL_FCD_OFFSET]
	numChars := (nextOffset - offset) / 2
	if numChars != 0 {
		n.maybeYesCompositions = bytes.Uint16Slice(numChars)
		n.extraData = n.maybeYesCompositions[((MIN_NORMAL_MAYBE_YES - n.minMaybeYes) >> OFFSET_SHIFT):]
	}

	// smallFCD: new in formatVersion 2
	n.smallFCD = bytes.Uint8Slice(0x100)
	return nil
}

func Nfc() *normalizer {
	loadNormalizer()
	return nfc
}

func Nfkc() *normalizer {
	loadNormalizer()
	return nfkc
}

func (n *normalizer) AddPropertyStarts(u *uset.UnicodeSet) {
	var start, end rune
	var value uint32
	for {
		end, value = nfc.normTrie.GetRange(start, utrie.UCPMAP_RANGE_FIXED_LEAD_SURROGATES, INERT, nil)
		if end < 0 {
			break
		}
		u.AddRune(start)
		if start != end && n.isAlgorithmicNoNo(uint16(value)) && (value&DELTA_TCCC_MASK) > DELTA_TCCC_1 {
			// Range of code points with same-norm16-value algorithmic decompositions.
			// They might have different non-zero FCD16 values.
			prevFCD16 := n.GetFCD16(start)
			for {
				start++
				if start > end {
					break
				}
				fcd16 := n.GetFCD16(start)
				if fcd16 != prevFCD16 {
					u.AddRune(start)
					prevFCD16 = fcd16
				}
			}
		}
		start = end + 1
	}

	// add Hangul LV syllables and LV+1 because of skippables
	for c := HANGUL_BASE; c < HANGUL_LIMIT; c += JAMO_T_COUNT {
		u.AddRune(c)
		u.AddRune(c + 1)
	}
	u.AddRune(HANGUL_LIMIT)
}

func (n *normalizer) isAlgorithmicNoNo(norm16 uint16) bool {
	return n.limitNoNo <= norm16 && norm16 < n.minMaybeYes
}

func (n *normalizer) GetFCD16(c rune) uint16 {
	if c < n.minDecompNoCP {
		return 0
	} else if c <= 0xffff {
		if !n.singleLeadMightHaveNonZeroFCD16(c) {
			return 0
		}
	}
	return n.getFCD16FromNormData(c)
}

func (n *normalizer) singleLeadMightHaveNonZeroFCD16(lead rune) bool {
	// 0<=lead<=0xffff
	bits := n.smallFCD[lead>>8]
	if bits == 0 {
		return false
	}
	return ((bits >> ((lead >> 5) & 7)) & 1) != 0
}

func (n *normalizer) getFCD16FromNormData(c rune) uint16 {
	norm16 := n.GetNorm16(c)
	if norm16 >= n.limitNoNo {
		if norm16 >= MIN_NORMAL_MAYBE_YES {
			// combining mark
			norm16 = uint16(n.getCCFromNormalYesOrMaybe(norm16))
			return norm16 | (norm16 << 8)
		} else if norm16 >= n.minMaybeYes {
			return 0
		} else { // isDecompNoAlgorithmic(norm16)
			deltaTrailCC := norm16 & DELTA_TCCC_MASK
			if deltaTrailCC <= DELTA_TCCC_1 {
				return deltaTrailCC >> OFFSET_SHIFT
			}
			// Maps to an isCompYesAndZeroCC.
			c = n.mapAlgorithmic(c, norm16)
			norm16 = n.getRawNorm16(c)
		}
	}

	if norm16 <= n.minYesNo || n.isHangulLVT(norm16) {
		// no decomposition or Hangul syllable, all zeros
		return 0
	}
	// c decomposes, get everything from the variable-length extra data
	mapping := n.getMapping(norm16)
	firstUnit := mapping[1]
	if firstUnit&MAPPING_HAS_CCC_LCCC_WORD != 0 {
		norm16 |= mapping[0] & 0xff00
	}
	return norm16
}

func (n *normalizer) getMapping(norm16 uint16) []uint16 {
	return n.extraData[(norm16>>OFFSET_SHIFT)-1:]
}

func (n *normalizer) GetNorm16(c rune) uint16 {
	if utf16.IsLead(c) {
		return INERT
	}
	return n.getRawNorm16(c)
}

func (n *normalizer) getRawNorm16(c rune) uint16 {
	return uint16(n.normTrie.Get(c))
}

func (n *normalizer) getCCFromNormalYesOrMaybe(norm16 uint16) uint8 {
	return uint8(norm16 >> OFFSET_SHIFT)
}

func (n *normalizer) mapAlgorithmic(c rune, norm16 uint16) rune {
	return c + rune(norm16>>DELTA_SHIFT) - rune(n.centerNoNoDelta)
}

func (n *normalizer) isHangulLV(norm16 uint16) bool {
	return norm16 == n.minYesNo
}

func (n *normalizer) isHangulLVT(norm16 uint16) bool {
	return norm16 == n.hangulLVT()
}

func (n *normalizer) hangulLVT() uint16 {
	return n.minYesNoMappingsOnly | HAS_COMP_BOUNDARY_AFTER
}

func (n *normalizer) getComposeQuickCheck(c rune) UNormalizationCheckResult {
	return n.getCompQuickCheck(n.GetNorm16(c))
}

func (n *normalizer) getDecomposeQuickCheck(c rune) UNormalizationCheckResult {
	if n.isDecompYes(n.GetNorm16(c)) {
		return UNORM_YES
	}
	return UNORM_NO
}

func QuickCheck(c rune, mode UNormalizationMode) UNormalizationCheckResult {
	if mode <= UNORM_NONE || UNORM_FCD <= mode {
		return UNORM_YES
	}
	switch mode {
	case UNORM_NFC:
		return Nfc().getComposeQuickCheck(c)
	case UNORM_NFD:
		return Nfc().getDecomposeQuickCheck(c)
	case UNORM_NFKC:
		return Nfkc().getComposeQuickCheck(c)
	case UNORM_NFKD:
		return Nfkc().getDecomposeQuickCheck(c)
	default:
		return UNORM_MAYBE
	}
}

func IsInert(c rune, mode UNormalizationMode) bool {
	switch mode {
	case UNORM_NFC:
		return Nfc().isCompInert(c)
	case UNORM_NFD:
		return Nfc().isDecompInert(c)
	case UNORM_NFKC:
		return Nfkc().isCompInert(c)
	case UNORM_NFKD:
		return Nfkc().isDecompInert(c)
	default:
		return true
	}
}

func (n *normalizer) isDecompYes(norm16 uint16) bool {
	return norm16 < n.minYesNo || n.minMaybeYes <= norm16
}

func (n *normalizer) getCompQuickCheck(norm16 uint16) UNormalizationCheckResult {
	if norm16 < n.minNoNo || MIN_YES_YES_WITH_CC <= norm16 {
		return UNORM_YES
	} else if n.minMaybeYes <= norm16 {
		return UNORM_MAYBE
	} else {
		return UNORM_NO
	}
}

func (n *normalizer) isMaybeOrNonZeroCC(norm16 uint16) bool {
	return norm16 >= n.minMaybeYes
}

func (n *normalizer) isDecompNoAlgorithmic(norm16 uint16) bool {
	return norm16 >= n.limitNoNo
}

func (n *normalizer) IsCompNo(norm16 uint16) bool {
	return n.minNoNo <= norm16 && norm16 < n.minMaybeYes
}

func (n *normalizer) Decompose(c rune) []rune {
	norm16 := n.GetNorm16(c)
	if c < n.minDecompNoCP || n.isMaybeOrNonZeroCC(norm16) {
		// c does not decompose
		return nil
	}
	var decomp []rune

	if n.isDecompNoAlgorithmic(norm16) {
		// Maps to an isCompYesAndZeroCC.
		c = n.mapAlgorithmic(c, norm16)
		decomp = append(decomp, c)
		// The mapping might decompose further.
		norm16 = n.getRawNorm16(c)
	}
	if norm16 < n.minYesNo {
		return decomp
	} else if n.isHangulLV(norm16) || n.isHangulLVT(norm16) {
		// Hangul syllable: decompose algorithmically
		parts := hangulDecompose(c)
		for len(parts) > 0 {
			c = rune(parts[0])
			decomp = append(decomp, c)
			parts = parts[1:]
		}
		return decomp
	}
	// c decomposes, get everything from the variable-length extra data
	mapping := n.getMapping(norm16)
	length := mapping[1] & MAPPING_LENGTH_MASK
	mapping = mapping[2 : 2+length]

	for len(mapping) > 0 {
		c, mapping = utf16.NextUnsafe(mapping)
		decomp = append(decomp, c)
	}

	return decomp
}

func hangulDecompose(c rune) []uint16 {
	c -= HANGUL_BASE
	c2 := c % JAMO_T_COUNT
	c /= JAMO_T_COUNT
	var buffer []uint16
	buffer = append(buffer, uint16(JAMO_L_BASE+c/JAMO_V_COUNT))
	buffer = append(buffer, uint16(JAMO_V_BASE+c%JAMO_V_COUNT))
	if c2 != 0 {
		buffer = append(buffer, uint16(JAMO_T_BASE+c2))
	}
	return buffer
}

func (n *normalizer) isCompInert(c rune) bool {
	norm16 := n.GetNorm16(c)
	return n.isCompYesAndZeroCC(norm16) && (norm16&HAS_COMP_BOUNDARY_AFTER) != 0
}

func (n *normalizer) isDecompInert(c rune) bool {
	return n.isDecompYesAndZeroCC(n.GetNorm16(c))
}

func (n *normalizer) isCompYesAndZeroCC(norm16 uint16) bool {
	return norm16 < n.minNoNo
}

func (n *normalizer) isDecompYesAndZeroCC(norm16 uint16) bool {
	return norm16 < n.minYesNo ||
		norm16 == JAMO_VT ||
		(n.minMaybeYes <= norm16 && norm16 <= MIN_NORMAL_MAYBE_YES)
}

func (n *normalizer) CombiningClass(c rune) uint8 {
	return n.getCC(n.GetNorm16(c))
}

func (n *normalizer) getCC(norm16 uint16) uint8 {
	if norm16 >= MIN_NORMAL_MAYBE_YES {
		return n.getCCFromNormalYesOrMaybe(norm16)
	}
	if norm16 < n.minNoNo || n.limitNoNo <= norm16 {
		return 0
	}
	return n.getCCFromNoNo(norm16)

}

func (n *normalizer) getCCFromNoNo(norm16 uint16) uint8 {
	mapping := n.getMapping(norm16)
	if mapping[1]&MAPPING_HAS_CCC_LCCC_WORD != 0 {
		return uint8(mapping[0])
	} else {
		return 0
	}
}
