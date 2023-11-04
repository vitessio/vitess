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
	"errors"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/uset"
	"vitess.io/vitess/go/mysql/icuregex/internal/utf16"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

type Normalizer struct {
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

var nfc *Normalizer
var nfkc *Normalizer

var normalizerOnce sync.Once

func loadNormalizer() {
	normalizerOnce.Do(func() {
		nfc = &Normalizer{}
		if err := nfc.load(icudata.Nfc); err != nil {
			panic(err)
		}

		nfkc = &Normalizer{}
		if err := nfkc.load(icudata.Nfkc); err != nil {
			panic(err)
		}
	})
}

const ixNormTrieOffset = 0
const ixExtraDataOffset = 1
const ixSmallFcdOffset = 2
const ixReserved3Offset = 3
const ixTotalSize = 7

const ixMinDecompNoCp = 8
const ixMinCompNoMaybeCp = 9

/** Mappings & compositions in [minYesNo..minYesNoMappingsOnly[. */
const ixMinYesNo = 10

/** Mappings are comp-normalized. */
const ixMinNoNo = 11
const ixLimitNoNo = 12
const ixMinMaybeYes = 13

/** Mappings only in [minYesNoMappingsOnly..minNoNo[. */
const ixMinYesNoMappingsOnly = 14

/** Mappings are not comp-normalized but have a comp boundary before. */
const ixMinNoNoCompBoundaryBefore = 15

/** Mappings do not have a comp boundary before. */
const ixMinNoNoCompNoMaybeCc = 16

/** Mappings to the empty string. */
const ixMinNoNoEmpty = 17

const ixMinLcccCp = 18
const ixCount = 20

func (n *Normalizer) load(data []byte) error {
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
	if indexesLength <= ixMinLcccCp {
		return errors.New("normalizer2 data: not enough indexes")
	}
	indexes := make([]int32, indexesLength)
	indexes[0] = indexesLength * 4
	for i := int32(1); i < indexesLength; i++ {
		indexes[i] = bytes.Int32()
	}

	n.minDecompNoCP = indexes[ixMinDecompNoCp]
	n.minCompNoMaybeCP = indexes[ixMinCompNoMaybeCp]
	n.minLcccCP = indexes[ixMinLcccCp]

	n.minYesNo = uint16(indexes[ixMinYesNo])
	n.minYesNoMappingsOnly = uint16(indexes[ixMinYesNoMappingsOnly])
	n.minNoNo = uint16(indexes[ixMinNoNo])
	n.minNoNoCompBoundaryBefore = uint16(indexes[ixMinNoNoCompBoundaryBefore])
	n.minNoNoCompNoMaybeCC = uint16(indexes[ixMinNoNoCompNoMaybeCc])
	n.minNoNoEmpty = uint16(indexes[ixMinNoNoEmpty])
	n.limitNoNo = uint16(indexes[ixLimitNoNo])
	n.minMaybeYes = uint16(indexes[ixMinMaybeYes])

	n.centerNoNoDelta = uint16(indexes[ixMinMaybeYes]>>deltaShift) - maxDelta - 1

	offset := indexes[ixNormTrieOffset]
	nextOffset := indexes[ixExtraDataOffset]
	triePosition := bytes.Position()

	n.normTrie, err = utrie.UcpTrieFromBytes(bytes)
	if err != nil {
		return err
	}

	trieLength := bytes.Position() - triePosition
	if trieLength > nextOffset-offset {
		return errors.New("normalizer2 data: not enough bytes for normTrie")
	}
	bytes.Skip((nextOffset - offset) - trieLength) // skip padding after trie bytes

	// Read the composition and mapping data.
	offset = nextOffset
	nextOffset = indexes[ixSmallFcdOffset]
	numChars := (nextOffset - offset) / 2
	if numChars != 0 {
		n.maybeYesCompositions = bytes.Uint16Slice(numChars)
		n.extraData = n.maybeYesCompositions[((minNormalMaybeYes - n.minMaybeYes) >> offsetShift):]
	}

	// smallFCD: new in formatVersion 2
	n.smallFCD = bytes.Uint8Slice(0x100)
	return nil
}

func Nfc() *Normalizer {
	loadNormalizer()
	return nfc
}

func Nfkc() *Normalizer {
	loadNormalizer()
	return nfkc
}

func (n *Normalizer) AddPropertyStarts(u *uset.UnicodeSet) {
	var start, end rune
	var value uint32
	for {
		end, value = nfc.normTrie.GetRange(start, utrie.UcpMapRangeFixedLeadSurrogates, inert, nil)
		if end < 0 {
			break
		}
		u.AddRune(start)
		if start != end && n.isAlgorithmicNoNo(uint16(value)) && (value&deltaTcccMask) > deltaTccc1 {
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
	for c := hangulBase; c < hangulLimit; c += jamoTCount {
		u.AddRune(c)
		u.AddRune(c + 1)
	}
	u.AddRune(hangulLimit)
}

func (n *Normalizer) isAlgorithmicNoNo(norm16 uint16) bool {
	return n.limitNoNo <= norm16 && norm16 < n.minMaybeYes
}

func (n *Normalizer) GetFCD16(c rune) uint16 {
	if c < n.minDecompNoCP {
		return 0
	} else if c <= 0xffff {
		if !n.singleLeadMightHaveNonZeroFCD16(c) {
			return 0
		}
	}
	return n.getFCD16FromNormData(c)
}

func (n *Normalizer) singleLeadMightHaveNonZeroFCD16(lead rune) bool {
	// 0<=lead<=0xffff
	bits := n.smallFCD[lead>>8]
	if bits == 0 {
		return false
	}
	return ((bits >> ((lead >> 5) & 7)) & 1) != 0
}

func (n *Normalizer) getFCD16FromNormData(c rune) uint16 {
	norm16 := n.getNorm16(c)
	if norm16 >= n.limitNoNo {
		if norm16 >= minNormalMaybeYes {
			// combining mark
			norm16 = uint16(n.getCCFromNormalYesOrMaybe(norm16))
			return norm16 | (norm16 << 8)
		} else if norm16 >= n.minMaybeYes {
			return 0
		} else { // isDecompNoAlgorithmic(norm16)
			deltaTrailCC := norm16 & deltaTcccMask
			if deltaTrailCC <= deltaTccc1 {
				return deltaTrailCC >> offsetShift
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
	if firstUnit&mappingHasCccLcccWord != 0 {
		norm16 |= mapping[0] & 0xff00
	}
	return norm16
}

func (n *Normalizer) getMapping(norm16 uint16) []uint16 {
	return n.extraData[(norm16>>offsetShift)-1:]
}

func (n *Normalizer) getNorm16(c rune) uint16 {
	if utf16.IsLead(c) {
		return inert
	}
	return n.getRawNorm16(c)
}

func (n *Normalizer) getRawNorm16(c rune) uint16 {
	return uint16(n.normTrie.Get(c))
}

func (n *Normalizer) getCCFromNormalYesOrMaybe(norm16 uint16) uint8 {
	return uint8(norm16 >> offsetShift)
}

func (n *Normalizer) mapAlgorithmic(c rune, norm16 uint16) rune {
	return c + rune(norm16>>deltaShift) - rune(n.centerNoNoDelta)
}

func (n *Normalizer) isHangulLV(norm16 uint16) bool {
	return norm16 == n.minYesNo
}

func (n *Normalizer) isHangulLVT(norm16 uint16) bool {
	return norm16 == n.hangulLVT()
}

func (n *Normalizer) hangulLVT() uint16 {
	return n.minYesNoMappingsOnly | hasCompBoundaryAfter
}

func (n *Normalizer) getComposeQuickCheck(c rune) CheckResult {
	return n.getCompQuickCheck(n.getNorm16(c))
}

func (n *Normalizer) getDecomposeQuickCheck(c rune) CheckResult {
	if n.isDecompYes(n.getNorm16(c)) {
		return Yes
	}
	return No
}

func QuickCheck(c rune, mode Mode) CheckResult {
	if mode <= NormNone || NormFcd <= mode {
		return Yes
	}
	switch mode {
	case NormNfc:
		return Nfc().getComposeQuickCheck(c)
	case NormNfd:
		return Nfc().getDecomposeQuickCheck(c)
	case NormNfkc:
		return Nfkc().getComposeQuickCheck(c)
	case NormNfkd:
		return Nfkc().getDecomposeQuickCheck(c)
	default:
		return Maybe
	}
}

func IsInert(c rune, mode Mode) bool {
	switch mode {
	case NormNfc:
		return Nfc().isCompInert(c)
	case NormNfd:
		return Nfc().isDecompInert(c)
	case NormNfkc:
		return Nfkc().isCompInert(c)
	case NormNfkd:
		return Nfkc().isDecompInert(c)
	default:
		return true
	}
}

func (n *Normalizer) isDecompYes(norm16 uint16) bool {
	return norm16 < n.minYesNo || n.minMaybeYes <= norm16
}

func (n *Normalizer) getCompQuickCheck(norm16 uint16) CheckResult {
	if norm16 < n.minNoNo || minYesYesWithCC <= norm16 {
		return Yes
	} else if n.minMaybeYes <= norm16 {
		return Maybe
	} else {
		return No
	}
}

func (n *Normalizer) isMaybeOrNonZeroCC(norm16 uint16) bool {
	return norm16 >= n.minMaybeYes
}

func (n *Normalizer) isDecompNoAlgorithmic(norm16 uint16) bool {
	return norm16 >= n.limitNoNo
}

func (n *Normalizer) IsCompNo(c rune) bool {
	norm16 := n.getNorm16(c)
	return n.minNoNo <= norm16 && norm16 < n.minMaybeYes
}

func (n *Normalizer) Decompose(c rune) []rune {
	norm16 := n.getNorm16(c)
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
	length := mapping[1] & mappingLengthMask
	mapping = mapping[2 : 2+length]

	for len(mapping) > 0 {
		c, mapping = utf16.NextUnsafe(mapping)
		decomp = append(decomp, c)
	}

	return decomp
}

func hangulDecompose(c rune) []uint16 {
	c -= hangulBase
	c2 := c % jamoTCount
	c /= jamoTCount
	var buffer []uint16
	buffer = append(buffer, uint16(jamoLBase+c/jamoVCount))
	buffer = append(buffer, uint16(jamoVBase+c%jamoVCount))
	if c2 != 0 {
		buffer = append(buffer, uint16(jamoTBase+c2))
	}
	return buffer
}

func (n *Normalizer) isCompInert(c rune) bool {
	norm16 := n.getNorm16(c)
	return n.isCompYesAndZeroCC(norm16) && (norm16&hasCompBoundaryAfter) != 0
}

func (n *Normalizer) isDecompInert(c rune) bool {
	return n.isDecompYesAndZeroCC(n.getNorm16(c))
}

func (n *Normalizer) isCompYesAndZeroCC(norm16 uint16) bool {
	return norm16 < n.minNoNo
}

func (n *Normalizer) isDecompYesAndZeroCC(norm16 uint16) bool {
	return norm16 < n.minYesNo ||
		norm16 == jamoVt ||
		(n.minMaybeYes <= norm16 && norm16 <= minNormalMaybeYes)
}

func (n *Normalizer) CombiningClass(c rune) uint8 {
	return n.getCC(n.getNorm16(c))
}

func (n *Normalizer) getCC(norm16 uint16) uint8 {
	if norm16 >= minNormalMaybeYes {
		return n.getCCFromNormalYesOrMaybe(norm16)
	}
	if norm16 < n.minNoNo || n.limitNoNo <= norm16 {
		return 0
	}
	return n.getCCFromNoNo(norm16)

}

func (n *Normalizer) getCCFromNoNo(norm16 uint16) uint8 {
	mapping := n.getMapping(norm16)
	if mapping[1]&mappingHasCccLcccWord != 0 {
		return uint8(mapping[0])
	}
	return 0
}
