// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package colltab

import (
	"unicode"
)

const (
	defaultSecondary = 0x20
	maxTertiary      = 0x1F
)

// Elem is a representation of a collation element. This API provides ways to encode
// and decode Elems. Implementations of collation tables may use values greater
// or equal to PrivateUse for their own purposes.  However, these should never be
// returned by AppendNext.
type Elem uint32

const (
	maxCE       Elem = 0xAFFFFFFF
	maxContract Elem = 0xDFFFFFFF
	maxExpand   Elem = 0xEFFFFFFF
)

type ceType int

const (
	ceNormal           ceType = iota // ceNormal includes implicits (ce == 0)
	ceContractionIndex               // rune can be a start of a contraction
	ceExpansionIndex                 // rune expands into a sequence of collation elements
	ceDecompose                      // rune expands using NFKC decomposition
)

func (ce Elem) ctype() ceType {
	if ce <= maxCE {
		return ceNormal
	}
	if ce <= maxContract {
		return ceContractionIndex
	}
	if ce <= maxExpand {
		return ceExpansionIndex
	}
	return ceDecompose
}

// For normal collation elements, we assume that a collation element either has
// a primary or non-default secondary value, not both.
// Collation elements with a primary value are of the form
//
//	01pppppp pppppppp ppppppp0 ssssssss
//	  - p* is primary collation value
//	  - s* is the secondary collation value
//	00pppppp pppppppp ppppppps sssttttt, where
//	  - p* is primary collation value
//	  - s* offset of secondary from default value.
//	  - t* is the tertiary collation value
//	100ttttt cccccccc pppppppp pppppppp
//	  - t* is the tertiar collation value
//	  - c* is the canonical combining class
//	  - p* is the primary collation value
//
// Collation elements with a secondary value are of the form
//
//	1010cccc ccccssss ssssssss tttttttt, where
//	  - c* is the canonical combining class
//	  - s* is the secondary collation value
//	  - t* is the tertiary collation value
//	11qqqqqq qqqqqqqq qqqqqqq0 00000000
//	  - q* quaternary value
const (
	ceTypeMask            = 0xC0000000
	ceTypeMaskExt         = 0xE0000000
	ceType1               = 0x40000000
	ceType3or4            = 0x80000000
	ceType4               = 0xA0000000
	firstNonPrimary       = 0x80000000
	lastSpecialPrimary    = 0xA0000000
	primaryValueMask      = 0x3FFFFE00
	primaryShift          = 9
	compactSecondaryShift = 5
	minCompactSecondary   = defaultSecondary - 4
)

func makeImplicitCE(primary int) Elem {
	return ceType1 | Elem(primary<<primaryShift) | defaultSecondary
}

// CCC returns the canonical combining class associated with the underlying character,
// if applicable, or 0 otherwise.
func (ce Elem) CCC() uint8 {
	if ce&ceType3or4 != 0 {
		if ce&ceType4 == ceType3or4 {
			return uint8(ce >> 16)
		}
		return uint8(ce >> 20)
	}
	return 0
}

// Primary returns the primary collation weight for ce.
func (ce Elem) Primary() int {
	if ce >= firstNonPrimary {
		if ce > lastSpecialPrimary {
			return 0
		}
		return int(uint16(ce))
	}
	return int(ce&primaryValueMask) >> primaryShift
}

func (ce Elem) updateTertiary(t uint8) Elem {
	if ce&ceTypeMask == ceType1 {
		// convert to type 4
		nce := ce & primaryValueMask
		nce |= Elem(uint8(ce)-minCompactSecondary) << compactSecondaryShift
		ce = nce
	} else if ce&ceTypeMaskExt == ceType3or4 {
		ce &= ^Elem(maxTertiary << 24)
		return ce | (Elem(t) << 24)
	} else {
		// type 2 or 4
		ce &= ^Elem(maxTertiary)
	}
	return ce | Elem(t)
}

// For contractions, collation elements are of the form
// 110bbbbb bbbbbbbb iiiiiiii iiiinnnn, where
//   - n* is the size of the first node in the contraction trie.
//   - i* is the index of the first node in the contraction trie.
//   - b* is the offset into the contraction collation element table.
//
// See contract.go for details on the contraction trie.
const (
	maxNBits              = 4
	maxTrieIndexBits      = 12
	maxContractOffsetBits = 13
)

func splitContractIndex(ce Elem) (index, n, offset int) {
	n = int(ce & (1<<maxNBits - 1))
	ce >>= maxNBits
	index = int(ce & (1<<maxTrieIndexBits - 1))
	ce >>= maxTrieIndexBits
	offset = int(ce & (1<<maxContractOffsetBits - 1))
	return
}

func splitExpandIndex(ce Elem) (index int) {
	return int(uint16(ce))
}

// Some runes can be expanded using NFKD decomposition. Instead of storing the full
// sequence of collation elements, we decompose the rune and lookup the collation
// elements for each rune in the decomposition and modify the tertiary weights.
// The Elem, in this case, is of the form 11110000 00000000 wwwwwwww vvvvvvvv, where
//   - v* is the replacement tertiary weight for the first rune,
//   - w* is the replacement tertiary weight for the second rune,
//
// Tertiary weights of subsequent runes should be replaced with maxTertiary.
// See https://www.unicode.org/reports/tr10/#Compatibility_Decompositions for more details.
func splitDecompose(ce Elem) (t1, t2 uint8) {
	return uint8(ce), uint8(ce >> 8)
}

const (
	// These constants were taken from https://www.unicode.org/versions/Unicode6.0.0/ch12.pdf.
	minUnified       rune = 0x4E00
	maxUnified       rune = 0x9FFF
	minCompatibility rune = 0xF900
	maxCompatibility rune = 0xFAFF
)

const (
	commonUnifiedOffset = 0x10000
	rareUnifiedOffset   = 0x20000 // largest rune in common is U+FAFF
	otherOffset         = 0x50000 // largest rune in rare is U+2FA1D
)

// implicitPrimary returns the primary weight for the a rune
// for which there is no entry for the rune in the collation table.
// We take a different approach from the one specified in
// https://unicode.org/reports/tr10/#Implicit_Weights,
// but preserve the resulting relative ordering of the runes.
func implicitPrimary(r rune) int {
	if unicode.Is(unicode.Ideographic, r) {
		if r >= minUnified && r <= maxUnified {
			// The most common case for CJK.
			return int(r) + commonUnifiedOffset
		}
		if r >= minCompatibility && r <= maxCompatibility {
			// This will typically not hit. The DUCET explicitly specifies mappings
			// for all characters that do not decompose.
			return int(r) + commonUnifiedOffset
		}
		return int(r) + rareUnifiedOffset
	}
	return int(r) + otherOffset
}
