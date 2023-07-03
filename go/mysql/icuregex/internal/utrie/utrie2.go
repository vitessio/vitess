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

package utrie

import (
	"errors"
	"fmt"

	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utf16"
)

type UTrie2 struct {
	index  []uint16
	data16 []uint16
	data32 []uint32

	indexLength, dataLength int
	index2NullOffset        uint16
	dataNullOffset          uint16
	InitialValue            uint32
	ErrorValue              uint32

	HighStart      rune
	HighValueIndex int
}

func (t *UTrie2) SerializedLength() int32 {
	return 16 + int32(t.indexLength+t.dataLength)*2
}

func (t *UTrie2) getIndex(asciiOffset int, c rune) uint16 {
	return t.index[t.indexFromCp(asciiOffset, c)]
}

func (t *UTrie2) Get16(c rune) uint16 {
	return t.getIndex(t.indexLength, c)
}

func (t *UTrie2) indexFromCp(asciiOffset int, c rune) int {
	switch {
	case c < 0xd800:
		return indexRaw(0, t.index, c)
	case c <= 0xffff:
		var offset int32
		if c <= 0xdbff {
			offset = lscpIndex2Offset - (0xd800 >> shift2)
		}
		return indexRaw(offset, t.index, c)
	case c > 0x10ffff:
		return asciiOffset + badUtf8DataOffset
	case c >= t.HighStart:
		return t.HighValueIndex
	default:
		return indexFromSupp(t.index, c)
	}
}

type EnumRange func(start, end rune, value uint32) bool
type EnumValue func(value uint32) uint32

func (t *UTrie2) Enum(enumValue EnumValue, enumRange EnumRange) {
	t.enumEitherTrie(0, 0x110000, enumValue, enumRange)
}

func enumSameValue(value uint32) uint32 {
	return value
}

func min(a, b rune) rune {
	if a < b {
		return a
	}
	return b
}

func (t *UTrie2) enumEitherTrie(start, limit rune, enumValue EnumValue, enumRange EnumRange) {
	if enumRange == nil {
		return
	}
	if enumValue == nil {
		enumValue = enumSameValue
	}

	/* frozen trie */
	var (
		idx              = t.index
		data32           = t.data32
		index2NullOffset = int(t.index2NullOffset)
		nullBlock        = int(t.dataNullOffset)

		c         rune
		prev      = start
		highStart = t.HighStart

		/* get the enumeration value that corresponds to an initial-value trie data entry */
		initialValue = enumValue(t.InitialValue)

		/* set variables for previous range */
		i2Block     int
		block       int
		prevI2Block = -1
		prevBlock   = -1
		prevValue   = uint32(0)
	)

	/* enumerate index-2 blocks */
	for c = start; c < limit && c < highStart; {
		/* Code point limit for iterating inside this i2Block. */
		tempLimit := c + cpPerIndex1Entry
		if limit < tempLimit {
			tempLimit = limit
		}
		if c <= 0xffff {
			if !utf16.IsSurrogate(c) {
				i2Block = int(c >> shift2)
			} else if utf16.IsSurrogateLead(c) {
				/*
				 * Enumerate values for lead surrogate code points, not code units:
				 * This special block has half the normal length.
				 */
				i2Block = lscpIndex2Offset
				tempLimit = min(0xdc00, limit)
			} else {
				/*
				 * Switch back to the normal part of the index-2 table.
				 * Enumerate the second half of the surrogates block.
				 */
				i2Block = 0xd800 >> shift2
				tempLimit = min(0xe000, limit)
			}
		} else {
			/* supplementary code points */
			i2Block = int(idx[(index1Offset-omittedBmpIndex1Length)+(c>>shift1)])
			if i2Block == prevI2Block && (c-prev) >= cpPerIndex1Entry {
				/*
				 * The index-2 block is the same as the previous one, and filled with prevValue.
				 * Only possible for supplementary code points because the linear-BMP index-2
				 * table creates unique i2Block values.
				 */
				c += cpPerIndex1Entry
				continue
			}
		}
		prevI2Block = i2Block
		if i2Block == index2NullOffset {
			/* this is the null index-2 block */
			if prevValue != initialValue {
				if prev < c && !enumRange(prev, c-1, prevValue) {
					return
				}
				prevBlock = nullBlock
				prev = c
				prevValue = initialValue
			}
			c += cpPerIndex1Entry
		} else {
			/* enumerate data blocks for one index-2 block */
			var i2Limit int
			if (c >> shift1) == (tempLimit >> shift1) {
				i2Limit = int(tempLimit>>shift2) & index2Mask
			} else {
				i2Limit = index2BlockLength
			}
			for i2 := int(c>>shift2) & index2Mask; i2 < i2Limit; i2++ {
				block = int(idx[i2Block+i2] << indexShift)
				if block == prevBlock && (c-prev) >= dataBlockLength {
					/* the block is the same as the previous one, and filled with prevValue */
					c += dataBlockLength
					continue
				}
				prevBlock = block
				if block == nullBlock {
					/* this is the null data block */
					if prevValue != initialValue {
						if prev < c && !enumRange(prev, c-1, prevValue) {
							return
						}
						prev = c
						prevValue = initialValue
					}
					c += dataBlockLength
				} else {
					for j := 0; j < dataBlockLength; j++ {
						var value uint32
						if data32 != nil {
							value = data32[block+j]
						} else {
							value = uint32(idx[block+j])
						}
						value = enumValue(value)
						if value != prevValue {
							if prev < c && !enumRange(prev, c-1, prevValue) {
								return
							}
							prev = c
							prevValue = value
						}
						c++
					}
				}
			}
		}
	}

	if c > limit {
		c = limit /* could be higher if in the index2NullOffset */
	} else if c < limit {
		/* c==highStart<limit */
		var highValue uint32
		if data32 != nil {
			highValue = data32[t.HighValueIndex]
		} else {
			highValue = uint32(idx[t.HighValueIndex])
		}
		value := enumValue(highValue)
		if value != prevValue {
			if prev < c && !enumRange(prev, c-1, prevValue) {
				return
			}
			prev = c
			prevValue = value
		}
		c = limit
	}

	/* deliver last range */
	enumRange(prev, c-1, prevValue)
}

func indexFromSupp(index []uint16, c rune) int {
	i1 := int(index[(index1Offset-omittedBmpIndex1Length)+(c>>shift1)])
	return (int(index[i1+int((c>>shift2)&index2Mask)]) << indexShift) + int(c&dataMask)
}

func indexRaw(offset int32, index []uint16, c rune) int {
	return int(index[offset+(c>>shift2)]<<indexShift) + int(c&dataMask)
}

const (
	/** Shift size for getting the index-1 table offset. */
	shift1 = 6 + 5

	/** Shift size for getting the index-2 table offset. */
	shift2 = 5

	/**
	 * Difference between the two shift sizes,
	 * for getting an index-1 offset from an index-2 offset. 6=11-5
	 */
	shift1min2 = shift1 - shift2

	/**
	 * Number of index-1 entries for the BMP. 32=0x20
	 * This part of the index-1 table is omitted from the serialized form.
	 */
	omittedBmpIndex1Length = 0x10000 >> shift1

	/** Number of code points per index-1 table entry. 2048=0x800 */
	cpPerIndex1Entry = 1 << shift1

	/** Number of entries in an index-2 block. 64=0x40 */
	index2BlockLength = 1 << shift1min2

	/** Mask for getting the lower bits for the in-index-2-block offset. */
	index2Mask = index2BlockLength - 1

	/** Number of entries in a data block. 32=0x20 */
	dataBlockLength = 1 << shift2

	/** Mask for getting the lower bits for the in-data-block offset. */
	dataMask = dataBlockLength - 1

	/**
	 * Shift size for shifting left the index array values.
	 * Increases possible data size with 16-bit index values at the cost
	 * of compactability.
	 * This requires data blocks to be aligned by UTRIE2_DATA_GRANULARITY.
	 */
	indexShift = 2

	/** The alignment size of a data block. Also the granularity for compaction. */
	dataGranularity = 1 << indexShift

	/* Fixed layout of the first part of the index array. ------------------- */

	/**
	 * The part of the index-2 table for U+D800..U+DBFF stores values for
	 * lead surrogate code _units_ not code _points_.
	 * Values for lead surrogate code _points_ are indexed with this portion of the table.
	 * Length=32=0x20=0x400>>UTRIE2_SHIFT_2. (There are 1024=0x400 lead surrogates.)
	 */
	lscpIndex2Offset = 0x10000 >> shift2
	lscpIndex2Length = 0x400 >> shift2

	/** Count the lengths of both BMP pieces. 2080=0x820 */
	index2BmpLength = lscpIndex2Offset + lscpIndex2Length

	/**
	 * The 2-byte UTF-8 version of the index-2 table follows at offset 2080=0x820.
	 * Length 32=0x20 for lead bytes C0..DF, regardless of UTRIE2_SHIFT_2.
	 */
	utf82BIndex2Offset = index2BmpLength
	utf82BIndex2Length = 0x800 >> 6 /* U+0800 is the first code point after 2-byte UTF-8 */

	/**
	 * The index-1 table, only used for supplementary code points, at offset 2112=0x840.
	 * Variable length, for code points up to highStart, where the last single-value range starts.
	 * Maximum length 512=0x200=0x100000>>UTRIE2_SHIFT_1.
	 * (For 0x100000 supplementary code points U+10000..U+10ffff.)
	 *
	 * The part of the index-2 table for supplementary code points starts
	 * after this index-1 table.
	 *
	 * Both the index-1 table and the following part of the index-2 table
	 * are omitted completely if there is only BMP data.
	 */
	index1Offset    = utf82BIndex2Offset + utf82BIndex2Length
	maxIndex1Length = 0x100000 >> shift1

	/*
	 * Fixed layout of the first part of the data array. -----------------------
	 * Starts with 4 blocks (128=0x80 entries) for ASCII.
	 */

	/**
	 * The illegal-UTF-8 data block follows the ASCII block, at offset 128=0x80.
	 * Used with linear access for single bytes 0..0xbf for simple error handling.
	 * Length 64=0x40, not UTRIE2_DATA_BLOCK_LENGTH.
	 */
	badUtf8DataOffset = 0x80
)

func UTrie2FromBytes(bytes *udata.Bytes) (*UTrie2, error) {
	type utrie2Header struct {
		/** "Tri2" in big-endian US-ASCII (0x54726932) */
		signature uint32

		/**
		 * options bit field:
		 * 15.. 4   reserved (0)
		 *  3.. 0   UTrie2ValueBits valueBits
		 */
		options uint16

		/** UTRIE2_INDEX_1_OFFSET..UTRIE2_MAX_INDEX_LENGTH */
		indexLength uint16

		/** (UTRIE2_DATA_START_OFFSET..UTRIE2_MAX_DATA_LENGTH)>>UTRIE2_INDEX_SHIFT */
		shiftedDataLength uint16

		/** Null index and data blocks, not shifted. */
		index2NullOffset, dataNullOffset uint16

		/**
		 * First code point of the single-value range ending with U+10ffff,
		 * rounded up and then shifted right by UTRIE2_SHIFT_1.
		 */
		shiftedHighStart uint16
	}

	var header utrie2Header
	header.signature = bytes.Uint32()

	switch header.signature {
	case 0x54726932:
	case 0x32697254:
		return nil, errors.New("unsupported: BigEndian encoding")
	default:
		return nil, fmt.Errorf("invalid signature for Trie2: 0x%08x", header.signature)
	}

	header.options = bytes.Uint16()
	header.indexLength = bytes.Uint16()
	header.shiftedDataLength = bytes.Uint16()
	header.index2NullOffset = bytes.Uint16()
	header.dataNullOffset = bytes.Uint16()
	header.shiftedHighStart = bytes.Uint16()

	var width int
	switch header.options & 0xf {
	case 0:
		width = 16
	case 1:
		width = 32
	default:
		return nil, errors.New("invalid width for serialized UTrie2")
	}

	trie := &UTrie2{
		indexLength:      int(header.indexLength),
		dataLength:       int(header.shiftedDataLength) << indexShift,
		index2NullOffset: header.index2NullOffset,
		dataNullOffset:   header.dataNullOffset,
		HighStart:        rune(header.shiftedHighStart) << shift1,
	}

	trie.HighValueIndex = trie.dataLength - dataGranularity
	if width == 16 {
		trie.HighValueIndex += trie.indexLength
	}

	indexArraySize := trie.indexLength
	if width == 16 {
		indexArraySize += trie.dataLength
	}

	trie.index = bytes.Uint16Slice(int32(indexArraySize))

	if width == 16 {
		trie.data16 = trie.index[trie.indexLength:]
		trie.InitialValue = uint32(trie.index[trie.dataNullOffset])
		trie.ErrorValue = uint32(trie.index[trie.indexLength+badUtf8DataOffset])
	} else {
		trie.data32 = bytes.Uint32Slice(int32(trie.dataLength))
		trie.InitialValue = trie.data32[trie.dataNullOffset]
		trie.ErrorValue = trie.data32[badUtf8DataOffset]
	}

	return trie, nil
}
