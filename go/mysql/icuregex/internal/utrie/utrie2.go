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
	"fmt"

	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utf16"
)

type UTrie2 struct {
	Index  []uint16
	Data16 []uint16
	Data32 []uint32

	IndexLength, DataLength int
	Index2NullOffset        uint16
	DataNullOffset          uint16
	InitialValue            uint32
	ErrorValue              uint32

	HighStart      rune
	HighValueIndex int
}

func (t *UTrie2) SerializedLength() int32 {
	return 16 + int32(t.IndexLength+t.DataLength)*2
}

func (t *UTrie2) getIndex(asciiOffset int, c rune) uint16 {
	return t.Index[t.indexFromCp(asciiOffset, c)]
}

func (t *UTrie2) Get16(c rune) uint16 {
	return t.getIndex(t.IndexLength, c)
}

func (t *UTrie2) indexFromCp(asciiOffset int, c rune) int {
	switch {
	case c < 0xd800:
		return indexRaw(0, t.Index, c)
	case c <= 0xffff:
		var offset int32
		if c <= 0xdbff {
			offset = UTRIE2_LSCP_INDEX_2_OFFSET - (0xd800 >> UTRIE2_SHIFT_2)
		}
		return indexRaw(offset, t.Index, c)
	case c > 0x10ffff:
		return asciiOffset + UTRIE2_BAD_UTF8_DATA_OFFSET
	case c >= t.HighStart:
		return t.HighValueIndex
	default:
		return indexFromSupp(t.Index, c)
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
		idx              = t.Index
		data32           = t.Data32
		index2NullOffset = int(t.Index2NullOffset)
		nullBlock        = int(t.DataNullOffset)

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
		tempLimit := c + UTRIE2_CP_PER_INDEX_1_ENTRY
		if limit < tempLimit {
			tempLimit = limit
		}
		if c <= 0xffff {
			if !utf16.IsSurrogate(c) {
				i2Block = int(c >> UTRIE2_SHIFT_2)
			} else if utf16.IsSurrogateLead(c) {
				/*
				 * Enumerate values for lead surrogate code points, not code units:
				 * This special block has half the normal length.
				 */
				i2Block = UTRIE2_LSCP_INDEX_2_OFFSET
				tempLimit = min(0xdc00, limit)
			} else {
				/*
				 * Switch back to the normal part of the index-2 table.
				 * Enumerate the second half of the surrogates block.
				 */
				i2Block = 0xd800 >> UTRIE2_SHIFT_2
				tempLimit = min(0xe000, limit)
			}
		} else {
			/* supplementary code points */
			i2Block = int(idx[(UTRIE2_INDEX_1_OFFSET-UTRIE2_OMITTED_BMP_INDEX_1_LENGTH)+(c>>UTRIE2_SHIFT_1)])
			if i2Block == prevI2Block && (c-prev) >= UTRIE2_CP_PER_INDEX_1_ENTRY {
				/*
				 * The index-2 block is the same as the previous one, and filled with prevValue.
				 * Only possible for supplementary code points because the linear-BMP index-2
				 * table creates unique i2Block values.
				 */
				c += UTRIE2_CP_PER_INDEX_1_ENTRY
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
			c += UTRIE2_CP_PER_INDEX_1_ENTRY
		} else {
			/* enumerate data blocks for one index-2 block */
			var i2Limit int
			if (c >> UTRIE2_SHIFT_1) == (tempLimit >> UTRIE2_SHIFT_1) {
				i2Limit = int(tempLimit>>UTRIE2_SHIFT_2) & UTRIE2_INDEX_2_MASK
			} else {
				i2Limit = UTRIE2_INDEX_2_BLOCK_LENGTH
			}
			for i2 := int(c>>UTRIE2_SHIFT_2) & UTRIE2_INDEX_2_MASK; i2 < i2Limit; i2++ {
				block = int(idx[i2Block+i2] << UTRIE2_INDEX_SHIFT)
				if block == prevBlock && (c-prev) >= UTRIE2_DATA_BLOCK_LENGTH {
					/* the block is the same as the previous one, and filled with prevValue */
					c += UTRIE2_DATA_BLOCK_LENGTH
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
					c += UTRIE2_DATA_BLOCK_LENGTH
				} else {
					for j := 0; j < UTRIE2_DATA_BLOCK_LENGTH; j++ {
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
	i1 := int(index[(UTRIE2_INDEX_1_OFFSET-UTRIE2_OMITTED_BMP_INDEX_1_LENGTH)+(c>>UTRIE2_SHIFT_1)])
	return (int(index[i1+int((c>>UTRIE2_SHIFT_2)&UTRIE2_INDEX_2_MASK)]) << UTRIE2_INDEX_SHIFT) + int(c&UTRIE2_DATA_MASK)
}

func indexRaw(offset int32, index []uint16, c rune) int {
	return int(index[offset+(c>>UTRIE2_SHIFT_2)]<<UTRIE2_INDEX_SHIFT) + int(c&UTRIE2_DATA_MASK)
}

const (
	/** Shift size for getting the index-1 table offset. */
	UTRIE2_SHIFT_1 = 6 + 5

	/** Shift size for getting the index-2 table offset. */
	UTRIE2_SHIFT_2 = 5

	/**
	 * Difference between the two shift sizes,
	 * for getting an index-1 offset from an index-2 offset. 6=11-5
	 */
	UTRIE2_SHIFT_1_2 = UTRIE2_SHIFT_1 - UTRIE2_SHIFT_2

	/**
	 * Number of index-1 entries for the BMP. 32=0x20
	 * This part of the index-1 table is omitted from the serialized form.
	 */
	UTRIE2_OMITTED_BMP_INDEX_1_LENGTH = 0x10000 >> UTRIE2_SHIFT_1

	/** Number of code points per index-1 table entry. 2048=0x800 */
	UTRIE2_CP_PER_INDEX_1_ENTRY = 1 << UTRIE2_SHIFT_1

	/** Number of entries in an index-2 block. 64=0x40 */
	UTRIE2_INDEX_2_BLOCK_LENGTH = 1 << UTRIE2_SHIFT_1_2

	/** Mask for getting the lower bits for the in-index-2-block offset. */
	UTRIE2_INDEX_2_MASK = UTRIE2_INDEX_2_BLOCK_LENGTH - 1

	/** Number of entries in a data block. 32=0x20 */
	UTRIE2_DATA_BLOCK_LENGTH = 1 << UTRIE2_SHIFT_2

	/** Mask for getting the lower bits for the in-data-block offset. */
	UTRIE2_DATA_MASK = UTRIE2_DATA_BLOCK_LENGTH - 1

	/**
	 * Shift size for shifting left the index array values.
	 * Increases possible data size with 16-bit index values at the cost
	 * of compactability.
	 * This requires data blocks to be aligned by UTRIE2_DATA_GRANULARITY.
	 */
	UTRIE2_INDEX_SHIFT = 2

	/** The alignment size of a data block. Also the granularity for compaction. */
	UTRIE2_DATA_GRANULARITY = 1 << UTRIE2_INDEX_SHIFT

	/* Fixed layout of the first part of the index array. ------------------- */

	/**
	 * The BMP part of the index-2 table is fixed and linear and starts at offset 0.
	 * Length=2048=0x800=0x10000>>UTRIE2_SHIFT_2
	 */
	UTRIE2_INDEX_2_OFFSET = 0

	/**
	 * The part of the index-2 table for U+D800..U+DBFF stores values for
	 * lead surrogate code _units_ not code _points_.
	 * Values for lead surrogate code _points_ are indexed with this portion of the table.
	 * Length=32=0x20=0x400>>UTRIE2_SHIFT_2. (There are 1024=0x400 lead surrogates.)
	 */
	UTRIE2_LSCP_INDEX_2_OFFSET = 0x10000 >> UTRIE2_SHIFT_2
	UTRIE2_LSCP_INDEX_2_LENGTH = 0x400 >> UTRIE2_SHIFT_2

	/** Count the lengths of both BMP pieces. 2080=0x820 */
	UTRIE2_INDEX_2_BMP_LENGTH = UTRIE2_LSCP_INDEX_2_OFFSET + UTRIE2_LSCP_INDEX_2_LENGTH

	/**
	 * The 2-byte UTF-8 version of the index-2 table follows at offset 2080=0x820.
	 * Length 32=0x20 for lead bytes C0..DF, regardless of UTRIE2_SHIFT_2.
	 */
	UTRIE2_UTF8_2B_INDEX_2_OFFSET = UTRIE2_INDEX_2_BMP_LENGTH
	UTRIE2_UTF8_2B_INDEX_2_LENGTH = 0x800 >> 6 /* U+0800 is the first code point after 2-byte UTF-8 */

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
	UTRIE2_INDEX_1_OFFSET     = UTRIE2_UTF8_2B_INDEX_2_OFFSET + UTRIE2_UTF8_2B_INDEX_2_LENGTH
	UTRIE2_MAX_INDEX_1_LENGTH = 0x100000 >> UTRIE2_SHIFT_1

	/*
	 * Fixed layout of the first part of the data array. -----------------------
	 * Starts with 4 blocks (128=0x80 entries) for ASCII.
	 */

	/**
	 * The illegal-UTF-8 data block follows the ASCII block, at offset 128=0x80.
	 * Used with linear access for single bytes 0..0xbf for simple error handling.
	 * Length 64=0x40, not UTRIE2_DATA_BLOCK_LENGTH.
	 */
	UTRIE2_BAD_UTF8_DATA_OFFSET = 0x80

	/** The start of non-linear-ASCII data blocks, at offset 192=0xc0. */
	UTRIE2_DATA_START_OFFSET = 0xc0
)

func UTrie2FromBytes(bytes *udata.Bytes) (*UTrie2, error) {
	type UTrie2Header struct {
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

	var header UTrie2Header
	header.signature = bytes.Uint32()

	switch header.signature {
	case 0x54726932:
	case 0x32697254:
		return nil, fmt.Errorf("unsupported: BigEndian encoding")
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
		return nil, fmt.Errorf("invalid width for serialized UTrie2")
	}

	trie := &UTrie2{
		IndexLength:      int(header.indexLength),
		DataLength:       int(header.shiftedDataLength) << UTRIE2_INDEX_SHIFT,
		Index2NullOffset: header.index2NullOffset,
		DataNullOffset:   header.dataNullOffset,
		HighStart:        rune(header.shiftedHighStart) << UTRIE2_SHIFT_1,
	}

	trie.HighValueIndex = trie.DataLength - UTRIE2_DATA_GRANULARITY
	if width == 16 {
		trie.HighValueIndex += trie.IndexLength
	}

	indexArraySize := trie.IndexLength
	if width == 16 {
		indexArraySize += trie.DataLength
	}

	trie.Index = bytes.Uint16Slice(int32(indexArraySize))

	if width == 16 {
		trie.Data16 = trie.Index[trie.IndexLength:]
		trie.InitialValue = uint32(trie.Index[trie.DataNullOffset])
		trie.ErrorValue = uint32(trie.Index[trie.IndexLength+UTRIE2_BAD_UTF8_DATA_OFFSET])
	} else {
		trie.Data32 = bytes.Uint32Slice(int32(trie.DataLength))
		trie.InitialValue = trie.Data32[trie.DataNullOffset]
		trie.ErrorValue = trie.Data32[UTRIE2_BAD_UTF8_DATA_OFFSET]
	}

	return trie, nil
}
