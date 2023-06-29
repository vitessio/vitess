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
)

type UcpTrie struct {
	Index  []uint16
	Data8  []uint8
	Data16 []uint16
	Data32 []uint32

	IndexLength, DataLength int32
	/** Start of the last range which ends at U+10FFFF. @internal */
	HighStart          rune
	Shifted12HighStart uint16

	Type       UCPTrieType
	ValueWidth UCPTrieValueWidth

	/**
	 * Internal index-3 null block offset.
	 * Set to an impossibly high value (e.g., 0xffff) if there is no dedicated index-3 null block.
	 * @internal
	 */
	Index3NullOffset uint16
	/**
	 * Internal data null block offset, not shifted.
	 * Set to an impossibly high value (e.g., 0xfffff) if there is no dedicated data null block.
	 * @internal
	 */
	DataNullOffset int32

	NullValue uint32
}

/**
 * Selectors for the type of a UCPTrie.
 * Different trade-offs for size vs. speed.
 *
 * @see umutablecptrie_buildImmutable
 * @see ucptrie_openFromBinary
 * @see ucptrie_getType
 * @stable ICU 63
 */
type UCPTrieType int8

const (
	/**
	 * For ucptrie_openFromBinary() to accept any type.
	 * ucptrie_getType() will return the actual type.
	 * @stable ICU 63
	 */
	UCPTRIE_TYPE_ANY UCPTrieType = iota - 1
	/**
	 * Fast/simple/larger BMP data structure. Use functions and "fast" macros.
	 * @stable ICU 63
	 */
	UCPTRIE_TYPE_FAST
	/**
	 * Small/slower BMP data structure. Use functions and "small" macros.
	 * @stable ICU 63
	 */
	UCPTRIE_TYPE_SMALL
)

/**
 * Selectors for the number of bits in a UCPTrie data value.
 *
 * @see umutablecptrie_buildImmutable
 * @see ucptrie_openFromBinary
 * @see ucptrie_getValueWidth
 * @stable ICU 63
 */
type UCPTrieValueWidth int8

const (
	/**
	 * For ucptrie_openFromBinary() to accept any data value width.
	 * ucptrie_getValueWidth() will return the actual data value width.
	 * @stable ICU 63
	 */
	UCPTRIE_VALUE_BITS_ANY UCPTrieValueWidth = iota - 1
	/**
	 * The trie stores 16 bits per data value.
	 * It returns them as unsigned values 0..0xffff=65535.
	 * @stable ICU 63
	 */
	UCPTRIE_VALUE_BITS_16
	/**
	 * The trie stores 32 bits per data value.
	 * @stable ICU 63
	 */
	UCPTRIE_VALUE_BITS_32
	/**
	 * The trie stores 8 bits per data value.
	 * It returns them as unsigned values 0..0xff=255.
	 * @stable ICU 63
	 */
	UCPTRIE_VALUE_BITS_8
)

const UCPTRIE_SIG = 0x54726933
const UCPTRIE_OE_SIG = 0x33697254

/**
 * Constants for use with UCPTrieHeader.options.
 * @internal
 */
const (
	UCPTRIE_OPTIONS_DATA_LENGTH_MASK      = 0xf000
	UCPTRIE_OPTIONS_DATA_NULL_OFFSET_MASK = 0xf00
	UCPTRIE_OPTIONS_RESERVED_MASK         = 0x38
	UCPTRIE_OPTIONS_VALUE_BITS_MASK       = 7
	/**
	 * Value for index3NullOffset which indicates that there is no index-3 null block.
	 * Bit 15 is unused for this value because this bit is used if the index-3 contains
	 * 18-bit indexes.
	 */
	UCPTRIE_NO_INDEX3_NULL_OFFSET = 0x7fff
	UCPTRIE_NO_DATA_NULL_OFFSET   = 0xfffff
)

const (
	/** @internal */
	UCPTRIE_FAST_SHIFT = 6

	/** Number of entries in a data block for code points below the fast limit. 64=0x40 @internal */
	UCPTRIE_FAST_DATA_BLOCK_LENGTH = 1 << UCPTRIE_FAST_SHIFT

	/** Mask for getting the lower bits for the in-fast-data-block offset. @internal */
	UCPTRIE_FAST_DATA_MASK = UCPTRIE_FAST_DATA_BLOCK_LENGTH - 1

	/** @internal */
	UCPTRIE_SMALL_MAX = 0xfff

	/**
	 * Offset from dataLength (to be subtracted) for fetching the
	 * value returned for out-of-range code points and ill-formed UTF-8/16.
	 * @internal
	 */
	UCPTRIE_ERROR_VALUE_NEG_DATA_OFFSET = 1
	/**
	 * Offset from dataLength (to be subtracted) for fetching the
	 * value returned for code points highStart..U+10FFFF.
	 * @internal
	 */
	UCPTRIE_HIGH_VALUE_NEG_DATA_OFFSET = 2
)

// Internal constants.
const (
	/** The length of the BMP index table. 1024=0x400 */
	UCPTRIE_BMP_INDEX_LENGTH = 0x10000 >> UCPTRIE_FAST_SHIFT

	UCPTRIE_SMALL_LIMIT        = 0x1000
	UCPTRIE_SMALL_INDEX_LENGTH = UCPTRIE_SMALL_LIMIT >> UCPTRIE_FAST_SHIFT

	/** Shift size for getting the index-3 table offset. */
	UCPTRIE_SHIFT_3 = 4

	/** Shift size for getting the index-2 table offset. */
	UCPTRIE_SHIFT_2 = 5 + UCPTRIE_SHIFT_3

	/** Shift size for getting the index-1 table offset. */
	UCPTRIE_SHIFT_1 = 5 + UCPTRIE_SHIFT_2

	/**
	 * Difference between two shift sizes,
	 * for getting an index-2 offset from an index-3 offset. 5=9-4
	 */
	UCPTRIE_SHIFT_2_3 = UCPTRIE_SHIFT_2 - UCPTRIE_SHIFT_3

	/**
	 * Difference between two shift sizes,
	 * for getting an index-1 offset from an index-2 offset. 5=14-9
	 */
	UCPTRIE_SHIFT_1_2 = UCPTRIE_SHIFT_1 - UCPTRIE_SHIFT_2

	/**
	 * Number of index-1 entries for the BMP. (4)
	 * This part of the index-1 table is omitted from the serialized form.
	 */
	UCPTRIE_OMITTED_BMP_INDEX_1_LENGTH = 0x10000 >> UCPTRIE_SHIFT_1

	/** Number of entries in an index-2 block. 32=0x20 */
	UCPTRIE_INDEX_2_BLOCK_LENGTH = 1 << UCPTRIE_SHIFT_1_2

	/** Mask for getting the lower bits for the in-index-2-block offset. */
	UCPTRIE_INDEX_2_MASK = UCPTRIE_INDEX_2_BLOCK_LENGTH - 1

	/** Number of code points per index-2 table entry. 512=0x200 */
	UCPTRIE_CP_PER_INDEX_2_ENTRY = 1 << UCPTRIE_SHIFT_2

	/** Number of entries in an index-3 block. 32=0x20 */
	UCPTRIE_INDEX_3_BLOCK_LENGTH = 1 << UCPTRIE_SHIFT_2_3

	/** Mask for getting the lower bits for the in-index-3-block offset. */
	UCPTRIE_INDEX_3_MASK = UCPTRIE_INDEX_3_BLOCK_LENGTH - 1

	/** Number of entries in a small data block. 16=0x10 */
	UCPTRIE_SMALL_DATA_BLOCK_LENGTH = 1 << UCPTRIE_SHIFT_3

	/** Mask for getting the lower bits for the in-small-data-block offset. */
	UCPTRIE_SMALL_DATA_MASK = UCPTRIE_SMALL_DATA_BLOCK_LENGTH - 1
)

func UcpTrieFromBytes(bytes *udata.Bytes) (*UcpTrie, error) {
	type UcpHeader struct {
		/** "Tri3" in big-endian US-ASCII (0x54726933) */
		signature uint32

		/**
			 * Options bit field:
			 * Bits 15..12: Data length bits 19..16.
			 * Bits 11..8: Data null block offset bits 19..16.
			 * Bits 7..6: UCPTrieType
			 * Bits 5..3: Reserved (0).
		 	 * Bits 2..0: UCPTrieValueWidth
		*/
		options uint16

		/** Total length of the index tables. */
		indexLength uint16

		/** Data length bits 15..0. */
		dataLength uint16

		/** Index-3 null block offset, 0x7fff or 0xffff if none. */
		index3NullOffset uint16

		/** Data null block offset bits 15..0, 0xfffff if none. */
		dataNullOffset uint16

		/**
		 * First code point of the single-value range ending with U+10ffff,
		 * rounded up and then shifted right by UCPTRIE_SHIFT_2.
		 */
		shiftedHighStart uint16
	}

	var header UcpHeader
	header.signature = bytes.Uint32()

	switch header.signature {
	case UCPTRIE_SIG:
	case UCPTRIE_OE_SIG:
		return nil, fmt.Errorf("unsupported: BigEndian encoding")
	default:
		return nil, fmt.Errorf("invalid signature for UcpTrie: 0x%08x", header.signature)
	}

	header.options = bytes.Uint16()
	header.indexLength = bytes.Uint16()
	header.dataLength = bytes.Uint16()
	header.index3NullOffset = bytes.Uint16()
	header.dataNullOffset = bytes.Uint16()
	header.shiftedHighStart = bytes.Uint16()

	typeInt := (header.options >> 6) & 3
	valueWidthInt := header.options & UCPTRIE_OPTIONS_VALUE_BITS_MASK
	if typeInt > uint16(UCPTRIE_TYPE_SMALL) || valueWidthInt > uint16(UCPTRIE_VALUE_BITS_8) ||
		(header.options&UCPTRIE_OPTIONS_RESERVED_MASK) != 0 {
		return nil, fmt.Errorf("invalid options for serialized UcpTrie")
	}
	actualType := UCPTrieType(typeInt)
	actualValueWidth := UCPTrieValueWidth(valueWidthInt)

	trie := &UcpTrie{
		IndexLength:      int32(header.indexLength),
		DataLength:       int32(((header.options & UCPTRIE_OPTIONS_DATA_LENGTH_MASK) << 4) | header.dataLength),
		Index3NullOffset: header.index3NullOffset,
		DataNullOffset:   int32(((header.options & UCPTRIE_OPTIONS_DATA_NULL_OFFSET_MASK) << 8) | header.dataNullOffset),
		HighStart:        rune(header.shiftedHighStart) << UCPTRIE_SHIFT_2,
		Type:             actualType,
		ValueWidth:       actualValueWidth,
	}
	nullValueOffset := trie.DataNullOffset
	if nullValueOffset >= trie.DataLength {
		nullValueOffset = trie.DataLength - UCPTRIE_HIGH_VALUE_NEG_DATA_OFFSET
	}

	trie.Shifted12HighStart = uint16((trie.HighStart + 0xfff) >> 12)
	trie.Index = bytes.Uint16Slice(int32(header.indexLength))
	switch actualValueWidth {
	case UCPTRIE_VALUE_BITS_16:
		trie.Data16 = bytes.Uint16Slice(trie.DataLength)
		trie.NullValue = uint32(trie.Data16[nullValueOffset])
	case UCPTRIE_VALUE_BITS_32:
		trie.Data32 = bytes.Uint32Slice(trie.DataLength)
		trie.NullValue = trie.Data32[nullValueOffset]
	case UCPTRIE_VALUE_BITS_8:
		trie.Data8 = bytes.Uint8Slice(trie.DataLength)
		trie.NullValue = uint32(trie.Data8[nullValueOffset])
	}

	return trie, nil
}

func (t *UcpTrie) Get(c rune) uint32 {
	var dataIndex int32
	if c <= 0x7f {
		// linear ASCII
		dataIndex = c
	} else {
		var fastMax rune
		if t.Type == UCPTRIE_TYPE_FAST {
			fastMax = 0xffff
		} else {
			fastMax = UCPTRIE_SMALL_MAX
		}
		dataIndex = t.cpIndex(fastMax, c)
	}
	return t.getValue(dataIndex)
}

func (t *UcpTrie) getValue(dataIndex int32) uint32 {
	switch t.ValueWidth {
	case UCPTRIE_VALUE_BITS_16:
		return uint32(t.Data16[dataIndex])
	case UCPTRIE_VALUE_BITS_32:
		return t.Data32[dataIndex]
	case UCPTRIE_VALUE_BITS_8:
		return uint32(t.Data8[dataIndex])
	default:
		// Unreachable if the trie is properly initialized.
		return 0xffffffff
	}
}

/** Internal trie getter for a code point below the fast limit. Returns the data index. @internal */
func (t *UcpTrie) fastIndex(c rune) int32 {
	return int32(t.Index[c>>UCPTRIE_FAST_SHIFT]) + (c & UCPTRIE_FAST_DATA_MASK)
}

/** Internal trie getter for a code point at or above the fast limit. Returns the data index. @internal */
func (t *UcpTrie) smallIndex(c rune) int32 {
	if c >= t.HighStart {
		return t.DataLength - UCPTRIE_HIGH_VALUE_NEG_DATA_OFFSET
	}
	return t.internalSmallIndex(c)
}

func (t *UcpTrie) internalSmallIndex(c rune) int32 {
	i1 := c >> UCPTRIE_SHIFT_1
	if t.Type == UCPTRIE_TYPE_FAST {
		i1 += UCPTRIE_BMP_INDEX_LENGTH - UCPTRIE_OMITTED_BMP_INDEX_1_LENGTH
	} else {
		i1 += UCPTRIE_SMALL_INDEX_LENGTH
	}
	i3Block := int32(t.Index[int32(t.Index[i1])+((c>>UCPTRIE_SHIFT_2)&UCPTRIE_INDEX_2_MASK)])
	i3 := (c >> UCPTRIE_SHIFT_3) & UCPTRIE_INDEX_3_MASK
	var dataBlock int32
	if (i3Block & 0x8000) == 0 {
		// 16-bit indexes
		dataBlock = int32(t.Index[i3Block+i3])
	} else {
		// 18-bit indexes stored in groups of 9 entries per 8 indexes.
		i3Block = (i3Block & 0x7fff) + (i3 & ^7) + (i3 >> 3)
		i3 &= 7
		dataBlock = int32(t.Index[i3Block]) << (2 + (2 * i3)) & 0x30000
		i3Block++
		dataBlock |= int32(t.Index[i3Block+i3])
	}
	return dataBlock + (c & UCPTRIE_SMALL_DATA_MASK)
}

/**
 * Internal trie getter for a code point, with checking that c is in U+0000..10FFFF.
 * Returns the data index.
 * @internal
 */
func (t *UcpTrie) cpIndex(fastMax, c rune) int32 {
	if c <= fastMax {
		return t.fastIndex(c)
	}
	if c <= 0x10ffff {
		return t.smallIndex(c)
	}
	return t.DataLength - UCPTRIE_ERROR_VALUE_NEG_DATA_OFFSET
}

/**
 * Selectors for how ucpmap_getRange() etc. should report value ranges overlapping with surrogates.
 * Most users should use UCPMAP_RANGE_NORMAL.
 *
 * @see ucpmap_getRange
 * @see ucptrie_getRange
 * @see umutablecptrie_getRange
 * @stable ICU 63
 */
type UCPMapRangeOption int8

const (
	/**
	 * ucpmap_getRange() enumerates all same-value ranges as stored in the map.
	 * Most users should use this option.
	 * @stable ICU 63
	 */
	UCPMAP_RANGE_NORMAL UCPMapRangeOption = iota
	/**
	 * ucpmap_getRange() enumerates all same-value ranges as stored in the map,
	 * except that lead surrogates (U+D800..U+DBFF) are treated as having the
	 * surrogateValue, which is passed to getRange() as a separate parameter.
	 * The surrogateValue is not transformed via filter().
	 * See U_IS_LEAD(c).
	 *
	 * Most users should use UCPMAP_RANGE_NORMAL instead.
	 *
	 * This option is useful for maps that map surrogate code *units* to
	 * special values optimized for UTF-16 string processing
	 * or for special error behavior for unpaired surrogates,
	 * but those values are not to be associated with the lead surrogate code *points*.
	 * @stable ICU 63
	 */
	UCPMAP_RANGE_FIXED_LEAD_SURROGATES
	/**
	 * ucpmap_getRange() enumerates all same-value ranges as stored in the map,
	 * except that all surrogates (U+D800..U+DFFF) are treated as having the
	 * surrogateValue, which is passed to getRange() as a separate parameter.
	 * The surrogateValue is not transformed via filter().
	 * See U_IS_SURROGATE(c).
	 *
	 * Most users should use UCPMAP_RANGE_NORMAL instead.
	 *
	 * This option is useful for maps that map surrogate code *units* to
	 * special values optimized for UTF-16 string processing
	 * or for special error behavior for unpaired surrogates,
	 * but those values are not to be associated with the lead surrogate code *points*.
	 * @stable ICU 63
	 */
	UCPMAP_RANGE_FIXED_ALL_SURROGATES
)

/**
 * Callback function type: Modifies a map value.
 * Optionally called by ucpmap_getRange()/ucptrie_getRange()/umutablecptrie_getRange().
 * The modified value will be returned by the getRange function.
 *
 * Can be used to ignore some of the value bits,
 * make a filter for one of several values,
 * return a value index computed from the map value, etc.
 *
 * @param context an opaque pointer, as passed into the getRange function
 * @param value a value from the map
 * @return the modified value
 * @stable ICU 63
 */
type UCPMapValueFilter func(value uint32) uint32

/**
 * GetRange returns the last code point such that all those from start to there have the same value.
 * Can be used to efficiently iterate over all same-value ranges in a trie.
 * (This is normally faster than iterating over code points and get()ting each value,
 * but much slower than a data structure that stores ranges directly.)
 *
 * If the UCPMapValueFilter function pointer is not NULL, then
 * the value to be delivered is passed through that function, and the return value is the end
 * of the range where all values are modified to the same actual value.
 * The value is unchanged if that function pointer is NULL.
 *
 * Example:
 * \code
 * UChar32 start = 0, end;
 * uint32_t value;
 * while ((end = ucptrie_getRange(trie, start, UCPMAP_RANGE_NORMAL, 0,
 *                                NULL, NULL, &value)) >= 0) {
 *     // Work with the range start..end and its value.
 *     start = end + 1;
 * }
 * \endcode
 *
 * @param trie the trie
 * @param start range start
 * @param option defines whether surrogates are treated normally,
 *               or as having the surrogateValue; usually UCPMAP_RANGE_NORMAL
 * @param surrogateValue value for surrogates; ignored if option==UCPMAP_RANGE_NORMAL
 * @param filter a pointer to a function that may modify the trie data value,
 *     or NULL if the values from the trie are to be used unmodified
 * @param context an opaque pointer that is passed on to the filter function
 * @param pValue if not NULL, receives the value that every code point start..end has;
 *     may have been modified by filter(context, trie value)
 *     if that function pointer is not NULL
 * @return the range end code point, or -1 if start is not a valid code point
 * @stable ICU 63
 */
func (t *UcpTrie) GetRange(start rune, option UCPMapRangeOption, surrogateValue uint32, filter UCPMapValueFilter) (rune, uint32) {
	if option == UCPMAP_RANGE_NORMAL {
		return t.getRange(start, filter)
	}

	var surrEnd rune
	if option == UCPMAP_RANGE_FIXED_ALL_SURROGATES {
		surrEnd = 0xdfff
	} else {
		surrEnd = 0xdbff
	}
	end, value := t.getRange(start, filter)
	if end < 0xd7ff || start > surrEnd {
		return end, value
	}
	if value == surrogateValue {
		if end >= surrEnd {
			// Surrogates followed by a non-surrogateValue range,
			// or surrogates are part of a larger surrogateValue range.
			return end, value
		}
	} else {
		if start <= 0xd7ff {
			return 0xd7ff, value // Non-surrogateValue range ends before surrogateValue surrogates.
		}
		// Start is a surrogate with a non-surrogateValue code *unit* value.
		// Return a surrogateValue code *point* range.
		value = surrogateValue
		if end > surrEnd {
			return surrEnd, value // Surrogate range ends before non-surrogateValue rest of range.
		}
	}
	// See if the surrogateValue surrogate range can be merged with
	// an immediately following range.
	end2, value2 := t.getRange(surrEnd+1, filter)
	if value2 == surrogateValue {
		return end2, value
	}
	return surrEnd, value
}

const MAX_UNICODE = 0x10ffff

func (t *UcpTrie) getRange(start rune, filter UCPMapValueFilter) (rune, uint32) {
	if start > MAX_UNICODE {
		return -1, 0
	}

	if start >= t.HighStart {
		di := t.DataLength - UCPTRIE_HIGH_VALUE_NEG_DATA_OFFSET
		value := t.getValue(di)
		if filter != nil {
			value = filter(value)
		}
		return MAX_UNICODE, value
	}

	nullValue := t.NullValue
	if filter != nil {
		nullValue = filter(nullValue)
	}
	index := t.Index

	prevI3Block := int32(-1)
	prevBlock := int32(-1)
	c := start
	var trieValue uint32
	value := nullValue
	haveValue := false
	for {
		var i3Block, i3, i3BlockLength, dataBlockLength int32
		if c <= 0xffff && (t.Type == UCPTRIE_TYPE_FAST || c <= UCPTRIE_SMALL_MAX) {
			i3Block = 0
			i3 = c >> UCPTRIE_FAST_SHIFT
			if t.Type == UCPTRIE_TYPE_FAST {
				i3BlockLength = UCPTRIE_BMP_INDEX_LENGTH
			} else {
				i3BlockLength = UCPTRIE_SMALL_INDEX_LENGTH
			}
			dataBlockLength = UCPTRIE_FAST_DATA_BLOCK_LENGTH
		} else {
			// Use the multi-stage index.
			i1 := c >> UCPTRIE_SHIFT_1
			if t.Type == UCPTRIE_TYPE_FAST {
				i1 += UCPTRIE_BMP_INDEX_LENGTH - UCPTRIE_OMITTED_BMP_INDEX_1_LENGTH
			} else {
				i1 += UCPTRIE_SMALL_INDEX_LENGTH
			}
			shft := c >> UCPTRIE_SHIFT_2
			idx := int32(t.Index[i1]) + (shft & UCPTRIE_INDEX_2_MASK)
			i3Block = int32(t.Index[idx])
			if i3Block == prevI3Block && (c-start) >= UCPTRIE_CP_PER_INDEX_2_ENTRY {
				// The index-3 block is the same as the previous one, and filled with value.
				c += UCPTRIE_CP_PER_INDEX_2_ENTRY
				continue
			}
			prevI3Block = i3Block
			if i3Block == int32(t.Index3NullOffset) {
				// This is the index-3 null block.
				if haveValue {
					if nullValue != value {
						return c - 1, value
					}
				} else {
					trieValue = t.NullValue
					value = nullValue
					haveValue = true
				}
				prevBlock = t.DataNullOffset
				c = (c + UCPTRIE_CP_PER_INDEX_2_ENTRY) & ^(UCPTRIE_CP_PER_INDEX_2_ENTRY - 1)
				continue
			}
			i3 = (c >> UCPTRIE_SHIFT_3) & UCPTRIE_INDEX_3_MASK
			i3BlockLength = UCPTRIE_INDEX_3_BLOCK_LENGTH
			dataBlockLength = UCPTRIE_SMALL_DATA_BLOCK_LENGTH
		}

		// Enumerate data blocks for one index-3 block.
		for {
			var block int32
			if (i3Block & 0x8000) == 0 {
				block = int32(index[i3Block+i3])
			} else {
				// 18-bit indexes stored in groups of 9 entries per 8 indexes.
				group := (i3Block & 0x7fff) + (i3 & ^7) + (i3 >> 3)
				gi := i3 & 7
				block = (int32(index[group]) << (2 + (2 * gi))) & 0x30000
				group++
				block |= int32(index[group+gi])
			}
			if block == prevBlock && (c-start) >= dataBlockLength {
				// The block is the same as the previous one, and filled with value.
				c += dataBlockLength
			} else {
				dataMask := dataBlockLength - 1
				prevBlock = block
				if block == t.DataNullOffset {
					// This is the data null block.
					if haveValue {
						if nullValue != value {
							return c - 1, value
						}
					} else {
						trieValue = t.NullValue
						value = nullValue
						haveValue = true
					}
					c = (c + dataBlockLength) & ^dataMask
				} else {
					di := block + (c & dataMask)
					trieValue2 := t.getValue(di)
					if haveValue {
						if trieValue2 != trieValue {
							if filter == nil || maybeFilterValue(trieValue2, t.NullValue, nullValue, filter) != value {
								return c - 1, value
							}
							trieValue = trieValue2 // may or may not help
						}
					} else {
						trieValue = trieValue2
						value = maybeFilterValue(trieValue2, t.NullValue, nullValue, filter)
						haveValue = true
					}
					for {
						c++
						if c&dataMask == 0 {
							break
						}
						di++
						trieValue2 = t.getValue(di)
						if trieValue2 != trieValue {
							if filter == nil || maybeFilterValue(trieValue2, t.NullValue, nullValue, filter) != value {
								return c - 1, value
							}
							trieValue = trieValue2 // may or may not help
						}
					}
				}
			}
			i3++
			if i3 >= i3BlockLength {
				break
			}
		}
		if c >= t.HighStart {
			break
		}
	}

	di := t.DataLength - UCPTRIE_HIGH_VALUE_NEG_DATA_OFFSET
	highValue := t.getValue(di)
	if maybeFilterValue(highValue, t.NullValue, nullValue, filter) != value {
		return c - 1, value
	} else {
		return MAX_UNICODE, value
	}
}

func maybeFilterValue(value uint32, trieNullValue uint32, nullValue uint32, filter UCPMapValueFilter) uint32 {
	if value == trieNullValue {
		value = nullValue
	} else if filter != nil {
		value = filter(value)
	}
	return value
}
