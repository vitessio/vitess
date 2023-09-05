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
)

type UcpTrie struct {
	index  []uint16
	data8  []uint8
	data16 []uint16
	data32 []uint32

	indexLength, dataLength int32
	/** Start of the last range which ends at U+10FFFF. @internal */
	highStart          rune
	shifted12HighStart uint16

	typ        ucpTrieType
	valueWidth ucpTrieValueWidth

	/**
	 * Internal index-3 null block offset.
	 * Set to an impossibly high value (e.g., 0xffff) if there is no dedicated index-3 null block.
	 * @internal
	 */
	index3NullOffset uint16
	/**
	 * Internal data null block offset, not shifted.
	 * Set to an impossibly high value (e.g., 0xfffff) if there is no dedicated data null block.
	 * @internal
	 */
	dataNullOffset int32

	nullValue uint32
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
type ucpTrieType int8

const (
	/**
	 * For ucptrie_openFromBinary() to accept any type.
	 * ucptrie_getType() will return the actual type.
	 * @stable ICU 63
	 */
	typeAny ucpTrieType = iota - 1
	/**
	 * Fast/simple/larger BMP data structure. Use functions and "fast" macros.
	 * @stable ICU 63
	 */
	typeFast
	/**
	 * Small/slower BMP data structure. Use functions and "small" macros.
	 * @stable ICU 63
	 */
	typeSmall
)

/**
 * Selectors for the number of bits in a UCPTrie data value.
 *
 * @see umutablecptrie_buildImmutable
 * @see ucptrie_openFromBinary
 * @see ucptrie_getValueWidth
 * @stable ICU 63
 */
type ucpTrieValueWidth int8

const (
	/**
	 * For ucptrie_openFromBinary() to accept any data value width.
	 * ucptrie_getValueWidth() will return the actual data value width.
	 * @stable ICU 63
	 */
	valueBitsAny ucpTrieValueWidth = iota - 1
	/**
	 * The trie stores 16 bits per data value.
	 * It returns them as unsigned values 0..0xffff=65535.
	 * @stable ICU 63
	 */
	valueBits16
	/**
	 * The trie stores 32 bits per data value.
	 * @stable ICU 63
	 */
	valueBits32
	/**
	 * The trie stores 8 bits per data value.
	 * It returns them as unsigned values 0..0xff=255.
	 * @stable ICU 63
	 */
	valueBits8
)

const ucpTrieSig = 0x54726933
const ucpTrieOESig = 0x33697254

/**
 * Constants for use with UCPTrieHeader.options.
 * @internal
 */
const (
	optionsDataLengthMask     = 0xf000
	optionsDataNullOffsetMask = 0xf00
	optionsReservedMask       = 0x38
	optionsValueBitsMask      = 7
)

const (
	/** @internal */
	fastShift = 6

	/** Number of entries in a data block for code points below the fast limit. 64=0x40 @internal */
	fastDataBlockLength = 1 << fastShift

	/** Mask for getting the lower bits for the in-fast-data-block offset. @internal */
	fastDataMask = fastDataBlockLength - 1

	/** @internal */
	smallMax = 0xfff

	/**
	 * Offset from dataLength (to be subtracted) for fetching the
	 * value returned for out-of-range code points and ill-formed UTF-8/16.
	 * @internal
	 */
	errorValueNegDataOffset = 1
	/**
	 * Offset from dataLength (to be subtracted) for fetching the
	 * value returned for code points highStart..U+10FFFF.
	 * @internal
	 */
	highValueNegDataOffset = 2
)

// Internal constants.
const (
	/** The length of the BMP index table. 1024=0x400 */
	bmpIndexLength = 0x10000 >> fastShift

	smallLimit       = 0x1000
	smallIndexLength = smallLimit >> fastShift

	/** Shift size for getting the index-3 table offset. */
	ucpShift3 = 4

	/** Shift size for getting the index-2 table offset. */
	ucpShift2 = 5 + ucpShift3

	/** Shift size for getting the index-1 table offset. */
	ucpShift1 = 5 + ucpShift2

	/**
	 * Difference between two shift sizes,
	 * for getting an index-2 offset from an index-3 offset. 5=9-4
	 */
	ucpShift2Min3 = ucpShift2 - ucpShift3

	/**
	 * Difference between two shift sizes,
	 * for getting an index-1 offset from an index-2 offset. 5=14-9
	 */
	ucpShift1Min2 = ucpShift1 - ucpShift2

	/**
	 * Number of index-1 entries for the BMP. (4)
	 * This part of the index-1 table is omitted from the serialized form.
	 */
	ucpOmittedBmpIndex1Length = 0x10000 >> ucpShift1

	/** Number of entries in an index-2 block. 32=0x20 */
	ucpIndex2BlockLength = 1 << ucpShift1Min2

	/** Mask for getting the lower bits for the in-index-2-block offset. */
	ucpIndex2Mask = ucpIndex2BlockLength - 1

	/** Number of code points per index-2 table entry. 512=0x200 */
	ucpCpPerIndex2Entry = 1 << ucpShift2

	/** Number of entries in an index-3 block. 32=0x20 */
	ucpIndex3BlockLength = 1 << ucpShift2Min3

	/** Mask for getting the lower bits for the in-index-3-block offset. */
	ucpIndex3Mask = ucpIndex3BlockLength - 1

	/** Number of entries in a small data block. 16=0x10 */
	ucpSmallDataBlockLength = 1 << ucpShift3

	/** Mask for getting the lower bits for the in-small-data-block offset. */
	ucpSmallDataMask = ucpSmallDataBlockLength - 1
)

func UcpTrieFromBytes(bytes *udata.Bytes) (*UcpTrie, error) {
	type ucpHeader struct {
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

	var header ucpHeader
	header.signature = bytes.Uint32()

	switch header.signature {
	case ucpTrieSig:
	case ucpTrieOESig:
		return nil, errors.New("unsupported: BigEndian encoding")
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
	valueWidthInt := header.options & optionsValueBitsMask
	if typeInt > uint16(typeSmall) || valueWidthInt > uint16(valueBits8) ||
		(header.options&optionsReservedMask) != 0 {
		return nil, errors.New("invalid options for serialized UcpTrie")
	}
	actualType := ucpTrieType(typeInt)
	actualValueWidth := ucpTrieValueWidth(valueWidthInt)

	trie := &UcpTrie{
		indexLength:      int32(header.indexLength),
		dataLength:       int32(((header.options & optionsDataLengthMask) << 4) | header.dataLength),
		index3NullOffset: header.index3NullOffset,
		dataNullOffset:   int32(((header.options & optionsDataNullOffsetMask) << 8) | header.dataNullOffset),
		highStart:        rune(header.shiftedHighStart) << ucpShift2,
		typ:              actualType,
		valueWidth:       actualValueWidth,
	}
	nullValueOffset := trie.dataNullOffset
	if nullValueOffset >= trie.dataLength {
		nullValueOffset = trie.dataLength - highValueNegDataOffset
	}

	trie.shifted12HighStart = uint16((trie.highStart + 0xfff) >> 12)
	trie.index = bytes.Uint16Slice(int32(header.indexLength))
	switch actualValueWidth {
	case valueBits16:
		trie.data16 = bytes.Uint16Slice(trie.dataLength)
		trie.nullValue = uint32(trie.data16[nullValueOffset])
	case valueBits32:
		trie.data32 = bytes.Uint32Slice(trie.dataLength)
		trie.nullValue = trie.data32[nullValueOffset]
	case valueBits8:
		trie.data8 = bytes.Uint8Slice(trie.dataLength)
		trie.nullValue = uint32(trie.data8[nullValueOffset])
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
		if t.typ == typeFast {
			fastMax = 0xffff
		} else {
			fastMax = smallMax
		}
		dataIndex = t.cpIndex(fastMax, c)
	}
	return t.getValue(dataIndex)
}

func (t *UcpTrie) getValue(dataIndex int32) uint32 {
	switch t.valueWidth {
	case valueBits16:
		return uint32(t.data16[dataIndex])
	case valueBits32:
		return t.data32[dataIndex]
	case valueBits8:
		return uint32(t.data8[dataIndex])
	default:
		// Unreachable if the trie is properly initialized.
		return 0xffffffff
	}
}

/** Internal trie getter for a code point below the fast limit. Returns the data index. @internal */
func (t *UcpTrie) fastIndex(c rune) int32 {
	return int32(t.index[c>>fastShift]) + (c & fastDataMask)
}

/** Internal trie getter for a code point at or above the fast limit. Returns the data index. @internal */
func (t *UcpTrie) smallIndex(c rune) int32 {
	if c >= t.highStart {
		return t.dataLength - highValueNegDataOffset
	}
	return t.internalSmallIndex(c)
}

func (t *UcpTrie) internalSmallIndex(c rune) int32 {
	i1 := c >> ucpShift1
	if t.typ == typeFast {
		i1 += bmpIndexLength - ucpOmittedBmpIndex1Length
	} else {
		i1 += smallIndexLength
	}
	i3Block := int32(t.index[int32(t.index[i1])+((c>>ucpShift2)&ucpIndex2Mask)])
	i3 := (c >> ucpShift3) & ucpIndex3Mask
	var dataBlock int32
	if (i3Block & 0x8000) == 0 {
		// 16-bit indexes
		dataBlock = int32(t.index[i3Block+i3])
	} else {
		// 18-bit indexes stored in groups of 9 entries per 8 indexes.
		i3Block = (i3Block & 0x7fff) + (i3 & ^7) + (i3 >> 3)
		i3 &= 7
		dataBlock = int32(t.index[i3Block]) << (2 + (2 * i3)) & 0x30000
		i3Block++
		dataBlock |= int32(t.index[i3Block+i3])
	}
	return dataBlock + (c & ucpSmallDataMask)
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
	return t.dataLength - errorValueNegDataOffset
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
type UcpMapRangeOption int8

const (
	/**
	 * ucpmap_getRange() enumerates all same-value ranges as stored in the map.
	 * Most users should use this option.
	 * @stable ICU 63
	 */
	UcpMapRangeNormal UcpMapRangeOption = iota
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
	UcpMapRangeFixedLeadSurrogates
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
	UcpMapRangeFixedAllSurrogates
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
type UcpMapValueFilter func(value uint32) uint32

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
func (t *UcpTrie) GetRange(start rune, option UcpMapRangeOption, surrogateValue uint32, filter UcpMapValueFilter) (rune, uint32) {
	if option == UcpMapRangeNormal {
		return t.getRange(start, filter)
	}

	var surrEnd rune
	if option == UcpMapRangeFixedAllSurrogates {
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

const maxUnicode = 0x10ffff

func (t *UcpTrie) getRange(start rune, filter UcpMapValueFilter) (rune, uint32) {
	if start > maxUnicode {
		return -1, 0
	}

	if start >= t.highStart {
		di := t.dataLength - highValueNegDataOffset
		value := t.getValue(di)
		if filter != nil {
			value = filter(value)
		}
		return maxUnicode, value
	}

	nullValue := t.nullValue
	if filter != nil {
		nullValue = filter(nullValue)
	}
	index := t.index

	prevI3Block := int32(-1)
	prevBlock := int32(-1)
	c := start
	var trieValue uint32
	value := nullValue
	haveValue := false
	for {
		var i3Block, i3, i3BlockLength, dataBlockLength int32
		if c <= 0xffff && (t.typ == typeFast || c <= smallMax) {
			i3Block = 0
			i3 = c >> fastShift
			if t.typ == typeFast {
				i3BlockLength = bmpIndexLength
			} else {
				i3BlockLength = smallIndexLength
			}
			dataBlockLength = fastDataBlockLength
		} else {
			// Use the multi-stage index.
			i1 := c >> ucpShift1
			if t.typ == typeFast {
				i1 += bmpIndexLength - ucpOmittedBmpIndex1Length
			} else {
				i1 += smallIndexLength
			}
			shft := c >> ucpShift2
			idx := int32(t.index[i1]) + (shft & ucpIndex2Mask)
			i3Block = int32(t.index[idx])
			if i3Block == prevI3Block && (c-start) >= ucpCpPerIndex2Entry {
				// The index-3 block is the same as the previous one, and filled with value.
				c += ucpCpPerIndex2Entry
				continue
			}
			prevI3Block = i3Block
			if i3Block == int32(t.index3NullOffset) {
				// This is the index-3 null block.
				if haveValue {
					if nullValue != value {
						return c - 1, value
					}
				} else {
					trieValue = t.nullValue
					value = nullValue
					haveValue = true
				}
				prevBlock = t.dataNullOffset
				c = (c + ucpCpPerIndex2Entry) & ^(ucpCpPerIndex2Entry - 1)
				continue
			}
			i3 = (c >> ucpShift3) & ucpIndex3Mask
			i3BlockLength = ucpIndex3BlockLength
			dataBlockLength = ucpSmallDataBlockLength
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
				if block == t.dataNullOffset {
					// This is the data null block.
					if haveValue {
						if nullValue != value {
							return c - 1, value
						}
					} else {
						trieValue = t.nullValue
						value = nullValue
						haveValue = true
					}
					c = (c + dataBlockLength) & ^dataMask
				} else {
					di := block + (c & dataMask)
					trieValue2 := t.getValue(di)
					if haveValue {
						if trieValue2 != trieValue {
							if filter == nil || maybeFilterValue(trieValue2, t.nullValue, nullValue, filter) != value {
								return c - 1, value
							}
							trieValue = trieValue2 // may or may not help
						}
					} else {
						trieValue = trieValue2
						value = maybeFilterValue(trieValue2, t.nullValue, nullValue, filter)
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
							if filter == nil || maybeFilterValue(trieValue2, t.nullValue, nullValue, filter) != value {
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
		if c >= t.highStart {
			break
		}
	}

	di := t.dataLength - highValueNegDataOffset
	highValue := t.getValue(di)
	if maybeFilterValue(highValue, t.nullValue, nullValue, filter) != value {
		return c - 1, value
	}
	return maxUnicode, value
}

func maybeFilterValue(value uint32, trieNullValue uint32, nullValue uint32, filter UcpMapValueFilter) uint32 {
	if value == trieNullValue {
		value = nullValue
	} else if filter != nil {
		value = filter(value)
	}
	return value
}
