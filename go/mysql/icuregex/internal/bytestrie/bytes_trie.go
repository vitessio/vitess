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

package bytestrie

type BytesTrie struct {
	pos                  []byte
	original             []byte
	remainingMatchLength int32
}

func New(pos []byte) BytesTrie {
	return BytesTrie{pos: pos, original: pos, remainingMatchLength: -1}
}

type result int32

const ( /**
	 * The input unit(s) did not continue a matching string.
	 * Once current()/next() return NO_MATCH,
	 * all further calls to current()/next() will also return NO_MATCH,
	 * until the trie is reset to its original state or to a saved state.
	 * @stable ICU 4.8
	 */
	noMatch result = iota
	/**
	 * The input unit(s) continued a matching string
	 * but there is no value for the string so far.
	 * (It is a prefix of a longer string.)
	 * @stable ICU 4.8
	 */
	noValue
	/**
	 * The input unit(s) continued a matching string
	 * and there is a value for the string so far.
	 * This value will be returned by getValue().
	 * No further input byte/unit can continue a matching string.
	 * @stable ICU 4.8
	 */
	finalValue
	/**
	 * The input unit(s) continued a matching string
	 * and there is a value for the string so far.
	 * This value will be returned by getValue().
	 * Another input byte/unit can continue a matching string.
	 * @stable ICU 4.8
	 */
	intermediateValue
)

const (
	maxBranchLinearSubNodeLength = 5

	// 10..1f: Linear-match node, match 1..16 bytes and continue reading the next node.
	minLinearMatch       = 0x10
	maxLinearMatchLength = 0x10

	// 20..ff: Variable-length value node.
	// If odd, the value is final. (Otherwise, intermediate value or jump delta.)
	// Then shift-right by 1 bit.
	// The remaining lead byte value indicates the number of following bytes (0..4)
	// and contains the value's top bits.
	minValueLead = minLinearMatch + maxLinearMatchLength // 0x20
	// It is a final value if bit 0 is set.
	valueIsFinal = 1

	// Compact value: After testing bit 0, shift right by 1 and then use the following thresholds.
	minOneByteValueLead = minValueLead / 2 // 0x10
	maxOneByteValue     = 0x40             // At least 6 bits in the first byte.

	minTwoByteValueLead   = minOneByteValueLead + maxOneByteValue + 1 // 0x51
	maxTwoByteValue       = 0x1aff
	minThreeByteValueLead = minTwoByteValueLead + (maxTwoByteValue >> 8) + 1 // 0x6c
	fourByteValueLead     = 0x7e

	// Compact delta integers.
	maxOneByteDelta       = 0xbf
	minTwoByteDeltaLead   = maxOneByteDelta + 1 // 0xc0
	minThreeByteDeltaLead = 0xf0
	fourByteDeltaLead     = 0xfe
)

func (bt *BytesTrie) ContainsName(name string) bool {
	result := noValue
	for _, c := range []byte(name) {
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c == 0x2d || c == 0x5f || c == 0x20 || (0x09 <= c && c <= 0x0d) {
			continue
		}
		if result&1 == 0 {
			return false
		}
		result = bt.next(int32(c))
	}
	return result >= finalValue
}

func (bt *BytesTrie) next(inByte int32) result {
	pos := bt.pos
	if pos == nil {
		return noMatch
	}
	if inByte < 0 {
		inByte += 0x100
	}
	length := bt.remainingMatchLength // Actual remaining match length minus 1.
	if length >= 0 {
		match := inByte == int32(pos[0])
		pos = pos[1:]
		// Remaining part of a linear-match node.
		if match {
			length = length - 1
			bt.remainingMatchLength = length
			bt.pos = pos
			if length < 0 {
				node := int32(pos[0])
				if node >= minValueLead {
					return bt.valueResult(node)
				}
			}
			return noValue
		}
		bt.stop()
		return noMatch
	}
	return bt.nextImpl(pos, inByte)
}

func (bt *BytesTrie) nextImpl(pos []byte, inByte int32) result {
	for {
		node := int32(pos[0])
		pos = pos[1:]
		if node < minLinearMatch {
			return bt.branchNext(pos, node, inByte)
		} else if node < minValueLead {
			// Match the first of length+1 bytes.
			length := node - minLinearMatch // Actual match length minus 1.
			match := inByte == int32(pos[0])
			pos = pos[1:]
			if match {
				length = length - 1
				bt.remainingMatchLength = length
				bt.pos = pos
				if length < 0 {
					node = int32(pos[0])
					if node >= minValueLead {
						return bt.valueResult(node)
					}
				}
				return noValue
			}
			// No match.
			break
		} else if (node & valueIsFinal) != 0 {
			// No further matching bytes.
			break
		} else {
			// Skip intermediate value.
			pos = bt.skipValue2(pos, node)
			// The next node must not also be a value node.
		}
	}
	bt.stop()
	return noMatch
}

func (bt *BytesTrie) stop() {
	bt.pos = nil
}

func (bt *BytesTrie) valueResult(node int32) result {
	return intermediateValue - result(node&valueIsFinal)
}

func (bt *BytesTrie) branchNext(pos []byte, length int32, inByte int32) result {
	// Branch according to the current unit.
	if length == 0 {
		length = int32(pos[0])
		pos = pos[1:]
	}
	length++
	// The length of the branch is the number of units to select from.
	// The data structure encodes a binary search.
	for length > maxBranchLinearSubNodeLength {
		p := int32(pos[0])
		pos = pos[1:]
		if inByte < p {
			length >>= 1
			pos = bt.jumpByDelta(pos)
		} else {
			length = length - (length >> 1)
			pos = bt.skipDelta(pos)
		}
	}
	// Drop down to linear search for the last few bytes.
	// length>=2 because the loop body above sees length>kMaxBranchLinearSubNodeLength>=3
	// and divides length by 2.
	for {
		p := int32(pos[0])
		pos = pos[1:]
		if inByte == p {
			var result result
			node := int32(pos[0])
			if (node & valueIsFinal) != 0 {
				// Leave the final value for getValue() to read.
				result = finalValue
			} else {
				// Use the non-final value as the jump delta.
				pos = pos[1:]
				// int32_t delta=readValue(pos, node>>1);
				node >>= 1
				var delta int32
				if node < minTwoByteValueLead {
					delta = node - minOneByteValueLead
				} else if node < minThreeByteValueLead {
					delta = ((node - minTwoByteValueLead) << 8) | int32(pos[0])
					pos = pos[1:]
				} else if node < fourByteValueLead {
					delta = ((node - minThreeByteValueLead) << 16) | (int32(pos[0]) << 8) | int32(pos[1])
					pos = pos[2:]
				} else if node == fourByteValueLead {
					delta = (int32(pos[0]) << 16) | (int32(pos[1]) << 8) | int32(pos[2])
					pos = pos[3:]
				} else {
					delta = (int32(pos[0]) << 24) | (int32(pos[1]) << 16) | (int32(pos[2]) << 8) | int32(pos[3])
					pos = pos[4:]
				}
				// end readValue()
				pos = pos[delta:]
				node = int32(pos[0])
				if node >= minValueLead {
					result = bt.valueResult(node)
				} else {
					result = noValue
				}
			}
			bt.pos = pos
			return result
		}
		length--
		pos = bt.skipValue1(pos)
		if length <= 1 {
			break
		}
	}
	p := int32(pos[0])
	pos = pos[1:]
	if inByte == p {
		bt.pos = pos
		node := int32(pos[0])
		if node >= minValueLead {
			return bt.valueResult(node)
		}
		return noValue
	}
	bt.stop()
	return noMatch
}

func (bt *BytesTrie) skipValue1(pos []byte) []byte {
	leadByte := int32(pos[0])
	return bt.skipValue2(pos[1:], leadByte)
}

func (bt *BytesTrie) skipValue2(pos []byte, leadByte int32) []byte {
	if leadByte >= (minTwoByteValueLead << 1) {
		if leadByte < (minThreeByteValueLead << 1) {
			pos = pos[1:]
		} else if leadByte < (fourByteValueLead << 1) {
			pos = pos[2:]
		} else {
			pos = pos[3+((leadByte>>1)&1):]
		}
	}
	return pos
}

func (bt *BytesTrie) skipDelta(pos []byte) []byte {
	delta := int32(pos[0])
	pos = pos[1:]
	if delta >= minTwoByteDeltaLead {
		if delta < minThreeByteDeltaLead {
			pos = pos[1:]
		} else if delta < fourByteDeltaLead {
			pos = pos[2:]
		} else {
			pos = pos[3+(delta&1):]
		}
	}
	return pos
}

func (bt *BytesTrie) jumpByDelta(pos []byte) []byte {
	delta := int32(pos[0])
	pos = pos[1:]
	if delta < minTwoByteDeltaLead {
		// nothing to do
	} else if delta < minThreeByteDeltaLead {
		delta = ((delta - minTwoByteDeltaLead) << 8) | int32(pos[0])
		pos = pos[1:]
	} else if delta < fourByteDeltaLead {
		delta = ((delta - minThreeByteDeltaLead) << 16) | (int32(pos[0]) << 8) | int32(pos[1])
		pos = pos[2:]
	} else if delta == fourByteDeltaLead {
		delta = (int32(pos[0]) << 16) | (int32(pos[1]) << 8) | int32(pos[2])
		pos = pos[3:]
	} else {
		delta = (int32(pos[0]) << 24) | (int32(pos[1]) << 16) | (int32(pos[2]) << 8) | int32(pos[3])
		pos = pos[4:]
	}
	return pos[delta:]
}

func (bt *BytesTrie) GetValue() int32 {
	pos := bt.pos
	leadByte := int32(pos[0])
	return bt.readValue(pos[1:], leadByte>>1)
}

func (bt *BytesTrie) readValue(pos []byte, leadByte int32) int32 {
	var value int32
	if leadByte < minTwoByteValueLead {
		value = leadByte - minOneByteValueLead
	} else if leadByte < minThreeByteValueLead {
		value = ((leadByte - minTwoByteValueLead) << 8) | int32(pos[0])
	} else if leadByte < fourByteValueLead {
		value = ((leadByte - minThreeByteValueLead) << 16) | (int32(pos[0]) << 8) | int32(pos[1])
	} else if leadByte == fourByteValueLead {
		value = (int32(pos[0]) << 16) | (int32(pos[1]) << 8) | int32(pos[2])
	} else {
		value = (int32(pos[0]) << 24) | (int32(pos[1]) << 16) | (int32(pos[2]) << 8) | int32(pos[3])
	}
	return value
}
