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

type Result int32

const ( /**
	 * The input unit(s) did not continue a matching string.
	 * Once current()/next() return NO_MATCH,
	 * all further calls to current()/next() will also return NO_MATCH,
	 * until the trie is reset to its original state or to a saved state.
	 * @stable ICU 4.8
	 */
	NO_MATCH Result = iota
	/**
	 * The input unit(s) continued a matching string
	 * but there is no value for the string so far.
	 * (It is a prefix of a longer string.)
	 * @stable ICU 4.8
	 */
	NO_VALUE
	/**
	 * The input unit(s) continued a matching string
	 * and there is a value for the string so far.
	 * This value will be returned by getValue().
	 * No further input byte/unit can continue a matching string.
	 * @stable ICU 4.8
	 */
	FINAL_VALUE
	/**
	 * The input unit(s) continued a matching string
	 * and there is a value for the string so far.
	 * This value will be returned by getValue().
	 * Another input byte/unit can continue a matching string.
	 * @stable ICU 4.8
	 */
	INTERMEDIATE_VALUE
)

const (
	kMaxBranchLinearSubNodeLength = 5

	// 10..1f: Linear-match node, match 1..16 bytes and continue reading the next node.
	kMinLinearMatch       = 0x10
	kMaxLinearMatchLength = 0x10

	// 20..ff: Variable-length value node.
	// If odd, the value is final. (Otherwise, intermediate value or jump delta.)
	// Then shift-right by 1 bit.
	// The remaining lead byte value indicates the number of following bytes (0..4)
	// and contains the value's top bits.
	kMinValueLead = kMinLinearMatch + kMaxLinearMatchLength // 0x20
	// It is a final value if bit 0 is set.
	kValueIsFinal = 1

	// Compact value: After testing bit 0, shift right by 1 and then use the following thresholds.
	kMinOneByteValueLead = kMinValueLead / 2 // 0x10
	kMaxOneByteValue     = 0x40              // At least 6 bits in the first byte.

	kMinTwoByteValueLead   = kMinOneByteValueLead + kMaxOneByteValue + 1 // 0x51
	kMaxTwoByteValue       = 0x1aff
	kMinThreeByteValueLead = kMinTwoByteValueLead + (kMaxTwoByteValue >> 8) + 1 // 0x6c
	kFourByteValueLead     = 0x7e

	// Compact delta integers.
	kMaxOneByteDelta       = 0xbf
	kMinTwoByteDeltaLead   = kMaxOneByteDelta + 1 // 0xc0
	kMinThreeByteDeltaLead = 0xf0
	kFourByteDeltaLead     = 0xfe
)

func (bt *BytesTrie) ContainsName(name string) bool {
	result := NO_VALUE
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
	return result >= FINAL_VALUE
}

func (bt *BytesTrie) next(inByte int32) Result {
	pos := bt.pos
	if pos == nil {
		return NO_MATCH
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
				if node >= kMinValueLead {
					return bt.valueResult(node)
				}
			}
			return NO_VALUE
		} else {
			bt.stop()
			return NO_MATCH
		}
	}
	return bt.nextImpl(pos, inByte)
}

func (bt *BytesTrie) nextImpl(pos []byte, inByte int32) Result {
	for {
		node := int32(pos[0])
		pos = pos[1:]
		if node < kMinLinearMatch {
			return bt.branchNext(pos, node, inByte)
		} else if node < kMinValueLead {
			// Match the first of length+1 bytes.
			length := node - kMinLinearMatch // Actual match length minus 1.
			match := inByte == int32(pos[0])
			pos = pos[1:]
			if match {
				length = length - 1
				bt.remainingMatchLength = length
				bt.pos = pos
				if length < 0 {
					node = int32(pos[0])
					if node >= kMinValueLead {
						return bt.valueResult(node)
					}
				}
				return NO_VALUE
			} else {
				// No match.
				break
			}
		} else if (node & kValueIsFinal) != 0 {
			// No further matching bytes.
			break
		} else {
			// Skip intermediate value.
			pos = bt.skipValue2(pos, node)
			// The next node must not also be a value node.
			// U_ASSERT(*pos<kMinValueLead);
		}
	}
	bt.stop()
	return NO_MATCH
}

func (bt *BytesTrie) stop() {
	bt.pos = nil
}

func (bt *BytesTrie) valueResult(node int32) Result {
	return INTERMEDIATE_VALUE - Result(node&kValueIsFinal)
}

func (bt *BytesTrie) branchNext(pos []byte, length int32, inByte int32) Result {
	// Branch according to the current unit.
	if length == 0 {
		length = int32(pos[0])
		pos = pos[1:]
	}
	length++
	// The length of the branch is the number of units to select from.
	// The data structure encodes a binary search.
	for length > kMaxBranchLinearSubNodeLength {
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
			var result Result
			node := int32(pos[0])
			// U_ASSERT(node>=kMinValueLead);
			if (node & kValueIsFinal) != 0 {
				// Leave the final value for getValue() to read.
				result = FINAL_VALUE
			} else {
				// Use the non-final value as the jump delta.
				pos = pos[1:]
				// int32_t delta=readValue(pos, node>>1);
				node >>= 1
				var delta int32
				if node < kMinTwoByteValueLead {
					delta = node - kMinOneByteValueLead
				} else if node < kMinThreeByteValueLead {
					delta = ((node - kMinTwoByteValueLead) << 8) | int32(pos[0])
					pos = pos[1:]
				} else if node < kFourByteValueLead {
					delta = ((node - kMinThreeByteValueLead) << 16) | (int32(pos[0]) << 8) | int32(pos[1])
					pos = pos[2:]
				} else if node == kFourByteValueLead {
					delta = (int32(pos[0]) << 16) | (int32(pos[1]) << 8) | int32(pos[2])
					pos = pos[3:]
				} else {
					delta = (int32(pos[0]) << 24) | (int32(pos[1]) << 16) | (int32(pos[2]) << 8) | int32(pos[3])
					pos = pos[4:]
				}
				// end readValue()
				pos = pos[delta:]
				node = int32(pos[0])
				if node >= kMinValueLead {
					result = bt.valueResult(node)
				} else {
					result = NO_VALUE
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
		if node >= kMinValueLead {
			return bt.valueResult(node)
		}
		return NO_VALUE
	} else {
		bt.stop()
		return NO_MATCH
	}
}

func (bt *BytesTrie) skipValue1(pos []byte) []byte {
	leadByte := int32(pos[0])
	return bt.skipValue2(pos[1:], leadByte)
}

func (bt *BytesTrie) skipValue2(pos []byte, leadByte int32) []byte {
	if leadByte >= (kMinTwoByteValueLead << 1) {
		if leadByte < (kMinThreeByteValueLead << 1) {
			pos = pos[1:]
		} else if leadByte < (kFourByteValueLead << 1) {
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
	if delta >= kMinTwoByteDeltaLead {
		if delta < kMinThreeByteDeltaLead {
			pos = pos[1:]
		} else if delta < kFourByteDeltaLead {
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
	if delta < kMinTwoByteDeltaLead {
		// nothing to do
	} else if delta < kMinThreeByteDeltaLead {
		delta = ((delta - kMinTwoByteDeltaLead) << 8) | int32(pos[0])
		pos = pos[1:]
	} else if delta < kFourByteDeltaLead {
		delta = ((delta - kMinThreeByteDeltaLead) << 16) | (int32(pos[0]) << 8) | int32(pos[1])
		pos = pos[2:]
	} else if delta == kFourByteDeltaLead {
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
	if leadByte < kMinTwoByteValueLead {
		value = leadByte - kMinOneByteValueLead
	} else if leadByte < kMinThreeByteValueLead {
		value = ((leadByte - kMinTwoByteValueLead) << 8) | int32(pos[0])
	} else if leadByte < kFourByteValueLead {
		value = ((leadByte - kMinThreeByteValueLead) << 16) | (int32(pos[0]) << 8) | int32(pos[1])
	} else if leadByte == kFourByteValueLead {
		value = (int32(pos[0]) << 16) | (int32(pos[1]) << 8) | int32(pos[2])
	} else {
		value = (int32(pos[0]) << 24) | (int32(pos[1]) << 16) | (int32(pos[2]) << 8) | int32(pos[3])
	}
	return value
}
