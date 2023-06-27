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

package unames

import (
	"bytes"
	_ "embed"
	"math"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
)

var charNamesOnce sync.Once
var charNames *UCharNames

func loadCharNames() {
	validCharNames := func(info *udata.DataInfo) bool {
		return info.Size >= 20 &&
			info.IsBigEndian == 0 &&
			info.CharsetFamily == 0 &&
			info.DataFormat[0] == 0x75 && /* dataFormat="unam" */
			info.DataFormat[1] == 0x6e &&
			info.DataFormat[2] == 0x61 &&
			info.DataFormat[3] == 0x6d &&
			info.FormatVersion[0] == 1
	}

	charNamesOnce.Do(func() {
		b := udata.NewBytes(icudata.UNames)
		if err := b.ReadHeader(validCharNames); err != nil {
			panic(err)
		}
		charNames = (*UCharNames)(b.Pointer())
	})
}

type NameChoice int32

const (
	U_UNICODE_CHAR_NAME NameChoice = iota
	/**
	 * The Unicode_1_Name property value which is of little practical value.
	 * Beginning with ICU 49, ICU APIs return an empty string for this name choice.
	 * @deprecated ICU 49
	 */
	U_UNICODE_10_CHAR_NAME
	/** Standard or synthetic character name. @stable ICU 2.0 */
	U_EXTENDED_CHAR_NAME
	/** Corrected name from NameAliases.txt. @stable ICU 4.4 */
	U_CHAR_NAME_ALIAS
	/**
	 * One more than the highest normal UCharNameChoice value.
	 * @deprecated ICU 58 The numeric value may change over time, see ICU ticket #12420.
	 */
	U_CHAR_NAME_CHOICE_COUNT
)

type algorithmicRange struct {
	start, end     uint32
	type_, variant uint8
	size           uint16
}

func (ar *algorithmicRange) next() *algorithmicRange {
	return (*algorithmicRange)(unsafe.Add(unsafe.Pointer(ar), ar.size))
}

func (ar *algorithmicRange) ptrend(offset uintptr) unsafe.Pointer {
	return unsafe.Add(unsafe.Pointer(ar), unsafe.Sizeof(algorithmicRange{})+offset)
}

func (ar *algorithmicRange) slice8(offset uintptr) []uint8 {
	return unsafe.Slice((*uint8)(ar.ptrend(offset)), ar.size)
}

func (ar *algorithmicRange) slice16() []uint16 {
	return unsafe.Slice((*uint16)(ar.ptrend(0)), ar.size/2)
}

func (ar *algorithmicRange) findAlgName(choice NameChoice, otherName string) rune {
	switch ar.type_ {
	case 0:
		s := ar.slice8(0)

		for s[0] != 0 && len(otherName) > 0 {
			if s[0] != otherName[0] {
				return -1
			}
			s = s[1:]
			otherName = otherName[1:]
		}

		var code rune
		count := int(ar.variant)
		for i := 0; i < count && len(otherName) > 0; i++ {
			c := rune(otherName[0])
			otherName = otherName[1:]
			if '0' <= c && c <= '9' {
				code = (code << 4) | (c - '0')
			} else if 'A' <= c && c <= 'F' {
				code = (code << 4) | (c - 'A' + 10)
			} else {
				return -1
			}
		}

		if len(otherName) == 0 && ar.start <= uint32(code) && uint32(code) <= ar.end {
			return code
		}
	case 1:
		factors := ar.slice16()
		count := int(ar.variant)
		s := ar.slice8(2 * uintptr(count))

		for s[0] != 0 && len(otherName) > 0 {
			if s[0] != otherName[0] {
				return -1
			}
			s = s[1:]
			otherName = otherName[1:]
		}
		s = s[1:]

		start := rune(ar.start)
		limit := rune(ar.end + 1)

		var indexes [8]uint16
		var buf strings.Builder
		var elements [8][]byte
		var elementBases [8][]byte

		ar.writeFactorSuffix0(factors, count, s, &buf, &elements, &elementBases)
		if buf.String() == otherName {
			return start
		}

		for start+1 < limit {
			start++
			i := count

			for {
				i--
				idx := indexes[i] + 1
				if idx < factors[i] {
					indexes[i] = idx
					s = elements[i]
					s = s[bytes.IndexByte(s, 0)+1:]
					elements[i] = s
					break
				} else {
					indexes[i] = 0
					elements[i] = elementBases[i]
				}
			}

			t := otherName
			for i = 0; i < count; i++ {
				s = elements[i]

				for s[0] != 0 && len(t) > 0 {
					if s[0] != t[0] {
						s = nil
						i = 99
						break
					}
					s = s[1:]
					t = t[1:]
				}
			}
			if i < 99 && len(t) == 0 {
				return start
			}
		}
	}
	return -1
}

func (ar *algorithmicRange) writeFactorSuffix0(factors []uint16, count int, s []uint8, buf *strings.Builder, elements, elementBases *[8][]byte) {
	i := 0

	/* write each element */
	for {
		(*elements)[i] = s
		(*elementBases)[i] = s

		nul := bytes.IndexByte(s, 0)
		buf.Write(s[:nul])
		s = s[nul+1:]

		if i >= count {
			break
		}

		factor := int(factors[i] - 1)
		for factor > 0 {
			s = s[bytes.IndexByte(s, 0)+1:]
			factor--
		}

		i++
	}
}

func CharForName(nameChoice NameChoice, name string) rune {
	loadCharNames()

	lower := strings.ToLower(name)
	upper := strings.ToUpper(name)

	if lower[0] == '<' {
		if nameChoice == U_EXTENDED_CHAR_NAME && lower[len(lower)-1] == '>' {
			if limit := strings.LastIndexByte(lower, '-'); limit >= 2 {
				cp, err := strconv.ParseUint(lower[limit+1:len(lower)-1], 16, 32)
				if err != nil || cp > 0x10ffff {
					return -1
				}
				return rune(cp)
			}
		}
		return -1
	}

	p := charNames.ptr32(charNames.algNamesOffset)
	i := p[0]
	algRange := (*algorithmicRange)(unsafe.Pointer(unsafe.SliceData(p[1:])))
	for i > 0 {
		if cp := algRange.findAlgName(nameChoice, upper); cp != -1 {
			return cp
		}
		algRange = algRange.next()
		i--
	}

	return charNames.enumNames(0, 0x10ffff+1, upper, nameChoice)
}

type UCharNames struct {
	tokenStringOffset, groupsOffset, groupStringOffset, algNamesOffset uint32
}

const GROUP_SHIFT = 5
const LINES_PER_GROUP = 1 << GROUP_SHIFT
const GROUP_MASK = LINES_PER_GROUP - 1

const (
	GROUP_MSB = iota
	GROUP_OFFSET_HIGH
	GROUP_OFFSET_LOW
	GROUP_LENGTH
)

func (names *UCharNames) enumNames(start, limit rune, otherName string, nameChoice NameChoice) rune {
	startGroupMSB := uint16(start >> GROUP_SHIFT)
	endGroupMSB := uint16((limit - 1) >> GROUP_SHIFT)

	group := names.getGroup(start)

	if startGroupMSB < group[GROUP_MSB] && nameChoice == U_EXTENDED_CHAR_NAME {
		extLimit := rune(group[GROUP_MSB]) << GROUP_SHIFT
		if extLimit > limit {
			extLimit = limit
		}
		start = extLimit
	}

	if startGroupMSB == endGroupMSB {
		if startGroupMSB == group[GROUP_MSB] {
			return names.enumGroupNames(group, start, limit-1, otherName, nameChoice)
		}
	} else {
		if startGroupMSB == group[GROUP_MSB] {
			if start&GROUP_MASK != 0 {
				if cp := names.enumGroupNames(group, start, (rune(startGroupMSB)<<GROUP_SHIFT)+LINES_PER_GROUP-1, otherName, nameChoice); cp != -1 {
					return cp
				}
				group = group[GROUP_LENGTH:]
			}
		} else if startGroupMSB > group[GROUP_MSB] {
			group = group[GROUP_LENGTH:]
		}

		for len(group) > 0 && group[GROUP_MSB] < endGroupMSB {
			start = rune(group[GROUP_MSB]) << GROUP_SHIFT
			if cp := names.enumGroupNames(group, start, start+LINES_PER_GROUP-1, otherName, nameChoice); cp != -1 {
				return cp
			}
			group = group[GROUP_LENGTH:]
		}

		if len(group) > 0 && group[GROUP_MSB] == endGroupMSB {
			return names.enumGroupNames(group, (limit-1)&^GROUP_MASK, limit-1, otherName, nameChoice)
		}
	}

	return -1
}

func (names *UCharNames) ptr8(offset8 uint32) []byte {
	return unsafe.Slice((*uint8)(unsafe.Add(unsafe.Pointer(names), offset8)), math.MaxInt)
}

func (names *UCharNames) ptr16(offset8 uint32) []uint16 {
	return unsafe.Slice((*uint16)(unsafe.Add(unsafe.Pointer(names), offset8)), math.MaxInt/2)
}

func (names *UCharNames) ptr32(offset8 uint32) []uint32 {
	return unsafe.Slice((*uint32)(unsafe.Add(unsafe.Pointer(names), offset8)), math.MaxInt/4)
}

func (names *UCharNames) getGroup(code rune) []uint16 {
	groups := names.ptr16(names.groupsOffset)
	groupMSB := uint16(code >> GROUP_SHIFT)

	start := 0
	groupCount := int(groups[0])
	limit := groupCount
	groups = groups[1:]

	for start < limit-1 {
		number := (start + limit) / 2
		if groupMSB < groups[number*GROUP_LENGTH+GROUP_MSB] {
			limit = number
		} else {
			start = number
		}
	}

	return groups[start*GROUP_LENGTH : (groupCount-start)*GROUP_LENGTH]
}

func (names *UCharNames) getGroupOffset(group []uint16) uint32 {
	return (uint32(group[GROUP_OFFSET_HIGH]) << 16) | uint32(group[GROUP_OFFSET_LOW])
}

func (names *UCharNames) enumGroupNames(group []uint16, start, end rune, otherName string, choice NameChoice) rune {
	var offsets [LINES_PER_GROUP + 2]uint16
	var lengths [LINES_PER_GROUP + 2]uint16

	s := names.ptr8(names.groupStringOffset + names.getGroupOffset(group))
	s = expandGroupLengths(s, offsets[:0], lengths[:0])

	for start < end {
		name := s[offsets[start&GROUP_MASK]:]
		nameLen := lengths[start&GROUP_MASK]
		if names.compareName(name[:nameLen], choice, otherName) {
			return start
		}
		start++
	}
	return -1
}

func expandGroupLengths(s []uint8, offsets []uint16, lengths []uint16) []uint8 {
	/* read the lengths of the 32 strings in this group and get each string's offset */
	var i, offset, length uint16
	var lengthByte uint8

	/* all 32 lengths must be read to get the offset of the first group string */
	for i < LINES_PER_GROUP {
		lengthByte = s[0]
		s = s[1:]

		/* read even nibble - MSBs of lengthByte */
		if length >= 12 {
			/* double-nibble length spread across two bytes */
			length = ((length&0x3)<<4 | uint16(lengthByte)>>4) + 12
			lengthByte &= 0xf
		} else if (lengthByte /* &0xf0 */) >= 0xc0 {
			/* double-nibble length spread across this one byte */
			length = (uint16(lengthByte) & 0x3f) + 12
		} else {
			/* single-nibble length in MSBs */
			length = uint16(lengthByte) >> 4
			lengthByte &= 0xf
		}

		offsets = append(offsets, offset)
		lengths = append(lengths, length)

		offset += length
		i++

		/* read odd nibble - LSBs of lengthByte */
		if (lengthByte & 0xf0) == 0 {
			/* this nibble was not consumed for a double-nibble length above */
			length = uint16(lengthByte)
			if length < 12 {
				/* single-nibble length in LSBs */
				offsets = append(offsets, offset)
				lengths = append(lengths, length)

				offset += length
				i++
			}
		} else {
			length = 0 /* prevent double-nibble detection in the next iteration */
		}
	}

	/* now, s is at the first group string */
	return s
}

func (names *UCharNames) compareName(name []byte, choice NameChoice, otherName string) bool {
	tokens := names.ptr16(0)[8:]

	tokenCount := tokens[0]
	tokens = tokens[1:]

	tokenStrings := names.ptr8(names.tokenStringOffset)
	otherNameLen := len(otherName)

	for len(name) > 0 && len(otherName) > 0 {
		c := name[0]
		name = name[1:]

		if uint16(c) >= tokenCount {
			if c != ';' {
				if c != otherName[0] {
					return false
				}
				otherName = otherName[1:]
			} else {
				break
			}
		} else {
			token := tokens[c]
			if int16(token) == -2 {
				token = tokens[int(c)<<8|int(name[0])]
				name = name[1:]
			}
			if int16(token) == -1 {
				if c != ';' {
					if c != otherName[0] {
						return false
					}
					otherName = otherName[1:]
				} else {
					if len(otherName) == otherNameLen && choice == U_EXTENDED_CHAR_NAME {
						if ';' >= tokenCount || int16(tokens[';']) == -1 {
							continue
						}
					}
					break
				}
			} else {
				tokenString := tokenStrings[token:]
				for tokenString[0] != 0 && len(otherName) > 0 {
					if tokenString[0] != otherName[0] {
						return false
					}
					tokenString = tokenString[1:]
					otherName = otherName[1:]
				}
			}
		}
	}

	return len(otherName) == 0
}
