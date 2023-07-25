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
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
)

var charNamesOnce sync.Once
var charNames *unames

type unames struct {
	tokens       []uint16
	tokenStrings []uint8
	groups       []uint16
	groupNames   []uint8
	algNames     []algorithmicRange
}

func loadCharNames() {
	charNamesOnce.Do(func() {
		b := udata.NewBytes(icudata.UNames)
		if err := b.ReadHeader(func(info *udata.DataInfo) bool {
			return info.Size >= 20 &&
				info.IsBigEndian == 0 &&
				info.CharsetFamily == 0 &&
				info.DataFormat[0] == 0x75 && /* dataFormat="unam" */
				info.DataFormat[1] == 0x6e &&
				info.DataFormat[2] == 0x61 &&
				info.DataFormat[3] == 0x6d &&
				info.FormatVersion[0] == 1
		}); err != nil {
			panic(err)
		}

		tokenStringOffset := int32(b.Uint32() - 16)
		groupsOffset := int32(b.Uint32() - 16)
		groupStringOffset := int32(b.Uint32() - 16)
		algNamesOffset := int32(b.Uint32() - 16)
		charNames = &unames{
			tokens:       b.Uint16Slice(tokenStringOffset / 2),
			tokenStrings: b.Uint8Slice(groupsOffset - tokenStringOffset),
			groups:       b.Uint16Slice((groupStringOffset - groupsOffset) / 2),
			groupNames:   b.Uint8Slice(algNamesOffset - groupStringOffset),
		}

		algCount := b.Uint32()
		charNames.algNames = make([]algorithmicRange, 0, algCount)

		for i := uint32(0); i < algCount; i++ {
			ar := algorithmicRange{
				start:   b.Uint32(),
				end:     b.Uint32(),
				typ:     b.Uint8(),
				variant: b.Uint8(),
			}
			size := b.Uint16()
			switch ar.typ {
			case 0:
				ar.s = b.Uint8Slice(int32(size) - 12)
			case 1:
				ar.factors = b.Uint16Slice(int32(ar.variant))
				ar.s = b.Uint8Slice(int32(size) - 12 - int32(ar.variant)*2)
			}
			charNames.algNames = append(charNames.algNames, ar)
		}
	})
}

func (names *unames) getGroupName(group []uint16) []uint8 {
	return names.groupNames[names.getGroupOffset(group):]
}

type NameChoice int32

const (
	UnicodeCharName NameChoice = iota
	/**
	 * The Unicode_1_Name property value which is of little practical value.
	 * Beginning with ICU 49, ICU APIs return an empty string for this name choice.
	 * @deprecated ICU 49
	 */
	Unicode10CharName
	/** Standard or synthetic character name. @stable ICU 2.0 */
	ExtendedCharName
	/** Corrected name from NameAliases.txt. @stable ICU 4.4 */
	CharNameAlias
)

type algorithmicRange struct {
	start, end   uint32
	typ, variant uint8
	factors      []uint16
	s            []uint8
}

func (ar *algorithmicRange) findAlgName(otherName string) rune {
	switch ar.typ {
	case 0:
		s := ar.s

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
		factors := ar.factors
		s := ar.s

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

		ar.writeFactorSuffix0(factors, s, &buf, &elements, &elementBases)
		if buf.String() == otherName {
			return start
		}

		for start+1 < limit {
			start++
			i := len(factors)

			for {
				i--
				idx := indexes[i] + 1
				if idx < factors[i] {
					indexes[i] = idx
					s = elements[i]
					s = s[bytes.IndexByte(s, 0)+1:]
					elements[i] = s
					break
				}

				indexes[i] = 0
				elements[i] = elementBases[i]
			}

			t := otherName
			for i = 0; i < len(factors); i++ {
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

func (ar *algorithmicRange) writeFactorSuffix0(factors []uint16, s []uint8, buf *strings.Builder, elements, elementBases *[8][]byte) {
	/* write each element */
	for i := 0; i < len(factors); i++ {
		(*elements)[i] = s
		(*elementBases)[i] = s

		nul := bytes.IndexByte(s, 0)
		buf.Write(s[:nul])
		s = s[nul+1:]

		factor := int(factors[i] - 1)
		for factor > 0 {
			s = s[bytes.IndexByte(s, 0)+1:]
			factor--
		}
	}
}

func CharForName(nameChoice NameChoice, name string) rune {
	loadCharNames()

	lower := strings.ToLower(name)
	upper := strings.ToUpper(name)

	if lower[0] == '<' {
		if nameChoice == ExtendedCharName && lower[len(lower)-1] == '>' {
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

	for _, ar := range charNames.algNames {
		if cp := ar.findAlgName(upper); cp != -1 {
			return cp
		}
	}

	return charNames.enumNames(0, 0x10ffff+1, upper, nameChoice)
}

const groupShift = 5
const linesPerGroup = 1 << groupShift
const groupMask = linesPerGroup - 1

const (
	groupMsb = iota
	groupOffsetHigh
	groupOffsetLow
	groupLength
)

func (names *unames) enumNames(start, limit rune, otherName string, nameChoice NameChoice) rune {
	startGroupMSB := uint16(start >> groupShift)
	endGroupMSB := uint16((limit - 1) >> groupShift)

	group := names.getGroup(start)

	if startGroupMSB < group[groupMsb] && nameChoice == ExtendedCharName {
		extLimit := rune(group[groupMsb]) << groupShift
		if extLimit > limit {
			extLimit = limit
		}
		start = extLimit
	}

	if startGroupMSB == endGroupMSB {
		if startGroupMSB == group[groupMsb] {
			return names.enumGroupNames(group, start, limit-1, otherName, nameChoice)
		}
	} else {
		if startGroupMSB == group[groupMsb] {
			if start&groupMask != 0 {
				if cp := names.enumGroupNames(group, start, (rune(startGroupMSB)<<groupShift)+linesPerGroup-1, otherName, nameChoice); cp != -1 {
					return cp
				}
				group = group[groupLength:]
			}
		} else if startGroupMSB > group[groupMsb] {
			group = group[groupLength:]
		}

		for len(group) > 0 && group[groupMsb] < endGroupMSB {
			start = rune(group[groupMsb]) << groupShift
			if cp := names.enumGroupNames(group, start, start+linesPerGroup-1, otherName, nameChoice); cp != -1 {
				return cp
			}
			group = group[groupLength:]
		}

		if len(group) > 0 && group[groupMsb] == endGroupMSB {
			return names.enumGroupNames(group, (limit-1)&^groupMask, limit-1, otherName, nameChoice)
		}
	}

	return -1
}

func (names *unames) getGroup(code rune) []uint16 {
	groups := names.groups
	groupMSB := uint16(code >> groupShift)

	start := 0
	groupCount := int(groups[0])
	limit := groupCount
	groups = groups[1:]

	for start < limit-1 {
		number := (start + limit) / 2
		if groupMSB < groups[number*groupLength+groupMsb] {
			limit = number
		} else {
			start = number
		}
	}

	return groups[start*groupLength : (groupCount-start)*groupLength]
}

func (names *unames) getGroupOffset(group []uint16) uint32 {
	return (uint32(group[groupOffsetHigh]) << 16) | uint32(group[groupOffsetLow])
}

func (names *unames) enumGroupNames(group []uint16, start, end rune, otherName string, choice NameChoice) rune {
	var offsets [linesPerGroup + 2]uint16
	var lengths [linesPerGroup + 2]uint16

	s := names.getGroupName(group)
	s = expandGroupLengths(s, offsets[:0], lengths[:0])

	for start < end {
		name := s[offsets[start&groupMask]:]
		nameLen := lengths[start&groupMask]
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
	for i < linesPerGroup {
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

func (names *unames) compareName(name []byte, choice NameChoice, otherName string) bool {
	tokens := names.tokens

	tokenCount := tokens[0]
	tokens = tokens[1:]

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
					if len(otherName) == otherNameLen && choice == ExtendedCharName {
						if ';' >= tokenCount || int16(tokens[';']) == -1 {
							continue
						}
					}
					break
				}
			} else {
				tokenString := names.tokenStrings[token:]
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
