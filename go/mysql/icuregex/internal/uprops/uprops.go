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

package uprops

import (
	"fmt"

	"vitess.io/vitess/go/mysql/icuregex/internal/bytestrie"
	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/uchar"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
)

var pnames struct {
	valueMaps []uint32
	byteTrie  []uint8
}

const (
	ixValueMapsOffset  = 0
	ixByteTriesOffset  = 1
	ixNameGroupsOffset = 2
	ixReserved3Offset  = 3
)

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.DataFormat[0] == 0x70 &&
			info.DataFormat[1] == 0x6e &&
			info.DataFormat[2] == 0x61 &&
			info.DataFormat[3] == 0x6d &&
			info.FormatVersion[0] == 2
	})
	if err != nil {
		return err
	}

	count := bytes.Int32() / 4
	if count < 8 {
		return fmt.Errorf("indexes[0] too small in ucase.icu")
	}

	indexes := make([]int32, count)
	indexes[0] = count * 4

	for i := int32(1); i < count; i++ {
		indexes[i] = bytes.Int32()
	}

	offset := indexes[ixValueMapsOffset]
	nextOffset := indexes[ixByteTriesOffset]
	numInts := (nextOffset - offset) / 4

	pnames.valueMaps = bytes.Uint32Slice(numInts)

	offset = nextOffset
	nextOffset = indexes[ixNameGroupsOffset]
	numBytes := nextOffset - offset

	pnames.byteTrie = bytes.Uint8Slice(numBytes)
	return nil
}

func init() {
	b := udata.NewBytes(icudata.PNames)
	if err := readData(b); err != nil {
		panic(err)
	}
}

func (prop Property) source() propertySource {
	if prop < UCharBinaryStart {
		return srcNone /* undefined */
	} else if prop < uCharBinaryLimit {
		bprop := binProps[prop]
		if bprop.mask != 0 {
			return srcPropsvec
		}
		return bprop.column
	} else if prop < UCharIntStart {
		return srcNone /* undefined */
	} else if prop < uCharIntLimit {
		iprop := intProps[prop-UCharIntStart]
		if iprop.mask != 0 {
			return srcPropsvec
		}
		return iprop.column
	} else if prop < UCharStringStart {
		switch prop {
		case UCharGeneralCategoryMask,
			UCharNumericValue:
			return srcChar

		default:
			return srcNone
		}
	} else if prop < uCharStringLimit {
		switch prop {
		case UCharAge:
			return srcPropsvec

		case UCharBidiMirroringGlyph:
			return srcBidi

		case UCharCaseFolding,
			UCharLowercaseMapping,
			UCharSimpleCaseFolding,
			UCharSimpleLowercaseMapping,
			UcharSimpleTitlecaseMapping,
			UCharSimpleUppercaseMapping,
			UCharTitlecaseMapping,
			UCharUppercaseMapping:
			return srcCase

		/* UCHAR_ISO_COMMENT, UCHAR_UNICODE_1_NAME (deprecated) */
		case UCharName:
			return srcNames

		default:
			return srcNone
		}
	} else {
		switch prop {
		case UCharScriptExtensions:
			return srcPropsvec
		default:
			return srcNone /* undefined */
		}
	}
}

func getPropertyEnum(alias string) Property {
	return Property(getPropertyOrValueEnum(0, alias))
}

func getPropertyValueEnum(prop Property, alias string) int32 {
	valueMapIdx := findProperty(prop)
	if valueMapIdx == 0 {
		return -1
	}

	valueMapIdx = int32(pnames.valueMaps[valueMapIdx+1])
	if valueMapIdx == 0 {
		return -1
	}
	// valueMapIndex is the start of the property's valueMap,
	// where the first word is the BytesTrie offset.
	return getPropertyOrValueEnum(int32(pnames.valueMaps[valueMapIdx]), alias)
}

func findProperty(prop Property) int32 {
	var i = int32(1)
	for numRanges := int32(pnames.valueMaps[0]); numRanges > 0; numRanges-- {
		start := int32(pnames.valueMaps[i])
		limit := int32(pnames.valueMaps[i+1])
		i += 2
		if int32(prop) < start {
			break
		}
		if int32(prop) < limit {
			return i + (int32(prop)-start)*2
		}
		i += (limit - start) * 2
	}
	return 0
}

func getPropertyOrValueEnum(offset int32, alias string) int32 {
	trie := bytestrie.New(pnames.byteTrie[offset:])
	if trie.ContainsName(alias) {
		return trie.GetValue()
	}
	return -1
}

func comparePropertyNames(name1, name2 string) int {
	next := func(s string) (byte, string) {
		for len(s) > 0 && (s[0] == 0x2d || s[0] == 0x5f || s[0] == 0x20 || (0x09 <= s[0] && s[0] <= 0x0d)) {
			s = s[1:]
		}
		if len(s) == 0 {
			return 0, ""
		}
		c := s[0]
		s = s[1:]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		return c, s
	}

	var r1, r2 byte
	for {
		r1, name1 = next(name1)
		r2, name2 = next(name2)

		if r1 == 0 && r2 == 0 {
			return 0
		}

		/* Compare the lowercased characters */
		if r1 != r2 {
			return int(r1) - int(r2)
		}
	}
}

func getIntPropertyValue(c rune, which Property) int32 {
	if which < UCharIntStart {
		if UCharBinaryStart <= which && which < uCharBinaryLimit {
			prop := binProps[which]
			if prop.contains == nil {
				return 0
			}
			if prop.contains(prop, c, which) {
				return 1
			}
			return 0
		}
	} else if which < uCharIntLimit {
		iprop := intProps[which-UCharIntStart]
		return iprop.getValue(iprop, c, which)
	} else if which == UCharGeneralCategoryMask {
		return int32(uMask(uchar.CharType(c)))
	}
	return 0 // undefined
}

func mergeScriptCodeOrIndex(scriptX uint32) uint32 {
	return ((scriptX & scriptHighMask) >> scriptHighShift) |
		(scriptX & scriptLowMask)
}

func script(c rune) int32 {
	if c > 0x10ffff {
		return -1
	}
	scriptX := uchar.GetUnicodeProperties(c, 0) & scriptXMask
	codeOrIndex := mergeScriptCodeOrIndex(scriptX)

	if scriptX < scriptXWithCommon {
		return int32(codeOrIndex)
	} else if scriptX < scriptXWithInherited {
		return 0
	} else if scriptX < scriptXWithOther {
		return 1
	} else {
		return int32(uchar.ScriptExtension(codeOrIndex))
	}
}
