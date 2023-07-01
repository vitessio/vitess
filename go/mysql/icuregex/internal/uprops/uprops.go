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

func readData(bytes *udata.Bytes) error {
	const (
		IX_VALUE_MAPS_OFFSET  = 0
		IX_BYTE_TRIES_OFFSET  = 1
		IX_NAME_GROUPS_OFFSET = 2
		IX_RESERVED3_OFFSET   = 3
	)

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

	offset := indexes[IX_VALUE_MAPS_OFFSET]
	nextOffset := indexes[IX_BYTE_TRIES_OFFSET]
	numInts := (nextOffset - offset) / 4

	pnames.valueMaps = bytes.Uint32Slice(numInts)

	offset = nextOffset
	nextOffset = indexes[IX_NAME_GROUPS_OFFSET]
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

func (prop Property) Source() PropertySource {
	if prop < UCHAR_BINARY_START {
		return UPROPS_SRC_NONE /* undefined */
	} else if prop < UCHAR_BINARY_LIMIT {
		bprop := binProps[prop]
		if bprop.mask != 0 {
			return UPROPS_SRC_PROPSVEC
		} else {
			return bprop.column
		}
	} else if prop < UCHAR_INT_START {
		return UPROPS_SRC_NONE /* undefined */
	} else if prop < UCHAR_INT_LIMIT {
		iprop := intProps[prop-UCHAR_INT_START]
		if iprop.mask != 0 {
			return UPROPS_SRC_PROPSVEC
		} else {
			return iprop.column
		}
	} else if prop < UCHAR_STRING_START {
		switch prop {
		case UCHAR_GENERAL_CATEGORY_MASK,
			UCHAR_NUMERIC_VALUE:
			return UPROPS_SRC_CHAR

		default:
			return UPROPS_SRC_NONE
		}
	} else if prop < UCHAR_STRING_LIMIT {
		switch prop {
		case UCHAR_AGE:
			return UPROPS_SRC_PROPSVEC

		case UCHAR_BIDI_MIRRORING_GLYPH:
			return UPROPS_SRC_BIDI

		case UCHAR_CASE_FOLDING,
			UCHAR_LOWERCASE_MAPPING,
			UCHAR_SIMPLE_CASE_FOLDING,
			UCHAR_SIMPLE_LOWERCASE_MAPPING,
			UCHAR_SIMPLE_TITLECASE_MAPPING,
			UCHAR_SIMPLE_UPPERCASE_MAPPING,
			UCHAR_TITLECASE_MAPPING,
			UCHAR_UPPERCASE_MAPPING:
			return UPROPS_SRC_CASE

		/* UCHAR_ISO_COMMENT, UCHAR_UNICODE_1_NAME (deprecated) */
		case UCHAR_NAME:
			return UPROPS_SRC_NAMES

		default:
			return UPROPS_SRC_NONE
		}
	} else {
		switch prop {
		case UCHAR_SCRIPT_EXTENSIONS:
			return UPROPS_SRC_PROPSVEC
		default:
			return UPROPS_SRC_NONE /* undefined */
		}
	}
}

func GetPropertyEnum(alias string) Property {
	return Property(getPropertyOrValueEnum(0, alias))
}

func GetPropertyValueEnum(prop Property, alias string) int32 {
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

func ComparePropertyNames(name1, name2 string) int {
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

func GetIntPropertyValue(c rune, which Property) int32 {
	if which < UCHAR_INT_START {
		if UCHAR_BINARY_START <= which && which < UCHAR_BINARY_LIMIT {
			prop := binProps[which]
			if prop.contains == nil {
				return 0
			}
			if prop.contains(prop, c, which) {
				return 1
			}
			return 0
		}
	} else if which < UCHAR_INT_LIMIT {
		iprop := intProps[which-UCHAR_INT_START]
		return iprop.getValue(iprop, c, which)
	} else if which == UCHAR_GENERAL_CATEGORY_MASK {
		return int32(U_MASK(uchar.CharType(c)))
	}
	return 0 // undefined
}

const (
	UPROPS_SCRIPT_X_MASK  = 0x00f000ff
	UPROPS_SCRIPT_X_SHIFT = 22

	UPROPS_SCRIPT_HIGH_MASK  = 0x00300000
	UPROPS_SCRIPT_HIGH_SHIFT = 12
	UPROPS_MAX_SCRIPT        = 0x3ff

	UPROPS_SCRIPT_LOW_MASK = 0x000000ff

	UPROPS_SCRIPT_X_WITH_COMMON    = 0x400000
	UPROPS_SCRIPT_X_WITH_INHERITED = 0x800000
	UPROPS_SCRIPT_X_WITH_OTHER     = 0xc00000
)

func mergeScriptCodeOrIndex(scriptX uint32) uint32 {
	return ((scriptX & UPROPS_SCRIPT_HIGH_MASK) >> UPROPS_SCRIPT_HIGH_SHIFT) |
		(scriptX & UPROPS_SCRIPT_LOW_MASK)
}

func GetScript(c rune) int32 {
	if c > 0x10ffff {
		return -1
	}
	scriptX := uchar.GetUnicodeProperties(c, 0) & UPROPS_SCRIPT_X_MASK
	codeOrIndex := mergeScriptCodeOrIndex(scriptX)

	if scriptX < UPROPS_SCRIPT_X_WITH_COMMON {
		return int32(codeOrIndex)
	} else if scriptX < UPROPS_SCRIPT_X_WITH_INHERITED {
		return 0
	} else if scriptX < UPROPS_SCRIPT_X_WITH_OTHER {
		return 1
	} else {
		return int32(uchar.ScriptExtension(codeOrIndex))
	}
}
