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

package uchar

import (
	"fmt"
	"strconv"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var uprops struct {
	trie             *utrie.UTrie2
	trie2            *utrie.UTrie2
	vectorsColumns   int32
	vectors          []uint32
	scriptExtensions []uint16
}

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.FormatVersion[0] == 7
	})
	if err != nil {
		return err
	}

	propertyOffset := bytes.Int32()
	/* exceptionOffset = */ bytes.Int32()
	/* caseOffset = */ bytes.Int32()
	additionalOffset := bytes.Int32()
	additionalVectorsOffset := bytes.Int32()
	uprops.vectorsColumns = bytes.Int32()
	scriptExtensionsOffset := bytes.Int32()
	reservedOffset7 := bytes.Int32()
	/* reservedOffset8 = */ bytes.Int32()
	/* dataTopOffset = */ bytes.Int32()
	_ = bytes.Int32()
	_ = bytes.Int32()
	bytes.Skip((16 - 12) << 2)

	uprops.trie, err = utrie.UTrie2FromBytes(bytes)
	if err != nil {
		return err
	}

	expectedTrieLength := (propertyOffset - 16) * 4
	trieLength := uprops.trie.SerializedLength()

	if trieLength > expectedTrieLength {
		return fmt.Errorf("ucase.icu: not enough bytes for the trie")
	}

	bytes.Skip(expectedTrieLength - trieLength)
	bytes.Skip((additionalOffset - propertyOffset) * 4)

	if uprops.vectorsColumns > 0 {
		uprops.trie2, err = utrie.UTrie2FromBytes(bytes)
		if err != nil {
			return err
		}

		expectedTrieLength = (additionalVectorsOffset - additionalOffset) * 4
		trieLength = uprops.trie2.SerializedLength()

		if trieLength > expectedTrieLength {
			return fmt.Errorf("ucase.icu: not enough bytes for the trie")
		}

		bytes.Skip(expectedTrieLength - trieLength)
		uprops.vectors = bytes.Uint32Slice(scriptExtensionsOffset - additionalVectorsOffset)
	}

	if n := (reservedOffset7 - scriptExtensionsOffset) * 2; n > 0 {
		uprops.scriptExtensions = bytes.Uint16Slice(n)
	}

	return nil
}

func init() {
	b := udata.NewBytes(icudata.UProps)
	if err := readData(b); err != nil {
		panic(err)
	}
}

type PropertySet interface {
	AddRune(ch rune)
}

func VecAddPropertyStarts(sa PropertySet) {
	uprops.trie2.Enum(nil, func(start, _ rune, _ uint32) bool {
		sa.AddRune(start)
		return true
	})
}

func AddPropertyStarts(sa PropertySet) {
	const (
		TAB      = 0x0009
		LF       = 0x000a
		FF       = 0x000c
		CR       = 0x000d
		NBSP     = 0x00a0
		CGJ      = 0x034f
		FIGURESP = 0x2007
		HAIRSP   = 0x200a
		ZWNJ     = 0x200c
		ZWJ      = 0x200d
		RLM      = 0x200f
		NNBSP    = 0x202f
		ZWNBSP   = 0xfef
	)

	/* add the start code point of each same-value range of the main trie */
	uprops.trie.Enum(nil, func(start, _ rune, _ uint32) bool {
		sa.AddRune(start)
		return true
	})

	/* add code points with hardcoded properties, plus the ones following them */

	/* add for u_isblank() */
	sa.AddRune(TAB)
	sa.AddRune(TAB + 1)

	/* add for IS_THAT_CONTROL_SPACE() */
	sa.AddRune(CR + 1) /* range TAB..CR */
	sa.AddRune(0x1c)
	sa.AddRune(0x1f + 1)
	sa.AddRune(0x85) // NEXT LINE (NEL)
	sa.AddRune(0x85 + 1)

	/* add for u_isIDIgnorable() what was not added above */
	sa.AddRune(0x7f) /* range DEL..NBSP-1, NBSP added below */
	sa.AddRune(HAIRSP)
	sa.AddRune(RLM + 1)
	sa.AddRune(0x206a)     // INHIBIT SYMMETRIC SWAPPING
	sa.AddRune(0x206f + 1) // NOMINAL DIGIT SHAPES
	sa.AddRune(ZWNBSP)
	sa.AddRune(ZWNBSP + 1)

	/* add no-break spaces for u_isWhitespace() what was not added above */
	sa.AddRune(NBSP)
	sa.AddRune(NBSP + 1)
	sa.AddRune(FIGURESP)
	sa.AddRune(FIGURESP + 1)
	sa.AddRune(NNBSP)
	sa.AddRune(NNBSP + 1)

	/* add for u_digit() */
	sa.AddRune('a')
	sa.AddRune('z' + 1)
	sa.AddRune('A')
	sa.AddRune('Z' + 1)
	// fullwidth
	sa.AddRune('ａ')
	sa.AddRune('ｚ' + 1)
	sa.AddRune('Ａ')
	sa.AddRune('Ｚ' + 1)

	/* add for u_isxdigit() */
	sa.AddRune('f' + 1)
	sa.AddRune('F' + 1)
	// fullwidth
	sa.AddRune('ｆ' + 1)
	sa.AddRune('Ｆ' + 1)

	/* add for UCHAR_DEFAULT_IGNORABLE_CODE_POINT what was not added above */
	sa.AddRune(0x2060) /* range 2060..206f */
	sa.AddRune(0xfff0)
	sa.AddRune(0xfffb + 1)
	sa.AddRune(0xe0000)
	sa.AddRune(0xe0fff + 1)

	/* add for UCHAR_GRAPHEME_BASE and others */
	sa.AddRune(CGJ)
	sa.AddRune(CGJ + 1)
}

func CharType(c rune) int8 {
	props := uprops.trie.Get16(c)
	return GET_CATEGORY(props)
}

func GetProperties(c rune) uint16 {
	return uprops.trie.Get16(c)
}

func GET_CATEGORY(props uint16) int8 {
	return int8(props & 0x1f)
}

func GetUnicodeProperties(c rune, column int) uint32 {
	if column >= int(uprops.vectorsColumns) {
		return 0
	}
	vecIndex := uprops.trie2.Get16(c)
	return uprops.vectors[int(vecIndex)+column]
}

func ScriptExtension(idx uint32) uint16 {
	return uprops.scriptExtensions[idx]
}

func ScriptExtensions(idx uint32) []uint16 {
	return uprops.scriptExtensions[idx:]
}

func IsDigit(c rune) bool {
	return CharType(c) == U_DECIMAL_DIGIT_NUMBER
}

func IsPOSIXPrint(c rune) bool {
	return CharType(c) == U_SPACE_SEPARATOR || IsGraphPOSIX(c)
}

func IsGraphPOSIX(c rune) bool {
	props := uprops.trie.Get16(c)
	/* \p{space}\p{gc=Control} == \p{gc=Z}\p{Control} */
	/* comparing ==0 returns FALSE for the categories mentioned */
	return U_MASK(GET_CATEGORY(props))&(U_GC_CC_MASK|U_GC_CS_MASK|U_GC_CN_MASK|U_GC_Z_MASK) == 0
}

func IsXDigit(c rune) bool {
	/* check ASCII and Fullwidth ASCII a-fA-F */
	if (c <= 0x66 && c >= 0x41 && (c <= 0x46 || c >= 0x61)) ||
		(c >= 0xff21 && c <= 0xff46 && (c <= 0xff26 || c >= 0xff41)) {
		return true
	}
	return IsDigit(c)
}

func IsBlank(c rune) bool {
	if c <= 0x9f {
		return c == 9 || c == 0x20 /* TAB or SPACE */
	}
	/* Zs */
	return CharType(c) == U_SPACE_SEPARATOR
}

func CharAge(c rune) UVersionInfo {
	version := GetUnicodeProperties(c, 0) >> UPROPS_AGE_SHIFT
	return UVersionInfo{uint8(version >> 4), uint8(version & 0xf), 0, 0}
}

func VersionFromString(str string) (version UVersionInfo) {
	part := 0
	for len(str) > 0 && part < U_MAX_VERSION_LENGTH {
		if str[0] == U_VERSION_DELIMITER {
			str = str[1:]
		}
		str, version[part] = parseInt(str)
		part++
	}
	return
}

// parseInt is simplified but aims to mimic strtoul usage
// as it is used for ICU version parsing.
func parseInt(str string) (string, uint8) {
	if str == "" {
		return str, 0
	}

	start := 0
	end := 0
	for i := 0; i < len(str); i++ {
		switch str[i] {
		case ' ', '\f', '\n', '\r', '\t', '\v':
			start++
			continue
		default:
			break
		}
	}
	str = str[start:]

	for i := 0; i < len(str); i++ {
		if str[i] < '0' || str[i] > '9' {
			end = i
			break
		}
		end++
	}

	val, err := strconv.ParseUint(str[start:end], 10, 8)
	if err != nil {
		return str[end:], 0
	}
	return str[end:], uint8(val)
}

const UPROPS_NUMERIC_TYPE_VALUE_SHIFT = 6

func NumericTypeValue(c rune) uint16 {
	props := uprops.trie.Get16(c)
	return props >> UPROPS_NUMERIC_TYPE_VALUE_SHIFT
}

func NumericValue(c rune) float64 {
	ntv := int32(NumericTypeValue(c))

	if ntv == UPROPS_NTV_NONE {
		return U_NO_NUMERIC_VALUE
	} else if ntv < UPROPS_NTV_DIGIT_START {
		/* decimal digit */
		return float64(ntv - UPROPS_NTV_DECIMAL_START)
	} else if ntv < UPROPS_NTV_NUMERIC_START {
		/* other digit */
		return float64(ntv - UPROPS_NTV_DIGIT_START)
	} else if ntv < UPROPS_NTV_FRACTION_START {
		/* small integer */
		return float64(ntv - UPROPS_NTV_NUMERIC_START)
	} else if ntv < UPROPS_NTV_LARGE_START {
		/* fraction */
		numerator := (ntv >> 4) - 12
		denominator := (ntv & 0xf) + 1
		return float64(numerator) / float64(denominator)
	} else if ntv < UPROPS_NTV_BASE60_START {
		/* large, single-significant-digit integer */
		mant := (ntv >> 5) - 14
		exp := (ntv & 0x1f) + 2
		numValue := float64(mant)

		/* multiply by 10^exp without math.h */
		for exp >= 4 {
			numValue *= 10000.
			exp -= 4
		}
		switch exp {
		case 3:
			numValue *= 1000.0
		case 2:
			numValue *= 100.0
		case 1:
			numValue *= 10.0
		case 0:
		default:
		}

		return numValue
	} else if ntv < UPROPS_NTV_FRACTION20_START {
		/* sexagesimal (base 60) integer */
		numValue := (ntv >> 2) - 0xbf
		exp := (ntv & 3) + 1

		switch exp {
		case 4:
			numValue *= 60 * 60 * 60 * 60
		case 3:
			numValue *= 60 * 60 * 60
		case 2:
			numValue *= 60 * 60
		case 1:
			numValue *= 60
		case 0:
		default:
		}

		return float64(numValue)
	} else if ntv < UPROPS_NTV_FRACTION32_START {
		// fraction-20 e.g. 3/80
		frac20 := ntv - UPROPS_NTV_FRACTION20_START // 0..0x17
		numerator := 2*(frac20&3) + 1
		denominator := 20 << (frac20 >> 2)
		return float64(numerator) / float64(denominator)
	} else if ntv < UPROPS_NTV_RESERVED_START {
		// fraction-32 e.g. 3/64
		frac32 := ntv - UPROPS_NTV_FRACTION32_START // 0..15
		numerator := 2*(frac32&3) + 1
		denominator := 32 << (frac32 >> 2)
		return float64(numerator) / float64(denominator)
	} else {
		/* reserved */
		return U_NO_NUMERIC_VALUE
	}
}
