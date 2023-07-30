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
	"strconv"
)

type PropertySet interface {
	AddRune(ch rune)
}

func VecAddPropertyStarts(sa PropertySet) {
	trie2().Enum(nil, func(start, _ rune, _ uint32) bool {
		sa.AddRune(start)
		return true
	})
}

const (
	tab      = 0x0009
	lf       = 0x000a
	ff       = 0x000c
	cr       = 0x000d
	nbsp     = 0x00a0
	cgj      = 0x034f
	figuresp = 0x2007
	hairsp   = 0x200a
	zwnj     = 0x200c
	zwj      = 0x200d
	rlm      = 0x200f
	nnbsp    = 0x202f
	zwnbsp   = 0xfef
)

func AddPropertyStarts(sa PropertySet) {
	/* add the start code point of each same-value range of the main trie */
	trie().Enum(nil, func(start, _ rune, _ uint32) bool {
		sa.AddRune(start)
		return true
	})

	/* add code points with hardcoded properties, plus the ones following them */

	/* add for u_isblank() */
	sa.AddRune(tab)
	sa.AddRune(tab + 1)

	/* add for IS_THAT_CONTROL_SPACE() */
	sa.AddRune(cr + 1) /* range TAB..CR */
	sa.AddRune(0x1c)
	sa.AddRune(0x1f + 1)
	sa.AddRune(0x85) // NEXT LINE (NEL)
	sa.AddRune(0x85 + 1)

	/* add for u_isIDIgnorable() what was not added above */
	sa.AddRune(0x7f) /* range DEL..NBSP-1, NBSP added below */
	sa.AddRune(hairsp)
	sa.AddRune(rlm + 1)
	sa.AddRune(0x206a)     // INHIBIT SYMMETRIC SWAPPING
	sa.AddRune(0x206f + 1) // NOMINAL DIGIT SHAPES
	sa.AddRune(zwnbsp)
	sa.AddRune(zwnbsp + 1)

	/* add no-break spaces for u_isWhitespace() what was not added above */
	sa.AddRune(nbsp)
	sa.AddRune(nbsp + 1)
	sa.AddRune(figuresp)
	sa.AddRune(figuresp + 1)
	sa.AddRune(nnbsp)
	sa.AddRune(nnbsp + 1)

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
	sa.AddRune(cgj)
	sa.AddRune(cgj + 1)
}

func CharType(c rune) Category {
	props := trie().Get16(c)
	return getCategory(props)
}

func getCategory(props uint16) Category {
	return Category(props & 0x1f)
}

func GetUnicodeProperties(c rune, column int) uint32 {
	if column >= int(vectorsColumns()) {
		return 0
	}
	vecIndex := trie2().Get16(c)
	return vectors()[int(vecIndex)+column]
}

func ScriptExtension(idx uint32) uint16 {
	return scriptExtensions()[idx]
}

func ScriptExtensions(idx uint32) []uint16 {
	return scriptExtensions()[idx:]
}

func IsDigit(c rune) bool {
	return CharType(c) == DecimalDigitNumber
}

func IsPOSIXPrint(c rune) bool {
	return CharType(c) == SpaceSeparator || IsGraphPOSIX(c)
}

func IsGraphPOSIX(c rune) bool {
	props := trie().Get16(c)
	/* \p{space}\p{gc=Control} == \p{gc=Z}\p{Control} */
	/* comparing ==0 returns FALSE for the categories mentioned */
	return uMask(getCategory(props))&(GcCcMask|GcCsMask|GcCnMask|GcZMask) == 0
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
	return CharType(c) == SpaceSeparator
}

func CharAge(c rune) UVersionInfo {
	version := GetUnicodeProperties(c, 0) >> upropsAgeShift
	return UVersionInfo{uint8(version >> 4), uint8(version & 0xf), 0, 0}
}

func VersionFromString(str string) (version UVersionInfo) {
	part := 0
	for len(str) > 0 && part < maxVersionLength {
		if str[0] == versionDelimiter {
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
whitespace:
	for i := 0; i < len(str); i++ {
		switch str[i] {
		case ' ', '\f', '\n', '\r', '\t', '\v':
			start++
			continue
		default:
			break whitespace
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

const upropsNumericTypeValueShift = 6

func NumericTypeValue(c rune) uint16 {
	props := trie().Get16(c)
	return props >> upropsNumericTypeValueShift
}

func NumericValue(c rune) float64 {
	ntv := int32(NumericTypeValue(c))

	if ntv == UPropsNtvNone {
		return noNumericValue
	} else if ntv < UPropsNtvDigitStart {
		/* decimal digit */
		return float64(ntv - UPropsNtvDecimalStart)
	} else if ntv < UPropsNtvNumericStart {
		/* other digit */
		return float64(ntv - UPropsNtvDigitStart)
	} else if ntv < UPropsNtvFractionStart {
		/* small integer */
		return float64(ntv - UPropsNtvNumericStart)
	} else if ntv < UPropsNtvLargeStart {
		/* fraction */
		numerator := (ntv >> 4) - 12
		denominator := (ntv & 0xf) + 1
		return float64(numerator) / float64(denominator)
	} else if ntv < UPropsNtvBase60Start {
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
	} else if ntv < UPropsNtvFraction20Start {
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
	} else if ntv < UPropsNtvFraction32Start {
		// fraction-20 e.g. 3/80
		frac20 := ntv - UPropsNtvFraction20Start // 0..0x17
		numerator := 2*(frac20&3) + 1
		denominator := 20 << (frac20 >> 2)
		return float64(numerator) / float64(denominator)
	} else if ntv < UPropsNtvReservedStart {
		// fraction-32 e.g. 3/64
		frac32 := ntv - UPropsNtvFraction32Start // 0..15
		numerator := 2*(frac32&3) + 1
		denominator := 32 << (frac32 >> 2)
		return float64(numerator) / float64(denominator)
	} else {
		/* reserved */
		return noNumericValue
	}
}
