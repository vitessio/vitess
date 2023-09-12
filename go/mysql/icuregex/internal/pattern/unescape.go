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

package pattern

import (
	"strings"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/icuregex/internal/utf16"
)

/* Convert one octal digit to a numeric value 0..7, or -1 on failure */
func _digit8(c rune) rune {
	if c >= 0x0030 && c <= 0x0037 {
		return (c - 0x0030)
	}
	return -1
}

/* Convert one hex digit to a numeric value 0..F, or -1 on failure */
func _digit16(c rune) rune {
	if c >= 0x0030 && c <= 0x0039 {
		return (c - 0x0030)
	}
	if c >= 0x0041 && c <= 0x0046 {
		return (c - (0x0041 - 10))
	}
	if c >= 0x0061 && c <= 0x0066 {
		return (c - (0x0061 - 10))
	}
	return -1
}

var unscapeMap = []byte{
	/*"   0x22, 0x22 */
	/*'   0x27, 0x27 */
	/*?   0x3F, 0x3F */
	/*\   0x5C, 0x5C */
	/*a*/ 0x61, 0x07,
	/*b*/ 0x62, 0x08,
	/*e*/ 0x65, 0x1b,
	/*f*/ 0x66, 0x0c,
	/*n*/ 0x6E, 0x0a,
	/*r*/ 0x72, 0x0d,
	/*t*/ 0x74, 0x09,
	/*v*/ 0x76, 0x0b,
}

func Unescape(str string) (string, bool) {
	var idx int
	if idx = strings.IndexByte(str, '\\'); idx < 0 {
		return str, true
	}

	var result strings.Builder
	result.WriteString(str[:idx])
	str = str[idx:]

	for len(str) > 0 {
		if str[0] == '\\' {
			var r rune
			r, str = UnescapeAt(str[1:])
			if r < 0 {
				return "", false
			}
			result.WriteRune(r)
		} else {
			result.WriteByte(str[0])
			str = str[1:]
		}
	}
	return result.String(), true
}

func UnescapeAt(str string) (rune, string) {
	c, w := utf8.DecodeRuneInString(str)
	str = str[w:]
	if c == utf8.RuneError && (w == 0 || w == 1) {
		return -1, str
	}

	var minDig, maxDig, n int
	var braces bool
	var bitsPerDigit = 4
	var result rune

	switch c {
	case 'u':
		minDig = 4
		maxDig = 4
	case 'U':
		minDig = 8
		maxDig = 8
	case 'x':
		minDig = 1
		if len(str) > 0 && str[0] == '{' {
			str = str[1:]
			braces = true
			maxDig = 8
		} else {
			maxDig = 2
		}
	default:
		if dig := _digit8(c); dig >= 0 {
			minDig = 1
			maxDig = 4
			n = 1
			bitsPerDigit = 3
			result = dig
		}
	}

	if minDig != 0 {
		for n < maxDig && len(str) > 0 {
			c, w = utf8.DecodeRuneInString(str)
			if c == utf8.RuneError && w == 1 {
				return -1, str
			}

			var dig rune
			if bitsPerDigit == 3 {
				dig = _digit8(c)
			} else {
				dig = _digit16(c)
			}
			if dig < 0 {
				break
			}
			result = (result << bitsPerDigit) | dig
			str = str[w:]
			n++
		}
		if n < minDig {
			return -1, str
		}
		if braces {
			if c != '}' {
				return -1, str
			}
			str = str[1:]
		}
		if result < 0 || result > utf8.MaxRune {
			return -1, str
		}
		if len(str) > 0 && utf16.IsLead(result) {
			c, w = utf8.DecodeRuneInString(str)
			if c == utf8.RuneError && (w == 0 || w == 1) {
				return -1, str
			}
			if c == '\\' {
				var str2 string
				c, str2 = UnescapeAt(str[1:])
				if utf16.IsTrail(c) {
					result = utf16.DecodeRune(result, c)
					str = str2
				}
			}
		}
		return result, str
	}

	if c < utf8.RuneSelf {
		for i := 0; i < len(unscapeMap); i += 2 {
			if byte(c) == unscapeMap[i] {
				return rune(unscapeMap[i+1]), str
			}
			if byte(c) < unscapeMap[i] {
				break
			}
		}
	}

	if c == 'c' && len(str) > 0 {
		c, w = utf8.DecodeRuneInString(str)
		if c == utf8.RuneError && (w == 0 || w == 1) {
			return -1, str
		}
		return 0x1f & c, str[w:]
	}

	return c, str
}

func UnescapeAtRunes(str []rune) (rune, []rune) {
	if len(str) == 0 {
		return -1, str
	}

	c := str[0]
	str = str[1:]
	if c == utf8.RuneError {
		return -1, str
	}

	var minDig, maxDig, n int
	var braces bool
	var bitsPerDigit = 4
	var result rune

	switch c {
	case 'u':
		minDig = 4
		maxDig = 4
	case 'U':
		minDig = 8
		maxDig = 8
	case 'x':
		minDig = 1
		if len(str) > 0 && str[0] == '{' {
			str = str[1:]
			braces = true
			maxDig = 8
		} else {
			maxDig = 2
		}
	default:
		if dig := _digit8(c); dig >= 0 {
			minDig = 1
			maxDig = 4
			n = 1
			bitsPerDigit = 3
			result = dig
		}
	}

	if minDig != 0 {
		for n < maxDig && len(str) > 0 {
			c = str[0]
			if c == utf8.RuneError {
				return -1, str
			}

			var dig rune
			if bitsPerDigit == 3 {
				dig = _digit8(c)
			} else {
				dig = _digit16(c)
			}
			if dig < 0 {
				break
			}
			result = (result << bitsPerDigit) | dig
			str = str[1:]
			n++
		}
		if n < minDig {
			return -1, str
		}
		if braces {
			if c != '}' {
				return -1, str
			}
			str = str[1:]
		}
		if result < 0 || result > utf8.MaxRune {
			return -1, str
		}
		if len(str) > 0 && utf16.IsLead(result) {
			c = str[0]
			if c == utf8.RuneError {
				return -1, str
			}
			if c == '\\' {
				var str2 []rune
				c, str2 = UnescapeAtRunes(str[1:])
				if utf16.IsTrail(c) {
					result = utf16.DecodeRune(result, c)
					str = str2
				}
			}
		}
		return result, str
	}

	if c < utf8.RuneSelf {
		for i := 0; i < len(unscapeMap); i += 2 {
			if byte(c) == unscapeMap[i] {
				return rune(unscapeMap[i+1]), str
			}
			if byte(c) < unscapeMap[i] {
				break
			}
		}
	}

	if c == 'c' && len(str) > 0 {
		c = str[0]
		if c == utf8.RuneError {
			return -1, str
		}
		return 0x1f & c, str[1:]
	}

	return c, str
}
