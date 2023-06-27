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

package uset

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/pattern"
	"vitess.io/vitess/go/mysql/icuregex/internal/ubidi"
	"vitess.io/vitess/go/mysql/icuregex/internal/ucase"
	uchar2 "vitess.io/vitess/go/mysql/icuregex/internal/uchar"
	"vitess.io/vitess/go/mysql/icuregex/internal/uerror"
	"vitess.io/vitess/go/mysql/icuregex/internal/ulayout"
	"vitess.io/vitess/go/mysql/icuregex/internal/unames"
	uprops2 "vitess.io/vitess/go/mysql/icuregex/internal/uprops"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var inclusionsMu sync.Mutex
var inclusionsForSource = make(map[uprops2.PropertySource]*UnicodeSet)
var inclusionsForProperty = make(map[uprops2.Property]*UnicodeSet)

func GetInclusionsForBinaryProperty(prop uprops2.Property) *UnicodeSet {
	inclusionsMu.Lock()
	defer inclusionsMu.Unlock()
	return getInclusionsForBinaryProperty(prop)
}

func getInclusionsForSource(src uprops2.PropertySource) *UnicodeSet {
	if inc, ok := inclusionsForSource[src]; ok {
		return inc
	}

	u := New()

	switch src {
	case uprops2.UPROPS_SRC_CHAR:
		uchar2.AddPropertyStarts(u)
	case uprops2.UPROPS_SRC_PROPSVEC:
		uchar2.VecAddPropertyStarts(u)
	case uprops2.UPROPS_SRC_CHAR_AND_PROPSVEC:
		uchar2.AddPropertyStarts(u)
		uchar2.VecAddPropertyStarts(u)
	case uprops2.UPROPS_SRC_CASE_AND_NORM:
		panic("TODO")
	case uprops2.UPROPS_SRC_NFC:
		panic("TODO")
	case uprops2.UPROPS_SRC_NFKC:
		panic("TODO")
	case uprops2.UPROPS_SRC_NFKC_CF:
		panic("TODO")
	case uprops2.UPROPS_SRC_NFC_CANON_ITER:
		panic("TODO")
	case uprops2.UPROPS_SRC_CASE:
		ucase.AddPropertyStarts(u)
	case uprops2.UPROPS_SRC_BIDI:
		ubidi.AddPropertyStarts(u)
	case uprops2.UPROPS_SRC_INPC, uprops2.UPROPS_SRC_INSC, uprops2.UPROPS_SRC_VO:
		AddULayoutPropertyStarts(src, u)
	default:
		panic(fmt.Sprintf("unsupported property source: %v", src))
	}

	inclusionsForSource[src] = u
	return u
}

func getInclusionsForProperty(prop uprops2.Property) *UnicodeSet {
	if uprops2.UCHAR_INT_START <= prop && prop < uprops2.UCHAR_INT_LIMIT {
		return getInclusionsForIntProperty(prop)
	}
	return getInclusionsForSource(prop.Source())
}

func GetInclusionsForProperty(prop uprops2.Property) *UnicodeSet {
	inclusionsMu.Lock()
	defer inclusionsMu.Unlock()
	return getInclusionsForProperty(prop)
}

func getInclusionsForBinaryProperty(prop uprops2.Property) *UnicodeSet {
	if inc, ok := inclusionsForProperty[prop]; ok {
		return inc
	}

	incl := getInclusionsForProperty(prop)
	set := New()

	numRanges := incl.rangeCount()
	startHasProperty := rune(-1)

	for i := 0; i < numRanges; i++ {
		rangeEnd := incl.rangeEnd(i)
		for c := incl.rangeStart(i); c <= rangeEnd; c++ {
			if uprops2.HasBinaryProperty(c, prop) {
				if startHasProperty < 0 {
					startHasProperty = c
				}
			} else if startHasProperty >= 0 {
				set.AddRuneRange(startHasProperty, c-1)
				startHasProperty = -1
			}
		}
	}
	if startHasProperty >= 0 {
		set.AddRuneRange(startHasProperty, MAX_VALUE)
	}

	inclusionsForProperty[prop] = set
	return set
}

func getInclusionsForIntProperty(prop uprops2.Property) *UnicodeSet {
	if inc, ok := inclusionsForProperty[prop]; ok {
		return inc
	}

	src := prop.Source()
	incl := getInclusionsForSource(src)

	intPropIncl := New()
	intPropIncl.AddRune(0)

	numRanges := incl.rangeCount()
	prevValue := int32(0)

	for i := 0; i < numRanges; i++ {
		rangeEnd := incl.rangeEnd(i)
		for c := incl.rangeStart(i); c <= rangeEnd; c++ {
			value := uprops2.GetIntPropertyValue(c, prop)
			if value != prevValue {
				intPropIncl.AddRune(c)
				prevValue = value
			}
		}
	}

	inclusionsForProperty[prop] = intPropIncl
	return intPropIncl
}

func (u *UnicodeSet) ApplyIntPropertyValue(prop uprops2.Property, value int32) {
	switch {
	case prop == uprops2.UCHAR_GENERAL_CATEGORY_MASK:
		inclusions := GetInclusionsForProperty(prop)
		u.applyFilter(inclusions, func(ch rune) bool {
			return (uprops2.U_MASK(uchar2.CharType(ch)) & uint32(value)) != 0
		})
	case prop == uprops2.UCHAR_SCRIPT_EXTENSIONS:
		inclusions := GetInclusionsForProperty(prop)
		u.applyFilter(inclusions, func(ch rune) bool {
			return uprops2.UScriptHasScript(ch, uprops2.UScriptCode(value))
		})
	case 0 <= prop && prop < uprops2.UCHAR_BINARY_LIMIT:
		if value == 0 || value == 1 {
			set := GetInclusionsForBinaryProperty(prop)
			u.CopyFrom(set)
			if value == 0 {
				u.Complement()
			}
		} else {
			u.Clear()
		}

	case uprops2.UCHAR_INT_START <= prop && prop < uprops2.UCHAR_INT_LIMIT:
		inclusions := GetInclusionsForProperty(prop)
		u.applyFilter(inclusions, func(ch rune) bool {
			return uprops2.GetIntPropertyValue(ch, prop) == value
		})

	default:
		panic("invalid Property type")
	}
}

func mungeCharName(charname string) string {
	out := make([]byte, 0, len(charname))
	for _, ch := range []byte(charname) {
		j := len(out)
		if ch == ' ' && (j == 0 || out[j-1] == ' ') {
			continue
		}
		out = append(out, ch)
	}
	return string(out)
}

func (u *UnicodeSet) ApplyPropertyPattern(pat string) error {
	if len(pat) < 5 {
		return uerror.U_ILLEGAL_ARGUMENT_ERROR
	}

	var posix, isName, invert bool

	if isPOSIXOpen(pat) {
		posix = true
		pat = pattern.SkipWhitespace(pat[2:])
		if len(pat) > 0 && pat[0] == '^' {
			pat = pat[1:]
			invert = true
		}
	} else if isPerlOpen(pat) || isNameOpen(pat) {
		c := pat[1]
		invert = c == 'P'
		isName = c == 'N'
		pat = pattern.SkipWhitespace(pat[2:])
		if len(pat) == 0 || pat[0] != '{' {
			return uerror.U_ILLEGAL_ARGUMENT_ERROR
		}
		pat = pat[1:]
	} else {
		return uerror.U_ILLEGAL_ARGUMENT_ERROR
	}

	var close int
	if posix {
		close = strings.Index(pat, ":]")
	} else {
		close = strings.IndexByte(pat, '}')
	}
	if close < 0 {
		return uerror.U_ILLEGAL_ARGUMENT_ERROR
	}

	equals := strings.IndexByte(pat, '=')
	var propName, valueName string
	if equals >= 0 && equals < close && !isName {
		propName = pat[:equals]
		valueName = pat[equals+1 : close]
	} else {
		propName = pat[:close]
		if isName {
			valueName = propName
			propName = "na"
		}
	}

	if err := u.ApplyPropertyAlias(propName, valueName); err != nil {
		return err
	}
	if invert {
		u.Complement()
	}
	return nil
}

func isPOSIXOpen(pattern string) bool {
	return pattern[0] == '[' && pattern[1] == ':'
}

func isNameOpen(pattern string) bool {
	return pattern[0] == '\\' && pattern[1] == 'N'
}

func isPerlOpen(pattern string) bool {
	return pattern[0] == '\\' && (pattern[1] == 'p' || pattern[1] == 'P')
}

func (u *UnicodeSet) ApplyPropertyAlias(prop, value string) error {
	var p uprops2.Property
	var v int32
	var invert bool

	if len(value) > 0 {
		p = uprops2.GetPropertyEnum(prop)
		if p == -1 {
			return uerror.U_ILLEGAL_ARGUMENT_ERROR
		}
		if p == uprops2.UCHAR_GENERAL_CATEGORY {
			p = uprops2.UCHAR_GENERAL_CATEGORY_MASK
		}

		if (p >= uprops2.UCHAR_BINARY_START && p < uprops2.UCHAR_BINARY_LIMIT) ||
			(p >= uprops2.UCHAR_INT_START && p < uprops2.UCHAR_INT_LIMIT) ||
			(p >= uprops2.UCHAR_MASK_START && p < uprops2.UCHAR_MASK_LIMIT) {
			v = uprops2.GetPropertyValueEnum(p, value)
			if v == -1 {
				// Handle numeric CCC
				if p == uprops2.UCHAR_CANONICAL_COMBINING_CLASS ||
					p == uprops2.UCHAR_TRAIL_CANONICAL_COMBINING_CLASS ||
					p == uprops2.UCHAR_LEAD_CANONICAL_COMBINING_CLASS {
					val, err := strconv.ParseUint(value, 10, 8)
					if err != nil {
						return uerror.U_ILLEGAL_ARGUMENT_ERROR
					}
					v = int32(val)
				} else {
					return uerror.U_ILLEGAL_ARGUMENT_ERROR
				}
			}
		} else {
			switch p {
			case uprops2.UCHAR_NUMERIC_VALUE:
				val, err := strconv.ParseFloat(value, 64)
				if err != nil {
					return uerror.U_ILLEGAL_ARGUMENT_ERROR
				}
				u.applyFilter(GetInclusionsForProperty(p), func(ch rune) bool {
					return uchar2.NumericValue(ch) == val
				})
				return nil
			case uprops2.UCHAR_NAME:
				// Must munge name, since u_charFromName() does not do
				// 'loose' matching.
				charName := mungeCharName(value)
				ch := unames.CharForName(unames.U_EXTENDED_CHAR_NAME, charName)
				if ch < 0 {
					return uerror.U_ILLEGAL_ARGUMENT_ERROR
				}
				u.Clear()
				u.AddRune(ch)
				return nil
			case uprops2.UCHAR_AGE:
				// Must munge name, since u_versionFromString() does not do
				// 'loose' matching.
				charName := mungeCharName(value)
				version := uchar2.VersionFromString(charName)
				u.applyFilter(GetInclusionsForProperty(p), func(ch rune) bool {
					return uchar2.CharAge(ch) == version
				})
				return nil
			case uprops2.UCHAR_SCRIPT_EXTENSIONS:
				v = uprops2.GetPropertyValueEnum(uprops2.UCHAR_SCRIPT, value)
				if v == -1 {
					return uerror.U_ILLEGAL_ARGUMENT_ERROR
				}
			default:
				// p is a non-binary, non-enumerated property that we
				// don't support (yet).
				return uerror.U_ILLEGAL_ARGUMENT_ERROR
			}
		}
	} else {
		// value is empty.  Interpret as General Category, Script, or
		// Binary property.
		p = uprops2.UCHAR_GENERAL_CATEGORY_MASK
		v = uprops2.GetPropertyValueEnum(p, prop)
		if v == -1 {
			p = uprops2.UCHAR_SCRIPT
			v = uprops2.GetPropertyValueEnum(p, prop)
			if v == -1 {
				p = uprops2.GetPropertyEnum(prop)
				if p >= uprops2.UCHAR_BINARY_START && p < uprops2.UCHAR_BINARY_LIMIT {
					v = 1
				} else if 0 == uprops2.ComparePropertyNames("ANY", prop) {
					u.Clear()
					u.AddRuneRange(MIN_VALUE, MAX_VALUE)
					return nil
				} else if 0 == uprops2.ComparePropertyNames("ASCII", prop) {
					u.Clear()
					u.AddRuneRange(0, 0x7F)
					return nil
				} else if 0 == uprops2.ComparePropertyNames("Assigned", prop) {
					// [:Assigned:]=[:^Cn:]
					p = uprops2.UCHAR_GENERAL_CATEGORY_MASK
					v = int32(uchar2.U_GC_CN_MASK)
					invert = true
				} else {
					return uerror.U_ILLEGAL_ARGUMENT_ERROR
				}
			}
		}
	}

	u.ApplyIntPropertyValue(p, v)
	if invert {
		u.Complement()
	}
	return nil
}

func AddULayoutPropertyStarts(src uprops2.PropertySource, u *UnicodeSet) {
	var trie *utrie.UcpTrie
	switch src {
	case uprops2.UPROPS_SRC_INPC:
		trie = ulayout.InpcTrie()
	case uprops2.UPROPS_SRC_INSC:
		trie = ulayout.InscTrie()
	case uprops2.UPROPS_SRC_VO:
		trie = ulayout.VoTrie()
	default:
		panic("unreachable")
	}

	// Add the start code point of each same-value range of the trie.
	var start, end rune
	for {
		end, _ = trie.GetRange(start, utrie.UCPMAP_RANGE_NORMAL, 0, nil)
		if end < 0 {
			break
		}
		u.AddRune(start)
		start = end + 1
	}
}
