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
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/normalizer"
	"vitess.io/vitess/go/mysql/icuregex/internal/pattern"
	"vitess.io/vitess/go/mysql/icuregex/internal/ubidi"
	"vitess.io/vitess/go/mysql/icuregex/internal/ucase"
	"vitess.io/vitess/go/mysql/icuregex/internal/uchar"
	"vitess.io/vitess/go/mysql/icuregex/internal/uerror"
	"vitess.io/vitess/go/mysql/icuregex/internal/ulayout"
	"vitess.io/vitess/go/mysql/icuregex/internal/unames"
	"vitess.io/vitess/go/mysql/icuregex/internal/uset"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var inclusionsMu sync.Mutex
var inclusionsForSource = make(map[PropertySource]*uset.UnicodeSet)
var inclusionsForProperty = make(map[Property]*uset.UnicodeSet)

func GetInclusionsForBinaryProperty(prop Property) (*uset.UnicodeSet, error) {
	inclusionsMu.Lock()
	defer inclusionsMu.Unlock()
	return getInclusionsForBinaryProperty(prop)
}

func getInclusionsForSource(src PropertySource) (*uset.UnicodeSet, error) {
	if inc, ok := inclusionsForSource[src]; ok {
		return inc, nil
	}

	u := uset.New()

	switch src {
	case UPROPS_SRC_CHAR:
		uchar.AddPropertyStarts(u)
	case UPROPS_SRC_PROPSVEC:
		uchar.VecAddPropertyStarts(u)
	case UPROPS_SRC_CHAR_AND_PROPSVEC:
		uchar.AddPropertyStarts(u)
		uchar.VecAddPropertyStarts(u)
	case UPROPS_SRC_CASE_AND_NORM:
		normalizer.Nfc().AddPropertyStarts(u)
		ucase.AddPropertyStarts(u)
	case UPROPS_SRC_NFC:
		normalizer.Nfc().AddPropertyStarts(u)
	case UPROPS_SRC_NFKC:
		normalizer.Nfkc().AddPropertyStarts(u)
	case UPROPS_SRC_NFKC_CF:
		return nil, uerror.UnsupportedError
	case UPROPS_SRC_NFC_CANON_ITER:
		return nil, uerror.UnsupportedError
	case UPROPS_SRC_CASE:
		ucase.AddPropertyStarts(u)
	case UPROPS_SRC_BIDI:
		ubidi.AddPropertyStarts(u)
	case UPROPS_SRC_INPC, UPROPS_SRC_INSC, UPROPS_SRC_VO:
		AddULayoutPropertyStarts(src, u)
	default:
		return nil, uerror.UnsupportedError
	}

	inclusionsForSource[src] = u
	return u, nil
}

func getInclusionsForProperty(prop Property) (*uset.UnicodeSet, error) {
	if UCHAR_INT_START <= prop && prop < UCHAR_INT_LIMIT {
		return getInclusionsForIntProperty(prop)
	}
	return getInclusionsForSource(prop.Source())
}

func GetInclusionsForProperty(prop Property) (*uset.UnicodeSet, error) {
	inclusionsMu.Lock()
	defer inclusionsMu.Unlock()
	return getInclusionsForProperty(prop)
}

func getInclusionsForBinaryProperty(prop Property) (*uset.UnicodeSet, error) {
	if inc, ok := inclusionsForProperty[prop]; ok {
		return inc, nil
	}

	incl, err := getInclusionsForProperty(prop)
	if err != nil {
		return nil, err
	}
	set := uset.New()

	numRanges := incl.RangeCount()
	startHasProperty := rune(-1)

	for i := 0; i < numRanges; i++ {
		rangeEnd := incl.RangeEnd(i)
		for c := incl.RangeStart(i); c <= rangeEnd; c++ {
			if HasBinaryProperty(c, prop) {
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
		set.AddRuneRange(startHasProperty, uset.MAX_VALUE)
	}

	inclusionsForProperty[prop] = set
	return set, nil
}

func getInclusionsForIntProperty(prop Property) (*uset.UnicodeSet, error) {
	if inc, ok := inclusionsForProperty[prop]; ok {
		return inc, nil
	}

	src := prop.Source()
	incl, err := getInclusionsForSource(src)
	if err != nil {
		return nil, err
	}

	intPropIncl := uset.New()
	intPropIncl.AddRune(0)

	numRanges := incl.RangeCount()
	prevValue := int32(0)

	for i := 0; i < numRanges; i++ {
		rangeEnd := incl.RangeEnd(i)
		for c := incl.RangeStart(i); c <= rangeEnd; c++ {
			value := GetIntPropertyValue(c, prop)
			if value != prevValue {
				intPropIncl.AddRune(c)
				prevValue = value
			}
		}
	}

	inclusionsForProperty[prop] = intPropIncl
	return intPropIncl, nil
}

func ApplyIntPropertyValue(u *uset.UnicodeSet, prop Property, value int32) error {
	switch {
	case prop == UCHAR_GENERAL_CATEGORY_MASK:
		inclusions, err := GetInclusionsForProperty(prop)
		if err != nil {
			return err
		}
		u.ApplyFilter(inclusions, func(ch rune) bool {
			return (U_MASK(uchar.CharType(ch)) & uint32(value)) != 0
		})
	case prop == UCHAR_SCRIPT_EXTENSIONS:
		inclusions, err := GetInclusionsForProperty(prop)
		if err != nil {
			return err
		}
		u.ApplyFilter(inclusions, func(ch rune) bool {
			return UScriptHasScript(ch, UScriptCode(value))
		})
	case 0 <= prop && prop < UCHAR_BINARY_LIMIT:
		if value == 0 || value == 1 {
			set, err := GetInclusionsForBinaryProperty(prop)
			if err != nil {
				return err
			}
			u.CopyFrom(set)
			if value == 0 {
				u.Complement()
			}
		} else {
			u.Clear()
		}

	case UCHAR_INT_START <= prop && prop < UCHAR_INT_LIMIT:
		inclusions, err := GetInclusionsForProperty(prop)
		if err != nil {
			return err
		}
		u.ApplyFilter(inclusions, func(ch rune) bool {
			return GetIntPropertyValue(ch, prop) == value
		})
	default:
		return uerror.UnsupportedError
	}
	return nil
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

func ApplyPropertyPattern(u *uset.UnicodeSet, pat string) error {
	if len(pat) < 5 {
		return uerror.IllegalArgumentError
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
			return uerror.IllegalArgumentError
		}
		pat = pat[1:]
	} else {
		return uerror.IllegalArgumentError
	}

	var close int
	if posix {
		close = strings.Index(pat, ":]")
	} else {
		close = strings.IndexByte(pat, '}')
	}
	if close < 0 {
		return uerror.IllegalArgumentError
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

	if err := ApplyPropertyAlias(u, propName, valueName); err != nil {
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

func ApplyPropertyAlias(u *uset.UnicodeSet, prop, value string) error {
	var p Property
	var v int32
	var invert bool

	if len(value) > 0 {
		p = GetPropertyEnum(prop)
		if p == -1 {
			return uerror.IllegalArgumentError
		}
		if p == UCHAR_GENERAL_CATEGORY {
			p = UCHAR_GENERAL_CATEGORY_MASK
		}

		if (p >= UCHAR_BINARY_START && p < UCHAR_BINARY_LIMIT) ||
			(p >= UCHAR_INT_START && p < UCHAR_INT_LIMIT) ||
			(p >= UCHAR_MASK_START && p < UCHAR_MASK_LIMIT) {
			v = GetPropertyValueEnum(p, value)
			if v == -1 {
				// Handle numeric CCC
				if p == UCHAR_CANONICAL_COMBINING_CLASS ||
					p == UCHAR_TRAIL_CANONICAL_COMBINING_CLASS ||
					p == UCHAR_LEAD_CANONICAL_COMBINING_CLASS {
					val, err := strconv.ParseUint(value, 10, 8)
					if err != nil {
						return uerror.IllegalArgumentError
					}
					v = int32(val)
				} else {
					return uerror.IllegalArgumentError
				}
			}
		} else {
			switch p {
			case UCHAR_NUMERIC_VALUE:
				val, err := strconv.ParseFloat(value, 64)
				if err != nil {
					return uerror.IllegalArgumentError
				}
				incl, err := GetInclusionsForProperty(p)
				if err != nil {
					return err
				}
				u.ApplyFilter(incl, func(ch rune) bool {
					return uchar.NumericValue(ch) == val
				})
				return nil
			case UCHAR_NAME:
				// Must munge name, since u_charFromName() does not do
				// 'loose' matching.
				charName := mungeCharName(value)
				ch := unames.CharForName(unames.U_EXTENDED_CHAR_NAME, charName)
				if ch < 0 {
					return uerror.IllegalArgumentError
				}
				u.Clear()
				u.AddRune(ch)
				return nil
			case UCHAR_AGE:
				// Must munge name, since u_versionFromString() does not do
				// 'loose' matching.
				charName := mungeCharName(value)
				version := uchar.VersionFromString(charName)
				incl, err := GetInclusionsForProperty(p)
				if err != nil {
					return err
				}
				u.ApplyFilter(incl, func(ch rune) bool {
					return uchar.CharAge(ch) == version
				})
				return nil
			case UCHAR_SCRIPT_EXTENSIONS:
				v = GetPropertyValueEnum(UCHAR_SCRIPT, value)
				if v == -1 {
					return uerror.IllegalArgumentError
				}
			default:
				// p is a non-binary, non-enumerated property that we
				// don't support (yet).
				return uerror.IllegalArgumentError
			}
		}
	} else {
		// value is empty.  Interpret as General Category, Script, or
		// Binary property.
		p = UCHAR_GENERAL_CATEGORY_MASK
		v = GetPropertyValueEnum(p, prop)
		if v == -1 {
			p = UCHAR_SCRIPT
			v = GetPropertyValueEnum(p, prop)
			if v == -1 {
				p = GetPropertyEnum(prop)
				if p >= UCHAR_BINARY_START && p < UCHAR_BINARY_LIMIT {
					v = 1
				} else if 0 == ComparePropertyNames("ANY", prop) {
					u.Clear()
					u.AddRuneRange(uset.MIN_VALUE, uset.MAX_VALUE)
					return nil
				} else if 0 == ComparePropertyNames("ASCII", prop) {
					u.Clear()
					u.AddRuneRange(0, 0x7F)
					return nil
				} else if 0 == ComparePropertyNames("Assigned", prop) {
					// [:Assigned:]=[:^Cn:]
					p = UCHAR_GENERAL_CATEGORY_MASK
					v = int32(uchar.U_GC_CN_MASK)
					invert = true
				} else {
					return uerror.IllegalArgumentError
				}
			}
		}
	}

	err := ApplyIntPropertyValue(u, p, v)
	if err != nil {
		return err
	}
	if invert {
		u.Complement()
	}
	return nil
}

func AddULayoutPropertyStarts(src PropertySource, u *uset.UnicodeSet) {
	var trie *utrie.UcpTrie
	switch src {
	case UPROPS_SRC_INPC:
		trie = ulayout.InpcTrie()
	case UPROPS_SRC_INSC:
		trie = ulayout.InscTrie()
	case UPROPS_SRC_VO:
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

func AddCategory(u *uset.UnicodeSet, mask uint32) error {
	set := uset.New()
	err := ApplyIntPropertyValue(set, UCHAR_GENERAL_CATEGORY_MASK, int32(mask))
	if err != nil {
		return err
	}
	u.AddAll(set)
	return nil
}

func NewUnicodeSetFomPattern(pattern string, flags uset.USet) (*uset.UnicodeSet, error) {
	u := uset.New()
	if err := ApplyPropertyPattern(u, pattern); err != nil {
		return nil, err
	}
	if flags&uset.USET_CASE_INSENSITIVE != 0 {
		u.CloseOver(uset.USET_CASE_INSENSITIVE)
	}
	return u, nil
}

func MustNewUnicodeSetFomPattern(pattern string, flags uset.USet) *uset.UnicodeSet {
	u, err := NewUnicodeSetFomPattern(pattern, flags)
	if err != nil {
		panic(err)
	}
	return u
}
