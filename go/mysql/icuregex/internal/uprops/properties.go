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

	"vitess.io/vitess/go/mysql/icuregex/errors"
	"vitess.io/vitess/go/mysql/icuregex/internal/normalizer"
	"vitess.io/vitess/go/mysql/icuregex/internal/pattern"
	"vitess.io/vitess/go/mysql/icuregex/internal/ubidi"
	"vitess.io/vitess/go/mysql/icuregex/internal/ucase"
	"vitess.io/vitess/go/mysql/icuregex/internal/uchar"
	"vitess.io/vitess/go/mysql/icuregex/internal/ulayout"
	"vitess.io/vitess/go/mysql/icuregex/internal/unames"
	"vitess.io/vitess/go/mysql/icuregex/internal/uset"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var inclusionsMu sync.Mutex
var inclusionsForSource = make(map[propertySource]*uset.UnicodeSet)
var inclusionsForProperty = make(map[Property]*uset.UnicodeSet)

func getInclusionsForSource(src propertySource) (*uset.UnicodeSet, error) {
	if inc, ok := inclusionsForSource[src]; ok {
		return inc, nil
	}

	u := uset.New()

	switch src {
	case srcChar:
		uchar.AddPropertyStarts(u)
	case srcPropsvec:
		uchar.VecAddPropertyStarts(u)
	case srcCharAndPropsvec:
		uchar.AddPropertyStarts(u)
		uchar.VecAddPropertyStarts(u)
	case srcCaseAndNorm:
		normalizer.Nfc().AddPropertyStarts(u)
		ucase.AddPropertyStarts(u)
	case srcNfc:
		normalizer.Nfc().AddPropertyStarts(u)
	case srcNfkc:
		normalizer.Nfkc().AddPropertyStarts(u)
	case srcNfkcCf:
		return nil, errors.ErrUnsupported
	case srcNfcCanonIter:
		return nil, errors.ErrUnsupported
	case srcCase:
		ucase.AddPropertyStarts(u)
	case srcBidi:
		ubidi.AddPropertyStarts(u)
	case srcInpc, srcInsc, srcVo:
		AddULayoutPropertyStarts(src, u)
	default:
		return nil, errors.ErrUnsupported
	}

	inclusionsForSource[src] = u
	return u, nil
}

func getInclusionsForPropertyLocked(prop Property) (*uset.UnicodeSet, error) {
	if UCharIntStart <= prop && prop < uCharIntLimit {
		return getInclusionsForIntProperty(prop)
	}
	return getInclusionsForSource(prop.source())
}

func getInclusionsForProperty(prop Property) (*uset.UnicodeSet, error) {
	inclusionsMu.Lock()
	defer inclusionsMu.Unlock()
	return getInclusionsForPropertyLocked(prop)
}

func getInclusionsForBinaryProperty(prop Property) (*uset.UnicodeSet, error) {
	inclusionsMu.Lock()
	defer inclusionsMu.Unlock()
	if inc, ok := inclusionsForProperty[prop]; ok {
		return inc, nil
	}

	incl, err := getInclusionsForPropertyLocked(prop)
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
		set.AddRuneRange(startHasProperty, uset.MaxValue)
	}

	inclusionsForProperty[prop] = set
	return set, nil
}

func getInclusionsForIntProperty(prop Property) (*uset.UnicodeSet, error) {
	if inc, ok := inclusionsForProperty[prop]; ok {
		return inc, nil
	}

	src := prop.source()
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
			value := getIntPropertyValue(c, prop)
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
	case prop == UCharGeneralCategoryMask:
		inclusions, err := getInclusionsForProperty(prop)
		if err != nil {
			return err
		}
		u.ApplyFilter(inclusions, func(ch rune) bool {
			return (uMask(uchar.CharType(ch)) & uint32(value)) != 0
		})
	case prop == UCharScriptExtensions:
		inclusions, err := getInclusionsForProperty(prop)
		if err != nil {
			return err
		}
		u.ApplyFilter(inclusions, func(ch rune) bool {
			return uscriptHasScript(ch, code(value))
		})
	case 0 <= prop && prop < uCharBinaryLimit:
		if value == 0 || value == 1 {
			set, err := getInclusionsForBinaryProperty(prop)
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

	case UCharIntStart <= prop && prop < uCharIntLimit:
		inclusions, err := getInclusionsForProperty(prop)
		if err != nil {
			return err
		}
		u.ApplyFilter(inclusions, func(ch rune) bool {
			return getIntPropertyValue(ch, prop) == value
		})
	default:
		return errors.ErrUnsupported
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
		return errors.ErrIllegalArgument
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
			return errors.ErrIllegalArgument
		}
		pat = pat[1:]
	} else {
		return errors.ErrIllegalArgument
	}

	var closePos int
	if posix {
		closePos = strings.Index(pat, ":]")
	} else {
		closePos = strings.IndexByte(pat, '}')
	}
	if closePos < 0 {
		return errors.ErrIllegalArgument
	}

	equals := strings.IndexByte(pat, '=')
	var propName, valueName string
	if equals >= 0 && equals < closePos && !isName {
		propName = pat[:equals]
		valueName = pat[equals+1 : closePos]
	} else {
		propName = pat[:closePos]
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
		p = getPropertyEnum(prop)
		if p == -1 {
			return errors.ErrIllegalArgument
		}
		if p == UCharGeneralCategory {
			p = UCharGeneralCategoryMask
		}

		if (p >= UCharBinaryStart && p < uCharBinaryLimit) ||
			(p >= UCharIntStart && p < uCharIntLimit) ||
			(p >= UCharMaskStart && p < uCharMaskLimit) {
			v = getPropertyValueEnum(p, value)
			if v == -1 {
				// Handle numeric CCC
				if p == UCharCanonicalCombiningClass ||
					p == UCharTrailCanonicalCombiningClass ||
					p == UCharLeadCanonicalCombiningClass {
					val, err := strconv.ParseUint(value, 10, 8)
					if err != nil {
						return errors.ErrIllegalArgument
					}
					v = int32(val)
				} else {
					return errors.ErrIllegalArgument
				}
			}
		} else {
			switch p {
			case UCharNumericValue:
				val, err := strconv.ParseFloat(value, 64)
				if err != nil {
					return errors.ErrIllegalArgument
				}
				incl, err := getInclusionsForProperty(p)
				if err != nil {
					return err
				}
				u.ApplyFilter(incl, func(ch rune) bool {
					return uchar.NumericValue(ch) == val
				})
				return nil
			case UCharName:
				// Must munge name, since u_charFromName() does not do
				// 'loose' matching.
				charName := mungeCharName(value)
				ch := unames.CharForName(unames.ExtendedCharName, charName)
				if ch < 0 {
					return errors.ErrIllegalArgument
				}
				u.Clear()
				u.AddRune(ch)
				return nil
			case UCharAge:
				// Must munge name, since u_versionFromString() does not do
				// 'loose' matching.
				charName := mungeCharName(value)
				version := uchar.VersionFromString(charName)
				incl, err := getInclusionsForProperty(p)
				if err != nil {
					return err
				}
				u.ApplyFilter(incl, func(ch rune) bool {
					return uchar.CharAge(ch) == version
				})
				return nil
			case UCharScriptExtensions:
				v = getPropertyValueEnum(UCharScript, value)
				if v == -1 {
					return errors.ErrIllegalArgument
				}
			default:
				// p is a non-binary, non-enumerated property that we
				// don't support (yet).
				return errors.ErrIllegalArgument
			}
		}
	} else {
		// value is empty.  Interpret as General Category, Script, or
		// Binary property.
		p = UCharGeneralCategoryMask
		v = getPropertyValueEnum(p, prop)
		if v == -1 {
			p = UCharScript
			v = getPropertyValueEnum(p, prop)
			if v == -1 {
				p = getPropertyEnum(prop)
				if p >= UCharBinaryStart && p < uCharBinaryLimit {
					v = 1
				} else if 0 == comparePropertyNames("ANY", prop) {
					u.Clear()
					u.AddRuneRange(uset.MinValue, uset.MaxValue)
					return nil
				} else if 0 == comparePropertyNames("ASCII", prop) {
					u.Clear()
					u.AddRuneRange(0, 0x7F)
					return nil
				} else if 0 == comparePropertyNames("Assigned", prop) {
					// [:Assigned:]=[:^Cn:]
					p = UCharGeneralCategoryMask
					v = int32(uchar.GcCnMask)
					invert = true
				} else {
					return errors.ErrIllegalArgument
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

func AddULayoutPropertyStarts(src propertySource, u *uset.UnicodeSet) {
	var trie *utrie.UcpTrie
	switch src {
	case srcInpc:
		trie = ulayout.InpcTrie()
	case srcInsc:
		trie = ulayout.InscTrie()
	case srcVo:
		trie = ulayout.VoTrie()
	default:
		panic("unreachable")
	}

	// Add the start code point of each same-value range of the trie.
	var start, end rune
	for {
		end, _ = trie.GetRange(start, utrie.UcpMapRangeNormal, 0, nil)
		if end < 0 {
			break
		}
		u.AddRune(start)
		start = end + 1
	}
}

func AddCategory(u *uset.UnicodeSet, mask uint32) error {
	set := uset.New()
	err := ApplyIntPropertyValue(set, UCharGeneralCategoryMask, int32(mask))
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
	if flags&uset.CaseInsensitive != 0 {
		u.CloseOver(uset.CaseInsensitive)
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
