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

package icuregex

import (
	"vitess.io/vitess/go/mysql/icuregex/internal/uprops"
	"vitess.io/vitess/go/mysql/icuregex/internal/uset"
)

var staticPropertySets [13]*uset.UnicodeSet

func init() {
	staticPropertySets[urxIswordSet] = func() *uset.UnicodeSet {
		s := uset.New()
		s.AddAll(uprops.MustNewUnicodeSetFomPattern(`\p{Alphabetic}`, 0))
		s.AddAll(uprops.MustNewUnicodeSetFomPattern(`\p{M}`, 0))
		s.AddAll(uprops.MustNewUnicodeSetFomPattern(`\p{Nd}`, 0))
		s.AddAll(uprops.MustNewUnicodeSetFomPattern(`\p{Pc}`, 0))
		s.AddRune(0x200c)
		s.AddRune(0x200d)
		return s.Freeze()
	}()

	staticPropertySets[urxIsspaceSet] = uprops.MustNewUnicodeSetFomPattern(`\p{Whitespace}`, 0).Freeze()

	staticPropertySets[urxGcExtend] = uprops.MustNewUnicodeSetFomPattern(`\p{Grapheme_Extend}`, 0).Freeze()
	staticPropertySets[urxGcControl] = func() *uset.UnicodeSet {
		s := uset.New()
		s.AddAll(uprops.MustNewUnicodeSetFomPattern(`[:Zl:]`, 0))
		s.AddAll(uprops.MustNewUnicodeSetFomPattern(`[:Zp:]`, 0))
		s.AddAll(uprops.MustNewUnicodeSetFomPattern(`[:Cc:]`, 0))
		s.AddAll(uprops.MustNewUnicodeSetFomPattern(`[:Cf:]`, 0))
		s.RemoveAll(uprops.MustNewUnicodeSetFomPattern(`[:Grapheme_Extend:]`, 0))
		return s.Freeze()
	}()
	staticPropertySets[urxGcL] = uprops.MustNewUnicodeSetFomPattern(`\p{Hangul_Syllable_Type=L}`, 0).Freeze()
	staticPropertySets[urxGcLv] = uprops.MustNewUnicodeSetFomPattern(`\p{Hangul_Syllable_Type=LV}`, 0).Freeze()
	staticPropertySets[urxGcLvt] = uprops.MustNewUnicodeSetFomPattern(`\p{Hangul_Syllable_Type=LVT}`, 0).Freeze()
	staticPropertySets[urxGcV] = uprops.MustNewUnicodeSetFomPattern(`\p{Hangul_Syllable_Type=V}`, 0).Freeze()
	staticPropertySets[urxGcT] = uprops.MustNewUnicodeSetFomPattern(`\p{Hangul_Syllable_Type=T}`, 0).Freeze()

	staticPropertySets[urxGcNormal] = func() *uset.UnicodeSet {
		s := uset.New()
		s.Complement()
		s.RemoveRuneRange(0xac00, 0xd7a4)
		s.RemoveAll(staticPropertySets[urxGcControl])
		s.RemoveAll(staticPropertySets[urxGcL])
		s.RemoveAll(staticPropertySets[urxGcV])
		s.RemoveAll(staticPropertySets[urxGcT])
		return s.Freeze()
	}()
}

var staticSetUnescape = func() *uset.UnicodeSet {
	u := uset.New()
	u.AddString("acefnrtuUx")
	return u.Freeze()
}()

const (
	ruleSetDigitChar   = 128
	ruleSetASCIILetter = 129
	ruleSetRuleChar    = 130
	ruleSetCount       = 131 - 128
)

var staticRuleSet = [ruleSetCount]*uset.UnicodeSet{
	func() *uset.UnicodeSet {
		u := uset.New()
		u.AddRuneRange('0', '9')
		return u.Freeze()
	}(),
	func() *uset.UnicodeSet {
		u := uset.New()
		u.AddRuneRange('A', 'Z')
		u.AddRuneRange('a', 'z')
		return u.Freeze()
	}(),
	func() *uset.UnicodeSet {
		u := uset.New()
		u.AddString("*?+[(){}^$|\\.")
		u.Complement()
		return u.Freeze()
	}(),
}
