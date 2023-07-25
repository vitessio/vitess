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
	"testing"
)

func TestStaticSetContents(t *testing.T) {
	// These are the number of codepoints contained in each of the static sets as of ICU69-1,
	// as to sanity check that we're re-creating the sets properly.
	// This table must be re-created when updating Unicode versions.
	var ExpectedSetSizes = map[int]int{
		1:  134564,
		4:  25,
		5:  1102451,
		6:  1979,
		7:  131,
		8:  125,
		9:  399,
		10: 10773,
		11: 95,
		12: 137,
	}

	for setid, expected := range ExpectedSetSizes {
		if got := staticPropertySets[setid].Len(); got != expected {
			t.Fatalf("static set [%d] has wrong size: got %d, expected %d", setid, got, expected)
		}
	}
}

func TestStaticFreeze(t *testing.T) {
	for _, s := range staticPropertySets {
		if err := s.FreezeCheck_(); err != nil {
			t.Error(err)
		}
	}
	for _, s := range staticRuleSet {
		if err := s.FreezeCheck_(); err != nil {
			t.Error(err)
		}
	}
	if err := staticSetUnescape.FreezeCheck_(); err != nil {
		t.Error(err)
	}
}
