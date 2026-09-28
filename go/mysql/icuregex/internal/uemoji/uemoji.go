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

package uemoji

import (
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

type propertySet interface {
	AddRune(ch rune)
	AddRuneRange(from rune, to rune)
}

func AddPropertyStarts(sa propertySet) {
	// Add the start code point of each same-value range of the trie.
	var start, end rune
	for {
		end, _ = trie().GetRange(start, utrie.UcpMapRangeNormal, 0, nil)
		if end < 0 {
			break
		}
		sa.AddRune(start)
		start = end + 1
	}
}

const (
	bitEmoji                = 0
	bitEmojiPresentation    = 1
	bitEmojiModifier        = 2
	bitEmojiModifierBase    = 3
	bitEmojiComponent       = 4
	bitExtendedPictographic = 5
	bitBasicEmoji           = 6
)

// Note: REGIONAL_INDICATOR is a single, hardcoded range implemented elsewhere.
var bitFlags = []int8{
	bitEmoji,
	bitEmojiPresentation,
	bitEmojiModifier,
	bitEmojiModifierBase,
	bitEmojiComponent,
	-1,
	-1,
	bitExtendedPictographic,
	bitBasicEmoji,
	-1,
	-1,
	-1,
	-1,
	-1,
	bitBasicEmoji,
}

func HasBinaryProperty(c rune, which int) bool {
	bit := bitFlags[which]
	if bit < 0 {
		return false // not a property that we support in this function
	}
	bits := trie().Get(c)
	return ((bits >> bit) & 1) != 0
}
