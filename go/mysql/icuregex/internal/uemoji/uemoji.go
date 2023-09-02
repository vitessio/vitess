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
