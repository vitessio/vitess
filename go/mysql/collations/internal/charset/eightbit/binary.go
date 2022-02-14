package eightbit

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/internal/charset/types"
)

type Charset_binary struct{}

func (Charset_binary) Name() string {
	return "binary"
}

func (Charset_binary) IsSuperset(_ types.Charset) bool {
	return true
}

func (Charset_binary) SupportsSupplementaryChars() bool {
	return true
}

func (c Charset_binary) EncodeRune(dst []byte, r rune) int {
	if r > 0xFF {
		return -1
	}
	dst[0] = byte(r)
	return 1
}

func (c Charset_binary) DecodeRune(bytes []byte) (rune, int) {
	if len(bytes) < 1 {
		return utf8.RuneError, 0
	}
	return rune(bytes[0]), 1
}

func (c Charset_binary) Convert(_, in []byte, _ types.Charset) ([]byte, error) {
	return in, nil
}
