package eightbit

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/internal/charset/types"
)

type UnicodeMapping struct {
	From, To uint16
	Range    []byte `json:"Tab"`
}

type Charset_8bit struct {
	Name_       string
	ToUnicode   *[256]uint16
	FromUnicode []UnicodeMapping
}

func (e *Charset_8bit) Name() string {
	return e.Name_
}

func (e *Charset_8bit) IsSuperset(other types.Charset) bool {
	switch other := other.(type) {
	case *Charset_8bit:
		return e.Name_ == other.Name_
	default:
		return false
	}
}

func (e *Charset_8bit) SupportsSupplementaryChars() bool {
	return false
}

func (e *Charset_8bit) DecodeRune(bytes []byte) (rune, int) {
	if len(bytes) < 1 {
		return utf8.RuneError, 0
	}
	return rune(e.ToUnicode[bytes[0]]), 1
}

func (e *Charset_8bit) EncodeRune(dst []byte, r rune) int {
	if r > 0xFFFF {
		return -1
	}
	cp := uint16(r)
	for _, mapping := range e.FromUnicode {
		if cp >= mapping.From && cp <= mapping.To {
			dst[0] = mapping.Range[cp-mapping.From]
			if dst[0] == 0 && cp != 0 {
				return -1
			}
			return 1
		}
	}
	return -1
}
