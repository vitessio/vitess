package charset

import (
	"errors"
	"unicode/utf8"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/encoding/unicode/utf32"
)

const RuneError = utf8.RuneError

type UnicodeCharset int

const (
	Charset_utf8mb4 UnicodeCharset = iota
	Charset_utf8
	Charset_utf16
	Charset_ucs2
	Charset_utf32
)

func (ch UnicodeCharset) SupportsSupplementaryChars() bool {
	if ch == Charset_utf8 || ch == Charset_ucs2 {
		return false
	}
	return true
}

type CodepointIterator func([]byte) (rune, int)

func (ch UnicodeCharset) Iterator() CodepointIterator {
	switch ch {
	case Charset_utf8mb4:
		return utf8.DecodeRune
	case Charset_utf8:
		return iteratorUTF8mb3
	case Charset_utf16:
		return iteratorUTF16BE
	case Charset_ucs2:
		return iteratorUCS2
	case Charset_utf32:
		return iteratorUTF32
	default:
		panic("bad charset")
	}
}

var defaultUTF16 = unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM)
var defaultUTF32 = utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM)
var errBMPRange = errors.New("input string contains characters outside of BMP range (cp > 0xFFFF)")

func ensureBMPRange(in []byte) error {
	for _, cp := range string(in) {
		if cp > 0xFFFF {
			return errBMPRange
		}
	}
	return nil
}

func (ch UnicodeCharset) EncodeFromUTF8(in []byte) ([]byte, error) {
	switch ch {
	case Charset_utf8mb4:
		return in, nil

	case Charset_utf8:
		if err := ensureBMPRange(in); err != nil {
			return nil, err
		}
		return in, nil

	case Charset_utf16:
		return defaultUTF16.NewEncoder().Bytes(in)

	case Charset_ucs2:
		if err := ensureBMPRange(in); err != nil {
			return nil, err
		}
		return defaultUTF16.NewEncoder().Bytes(in)

	case Charset_utf32:
		return defaultUTF32.NewEncoder().Bytes(in)

	default:
		panic("bad charset")
	}
}
