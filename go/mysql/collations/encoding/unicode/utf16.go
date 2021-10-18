package unicode

import "golang.org/x/text/encoding/unicode"

var defaultUTF16 = unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM)

type Encoding_utf16 struct{}

func (Encoding_utf16) Name() string {
	return "utf16be"
}

func (Encoding_utf16) DecodeRune(b []byte) (rune, int) {
	// 0xd800-0xdc00 encodes the high 10 bits of a pair.
	// 0xdc00-0xe000 encodes the low 10 bits of a pair.
	// the value is those 20 bits plus 0x10000.
	const (
		surr1    = 0xd800
		surr2    = 0xdc00
		surr3    = 0xe000
		surrSelf = 0x10000
	)

	if len(b) < 2 {
		return RuneError, 0
	}

	r1 := uint16(b[1]) | uint16(b[0])<<8
	if r1 < surr1 || surr3 <= r1 {
		return rune(r1), 2
	}

	if len(b) < 4 {
		return RuneError, 0
	}

	r2 := uint16(b[3]) | uint16(b[2])<<8
	if surr1 <= r1 && r1 < surr2 && surr2 <= r2 && r2 < surr3 {
		return (rune(r1)-surr1)<<10 | (rune(r2) - surr2) + surrSelf, 4
	}

	return RuneError, 1
}

func (Encoding_utf16) SupportsSupplementaryChars() bool {
	return true
}

func (Encoding_utf16) EncodeFromUTF8(in []byte) ([]byte, error) {
	return defaultUTF16.NewEncoder().Bytes(in)
}

type Encoding_ucs2 struct{}

func (Encoding_ucs2) Name() string {
	return "ucs2"
}

func (Encoding_ucs2) DecodeRune(p []byte) (rune, int) {
	if len(p) < 2 {
		return RuneError, 0
	}
	return rune(p[0])<<8 | rune(p[1]), 2
}

func (Encoding_ucs2) SupportsSupplementaryChars() bool {
	return false
}

func (Encoding_ucs2) EncodeFromUTF8(in []byte) ([]byte, error) {
	if err := ensureBMPRange(in); err != nil {
		return nil, err
	}
	return defaultUTF16.NewEncoder().Bytes(in)
}
