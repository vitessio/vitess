package unicode

import "unicode/utf8"

const (
	maskx = 0b00111111
	mask2 = 0b00011111
	mask3 = 0b00001111

	// The default lowest and highest continuation byte.
	locb = 0b10000000
	hicb = 0b10111111

	// These names of these constants are chosen to give nice alignment in the
	// table below. The first nibble is an index into acceptRanges or F for
	// special one-byte cases. The second nibble is the Rune length or the
	// Status for the special one-byte case.
	xx = 0xF1 // invalid: size 1
	as = 0xF0 // ASCII: size 1
	s1 = 0x02 // accept 0, size 2
	s2 = 0x13 // accept 1, size 3
	s3 = 0x03 // accept 0, size 3
	s4 = 0x23 // accept 2, size 3
)

// first is information about the first byte in a UTF-8 sequence.
var first = [256]uint8{
	//   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
	as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, // 0x00-0x0F
	as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, // 0x10-0x1F
	as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, // 0x20-0x2F
	as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, // 0x30-0x3F
	as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, // 0x40-0x4F
	as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, // 0x50-0x5F
	as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, // 0x60-0x6F
	as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, as, // 0x70-0x7F
	//   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
	xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, // 0x80-0x8F
	xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, // 0x90-0x9F
	xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, // 0xA0-0xAF
	xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, // 0xB0-0xBF
	xx, xx, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, // 0xC0-0xCF
	s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, s1, // 0xD0-0xDF
	s2, s3, s3, s3, s3, s3, s3, s3, s3, s3, s3, s3, s3, s4, s3, s3, // 0xE0-0xEF
	xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, xx, // 0xF0-0xFF
}

// acceptRange gives the range of valid values for the second byte in a UTF-8
// sequence.
type acceptRange struct {
	lo uint8 // lowest value for second byte.
	hi uint8 // highest value for second byte.
}

// acceptRanges has size 16 to avoid bounds checks in the code that uses it.
var acceptRanges = [16]acceptRange{
	0: {locb, hicb},
	1: {0xA0, hicb},
	2: {locb, 0x9F},
	3: {0x90, hicb},
	4: {locb, 0x8F},
}

type Encoding_utf8 struct{}

func (u Encoding_utf8) Name() string {
	return "utf8mb3"
}

func (Encoding_utf8) DecodeRune(p []byte) (rune, int) {
	n := len(p)
	if n < 1 {
		return RuneError, 0
	}
	p0 := p[0]
	x := first[p0]
	if x >= as {
		// The following code simulates an additional check for x == xx and
		// handling the ASCII and invalid cases accordingly. This mask-and-or
		// approach prevents an additional branch.
		mask := rune(x) << 31 >> 31 // Create 0x0000 or 0xFFFF.
		return rune(p[0])&^mask | RuneError&mask, 1
	}
	sz := int(x & 7)
	accept := acceptRanges[x>>4]
	if n < sz {
		return RuneError, 1
	}
	b1 := p[1]
	if b1 < accept.lo || accept.hi < b1 {
		return RuneError, 1
	}
	if sz <= 2 { // <= instead of == to help the compiler eliminate some bounds checks
		return rune(p0&mask2)<<6 | rune(b1&maskx), 2
	}
	b2 := p[2]
	if b2 < locb || hicb < b2 {
		return RuneError, 1
	}
	if sz <= 3 {
		return rune(p0&mask3)<<12 | rune(b1&maskx)<<6 | rune(b2&maskx), 3
	}
	return RuneError, 1
}

func (Encoding_utf8) SupportsSupplementaryChars() bool {
	return false
}

func (Encoding_utf8) EncodeFromUTF8(in []byte) ([]byte, error) {
	if err := ensureBMPRange(in); err != nil {
		return nil, err
	}
	return in, nil
}

type Encoding_utf8mb4 struct{}

func (Encoding_utf8mb4) Name() string {
	return "utf8mb4"
}

func (Encoding_utf8mb4) DecodeRune(p []byte) (rune, int) {
	return utf8.DecodeRune(p)
}

func (Encoding_utf8mb4) SupportsSupplementaryChars() bool {
	return true
}

func (Encoding_utf8mb4) EncodeFromUTF8(in []byte) ([]byte, error) {
	return in, nil
}
