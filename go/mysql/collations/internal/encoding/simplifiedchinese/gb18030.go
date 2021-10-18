package simplifiedchinese

import (
	"unicode/utf8"

	"golang.org/x/text/encoding/simplifiedchinese"
)

type Encoding_gb18030 struct{}

func (Encoding_gb18030) Name() string {
	return "gb18030"
}

func (Encoding_gb18030) DecodeRune(src []byte) (rune, int) {
	const isgb18030 = true
	if len(src) < 1 {
		return utf8.RuneError, 0
	}

	switch c0 := src[0]; {
	case c0 < utf8.RuneSelf:
		return rune(c0), 1

	// Microsoft's Code Page 936 extends GBK 1.0 to encode the euro sign U+20AC
	// as 0x80. The HTML5 specification at http://encoding.spec.whatwg.org/#gbk
	// says to treat "gbk" as Code Page 936.
	case c0 == 0x80:
		return '€', 1

	case c0 < 0xff:
		if len(src) < 2 {
			return utf8.RuneError, 1
		}

		c1 := src[1]
		switch {
		case 0x40 <= c1 && c1 < 0x7f:
			c1 -= 0x40
		case 0x80 <= c1 && c1 < 0xff:
			c1 -= 0x41
		case isgb18030 && 0x30 <= c1 && c1 < 0x40:
			if len(src) < 4 {
				// The second byte here is always ASCII, so we can set size
				// to 1 in all cases.
				return utf8.RuneError, 1
			}
			c2 := src[2]
			if c2 < 0x81 || 0xff <= c2 {
				return utf8.RuneError, 1
			}
			c3 := src[3]
			if c3 < 0x30 || 0x3a <= c3 {
				return utf8.RuneError, 1
			}
			var r = ((rune(c0-0x81)*10+rune(c1-0x30))*126+rune(c2-0x81))*10 + rune(c3-0x30)
			if r < 39420 {
				i, j := 0, len(gb18030)
				for i < j {
					h := i + (j-i)/2
					if r >= rune(gb18030[h][0]) {
						i = h + 1
					} else {
						j = h
					}
				}
				dec := &gb18030[i-1]
				r += rune(dec[1]) - rune(dec[0])
				return r, 4
			}
			r -= 189000
			if 0 <= r && r < 0x100000 {
				r += 0x10000
			} else {
				return utf8.RuneError, 1
			}
			return r, 4
		default:
			return utf8.RuneError, 1
		}
		r := utf8.RuneError
		if i := int(c0-0x81)*190 + int(c1); i < len(decode) {
			r = rune(decode[i])
			if r == 0 {
				r = utf8.RuneError
			}
		}
		return r, 2

	default:
		return utf8.RuneError, 1
	}
}

func (c Encoding_gb18030) SupportsSupplementaryChars() bool {
	return true
}

func (c Encoding_gb18030) EncodeFromUTF8(in []byte) ([]byte, error) {
	return simplifiedchinese.GB18030.NewEncoder().Bytes(in)
}
