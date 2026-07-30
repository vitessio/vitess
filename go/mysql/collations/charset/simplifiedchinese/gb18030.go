/*
Copyright 2021 The Vitess Authors.

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

package simplifiedchinese

import (
	"sort"
	"unicode/utf8"
	_ "unsafe"

	"vitess.io/vitess/go/mysql/collations/charset/types"
)

type Charset_gb18030 struct{}

func (Charset_gb18030) Name() string {
	return "gb18030"
}

func (Charset_gb18030) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_gb18030:
		return true
	default:
		return false
	}
}

const isgb18030 = true

// gb18030PUA lists the two-byte code positions that carry no character but
// still map bijectively to Unicode in GB18030-2005: mostly private-use-area
// codepoints, plus U+1E3F, whose code position A8BC gained a character in the
// 2005 revision, and U+E5E5 at position A3A0. Each entry maps a contiguous
// run of positions in the decode table's linear index space, which is
// (c0-0x81)*190 + (c1-0x40) with trail bytes above 0x7F shifted down by one,
// to a contiguous run of codepoints. The data matches MySQL 8.0's gb18030
// conversion tables.
var gb18030PUA = [...][3]uint16{
	{6080, 6175, 0xE4C6},
	{6270, 6365, 0xE526},
	{6376, 6381, 0xE766},
	{6433, 6433, 0xE76D},
	{6444, 6445, 0xE76E},
	{6458, 6459, 0xE770},
	{6460, 6555, 0xE586},
	{6650, 6745, 0xE5E6},
	{6829, 6839, 0xE772},
	{6840, 6935, 0xE646},
	{7022, 7029, 0xE77D},
	{7030, 7125, 0xE6A6},
	{7150, 7157, 0xE785},
	{7182, 7188, 0xE78D},
	{7201, 7202, 0xE794},
	{7208, 7208, 0xE796},
	{7211, 7219, 0xE797},
	{7220, 7315, 0xE706},
	{7349, 7363, 0xE7A0},
	{7397, 7409, 0xE7AF},
	{7495, 7505, 0xE7BC},
	{7533, 7533, 0x1E3F},
	{7538, 7541, 0xE7C9},
	{7579, 7599, 0xE7CD},
	{7624, 7624, 0xE7E2},
	{7627, 7627, 0xE7E3},
	{7629, 7631, 0xE7E4},
	{7686, 7698, 0xE7F4},
	{7775, 7789, 0xE801},
	{7886, 7979, 0xE000},
	{8076, 8169, 0xE05E},
	{8266, 8359, 0xE0BC},
	{8456, 8549, 0xE11A},
	{8646, 8739, 0xE178},
	{8836, 8929, 0xE1D6},
	{16525, 16529, 0xE810},
	{22706, 22799, 0xE234},
	{22896, 22989, 0xE292},
	{23086, 23179, 0xE2F0},
	{23276, 23369, 0xE34E},
	{23466, 23559, 0xE3AC},
	{23656, 23749, 0xE40A},
	{23767, 23769, 0xE816},
	{23775, 23775, 0xE81E},
	{23783, 23783, 0xE826},
	{23788, 23789, 0xE82B},
	{23794, 23795, 0xE831},
	{23804, 23804, 0xE83B},
	{23812, 23812, 0xE843},
	{23829, 23830, 0xE854},
	{23845, 23845, 0xE864},
	{23846, 23939, 0xE468},
}

// gb18030PUAByCp is gb18030PUA reordered for encoding lookups by codepoint.
var gb18030PUAByCp [len(gb18030PUA)][3]uint16

// gb18030Decode extends the shared GBK decode table with the code positions
// of gb18030PUA, spanning every structurally valid two-byte sequence.
var gb18030Decode [(0xFE - 0x81 + 1) * 190]uint16

func init() {
	copy(gb18030Decode[:], decode[:])
	for _, run := range gb18030PUA {
		for i := run[0]; i <= run[1]; i++ {
			gb18030Decode[i] = run[2] + i - run[0]
		}
	}
	copy(gb18030PUAByCp[:], gb18030PUA[:])
	sort.Slice(gb18030PUAByCp[:], func(i, j int) bool {
		return gb18030PUAByCp[i][2] < gb18030PUAByCp[j][2]
	})
}

func gb18030EncodePUA(dst []byte, r rune) int {
	i, j := 0, len(gb18030PUAByCp)
	for i < j {
		h := i + (j-i)/2
		run := &gb18030PUAByCp[h]
		if r > rune(run[2])+rune(run[1]-run[0]) {
			i = h + 1
		} else {
			j = h
		}
	}
	if i == len(gb18030PUAByCp) || r < rune(gb18030PUAByCp[i][2]) {
		return -1
	}
	run := &gb18030PUAByCp[i]
	pos := run[0] + uint16(r) - run[2]
	c1 := byte(pos % 190)
	if c1 <= 0x3e {
		c1 += 0x40
	} else {
		c1 += 0x41
	}
	dst[0] = byte(pos/190) + 0x81
	dst[1] = c1
	return 2
}

func (Charset_gb18030) EncodeRune(dst []byte, r rune) int {
	_ = dst[3]

	var r2 rune
	switch {
	case r < utf8.RuneSelf:
		goto write1
	case encode0Low <= r && r < encode0High:
		if r2 = rune(encode0[r-encode0Low]); r2 != 0 {
			goto write2
		}
	case encode1Low <= r && r < encode1High:
		if r2 = rune(encode1[r-encode1Low]); r2 != 0 {
			goto write2
		}
	case encode2Low <= r && r < encode2High:
		if r2 = rune(encode2[r-encode2Low]); r2 != 0 {
			goto write2
		}
	case encode3Low <= r && r < encode3High:
		if r2 = rune(encode3[r-encode3Low]); r2 != 0 {
			goto write2
		}
	case encode4Low <= r && r < encode4High:
		if r2 = rune(encode4[r-encode4Low]); r2 != 0 {
			goto write2
		}
	}

	if isgb18030 {
		if r == 0xE7C7 {
			// GB18030-2005 swapped this codepoint with U+1E3F: it moved to
			// the four-byte sequence that used to encode U+1E3F.
			dst[0], dst[1], dst[2], dst[3] = 0x81, 0x35, 0xF4, 0x37
			return 4
		}
		if n := gb18030EncodePUA(dst, r); n > 0 {
			return n
		}
		if r < 0x10000 {
			i, j := 0, len(gb18030)
			for i < j {
				h := i + (j-i)/2
				if r >= rune(gb18030[h][1]) {
					i = h + 1
				} else {
					j = h
				}
			}
			dec := &gb18030[i-1]
			r += rune(dec[0]) - rune(dec[1])
			goto write4
		} else if r < 0x110000 {
			r += 189000 - 0x10000
			goto write4
		}
	}
	return -1

write1:
	dst[0] = uint8(r)
	return 1

write2:
	dst[0] = uint8(r2 >> 8)
	dst[1] = uint8(r2)
	return 2

write4:
	dst[3] = uint8(r%10 + 0x30)
	r /= 10
	dst[2] = uint8(r%126 + 0x81)
	r /= 126
	dst[1] = uint8(r%10 + 0x30)
	r /= 10
	dst[0] = uint8(r + 0x81)
	return 4
}

func (Charset_gb18030) DecodeRune(src []byte) (rune, int, bool) {
	if len(src) < 1 {
		return utf8.RuneError, 0, false
	}

	switch c0 := src[0]; {
	case c0 < utf8.RuneSelf:
		return rune(c0), 1, true

	case 0x81 <= c0 && c0 < 0xff:
		if len(src) < 2 {
			return utf8.RuneError, 1, false
		}

		c1 := src[1]
		switch {
		case 0x40 <= c1 && c1 < 0x7f:
			c1 -= 0x40
		case 0x80 <= c1 && c1 < 0xff:
			c1 -= 0x41
		case isgb18030 && 0x30 <= c1 && c1 < 0x3a:
			if len(src) < 4 {
				// The second byte here is always ASCII, so we can set size
				// to 1 in all cases.
				return utf8.RuneError, 1, false
			}
			c2 := src[2]
			if c2 < 0x81 || 0xfe < c2 {
				return utf8.RuneError, 1, false
			}
			c3 := src[3]
			if c3 < 0x30 || 0x3a <= c3 {
				return utf8.RuneError, 1, false
			}
			r := ((rune(c0-0x81)*10+rune(c1-0x30))*126+rune(c2-0x81))*10 + rune(c3-0x30)
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
				if r == 0x1E3F {
					// GB18030-2005 assigns this four-byte sequence to U+E7C7;
					// U+1E3F moved to the two-byte position A8BC.
					r = 0xE7C7
				}
				return r, 4, true
			}
			r -= 189000
			if 0 <= r && r < 0x100000 {
				r += 0x10000
			} else {
				return utf8.RuneError, 4, false
			}
			return r, 4, true
		default:
			return utf8.RuneError, 1, false
		}
		if i := int(c0-0x81)*190 + int(c1); i < len(gb18030Decode) {
			if r := rune(gb18030Decode[i]); r != 0 {
				return r, 2, true
			}
		}
		return utf8.RuneError, 2, false

	default:
		return utf8.RuneError, 1, false
	}
}

func (c Charset_gb18030) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_gb18030) MaxWidth() int {
	return 4
}

func (Charset_gb18030) MinWidth() int {
	return 1
}
