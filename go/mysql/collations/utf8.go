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

package collations

import (
	"bytes"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/internal/encoding"
)

func init() {
	register(&Collation_utf8mb4_general_ci{unicase: unicaseInfo_default})
	register(&Collation_utf8mb4_bin{})
}

type Collation_utf8mb4_general_ci struct {
	unicase *UnicaseInfo
}

func (c *Collation_utf8mb4_general_ci) init() {}

func (c *Collation_utf8mb4_general_ci) Id() ID {
	return 45
}

func (c *Collation_utf8mb4_general_ci) Name() string {
	return "utf8mb4_general_ci"
}

func (c *Collation_utf8mb4_general_ci) Collate(left, right []byte, isPrefix bool) int {
	unicaseInfo := c.unicase

	for len(left) > 0 && len(right) > 0 {
		l, lWidth := utf8.DecodeRune(left)
		r, rWidth := utf8.DecodeRune(right)

		if (l == utf8.RuneError && lWidth < 3) || (r == utf8.RuneError && rWidth < 3) {
			return bytes.Compare(left, right)
		}

		lRune := unicaseInfo.unicodeSort(l)
		rRune := unicaseInfo.unicodeSort(r)

		if lRune > rRune {
			return 1
		} else if lRune < rRune {
			return -1
		}

		left = left[lWidth:]
		right = right[rWidth:]
	}

	panic("TODO")
}

func (c *Collation_utf8mb4_general_ci) WeightString(dst, src []byte, numCodepoints int) []byte {
	return weightStringUnicodePad(c.unicase, encoding.Encoding_utf8mb4{}, dst, src, numCodepoints)
}

func (c *Collation_utf8mb4_general_ci) WeightStringLen(numBytes int) int {
	return ((numBytes + 3) / 4) * 2
}

func weightStringUnicodePad(unicaseInfo *UnicaseInfo, enc encoding.Encoding, dst []byte, src []byte, numCodepoints int) []byte {
	if numCodepoints == 0 || numCodepoints == PadToMax {
		for {
			r, width := enc.DecodeRune(src)
			if r == encoding.RuneError && width < 3 {
				break
			}

			src = src[width:]
			sorted := unicaseInfo.unicodeSort(r)
			dst = append(dst, byte(sorted>>8), byte(sorted))
		}

		if numCodepoints == PadToMax {
			for len(dst)+1 < cap(dst) {
				dst = append(dst, 0x00, 0x20)
			}
			if len(dst) < cap(dst) {
				dst = append(dst, 0x00)
			}
		}
	} else {
		for numCodepoints > 0 {
			r, width := enc.DecodeRune(src)
			if r == encoding.RuneError && width < 3 {
				break
			}

			src = src[width:]
			sorted := unicaseInfo.unicodeSort(r)
			dst = append(dst, byte(sorted>>8), byte(sorted))
			numCodepoints--
		}
		for numCodepoints > 0 {
			dst = append(dst, 0x00, 0x20)
			numCodepoints--
		}
	}

	return dst
}

type Collation_utf8mb4_bin struct{}

func (c *Collation_utf8mb4_bin) init() {}

func (c *Collation_utf8mb4_bin) Id() ID {
	return 46
}

func (c *Collation_utf8mb4_bin) Name() string {
	return "utf8mb4_bin"
}

func (c *Collation_utf8mb4_bin) Collate(left, right []byte, isPrefix bool) int {
	return collationBinary(left, right, isPrefix)
}

func (c *Collation_utf8mb4_bin) WeightString(dst, src []byte, numCodepoints int) []byte {
	return weightStringUnicodeBinPad(encoding.Encoding_utf8mb4{}, dst, src, numCodepoints)
}

func (c *Collation_utf8mb4_bin) WeightStringLen(numBytes int) int {
	return ((numBytes + 3) / 4) * 3
}

func collationBinary(left, right []byte, rightPrefix bool) int {
	minLen := minInt(len(left), len(right))
	if diff := bytes.Compare(left[:minLen], right[:minLen]); diff != 0 {
		return diff
	}
	if rightPrefix {
		left = left[:minLen]
	}
	return len(left) - len(right)
}

func weightStringUnicodeBinPad(enc encoding.Encoding, dst []byte, src []byte, numCodepoints int) []byte {
	if numCodepoints == 0 || numCodepoints == PadToMax {
		for {
			r, width := enc.DecodeRune(src)
			if r == encoding.RuneError && width < 3 {
				break
			}

			src = src[width:]
			dst = append(dst, byte((r>>16)&0xFF), byte((r>>8)&0xFF), byte(r&0xFF))
		}

		if numCodepoints == PadToMax {
			for len(dst)+2 < cap(dst) {
				dst = append(dst, 0x00, 0x00, 0x20)
			}
			switch cap(dst) - len(dst) {
			case 0:
			case 1:
				dst = append(dst, 0x00)
			case 2:
				dst = append(dst, 0x00, 0x00)
			default:
				panic("unreachable")
			}
		}
	} else {
		for numCodepoints > 0 {
			r, width := enc.DecodeRune(src)
			if r == encoding.RuneError && width < 3 {
				break
			}

			src = src[width:]
			dst = append(dst, byte((r>>16)&0xFF), byte((r>>8)&0xFF), byte(r&0xFF))
			numCodepoints--
		}
		for numCodepoints > 0 {
			dst = append(dst, 0x00, 0x00, 0x20)
			numCodepoints--
		}
	}

	return dst
}
