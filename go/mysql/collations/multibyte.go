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
	"vitess.io/vitess/go/mysql/collations/internal/charset"
)

type Collation_multibyte struct {
	id      ID
	name    string
	sort    []byte
	charset charset.Charset
}

func (c *Collation_multibyte) init() {}

func (c *Collation_multibyte) Id() ID {
	return c.id
}

func (c *Collation_multibyte) Name() string {
	return c.name
}

func (c *Collation_multibyte) Collate(left, right []byte, isPrefix bool) int {
	if c.sort == nil {
		return collationBinary(left, right, isPrefix)
	}

	cmpLen := minInt(len(left), len(right))
	cs := c.charset
	sortOrder := c.sort[:256]
	for i := 0; i < cmpLen; i++ {
		sortL, sortR := left[i], right[i]
		if sortL > 127 {
			if sortL != sortR {
				return int(sortL) - int(sortR)
			}
			_, widthL := cs.DecodeRune(left[i:])
			_, widthR := cs.DecodeRune(right[i:])
			switch minInt(widthL, widthR) {
			case 4:
				i++
				if left[i] != right[i] {
					return int(left[i]) - int(right[i])
				}
				fallthrough
			case 3:
				i++
				if left[i] != right[i] {
					return int(left[i]) - int(right[i])
				}
				fallthrough
			case 2:
				i++
				if left[i] != right[i] {
					return int(left[i]) - int(right[i])
				}
				fallthrough
			case 1:
			}
		} else {
			sortL, sortR = sortOrder[sortL], sortOrder[sortR]
			if sortL != sortR {
				return int(sortL) - int(sortR)
			}
		}
	}

	if isPrefix {
		left = left[:cmpLen]
	}
	return len(left) - len(right)
}

func (c *Collation_multibyte) WeightString(dst, src []byte, numCodepoints int) []byte {
	cs := c.charset
	sortOrder := c.sort

	if numCodepoints == 0 || numCodepoints == PadToMax {
		for len(src) > 0 {
			w := src[0]
			if w <= 127 {
				if sortOrder != nil {
					w = sortOrder[w]
				}
				dst = append(dst, w)
				src = src[1:]
			} else {
				_, width := cs.DecodeRune(src)
				dst = append(dst, src[:width]...)
				src = src[width:]
			}
		}
		if numCodepoints == PadToMax {
			for len(dst) < cap(dst) {
				dst = append(dst, ' ')
			}
		}
	} else {
		for len(src) > 0 && numCodepoints > 0 {
			w := src[0]
			if w <= 127 {
				if sortOrder != nil {
					w = sortOrder[w]
				}
				dst = append(dst, w)
				src = src[1:]
			} else {
				_, width := cs.DecodeRune(src)
				dst = append(dst, src[:width]...)
				src = src[width:]
			}
			numCodepoints--
		}
		for numCodepoints > 0 {
			dst = append(dst, ' ')
			numCodepoints--
		}
	}

	return dst
}

func (c *Collation_multibyte) WeightStringLen(numCodepoints int) int {
	return numCodepoints
}

func (c *Collation_multibyte) Charset() charset.Charset {
	return c.charset
}
