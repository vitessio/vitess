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

func init() {
	register(&Collation_binary{})
}

type simpletables struct {
	tounicode []uint16
	tolower   []byte
	toupper   []byte
	ctype     []byte
	sort      []byte
}

type Collation_8bit_bin struct {
	id   ID
	name string
	simpletables
}

func (c *Collation_8bit_bin) init() {}

func (c *Collation_8bit_bin) Name() string {
	return c.name
}
func (c *Collation_8bit_bin) Id() ID {
	return c.id
}

func (c *Collation_8bit_bin) Collate(left, right []byte, rightIsPrefix bool) int {
	return collationBinary(left, right, rightIsPrefix)
}

func (c *Collation_8bit_bin) WeightString(dst, src []byte, numCodepoints int) []byte {
	copyCodepoints := minInt(len(src), cap(dst))

	var padToMax bool
	switch numCodepoints {
	case 0:
		numCodepoints = copyCodepoints
	case PadToMax:
		padToMax = true
	default:
		copyCodepoints = minInt(copyCodepoints, numCodepoints)
	}

	dst = append(dst, src[:copyCodepoints]...)
	return weightStringPadingSimple(' ', dst, numCodepoints-copyCodepoints, padToMax)
}

func (c *Collation_8bit_bin) WeightStringLen(numBytes int) int {
	return numBytes
}

type Collation_8bit_simple_ci struct {
	id   ID
	name string
	simpletables
}

func (c *Collation_8bit_simple_ci) init() {}

func (c *Collation_8bit_simple_ci) Name() string {
	return c.name
}

func (c *Collation_8bit_simple_ci) Id() ID {
	return c.id
}

func (c *Collation_8bit_simple_ci) Collate(left, right []byte, rightIsPrefix bool) int {
	sortOrder := c.sort
	cmpLen := minInt(len(left), len(right))

	for i := 0; i < cmpLen; i++ {
		sortL, sortR := sortOrder[int(left[i])], sortOrder[int(right[i])]
		if sortL != sortR {
			return int(sortL) - int(sortR)
		}
	}
	if rightIsPrefix {
		left = left[:cmpLen]
	}
	return len(left) - len(right)
}

func (c *Collation_8bit_simple_ci) WeightString(dst, src []byte, numCodepoints int) []byte {
	padToMax := false
	sortOrder := c.sort
	copyCodepoints := minInt(len(src), cap(dst))

	switch numCodepoints {
	case 0:
		numCodepoints = copyCodepoints
	case PadToMax:
		padToMax = true
	default:
		copyCodepoints = minInt(copyCodepoints, numCodepoints)
	}

	for _, ch := range src[:copyCodepoints] {
		dst = append(dst, sortOrder[int(ch)])
	}
	return weightStringPadingSimple(' ', dst, numCodepoints-copyCodepoints, padToMax)
}

func (c *Collation_8bit_simple_ci) WeightStringLen(numBytes int) int {
	return numBytes
}

func weightStringPadingSimple(padChar byte, dst []byte, numCodepoints int, padToMax bool) []byte {
	if padToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, padChar)
		}
	} else {
		for numCodepoints > 0 {
			dst = append(dst, padChar)
			numCodepoints--
		}
	}
	return dst
}

type Collation_binary struct{}

func (c *Collation_binary) init() {}

func (c *Collation_binary) Id() ID {
	return 63
}

func (c *Collation_binary) Name() string {
	return "binary"
}

func (c *Collation_binary) Collate(left, right []byte, isPrefix bool) int {
	return collationBinary(left, right, isPrefix)
}

func (c *Collation_binary) WeightString(dst, src []byte, numCodepoints int) []byte {
	padToMax := false
	copyCodepoints := minInt(len(src), cap(dst))

	switch numCodepoints {
	case 0: // no-op
	case PadToMax:
		padToMax = true
	default:
		copyCodepoints = minInt(copyCodepoints, numCodepoints)
	}

	dst = append(dst, src[:copyCodepoints]...)
	if padToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, 0x0)
		}
	}
	return dst
}

func (c *Collation_binary) WeightStringLen(numBytes int) int {
	return numBytes
}
