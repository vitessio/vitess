package bitset

import (
	"math/bits"
	"unsafe"
)

type Bitset string

const bitsetWidth = 8

func bitsetWordSize(max int) int {
	return max/bitsetWidth + 1
}

func toBitset(words []byte) Bitset {
	if len(words) == 0 {
		return ""
	}
	return *(*Bitset)(unsafe.Pointer(&words))
}

func minlen(a, b Bitset) int {
	if len(a) < len(b) {
		return len(a)
	}
	return len(b)
}

func (bs Bitset) Overlaps(b2 Bitset) bool {
	min := minlen(bs, b2)
	for i := 0; i < min; i++ {
		if bs[i]&b2[i] != 0 {
			return true
		}
	}
	return false
}

func (bs Bitset) Or(b2 Bitset) Bitset {
	if len(bs) == 0 {
		return b2
	}
	if len(b2) == 0 {
		return bs
	}

	small, large := bs, b2
	if len(small) > len(large) {
		small, large = large, small
	}

	merged := make([]byte, len(large))
	m := 0

	for m < len(small) {
		merged[m] = small[m] | large[m]
		m++
	}
	for m < len(large) {
		merged[m] = large[m]
		m++
	}
	return toBitset(merged)
}

func (bs Bitset) AndNot(b2 Bitset) Bitset {
	if len(b2) == 0 {
		return bs
	}

	merged := make([]byte, len(bs))
	m := 0

	for m = 0; m < len(bs); m++ {
		if m < len(b2) {
			merged[m] = bs[m] & ^b2[m]
		} else {
			merged[m] = bs[m]
		}
	}
	for ; m > 0; m-- {
		if merged[m-1] != 0 {
			break
		}
	}
	return toBitset(merged[:m])
}

func (bs Bitset) And(b2 Bitset) Bitset {
	if len(bs) == 0 || len(b2) == 0 {
		return ""
	}

	merged := make([]byte, minlen(bs, b2))
	m := 0

	for m = 0; m < len(merged); m++ {
		merged[m] = bs[m] & b2[m]
	}
	for ; m > 0; m-- {
		if merged[m-1] != 0 {
			break
		}
	}
	return toBitset(merged[:m])
}

func (bs Bitset) Set(offset int) Bitset {
	alloc := len(bs)
	if max := bitsetWordSize(offset); max > alloc {
		alloc = max
	}

	words := make([]byte, alloc)
	copy(words, bs)
	words[offset/bitsetWidth] |= 1 << (offset % bitsetWidth)
	return toBitset(words)
}

func (bs Bitset) SingleBit() int {
	offset := -1
	for i := 0; i < len(bs); i++ {
		t := bs[i]
		if t == 0 {
			continue
		}
		if offset >= 0 || bits.OnesCount8(t) != 1 {
			return -1
		}
		offset = i*bitsetWidth + bits.TrailingZeros8(t)
	}
	return offset
}

func (bs Bitset) IsContainedBy(b2 Bitset) bool {
	if len(bs) > len(b2) {
		return false
	}
	for i := 0; i < len(bs); i++ {
		left := bs[i]
		rigt := b2[i]
		if left&rigt != left {
			return false
		}
	}
	return true
}

func (bs Bitset) Popcount() (count int) {
	for i := 0; i < len(bs); i++ {
		count += bits.OnesCount8(bs[i])
	}
	return
}

func (bs Bitset) ForEach(yield func(int)) {
	for i := 0; i < len(bs); i++ {
		bitset := bs[i]
		for bitset != 0 {
			t := bitset & -bitset
			r := bits.TrailingZeros8(bitset)
			yield(i*bitsetWidth + r)
			bitset ^= t
		}
	}
}

func Build(bits ...int) Bitset {
	if len(bits) == 0 {
		return ""
	}

	max := bits[0]
	for _, b := range bits[1:] {
		if b > max {
			max = b
		}
	}

	words := make([]byte, bitsetWordSize(max))
	for _, b := range bits {
		words[b/bitsetWidth] |= 1 << (b % bitsetWidth)
	}
	return toBitset(words)
}

const singleton = "\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x20\x00\x00\x00\x40\x00\x00\x00\x80"

func Single(bit int) Bitset {
	switch {
	case bit < 8:
		bit = (bit + 1) << 2
		return Bitset(singleton[bit-1 : bit])
	case bit < 16:
		bit = (bit + 1 - 8) << 2
		return Bitset(singleton[bit-2 : bit])
	case bit < 24:
		bit = (bit + 1 - 16) << 2
		return Bitset(singleton[bit-3 : bit])
	case bit < 32:
		bit = (bit + 1 - 24) << 2
		return Bitset(singleton[bit-4 : bit])
	default:
		words := make([]byte, bitsetWordSize(bit))
		words[bit/bitsetWidth] |= 1 << (bit % bitsetWidth)
		return toBitset(words)
	}
}
