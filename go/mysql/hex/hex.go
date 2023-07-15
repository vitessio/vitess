package hex

import (
	"encoding/hex"
	"math/bits"
)

const hextable = "0123456789ABCDEF"

func EncodeBytes(src []byte) []byte {
	j := 0
	dst := make([]byte, len(src)*2)
	for _, v := range src {
		dst[j] = hextable[v>>4]
		dst[j+1] = hextable[v&0x0f]
		j += 2
	}
	return dst
}

func EncodeUint(u uint64) []byte {
	var a [16 + 1]byte
	i := len(a)
	shift := uint(bits.TrailingZeros(uint(16))) & 7
	b := uint64(16)
	m := uint(16) - 1 // == 1<<shift - 1

	for u >= b {
		i--
		a[i] = hextable[uint(u)&m]
		u >>= shift
	}

	// u < base
	i--
	a[i] = hextable[uint(u)]
	return a[i:]
}

func DecodeUint(u uint64) []byte {
	if u == 0 {
		return []byte{0}
	}
	var decoded []byte
	for u > 0 {
		c1 := u % 10
		c2 := u % 100 / 10
		decoded = append([]byte{byte(c1 + c2<<4)}, decoded...)
		u /= 100
	}
	return decoded
}

func DecodedLen(src []byte) int {
	return (len(src) + 1) / 2
}

func DecodeBytes(dst, src []byte) bool {
	if len(src)&1 == 1 {
		src = append([]byte{'0'}, src...)
	}
	_, err := hex.Decode(dst, src)
	return err == nil
}
