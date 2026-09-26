//go:build goexperiment.simd && amd64

package simdbench

import (
	"fmt"
	"math/bits"
	"simd/archsimd"
	"testing"
)

func nextEscapeAVX2Min(b []byte) int {
	i := 0
	if hasAVX2 {
		lim := archsimd.BroadcastUint8x32(0x1f)
		q := archsimd.BroadcastUint8x32('\'')
		bs := archsimd.BroadcastUint8x32('\\')
		for ; i+32 <= len(b); i += 32 {
			v := archsimd.LoadUint8x32Array((*[32]byte)(b[i : i+32]))
			// v <= 0x1f  <=>  min(v, 0x1f) == v
			m := v.Min(lim).Equal(v).Or(v.Equal(q)).Or(v.Equal(bs)).ToBits()
			if m != 0 {
				return i + bits.TrailingZeros32(m)
			}
		}
	}
	for ; i < len(b); i++ {
		if encodeMap[b[i]] != dontEscape {
			return i
		}
	}
	return -1
}

func BenchmarkNext(b *testing.B) {
	for _, n := range sizes {
		p := mkPayload(n, 0)
		for name, f := range map[string]func([]byte) int{"scalar": nextEscapeScalar, "avx2-less": nextEscapeAVX2, "avx2-min": nextEscapeAVX2Min} {
			b.Run(fmt.Sprintf("n=%d/%s", n, name), func(b *testing.B) {
				b.SetBytes(int64(n))
				for b.Loop() {
					f(p)
				}
			})
		}
	}
}
