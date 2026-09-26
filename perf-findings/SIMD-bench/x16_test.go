//go:build goexperiment.simd && amd64

package simdbench

import (
	"fmt"
	"math/bits"
	"simd/archsimd"
	"strings"
	"testing"
)

func nextEscapeSSE(b []byte) int {
	i := 0
	lim := archsimd.BroadcastUint8x16(0x1f)
	q := archsimd.BroadcastUint8x16('\'')
	bs := archsimd.BroadcastUint8x16('\\')
	for ; i+16 <= len(b); i += 16 {
		v := archsimd.LoadUint8x16Array((*[16]byte)(b[i : i+16]))
		m := v.Min(lim).Equal(v).Or(v.Equal(q)).Or(v.Equal(bs)).ToBits()
		if m != 0 {
			return i + bits.TrailingZeros16(m)
		}
	}
	for ; i < len(b); i++ {
		if encodeMap[b[i]] != dontEscape {
			return i
		}
	}
	return -1
}

func BenchmarkEscape2(b *testing.B) {
	for _, n := range sizes {
		for _, every := range []int{0, 64} {
			p := mkPayload(n, every)
			var sb strings.Builder
			sb.Grow(2*n + 2)
			for _, c := range []struct {
				name string
				f    func([]byte) int
			}{{"runs-scalar", nextEscapeScalar}, {"runs-avx2-min", nextEscapeAVX2Min}, {"runs-x16", nextEscapeSSE}} {
				b.Run(fmt.Sprintf("n=%d/esc1per%d/%s", n, every, c.name), func(b *testing.B) {
					b.SetBytes(int64(n))
					for b.Loop() {
						sb.Reset()
						escapeRuns(p, &sb, c.f)
					}
				})
			}
		}
	}
}

func TestSSE(t *testing.T) {
	for n := 0; n < 200; n++ {
		p := mkPayload(n, 7)
		if nextEscapeSSE(p) != nextEscapeScalar(p) {
			t.Fatal(n)
		}
	}
}
