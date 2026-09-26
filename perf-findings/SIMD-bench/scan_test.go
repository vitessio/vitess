//go:build goexperiment.simd && amd64

package simdbench

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

func mkPayload(n int, escapeEvery int) []byte {
	r := rand.New(rand.NewPCG(1, 2))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + r.IntN(26))
		if escapeEvery > 0 && i%escapeEvery == escapeEvery-1 {
			b[i] = '\n'
		}
	}
	return b
}

var sizes = []int{8, 32, 128, 1024, 16384}

func TestEquivalence(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	alphabet := []byte("abc'\\\n\x00\x1a%_\"é")
	for n := 0; n < 300; n++ {
		for k := 0; k < 20; k++ {
			b := make([]byte, n)
			for i := range b {
				b[i] = alphabet[r.IntN(len(alphabet))]
			}
			var a, s, v strings.Builder
			escapeCurrent(b, &a)
			escapeRuns(b, &s, nextEscapeScalar)
			escapeRuns(b, &v, nextEscapeAVX2)
			if a.String() != s.String() || a.String() != v.String() {
				t.Fatalf("mismatch on %q", b)
			}
			want := indexQuoteOrBackslashScalar(string(b), '\'')
			if got := indexQuoteOrBackslashAVX2Bytes(b, '\''); got != want {
				t.Fatalf("avx2 index %d != %d on %q", got, want, b)
			}
			if got := indexQuoteOrBackslashPortable(b, '\''); got != want {
				t.Fatalf("portable index %d != %d on %q", got, want, b)
			}
		}
	}
}

func BenchmarkFindDelim(b *testing.B) {
	for _, n := range sizes {
		p := mkPayload(n, 0)
		s := string(p)
		b.Run(fmt.Sprintf("n=%d/scalar", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				indexQuoteOrBackslashScalar(s, '\'')
			}
		})
		b.Run(fmt.Sprintf("n=%d/avx2", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				indexQuoteOrBackslashAVX2Bytes(p, '\'')
			}
		})
		b.Run(fmt.Sprintf("n=%d/portable", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				indexQuoteOrBackslashPortable(p, '\'')
			}
		})
		b.Run(fmt.Sprintf("n=%d/stdlib-IndexByte", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				bytes.IndexByte(p, '\'') // single-byte baseline (comments, '\n', etc.)
			}
		})
	}
}

func BenchmarkEscape(b *testing.B) {
	for _, n := range sizes {
		for _, every := range []int{0, 64} {
			p := mkPayload(n, every)
			var sb strings.Builder
			sb.Grow(2*n + 2)
			run := func(name string, f func()) {
				b.Run(fmt.Sprintf("n=%d/esc1per%d/%s", n, every, name), func(b *testing.B) {
					b.SetBytes(int64(n))
					for b.Loop() {
						sb.Reset()
						f()
					}
				})
			}
			run("current", func() { escapeCurrent(p, &sb) })
			run("runs-scalar", func() { escapeRuns(p, &sb, nextEscapeScalar) })
			run("runs-avx2", func() { escapeRuns(p, &sb, nextEscapeAVX2) })
		}
	}
}
