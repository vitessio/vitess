//go:build simd && goexperiment.simd && (amd64 || arm64)

/*
Copyright 2026 The Vitess Authors.

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

package uca

import (
	"fmt"
	"simd"
	"testing"

	"github.com/stretchr/testify/require"
)

// vectorWidth is the native vector width in bytes. The kernel has two
// guards, not one: it takes the scalar path below simdThreshold and again
// below one whole vector, so a test that knows only the threshold expects
// vector work the kernel does not do. The two coincide at 16-byte NEON,
// which is why arm64 alone never notices; at 64-byte AVX-512 the band
// between them is 32 bytes wide.
func vectorWidth() int {
	var zero simd.Uint8s
	return zero.Len()
}

// requireVectors skips when the simd package is emulating vectors in
// software. init raises simdThreshold past any input length in that build, so
// every case below would read as "under a guard" and the comparison against
// the scalar reference would never run -- the tests would pass having checked
// nothing. GODEBUG=simd=0 asks for emulation, and so does a CPU whose feature
// check comes back short. Skip out loud instead.
func requireVectors(tb testing.TB) {
	tb.Helper()
	if simd.Emulated() {
		tb.Skip("simd is emulated, so equalASCIIPrefix always returns 0 and there is no kernel to compare")
	}
}

// TestEqualASCIIPrefixMatchesReference pins the vectorized kernel to the
// scalar block loop for every input long enough to take the vector path,
// and pins it to the fallback for everything between the two guards. The
// count at the end fails the test if no case reached the comparison, so a
// future threshold change cannot quietly empty it out.
func TestEqualASCIIPrefixMatchesReference(t *testing.T) {
	requireVectors(t)
	w := vectorWidth()
	compared := 0
	for _, tc := range prefixCases() {
		n := min(len(tc.p1), len(tc.p2))
		switch {
		case n < simdThreshold:
			continue
		case n < w:
			require.Zerof(t, equalASCIIPrefix(tc.p1, tc.p2),
				"%s: %d bytes is under one %d-byte vector, so the scalar loop does the walk", tc.name, n, w)
		default:
			require.Equalf(t, refEqualASCIIPrefix(tc.p1, tc.p2), equalASCIIPrefix(tc.p1, tc.p2), tc.name)
			compared++
		}
	}
	require.NotZero(t, compared, "no case reached the reference comparison, so the kernel went unchecked")
}

// TestFastForward32SkipMatchesReference pins what the skip leaves behind,
// not just how far it goes: after FastForward32 the return value, both
// inputs and the unicode counter must match the loop that walked every
// block itself. A miscounted `it.unicode` would not change a comparison,
// only when the fast path switches itself off, which the collation golden
// tests cannot see. Counters at and past maxUnicodeBlocks cover the early
// return.
func TestFastForward32SkipMatchesReference(t *testing.T) {
	for _, tc := range prefixCases() {
		for _, unicode := range []int{0, 1, maxUnicodeBlocks, maxUnicodeBlocks + 1} {
			name := fmt.Sprintf("%s (unicode=%d)", tc.name, unicode)
			ref, ref2 := fastForwardIterator(tc.p1, unicode), fastForwardIterator(tc.p2, 0)
			got, got2 := fastForwardIterator(tc.p1, unicode), fastForwardIterator(tc.p2, 0)
			require.Equal(t, refFastForward32(ref, ref2), got.FastForward32(got2), "%s: return", name)
			require.Equal(t, len(ref.input), len(got.input), "%s: it.input", name)
			require.Equal(t, len(ref2.input), len(got2.input), "%s: it2.input", name)
			require.Equal(t, ref.unicode, got.unicode, "%s: it.unicode", name)
		}
	}
}

func FuzzEqualASCIIPrefix(f *testing.F) {
	requireVectors(f)
	f.Add(asciiRun(64), asciiRun(64))
	f.Add(asciiRun(33), append(asciiRun(32), 0xC3))
	f.Add(asciiRun(20), asciiRun(17))
	w := vectorWidth()
	f.Fuzz(func(t *testing.T, p1, p2 []byte) {
		got := equalASCIIPrefix(p1, p2)
		if n := min(len(p1), len(p2)); n < simdThreshold || n < w {
			if got != 0 {
				t.Fatalf("input of %d bytes is under a guard but took the vector path: got %d", n, got)
			}
			return
		}
		if ref := refEqualASCIIPrefix(p1, p2); got != ref {
			t.Fatalf("equalASCIIPrefix(%q, %q) = %d, want %d", p1, p2, got, ref)
		}
	})
}
