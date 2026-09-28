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

package bytes2

import "strings"

// byteSetMax is the largest set a ByteSet can hold. It is the number of
// broadcast compares the vectorized Index does per block, so it is kept small
// enough that a block costs less than the scalar table walk it replaces.
const byteSetMax = 8

// ByteSet is a set of up to eight byte values that Index scans for. It is the
// "find the first byte that needs escaping" primitive behind the SQL literal
// encoders: the escaping loop asks where the next special byte is and copies
// the clean run in front of it in one write, instead of testing and writing
// one byte at a time.
//
// A ByteSet must come from NewByteSet. The zero value is not an empty set:
// its membership table holds nothing while its broadcast rows read as
// {0x00}, so the scalar and vectorized Index disagree on it. There is no
// empty set to represent anyway -- NewByteSet panics on one.
//
// A ByteSet is built once and shared; Index is safe for concurrent use.
type ByteSet struct {
	// table is the membership table the scalar Index walks.
	table [256]bool
	// bcast holds each member repeated across the widest vector row, so the
	// SIMD path can load a broadcast instead of building one. Every build
	// fills it; sets are built once and shared.
	bcast [byteSetMax][bcastWidth]byte
}

// bcastWidth is the widest vector any supported architecture offers, in
// bytes (AVX-512).
const bcastWidth = 64

// NewByteSet returns the set of vals. It panics if vals is empty or has more
// than eight members.
func NewByteSet(vals ...byte) *ByteSet {
	if len(vals) == 0 || len(vals) > byteSetMax {
		panic("bytes2.NewByteSet: a set needs between 1 and 8 members")
	}
	s := &ByteSet{}
	for _, v := range vals {
		s.table[v] = true
	}
	for i := range s.bcast {
		v := vals[0]
		if i < len(vals) {
			v = vals[i]
		}
		for j := range s.bcast[i] {
			s.bcast[i][j] = v
		}
	}
	return s
}

// indexScalar is the reference Index: a table lookup per byte.
func (s *ByteSet) indexScalar(b []byte) int {
	for i, c := range b {
		if s.table[c] {
			return i
		}
	}
	return -1
}

// indexAny2Window is the first window IndexAny2 scans. It covers a typical
// string literal whole, so the common call is still two IndexByte scans over
// the input, and it bounds what a call can spend on a far-off byte.
const indexAny2Window = 256

// IndexAny2 returns the index of the first byte of s that is a or c, or -1 if
// neither occurs. It is two strings.IndexByte scans per window, the second
// only over the bytes in front of the first hit. IndexByte is hand-tuned
// assembly with a native movemask on every architecture Vitess builds for; a
// portable simd kernel was measured 2-3x slower than this on arm64 because it
// has to store and rescan the compare mask per block, so there is no simd
// variant. It takes a string because its one caller has one, and passing the
// bytes through an unsafe view to get here is not worth the conversion.
//
// The scan runs in windows that start at indexAny2Window bytes and quadruple,
// rather than over all of s at once, because c can only be searched in front
// of a: with no window, a literal whose backslash is early and whose closing
// quote is far away pays the whole scan to the quote before it can look for
// the backslash. Measured on arm64, a 64KB literal with an escape at byte 9
// costs 6ns windowed against 644ns unwindowed. The fixed first window is paid
// even for a nearby hit; after that, geometric growth keeps the total work
// bounded by the initial window plus a constant factor of the distance.
func IndexAny2(s string, a, c byte) int {
	window := indexAny2Window
	for off := 0; off < len(s); {
		end := off + min(window, len(s)-off)
		w := s[off:end]
		i := strings.IndexByte(w, a)
		if i >= 0 {
			w = w[:i]
		}
		if j := strings.IndexByte(w, c); j >= 0 {
			return off + j
		}
		if i >= 0 {
			return off + i
		}
		off = end
		remaining := len(s) - off
		if window > remaining/4 {
			window = remaining
		} else {
			window *= 4
		}
	}
	return -1
}
