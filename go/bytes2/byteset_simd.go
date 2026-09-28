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

package bytes2

import (
	"math"
	"math/bits"
	"simd"
)

// simdThreshold keeps short inputs on the scalar path; init disables SIMD
// when vectors are emulated. One comparison keeps Index within the inlining
// budget.
var simdThreshold = 16

func init() {
	if simd.Emulated() {
		simdThreshold = math.MaxInt
	}
}

// laneBuf holds a stored byte mask: 64 bytes covers the widest vector. The
// caller owns one per call rather than per block, since Store overwrites the
// words that are read and zeroing 64 bytes per block is measurable. It and
// firstLane stay unexported per §6 rule 6, so collations/uca carries a copy.
type laneBuf [8]uint64

// firstLane returns the index of the first true lane of m, given n lanes, or
// -1 if none is set. The simd package has no movemask, so the mask is stored
// as 0xFF-per-true-lane bytes and scanned a word at a time.
func firstLane(m simd.Mask8s, n int, tmp *laneBuf) int {
	m.ToInt8s().ToBits().ReshapeToUint64s().Store(tmp[:])
	for j := 0; j < n/8; j++ {
		if w := tmp[j]; w != 0 {
			return j*8 + bits.TrailingZeros64(w)/8
		}
	}
	return -1
}

// match8 reports the lanes of x equal to any of the eight broadcast set
// members. It is a plain function with vector parameters rather than a
// closure over the members: the compiler cannot clone closures or methods
// that hold vector values for the experiment (go1.27.1 fails with an
// internal error, "missing Types entry"), and a plain function inlines.
func match8(x, v0, v1, v2, v3, v4, v5, v6, v7 simd.Uint8s) simd.Mask8s {
	return x.Equal(v0).Or(x.Equal(v1)).Or(x.Equal(v2)).Or(x.Equal(v3)).
		Or(x.Equal(v4)).Or(x.Equal(v5)).Or(x.Equal(v6)).Or(x.Equal(v7))
}

// Index returns the index of the first byte of b that is in s, or -1 if none
// is.
//
// The vector work lives in indexSIMD, a plain function, because the go1.27.1
// compiler fails with an internal error when a method with a receiver uses
// simd vector values directly (see match8). A method that only calls such a
// function is fine.
func (s *ByteSet) Index(b []byte) int {
	if len(b) < simdThreshold {
		return s.indexScalar(b)
	}
	return indexSIMD(s, b)
}

func indexSIMD(s *ByteSet, b []byte) int {
	// The lane count comes from a zero vector so the "shorter than one
	// vector" check runs before any load: on the wider amd64 lanes the
	// threshold lets 16..n-1 byte inputs through, and they should not pay
	// for eight vector loads on their way to the table walk. The
	// overlapping tail below also needs at least one full block.
	var zero simd.Uint8s
	n := zero.Len()
	if len(b) < n {
		return s.indexScalar(b)
	}
	// Each member comes in as one vector load from its pre-broadcast row
	// rather than a scalar load, a lane insert and a duplicate.
	v0 := simd.LoadUint8s(s.bcast[0][:])
	v1 := simd.LoadUint8s(s.bcast[1][:])
	v2 := simd.LoadUint8s(s.bcast[2][:])
	v3 := simd.LoadUint8s(s.bcast[3][:])
	v4 := simd.LoadUint8s(s.bcast[4][:])
	v5 := simd.LoadUint8s(s.bcast[5][:])
	v6 := simd.LoadUint8s(s.bcast[6][:])
	v7 := simd.LoadUint8s(s.bcast[7][:])
	var tmp laneBuf

	i := 0
	for ; i+n <= len(b); i += n {
		x := simd.LoadUint8s(b[i : i+n])
		if k := firstLane(match8(x, v0, v1, v2, v3, v4, v5, v6, v7), n, &tmp); k >= 0 {
			return i + k
		}
	}
	if i < len(b) {
		// Read the tail as an overlapping full block; bytes the loop cleared
		// cannot flag, and avoiding a partial load also avoids spilling the
		// vectors.
		start := len(b) - n
		x := simd.LoadUint8s(b[start:])
		if k := firstLane(match8(x, v0, v1, v2, v3, v4, v5, v6, v7), n, &tmp); k >= 0 {
			return start + k
		}
	}
	return -1
}
