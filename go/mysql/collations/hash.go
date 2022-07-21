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

// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// all the code in this file has been adapted from the internal hash implementation in the Go runtime:
// https://golang.org/src/runtime/hash64.go

package collations

import (
	"math/bits"
	"unsafe"
)

func memhash(src []byte, seed uintptr) uintptr {
	srclen := len(src)
	if srclen == 0 {
		return seed ^ m1
	}
	return memhashraw(unsafe.Pointer(&src[0]), seed, uintptr(srclen))
}

func memhashraw(p unsafe.Pointer, seed, s uintptr) uintptr {
	var a, b uintptr
	seed ^= m1
	switch {
	case s == 0:
		return seed
	case s < 4:
		a = uintptr(*(*byte)(p))
		a |= uintptr(*(*byte)(unsafe.Add(p, s>>1))) << 8
		a |= uintptr(*(*byte)(unsafe.Add(p, s-1))) << 16
	case s == 4:
		a = r4(p)
		b = a
	case s < 8:
		a = r4(p)
		b = r4(unsafe.Add(p, s-4))
	case s == 8:
		a = r8(p)
		b = a
	case s <= 16:
		a = r8(p)
		b = r8(unsafe.Add(p, s-8))
	default:
		l := s
		if l > 48 {
			seed1 := seed
			seed2 := seed
			for ; l > 48; l -= 48 {
				seed = mix(r8(p)^m2, r8(unsafe.Add(p, 8))^seed)
				seed1 = mix(r8(unsafe.Add(p, 16))^m3, r8(unsafe.Add(p, 24))^seed1)
				seed2 = mix(r8(unsafe.Add(p, 32))^m4, r8(unsafe.Add(p, 40))^seed2)
				p = unsafe.Add(p, 48)
			}
			seed ^= seed1 ^ seed2
		}
		for ; l > 16; l -= 16 {
			seed = mix(r8(p)^m2, r8(unsafe.Add(p, 8))^seed)
			p = unsafe.Add(p, 16)
		}
		a = r8(unsafe.Add(p, l-16))
		b = r8(unsafe.Add(p, l-8))
	}

	return mix(m5^s, mix(a^m2, b^seed))
}

func memhash128(p unsafe.Pointer, seed uintptr) uintptr {
	a := r8(p)
	b := r8(unsafe.Add(p, 8))
	return mix(m5^16, mix(a^m2, b^seed^m1))
}

func memhash8(p uint8, seed uintptr) uintptr {
	a := uintptr(p)
	return mix(m5^1, mix(a^m2, a^seed^m1))
}

func memhash16(p uint16, seed uintptr) uintptr {
	a := uintptr(p)
	return mix(m5^2, mix(a^m2, a^seed^m1))
}

func memhash32(p uint32, seed uintptr) uintptr {
	a := uintptr(p)
	return mix(m5^4, mix(a^m2, a^seed^m1))
}

func mix(a, b uintptr) uintptr {
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	return uintptr(hi ^ lo)
}

func r4(p unsafe.Pointer) uintptr {
	return uintptr(readUnaligned32(p))
}

func r8(p unsafe.Pointer) uintptr {
	return uintptr(readUnaligned64(p))
}

func readUnaligned32(p unsafe.Pointer) uint32 {
	q := (*[4]byte)(p)
	return uint32(q[0]) | uint32(q[1])<<8 | uint32(q[2])<<16 | uint32(q[3])<<24
}

func readUnaligned64(p unsafe.Pointer) uint64 {
	q := (*[8]byte)(p)
	return uint64(q[0]) | uint64(q[1])<<8 | uint64(q[2])<<16 | uint64(q[3])<<24 | uint64(q[4])<<32 | uint64(q[5])<<40 | uint64(q[6])<<48 | uint64(q[7])<<56
}
