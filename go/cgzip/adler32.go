// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cgzip

/*
#cgo CFLAGS: -Werror=implicit
#cgo pkg-config: zlib

#include "zlib.h"
*/
import "C"

import (
	"hash"
	"unsafe"
)

type adler32Hash struct {
	adler C.uLong
}

// an empty buffer has an adler32 of '1' by default, so start with that
// (the go hash/adler32 does the same)
func NewAdler32() hash.Hash32 {
	a := &adler32Hash{}
	a.Reset()
	return a
}

// io.Writer interface
func (a *adler32Hash) Write(p []byte) (n int, err error) {
	if len(p) > 0 {
		a.adler = C.adler32(a.adler, (*C.Bytef)(unsafe.Pointer(&p[0])), (C.uInt)(len(p)))
	}
	return len(p), nil
}

// hash.Hash interface
func (a *adler32Hash) Sum(b []byte) []byte {
	s := a.Sum32()
	b = append(b, byte(s>>24))
	b = append(b, byte(s>>16))
	b = append(b, byte(s>>8))
	b = append(b, byte(s))
	return b
}

func (a *adler32Hash) Reset() {
	a.adler = C.adler32(0, (*C.Bytef)(unsafe.Pointer(nil)), 0)
}

func (a *adler32Hash) Size() int {
	return 4
}

func (a *adler32Hash) BlockSize() int {
	return 1
}

// hash.Hash32 interface
func (a *adler32Hash) Sum32() uint32 {
	return uint32(a.adler)
}

// helper method for partial checksums. From the zlib.h header:
//
//   Combine two Adler-32 checksums into one.  For two sequences of bytes, seq1
// and seq2 with lengths len1 and len2, Adler-32 checksums were calculated for
// each, adler1 and adler2.  adler32_combine() returns the Adler-32 checksum of
// seq1 and seq2 concatenated, requiring only adler1, adler2, and len2.
func Adler32Combine(adler1, adler2 uint32, len2 int) uint32 {
	return uint32(C.adler32_combine(C.uLong(adler1), C.uLong(adler2), C.z_off_t(len2)))
}
