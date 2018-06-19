// +build cgo

/*
Copyright 2017 Google Inc.

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

type crc32Hash struct {
	crc C.uLong
}

// NewCrc32 creates an empty buffer which has an crc32 of '1'. The go
// hash/crc32 does the same.
func NewCrc32() hash.Hash32 {
	c := &crc32Hash{}
	c.Reset()
	return c
}

// io.Writer interface
func (a *crc32Hash) Write(p []byte) (n int, err error) {
	if len(p) > 0 {
		a.crc = C.crc32(a.crc, (*C.Bytef)(unsafe.Pointer(&p[0])), (C.uInt)(len(p)))
	}
	return len(p), nil
}

// hash.Hash interface
func (a *crc32Hash) Sum(b []byte) []byte {
	s := a.Sum32()
	b = append(b, byte(s>>24))
	b = append(b, byte(s>>16))
	b = append(b, byte(s>>8))
	b = append(b, byte(s))
	return b
}

func (a *crc32Hash) Reset() {
	a.crc = C.crc32(0, (*C.Bytef)(unsafe.Pointer(nil)), 0)
}

func (a *crc32Hash) Size() int {
	return 4
}

func (a *crc32Hash) BlockSize() int {
	return 1
}

// hash.Hash32 interface
func (a *crc32Hash) Sum32() uint32 {
	return uint32(a.crc)
}
