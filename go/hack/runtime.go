//go:build gc && !wasm

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

package hack

import (
	"unsafe"
)

//go:noescape
//go:linkname strhash runtime.strhash
func strhash(p unsafe.Pointer, h uintptr) uintptr

// RuntimeStrhash provides access to the Go runtime's default hash function for strings.
// This is an optimal hash function which takes an input seed and is potentially implemented in hardware
// for most architectures. This is the same hash function that the language's `map` uses.
func RuntimeStrhash(str string, seed uint64) uint64 {
	return uint64(strhash(unsafe.Pointer(&str), uintptr(seed)))
}

//go:linkname roundupsize runtime.roundupsize
func roundupsize(size uintptr) uintptr

// RuntimeAllocSize returns size of the memory block that mallocgc will allocate if you ask for the size.
func RuntimeAllocSize(size int64) int64 {
	return int64(roundupsize(uintptr(size)))
}

//go:linkname Atof64 strconv.atof64
func Atof64(s string) (float64, int, error)

//go:linkname Atof32 strconv.atof32
func Atof32(s string) (float32, int, error)
