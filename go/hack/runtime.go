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
	"reflect"
	"unsafe"
)

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

//go:noescape
//go:linkname strhash runtime.strhash
func strhash(p unsafe.Pointer, h uintptr) uintptr

// RuntimeMemhash provides access to the Go runtime's default hash function for arbitrary bytes.
// This is an optimal hash function which takes an input seed and is potentially implemented in hardware
// for most architectures. This is the same hash function that the language's `map` uses.
func RuntimeMemhash(b []byte, seed uint64) uint64 {
	pstring := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return uint64(memhash(unsafe.Pointer(pstring.Data), uintptr(seed), uintptr(pstring.Len)))
}

// RuntimeStrhash provides access to the Go runtime's default hash function for strings.
// This is an optimal hash function which takes an input seed and is potentially implemented in hardware
// for most architectures. This is the same hash function that the language's `map` uses.
func RuntimeStrhash(str string, seed uint64) uint64 {
	return uint64(strhash(unsafe.Pointer(&str), uintptr(seed)))
}
