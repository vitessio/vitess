//go:build !gc || wasm

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

// String returns b as a string. This compat function allocates memory.
func String(b []byte) (s string) {
	return string(b)
}

// StringBytes returns as a []byte. This compat function allocates memory.
func StringBytes(s string) []byte {
	return []byte(s)
}

// RuntimeMemhash is a slow hash function for bytes, implemented for compatibility.
func RuntimeMemhash(b []byte, hash uint64) uint64 {
	for i := 0; i < len(b); i++ {
		hash *= 1099511628211
		hash ^= uint64(b[i])
	}
	return hash
}

// RuntimeStrhash is a slow hash function for bytes, implemented for compatibility.
func RuntimeStrhash(str string, hash uint64) uint64 {
	for i := 0; i < len(str); i++ {
		hash *= 1099511628211
		hash ^= uint64(str[i])
	}
	return hash
}

// ParseFloatPrefix exposes the internal strconv method from the standard library
//
//go:linkname ParseFloatPrefix strconv.parseFloatPrefix
func ParseFloatPrefix(s string, bitSize int) (float64, int, error)

// RuntimeAllocSize is a no-op when Vitess is not compiled with GC
func RuntimeAllocSize(size int64) int64 {
	return size
}
