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

//go:linkname roundupsize runtime.roundupsize
func roundupsize(size uintptr) uintptr

// RuntimeAllocSize returns size of the memory block that mallocgc will allocate if you ask for the size.
func RuntimeAllocSize(size int64) int64 {
	return int64(roundupsize(uintptr(size)))
}

//go:linkname ParseFloatPrefix strconv.parseFloatPrefix
func ParseFloatPrefix(s string, bitSize int) (float64, int, error)
