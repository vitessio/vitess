/*
Copyright (c) 2009 The Go Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// Copied from the Go runtime, msize_noallocheaders.go

package hack

import (
	"unsafe"
)

const (
	_MaxSmallSize   = 32768
	smallSizeDiv    = 8
	smallSizeMax    = 1024
	largeSizeDiv    = 128
	_NumSizeClasses = 68
	_PageShift      = 13
	_PageSize       = 1 << _PageShift
)

var class_to_size = [_NumSizeClasses]uint16{0, 8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768}
var size_to_class8 = [smallSizeMax/smallSizeDiv + 1]uint8{0, 1, 2, 3, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 15, 15, 16, 16, 17, 17, 18, 18, 19, 19, 19, 19, 20, 20, 20, 20, 21, 21, 21, 21, 22, 22, 22, 22, 23, 23, 23, 23, 24, 24, 24, 24, 25, 25, 25, 25, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27, 27, 27, 28, 28, 28, 28, 28, 28, 28, 28, 29, 29, 29, 29, 29, 29, 29, 29, 30, 30, 30, 30, 30, 30, 30, 30, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32}
var size_to_class128 = [(_MaxSmallSize-smallSizeMax)/largeSizeDiv + 1]uint8{32, 33, 34, 35, 36, 37, 37, 38, 38, 39, 39, 40, 40, 40, 41, 41, 41, 42, 43, 43, 44, 44, 44, 44, 44, 45, 45, 45, 45, 45, 45, 46, 46, 46, 46, 47, 47, 47, 47, 47, 47, 48, 48, 48, 49, 49, 50, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 52, 52, 52, 52, 52, 52, 52, 52, 52, 52, 53, 53, 54, 54, 54, 54, 55, 55, 55, 55, 55, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 57, 57, 57, 57, 57, 57, 57, 57, 57, 57, 58, 58, 58, 58, 58, 58, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 61, 61, 61, 61, 61, 62, 62, 62, 62, 62, 62, 62, 62, 62, 62, 62, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67}

func alignUp(n, a uintptr) uintptr {
	return (n + a - 1) &^ (a - 1)
}

func divRoundUp(n, a uintptr) uintptr {
	// a is generally a power of two. This will get inlined and
	// the compiler will optimize the division.
	return (n + a - 1) / a
}

func roundupsize(size uintptr) uintptr {
	if size < _MaxSmallSize {
		if size <= smallSizeMax-8 {
			return uintptr(class_to_size[size_to_class8[divRoundUp(size, smallSizeDiv)]])
		} else {
			return uintptr(class_to_size[size_to_class128[divRoundUp(size-smallSizeMax, largeSizeDiv)]])
		}
	}
	if size+_PageSize < size {
		return size
	}
	return alignUp(size, _PageSize)
}

// RuntimeAllocSize returns size of the memory block that mallocgc will allocate if you ask for the size.
func RuntimeAllocSize(size int64) int64 {
	return int64(roundupsize(uintptr(size)))
}

// RuntimeMapSize returns the size of the internal data structures of a Map.
// This is an internal implementation detail based on the Swiss Map implementation
// as of Go 1.24 and needs to be kept up-to-date after every Go release.
func RuntimeMapSize[K comparable, V any](themap map[K]V) (internalSize int64) {
	// The following are copies of internal data structures as of Go 1.24
	// THEY MUST BE KEPT UP TO DATE

	// internal/abi/type.go
	const SizeofType = 48

	// internal/abi/map_swiss.go
	type SwissMapType struct {
		pad       [SizeofType]uint8
		Key       unsafe.Pointer
		Elem      unsafe.Pointer
		Group     unsafe.Pointer
		Hasher    func(unsafe.Pointer, uintptr) uintptr
		GroupSize uintptr // == Group.Size_
		SlotSize  uintptr // size of key/elem slot
		ElemOff   uintptr // offset of elem in key/elem slot
		Flags     uint32
	}

	// internal/runtime/maps/map.go
	type Map struct {
		used        uint64
		seed        uintptr
		dirPtr      unsafe.Pointer
		dirLen      int
		globalDepth uint8
		globalShift uint8
		writing     uint8
		clearSeq    uint64
	}

	// internal/runtime/maps/groups.go
	type groupsReference struct {
		data       unsafe.Pointer // data *[length]typ.Group
		lengthMask uint64
	}

	// internal/runtime/maps/table.go
	type table struct {
		used       uint16
		capacity   uint16
		growthLeft uint16
		localDepth uint8
		index      int
		groups     groupsReference
	}

	directoryAt := func(m *Map, i uintptr) *table {
		const PtrSize = 4 << (^uintptr(0) >> 63)
		return *(**table)(unsafe.Pointer(uintptr(m.dirPtr) + PtrSize*i))
	}

	// Converting the map to an interface will result in two ptr-sized words;
	// like all interfaces, the first word points to an *abi.Type instance,
	// which is specialized as SwissMapType for maps, and to the actual
	// memory allocation, which is `*Map`
	type MapInterface struct {
		Type *SwissMapType
		Data *Map
	}

	mapiface := any(themap)
	iface := (*MapInterface)(unsafe.Pointer(&mapiface))
	m := iface.Data

	var groupSize = int64(iface.Type.GroupSize)
	internalSize = int64(unsafe.Sizeof(Map{}))

	if m.dirLen <= 0 {
		// Small map optimization: we don't allocate tables at all if all the
		// entries in a map fit in a single group
		if m.dirPtr != nil {
			internalSize += RuntimeAllocSize(groupSize)
		}
	} else {
		// For normal maps, we iterate each table and add the size of all the
		// groups it contains.
		// See: internal/runtime/maps/export_test.go
		var lastTab *table
		for i := range m.dirLen {
			t := directoryAt(m, uintptr(i))
			if t == lastTab {
				continue
			}
			lastTab = t

			gc := int64(t.groups.lengthMask + 1)
			internalSize += RuntimeAllocSize(groupSize * gc)
		}
	}

	return internalSize
}
