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

package uca

import (
	"math/bits"
	"unicode/utf8"
	"unsafe"
)

type FastIterator900 struct {
	iterator900
	fastTable []uint32
}

func (it *FastIterator900) Done() {
	it.original = nil
	it.input = nil
	it.iterpool.Put(it)
}

func (it *FastIterator900) reset(input []byte) {
	it.fastTable = fastweightTable_uca900_page000L0[:256]
	it.iterator900.reset(input)
}

func (it *FastIterator900) NextChunk(srcbytes []byte) int {
	chunk := (*[8]uint16)(unsafe.Pointer(&srcbytes[0]))[0:8]
	p := it.input

	if it.codepoint.ce == 0 && len(p) >= 8 {
		dword := *(*uint64)(unsafe.Pointer(&p[0]))
		if dword&0x8080808080808080 == 0 {
			const optimisticFastWrites = true

			table := it.fastTable[:256]

			if optimisticFastWrites {
				var mask uint32 = 0x20000
				weight := table[p[0]]
				mask &= weight
				chunk[0] = uint16(weight)

				weight = table[p[1]]
				mask &= weight
				chunk[1] = uint16(weight)

				weight = table[p[2]]
				mask &= weight
				chunk[2] = uint16(weight)

				weight = table[p[2]]
				mask &= weight
				chunk[2] = uint16(weight)

				weight = table[p[3]]
				mask &= weight
				chunk[3] = uint16(weight)

				weight = table[p[4]]
				mask &= weight
				chunk[4] = uint16(weight)

				weight = table[p[5]]
				mask &= weight
				chunk[5] = uint16(weight)

				weight = table[p[6]]
				mask &= weight
				chunk[6] = uint16(weight)

				weight = table[p[7]]
				mask &= weight
				chunk[7] = uint16(weight)

				if mask != 0 {
					it.input = it.input[8:]
					return 16
				}
			} else {
				dst := (*uint16)(unsafe.Pointer(&srcbytes[0]))
				weight := table[p[0]]
				*dst = uint16(weight)
				dst = (*uint16)(unsafe.Add(unsafe.Pointer(dst), weight>>16))

				weight = table[p[1]]
				*dst = uint16(weight)
				dst = (*uint16)(unsafe.Add(unsafe.Pointer(dst), weight>>16))

				weight = table[p[2]]
				*dst = uint16(weight)
				dst = (*uint16)(unsafe.Add(unsafe.Pointer(dst), weight>>16))

				weight = table[p[3]]
				*dst = uint16(weight)
				dst = (*uint16)(unsafe.Add(unsafe.Pointer(dst), weight>>16))

				weight = table[p[4]]
				*dst = uint16(weight)
				dst = (*uint16)(unsafe.Add(unsafe.Pointer(dst), weight>>16))

				weight = table[p[5]]
				*dst = uint16(weight)
				dst = (*uint16)(unsafe.Add(unsafe.Pointer(dst), weight>>16))

				weight = table[p[6]]
				*dst = uint16(weight)
				dst = (*uint16)(unsafe.Add(unsafe.Pointer(dst), weight>>16))

				weight = table[p[7]]
				*dst = uint16(weight)
				dst = (*uint16)(unsafe.Add(unsafe.Pointer(dst), weight>>16))

				written := uintptr(unsafe.Pointer(dst)) - uintptr(unsafe.Pointer(&srcbytes[0]))
				if written != 0 {
					it.input = it.input[written>>1:]
					return int(written)
				}
			}
		}
	}

	for i := 0; i < 8; i++ {
		w, ok := it.Next()
		if !ok {
			return i * 2
		}
		chunk[i] = bits.ReverseBytes16(w)
	}
	return 16
}

func (it *FastIterator900) resetForNextLevel() {
	it.input = it.original
	switch it.level {
	case 1:
		it.fastTable = fastweightTable_uca900_page000L1[:256]
	case 2:
		it.fastTable = fastweightTable_uca900_page000L2[:256]
	}
}

func (it *FastIterator900) Next() (uint16, bool) {
	for {
		if w, ok := it.codepoint.next(); ok {
			return w, true
		}

		cp, width := utf8.DecodeRune(it.input)
		if cp == utf8.RuneError && width < 3 {
			it.level++
			if it.level < it.maxLevel {
				it.resetForNextLevel()
				return 0, true
			}
			return 0, false
		}

		it.input = it.input[width:]
		it.codepoint.init(&it.iterator900, cp)
	}
}
