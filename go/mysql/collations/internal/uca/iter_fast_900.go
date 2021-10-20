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
	fastTable []uint16
	fastLimit uint32
}

func (it *FastIterator900) Done() {
	it.original = nil
	it.input = nil
	it.iterpool.Put(it)
}

func (it *FastIterator900) reset(input []byte) {
	it.fastTable = fastweightTable_uca900_page000L0[:256]
	it.fastLimit = fastweightTable_uca900_page000L0_min * 8
	it.iterator900.reset(input)
}

func (it *FastIterator900) NextChunk(srcbytes []byte) int {
	chunk := (*[8]uint16)(unsafe.Pointer(&srcbytes[0]))[0:8]
	p := it.input

	if it.codepoint.ce == 0 && len(p) >= 8 {
		dword := *(*uint64)(unsafe.Pointer(&p[0]))
		if dword&0x8080808080808080 == 0 {
			limit := uint32(0)
			table := it.fastTable[:256]
			weight := table[p[0]]
			limit += uint32(weight)
			chunk[0] = weight
			weight = table[p[1]]
			limit += uint32(weight)
			chunk[1] = weight
			weight = table[p[2]]
			limit += uint32(weight)
			chunk[2] = weight
			weight = table[p[3]]
			limit += uint32(weight)
			chunk[3] = weight
			weight = table[p[4]]
			limit += uint32(weight)
			chunk[4] = weight
			weight = table[p[5]]
			limit += uint32(weight)
			chunk[5] = weight
			weight = table[p[6]]
			limit += uint32(weight)
			chunk[6] = weight
			weight = table[p[7]]
			limit += uint32(weight)
			chunk[7] = weight

			if limit >= it.fastLimit {
				it.input = it.input[8:]
				return 16
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
		it.fastLimit = fastweightTable_uca900_page000L1_min * 8
	case 2:
		it.fastTable = fastweightTable_uca900_page000L2[:256]
		it.fastLimit = fastweightTable_uca900_page000L2_min * 8
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
