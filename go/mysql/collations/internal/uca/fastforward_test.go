//go:build simd && goexperiment.simd && (amd64 || arm64)

/*
Copyright 2026 The Vitess Authors.

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
	"fmt"
	"math/bits"
	"unsafe"
)

// refFastForward32 is FastForward32 as it was before the equalASCIIPrefix
// skip, verbatim. It is the definition of correct output for the skip: the
// return value, both inputs and the unicode counter must come out the same.
func refFastForward32(it *FastIterator900, it2 *FastIterator900) int {
	if it.unicode > maxUnicodeBlocks || it.codepoint.ce != 0 || it2.codepoint.ce != 0 {
		return 0
	}

	p1 := it.input
	p2 := it2.input
	var w1, w2 uint16

	for len(p1) >= 4 && len(p2) >= 4 {
		dword1 := *(*uint32)(unsafe.Pointer(&p1[0]))
		dword2 := *(*uint32)(unsafe.Pointer(&p2[0]))
		nonascii := (dword1 | dword2) & 0x80808080

		if nonascii == 0 {
			if dword1 != dword2 {
				table := it.fastTable
				if w1, w2 = uint16(table[p1[0]]), uint16(table[p2[0]]); w1 != w2 {
					goto mismatch
				}
				if w1, w2 = uint16(table[p1[1]]), uint16(table[p2[1]]); w1 != w2 {
					goto mismatch
				}
				if w1, w2 = uint16(table[p1[2]]), uint16(table[p2[2]]); w1 != w2 {
					goto mismatch
				}
				if w1, w2 = uint16(table[p1[3]]), uint16(table[p2[3]]); w1 != w2 {
					goto mismatch
				}
			}
			p1 = p1[4:]
			p2 = p2[4:]
			it.unicode--
			continue
		} else if bits.OnesCount32(nonascii) == 4 {
			it.unicode++
		}
		break
	}
	it.input = p1
	it2.input = p2
	return 0

mismatch:
	if w1 == 0 || w2 == 0 {
		it.input = p1
		it2.input = p2
		it.unicode++
		return 0
	}
	return int(bits.ReverseBytes16(w1)) - int(bits.ReverseBytes16(w2))
}

// fastForwardIterator builds a FastIterator900 with only what FastForward32
// reads: the level-0 fast table, the input and the unicode counter.
func fastForwardIterator(input []byte, unicode int) *FastIterator900 {
	it := &FastIterator900{}
	it.fastTable = &fastweightTable_uca900_page000L0
	it.input = input
	it.unicode = unicode
	return it
}

// refEqualASCIIPrefix is the 4-byte block loop FastForward32 runs, reduced
// to the question equalASCIIPrefix answers: how many leading bytes are in
// blocks that are byte-equal and all ASCII.
func refEqualASCIIPrefix(p1, p2 []byte) int {
	i := 0
	for i+4 <= len(p1) && i+4 <= len(p2) {
		for j := range 4 {
			if p1[i+j] != p2[i+j] || p1[i+j]&0x80 != 0 || p2[i+j]&0x80 != 0 {
				return i
			}
		}
		i += 4
	}
	return i
}

func asciiRun(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(i%26)
	}
	return b
}

// prefixCases returns pairs that share an equal-ASCII prefix and then
// diverge in every way the kernel has to notice: a differing byte (a case
// flip, so the weights stay equal and the scalar loop carries on), a
// non-ASCII byte on one side or both, or the shorter input ending. The
// trailing-block cases are the other ways the scalar loop resolves the
// first block the skip stops on: an ignorable, and a block that is all
// Unicode.
func prefixCases() []struct {
	name   string
	p1, p2 []byte
} {
	var cases []struct {
		name   string
		p1, p2 []byte
	}
	add := func(name string, p1, p2 []byte) {
		cases = append(cases, struct {
			name   string
			p1, p2 []byte
		}{name, p1, p2})
	}
	lastAt := -1
	for _, n := range []int{0, 3, 4, 8, 15, 16, 17, 20, 31, 32, 33, 47, 48, 63, 64, 65, 100, 128, 129} {
		add(fmt.Sprintf("equal %d", n), asciiRun(n), asciiRun(n))
		add(fmt.Sprintf("equal %d vs %d", n, n+3), asciiRun(n), asciiRun(n+3))
		for pos := range n {
			p2 := asciiRun(n)
			p2[pos] = 'A' + byte(pos%26)
			add(fmt.Sprintf("differ at %d of %d", pos, n), asciiRun(n), p2)
			p2 = asciiRun(n)
			p2[pos] = 0xC3
			add(fmt.Sprintf("non-ascii right at %d of %d", pos, n), asciiRun(n), p2)
			p1 := asciiRun(n)
			p1[pos] = 0xE2
			add(fmt.Sprintf("non-ascii left at %d of %d", pos, n), p1, asciiRun(n))
			p1, p2 = asciiRun(n), asciiRun(n)
			p1[pos], p2[pos] = 0xC3, 0xC3
			add(fmt.Sprintf("non-ascii both at %d of %d", pos, n), p1, p2)
		}
		p2 := asciiRun(n + 8)
		p2[n] = 0x01
		add(fmt.Sprintf("ignorable after %d", n), asciiRun(n+8), p2)
		// On the 4-byte block boundary at or after n, so the block is all
		// Unicode and the scalar loop's `it.unicode++` branch is what
		// resolves it. Neighbouring lengths round to the same boundary;
		// emit each once.
		if at := (n + 3) &^ 3; at != lastAt {
			lastAt = at
			p1, p2 := asciiRun(at+4), asciiRun(at+4)
			copy(p1[at:], "\xE2\x82\xAC\xC3")
			copy(p2[at:], "\xE2\x82\xAC\xC3")
			add(fmt.Sprintf("unicode block at %d", at), p1, p2)
		}
	}
	return cases
}
