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
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset"
)

type iterator900 struct {
	// Constant
	*Collation900
	original []byte

	// Internal state
	codepoint codepointIterator
	input     []byte
	level     int
}

type codepointIterator struct {
	weights []uint16
	scratch [16]uint16
	ce      int
	stride  int
}

func (it *codepointIterator) next() (uint16, bool) {
	for it.ce > 0 && it.weights[0] == 0x0 {
		if it.stride <= len(it.weights) {
			it.weights = it.weights[it.stride:]
		}
		it.ce--
	}
	if it.ce == 0 {
		return 0, false
	}
	weight := it.weights[0]
	if it.stride <= len(it.weights) {
		it.weights = it.weights[it.stride:]
	}
	it.ce--
	return weight, true
}

func (it *codepointIterator) init(parent *iterator900, cp rune) {
	p, offset := PageOffset(cp)
	page := parent.table[p]
	if page == nil {
		it.initImplicit(parent, cp)
		return
	}

	it.stride = CodepointsPerPage * 3
	it.weights = (*page)[(parent.level+1)*CodepointsPerPage+offset:]
	if it.weights[0] == 0x0 {
		it.ce = 0
	} else {
		it.ce = int((*page)[offset])
	}
}

func (it *codepointIterator) initContraction(weights []uint16, level int) {
	it.ce = len(weights) / 3
	it.weights = weights[level:]
	it.stride = 3
}

func (it *codepointIterator) initImplicit(parent *iterator900, cp rune) {
	if jamos := UnicodeDecomposeHangulSyllable(cp); jamos != nil {
		jweight := it.scratch[:0]
		for _, jamo := range jamos {
			p, offset := PageOffset(jamo)
			page := *parent.table[p]
			jweight = append(jweight,
				page[1*CodepointsPerPage+offset],
				page[2*CodepointsPerPage+offset],
				page[3*CodepointsPerPage+offset],
			)
		}

		it.weights = jweight[parent.level:]
		it.ce = len(jamos)
		it.stride = 3
		return
	}

	parent.implicits(it.scratch[:], cp)
	it.weights = it.scratch[parent.level:]
	it.ce = 2
	it.stride = 3
}

func (it *iterator900) Level() int {
	return it.level
}

func (it *iterator900) SkipLevel() int {
	it.codepoint.ce = 0
	it.input = it.original
	it.level++
	return it.level
}

func (it *iterator900) reset(input []byte) {
	it.input = input
	it.original = input
	it.level = 0
	it.codepoint.ce = 0
}

type WeightIterator interface {
	Next() (uint16, bool)
	Level() int
	SkipLevel() int
	Done()
	reset(input []byte)
}

type slowIterator900 struct {
	iterator900
}

func (it *slowIterator900) Done() {
	it.original = nil
	it.input = nil
	it.iterpool.Put(it)
}

func (it *slowIterator900) Next() (uint16, bool) {
	for {
		if w, ok := it.codepoint.next(); ok {
			return it.param.adjust(it.level, w), true
		}

		cp, width := utf8.DecodeRune(it.input)
		if cp == utf8.RuneError && width < 3 {
			it.level++

			if it.level < it.maxLevel {
				it.input = it.original
				return 0, true
			}
			return 0, false
		}

		it.input = it.input[width:]
		if it.contract != nil {
			if weights, remainder, _ := it.contract.Find(charset.Charset_utf8mb4{}, cp, it.input); weights != nil {
				it.codepoint.initContraction(weights, it.level)
				it.input = remainder
				continue
			}
		}
		it.codepoint.init(&it.iterator900, cp)
	}
}
