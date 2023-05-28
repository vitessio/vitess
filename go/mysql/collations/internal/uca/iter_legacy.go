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
	"vitess.io/vitess/go/mysql/collations/charset"
)

type WeightIteratorLegacy struct {
	// Constant
	*CollationLegacy

	// Internal state
	codepoint codepointIteratorLegacy
	input     []byte
	length    int
}

type codepointIteratorLegacy struct {
	weights []uint16
	scratch [2]uint16
}

func (it *codepointIteratorLegacy) next() (uint16, bool) {
	if len(it.weights) == 0 {
		return 0, false
	}
	weight := it.weights[0]
	it.weights = it.weights[1:]
	return weight, weight != 0x0
}

func (it *codepointIteratorLegacy) init(table Weights, cp rune) {
	p, offset := PageOffset(cp)
	page := table[p]
	if page == nil {
		UnicodeImplicitWeightsLegacy(it.scratch[:2], cp)
		it.weights = it.scratch[:2]
		return
	}

	stride := int((*page)[0])
	position := 1 + stride*offset
	it.weights = (*page)[position : position+stride]
}

func (it *codepointIteratorLegacy) initContraction(weights []uint16) {
	it.weights = weights
}

func (it *WeightIteratorLegacy) reset(input []byte) {
	it.input = input
	it.length = 0
	it.codepoint.weights = nil
}

func (it *WeightIteratorLegacy) DebugCodepoint() (rune, int) {
	return it.charset.DecodeRune(it.input)
}

func (it *WeightIteratorLegacy) Next() (uint16, bool) {
	for {
		if w, ok := it.codepoint.next(); ok {
			return w, true
		}

		cp, width := it.charset.DecodeRune(it.input)
		if cp == charset.RuneError && width < 3 {
			return 0, false
		}
		it.input = it.input[width:]
		it.length++

		if cp > it.maxCodepoint {
			return 0xFFFD, true
		}
		if it.contract != nil {
			if weights, remainder, skip := it.contract.Find(it.charset, cp, it.input); weights != nil {
				it.codepoint.initContraction(weights)
				it.input = remainder
				it.length += skip
				continue
			}
		}
		it.codepoint.init(it.table, cp)
	}
}

func (it *WeightIteratorLegacy) Length() int {
	return it.length
}
