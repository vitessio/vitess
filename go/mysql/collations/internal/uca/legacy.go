package uca

import (
	"vitess.io/vitess/go/mysql/collations/internal/encoding"
)

type WeightIteratorLegacy struct {
	// Constant
	CollationLegacy

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

func (it *codepointIteratorLegacy) init(table WeightTable, cp rune) {
	p, offset := pageOffset(cp)
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

func (it *WeightIteratorLegacy) Done() {
	it.input = nil
	it.iterpool.Put(it)
}

func (it *WeightIteratorLegacy) DebugCodepoint() (rune, int) {
	return it.encoding.DecodeRune(it.input)
}

func (it *WeightIteratorLegacy) Next() (uint16, bool) {
	for {
		if w, ok := it.codepoint.next(); ok {
			return w, true
		}

		cp, width := it.encoding.DecodeRune(it.input)
		if cp == encoding.RuneError && width < 3 {
			return 0, false
		}
		it.input = it.input[width:]
		it.length++

		if cp > it.maxCodepoint {
			return 0xFFFD, true
		}
		if weights, remainder, skip := it.contractions.weightForContractionAnyEncoding(cp, it.input, it.encoding); weights != nil {
			it.codepoint.initContraction(weights)
			it.input = remainder
			it.length += skip
			continue
		}
		it.codepoint.init(it.table, cp)
	}
}

func (it *WeightIteratorLegacy) Length() int {
	return it.length
}
