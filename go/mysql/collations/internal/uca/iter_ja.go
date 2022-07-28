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

import "unicode/utf8"

type jaIterator900 struct {
	iterator900
	queuedWeight  uint16
	prevCodepoint rune
	kanas         map[rune]byte
}

func (it *jaIterator900) adjustJapaneseWeights(weight uint16) uint16 {
	// based on the following weights dumped from MySQL:
	// {0x1C47, 0x1FB5, 0x1C47, 0x1FB5}, // ?? this is a no-op
	// {0x3D5A, 0x3D8B, 0x1FB6, 0x1FE7},
	// {0x1FB6, 0x3D59, 0x0000, 0x0000},
	// {0x3D8C, 0x54A3, 0x0000, 0x0000},
	if it.level == 0 && weight >= 0x1FB6 && weight <= 0x54A3 {
		switch {
		// FIXME: this weight adjustment seems like a no-op, but it comes from the MySQL dump
		// case weight >= 0x1C47 && weight <= 0x1FB5:
		// 	return weight
		case weight >= 0x3D5A && weight <= 0x3D8B:
			return weight - 0x3D5A + 0x1FB6
		case weight >= 0x1FB6 && weight <= 0x3D59 || weight >= 0x3D8C && weight <= 0x54A3:
			it.queuedWeight = weight
			return 0xFB86
		}
	}
	return weight
}

func (it *jaIterator900) cacheKana(cp rune) {
	if unicodeIsHiragana(cp) {
		if it.kanas == nil {
			it.kanas = make(map[rune]byte)
		}
		it.kanas[cp] = 0x2
	} else if unicodeIsKatakana(cp) {
		if it.kanas == nil {
			it.kanas = make(map[rune]byte)
		}
		it.kanas[cp] = 0x8
	}
}

func (it *jaIterator900) Done() {
	it.queuedWeight = 0x0
	it.prevCodepoint = 0
	it.kanas = nil
	it.original = nil
	it.input = nil
	it.iterpool.Put(it)
}

func (it *jaIterator900) Next() (uint16, bool) {
	for {
		if it.queuedWeight != 0x0 {
			var w uint16
			w, it.queuedWeight = it.queuedWeight, 0x0
			return w, true
		}
		if w, ok := it.codepoint.next(); ok {
			return it.adjustJapaneseWeights(w), true
		}

	decodeNext:
		cp, width := utf8.DecodeRune(it.input)
		if cp == utf8.RuneError && width < 3 {
			it.level++
			// if we're at level 3 (Kana-sensitive) and we haven't seen
			// any Kanas in the previous levels, there's nothing to yield
			if it.level == 3 && it.kanas == nil {
				return 0, false
			}
			if it.level < it.maxLevel {
				it.input = it.original
				return 0, true
			}
			return 0, false
		}

		it.input = it.input[width:]
		if weights := it.contract.FindContextual(cp, it.prevCodepoint); weights != nil {
			// if this is a Kana-sensitive iterator and we're at level 3 (the Kana level),
			// we cannot return the contraction's weight here, we need the actual weights in
			// our Kana cache.
			if it.level == 3 {
				if w, ok := it.kanas[it.prevCodepoint]; ok {
					it.prevCodepoint = 0
					return uint16(w), true
				}
			}
			it.codepoint.initContraction(weights, it.level)
			it.prevCodepoint = 0
			continue
		}
		it.prevCodepoint = cp

		// if this is a Kana-sensitive iterator, we want to keep track of any
		// kanas we've seen in a cache, so that when we reach level 3, we can
		// quickly skip over codepoints that are not Kanas, as level 3 will
		// only yield Kana-weights
		if it.maxLevel == 4 {
			switch it.level {
			case 0:
				if _, ok := it.kanas[cp]; !ok {
					it.cacheKana(cp)
				}
			case 3:
				if w, ok := it.kanas[cp]; ok {
					return uint16(w), true
				}
				goto decodeNext
			}
		}

		it.codepoint.init(&it.iterator900, cp)
	}
}
