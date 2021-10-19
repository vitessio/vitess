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
	"fmt"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/internal/encoding"
)

type trie struct {
	children map[rune]*trie
	weights  []uint16
}

func (t *trie) walk(remainder []byte) ([]uint16, []byte) {
	if len(remainder) > 0 {
		cp, width := utf8.DecodeRune(remainder)
		if cp == utf8.RuneError && width < 3 {
			return nil, nil
		}
		if ch := t.children[cp]; ch != nil {
			return ch.walk(remainder[width:])
		}
	}
	return t.weights, remainder
}

func (t *trie) walkAnyEncoding(enc encoding.Encoding, remainder []byte, depth int) ([]uint16, []byte, int) {
	if len(remainder) > 0 {
		cp, width := enc.DecodeRune(remainder)
		if cp == encoding.RuneError && width < 3 {
			return nil, nil, 0
		}
		if ch := t.children[cp]; ch != nil {
			return ch.walkAnyEncoding(enc, remainder[width:], depth+1)
		}
	}
	return t.weights, remainder, depth
}

func (t *trie) insert(path []rune, weights []uint16) {
	if len(path) == 0 {
		if t.weights != nil {
			panic("duplicate contraction")
		}
		t.weights = weights
		return
	}

	if t.children == nil {
		t.children = make(map[rune]*trie)
	}
	ch := t.children[path[0]]
	if ch == nil {
		ch = &trie{}
		t.children[path[0]] = ch
	}
	ch.insert(path[1:], weights)
}

type contractions struct {
	tr trie
}

func (ctr *contractions) insert(c *Contraction) {
	if len(c.Path) < 2 {
		panic("contraction is too short")
	}
	if len(c.Weights)%3 != 0 {
		panic(fmt.Sprintf("weights are not well-formed: %#v has len=%d", c.Weights, len(c.Weights)))
	}
	if c.Contextual && len(c.Path) != 2 {
		panic("contextual contractions can only span 2 codepoints")
	}
	ctr.tr.insert(c.Path, c.Weights)
}

func (ctr *contractions) weightForContraction(cp rune, remainder []byte) ([]uint16, []byte) {
	if ctr != nil {
		if tr := ctr.tr.children[cp]; tr != nil {
			return tr.walk(remainder)
		}
	}
	return nil, nil
}

func (ctr *contractions) weightForContractionAnyEncoding(cp rune, remainder []byte, enc encoding.Encoding) ([]uint16, []byte, int) {
	if ctr != nil {
		if tr := ctr.tr.children[cp]; tr != nil {
			return tr.walkAnyEncoding(enc, remainder, 0)
		}
	}
	return nil, nil, 0
}

func (ctr *contractions) weightForContextualContraction(cp, prev rune) []uint16 {
	if tr := ctr.tr.children[cp]; tr != nil {
		if trc := tr.children[prev]; trc != nil {
			return trc.weights
		}
	}
	return nil
}

func newContractions(all []Contraction) *contractions {
	if len(all) == 0 {
		return nil
	}
	ctr := &contractions{}
	for _, c := range all {
		ctr.insert(&c)
	}
	return ctr
}

type Contraction struct {
	Path       []rune
	Weights    []uint16
	Contextual bool
}
