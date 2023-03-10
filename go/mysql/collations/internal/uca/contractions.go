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

	"vitess.io/vitess/go/mysql/collations/charset"
)

type trie struct {
	children map[rune]*trie
	weights  []uint16
}

func (t *trie) walkUTF8(remainder []byte) ([]uint16, []byte) {
	if len(remainder) > 0 {
		cp, width := utf8.DecodeRune(remainder)
		if cp == utf8.RuneError && width < 3 {
			return nil, nil
		}
		if ch := t.children[cp]; ch != nil {
			return ch.walkUTF8(remainder[width:])
		}
	}
	return t.weights, remainder
}

func (t *trie) walkCharset(cs charset.Charset, remainder []byte, depth int) ([]uint16, []byte, int) {
	if len(remainder) > 0 {
		cp, width := cs.DecodeRune(remainder)
		if cp == charset.RuneError && width < 3 {
			return nil, nil, 0
		}
		if ch := t.children[cp]; ch != nil {
			return ch.walkCharset(cs, remainder[width:], depth+1)
		}
	}
	return t.weights, remainder, depth + 1
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

type trieContractor struct {
	tr trie
}

func (ctr *trieContractor) insert(c *Contraction) {
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

func (ctr *trieContractor) Find(cs charset.Charset, cp rune, remainder []byte) ([]uint16, []byte, int) {
	if tr := ctr.tr.children[cp]; tr != nil {
		return tr.walkCharset(cs, remainder, 0)
	}
	return nil, nil, 0
}

func (ctr *trieContractor) FindContextual(cp, prev rune) []uint16 {
	if tr := ctr.tr.children[cp]; tr != nil {
		if trc := tr.children[prev]; trc != nil {
			return trc.weights
		}
	}
	return nil
}

func NewTrieContractor(all []Contraction) Contractor {
	if len(all) == 0 {
		return nil
	}
	ctr := &trieContractor{}
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

type Contractor interface {
	Find(cs charset.Charset, cp rune, remainder []byte) ([]uint16, []byte, int)
	FindContextual(cp1, cp0 rune) []uint16
}
