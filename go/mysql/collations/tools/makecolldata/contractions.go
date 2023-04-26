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

package main

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/mysql/collations/internal/uca"
	"vitess.io/vitess/go/mysql/collations/tools/makecolldata/codegen"
)

func sortContractionTrie(trie map[rune][]uca.Contraction) (sorted []rune) {
	for cp := range trie {
		sorted = append(sorted, cp)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	return
}

type weightarray struct {
	name string
	ary  []uint16
}

func (wa *weightarray) push(weights []uint16) string {
	start := len(wa.ary)
	wa.ary = append(wa.ary, weights...)
	return fmt.Sprintf("%s[%d:%d]", wa.name, start, len(wa.ary))
}

func (wa *weightarray) print(g *codegen.Generator) {
	g.P("var ", wa.name, " = ", codegen.Array16(wa.ary))
}

func printContraction1(g *codegen.Generator, wa *weightarray, incont []uca.Contraction, depth int) {
	trie := make(map[rune][]uca.Contraction)
	var leaf *uca.Contraction
	for i := range incont {
		// Ensure local variable since we grab a pointer later
		// potentially.
		cont := incont[i]
		if depth < len(cont.Path) {
			r := cont.Path[depth]
			trie[r] = append(trie[r], cont)
		}
		if depth == len(cont.Path) {
			leaf = &cont // nolint:exportloopref
		}
	}

	if depth > 1 {
		g.P("b", depth-1, " := b", depth-2, "[width", depth-1, ":]")
	}
	if depth > 0 {
		g.P("cp", depth, ", width", depth, " := cs.DecodeRune(b", depth-1, ")")
	}

	g.P("switch cp", depth, " {")

	for _, cp := range sortContractionTrie(trie) {
		g.P("case ", cp, ":")
		cnt := trie[cp]

		if len(cnt) == 1 && len(cnt[0].Path) == depth+1 {
			weights := wa.push(cnt[0].Weights)
			g.P("return ", weights, ", b", depth-1, "[width", depth, ":], ", depth+1)
		} else {
			printContraction1(g, wa, cnt, depth+1)
		}
	}

	g.P("}")

	if leaf != nil {
		weights := wa.push(incont[0].Weights)
		g.P("return ", weights, ", b", depth-1, ", ", depth)
	}
}

func (g *TableGenerator) printFastContractionsCtx(name string, allContractions []uca.Contraction) {
	trie := make(map[rune][]uca.Contraction)
	for _, cont := range allContractions {
		cp := cont.Path[0]
		trie[cp] = append(trie[cp], cont)
	}

	g.P("func (", name, ") Find(", PkgCharset, ".Charset, rune, []byte) ([]uint16, []byte, int) {")
	g.P("return nil, nil, 0")
	g.P("}")

	var mapping = make(map[uint32][]uint16)
	var cp0min, cp1min rune = 0xFFFF, 0xFFFF
	for _, cp1 := range sortContractionTrie(trie) {
		for _, cnt := range trie[cp1] {
			cp0 := cnt.Path[1]
			if cp0 < cp0min {
				cp0min = cp0
			}
			if cp1 < cp1min {
				cp1min = cp1
			}

			mask := uint32(cp1)<<16 | uint32(cp0)
			mapping[mask] = cnt.Weights
		}
	}

	g.P("var ", name, "_weights = ", mapping)

	g.P("func (", name, ") FindContextual(cp1, cp0 rune) []uint16 {")
	g.P("if cp0 < ", cp0min, " || cp1 < ", cp1min, " || cp0 > 0xFFFF || cp1 > 0xFFFF {")
	g.P("return nil")
	g.P("}")
	g.P("return ", name, "_weights[uint32(cp1) << 16 | uint32(cp0)]")
	g.P("}")
}

func (g *TableGenerator) printContractionsFast(name string, allContractions []uca.Contraction) {
	contextual := false
	for i := range allContractions {
		ctr := &allContractions[i]
		for i := 0; i < len(ctr.Weights)-3; i += 3 {
			if ctr.Weights[i] == 0x0 && ctr.Weights[i+1] == 0x0 && ctr.Weights[i+2] == 0x0 {
				ctr.Weights = ctr.Weights[:i]
				break
			}
		}
		if ctr.Contextual {
			contextual = true
		}
		if contextual {
			if !ctr.Contextual {
				g.Fail("mixed Contextual and non-contextual contractions")
			}
			if len(ctr.Path) != 2 {
				g.Fail("Contextual contraction with Path != 2")
			}
		}
	}

	g.P("type ", name, " struct{}")
	g.P()

	if contextual {
		g.printFastContractionsCtx(name, allContractions)
		return
	}

	var wa = &weightarray{name: name + "_weights"}

	g.P("func (", name, ") Find(cs ", PkgCharset, ".Charset, cp0 rune, b0 []byte) ([]uint16, []byte, int) {")
	printContraction1(g.Generator, wa, allContractions, 0)
	g.P("return nil, nil, 0")
	g.P("}")
	g.P("func (", name, ") FindContextual(cp1, cp0 rune) []uint16 {")
	g.P("return nil")
	g.P("}")

	wa.print(g.Generator)
}
