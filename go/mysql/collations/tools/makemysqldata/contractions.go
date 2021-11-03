package main

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"vitess.io/vitess/go/mysql/collations/internal/uca"
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

func (wa *weightarray) print(out io.Writer) {
	fmt.Fprintf(out, "var %s = [...]uint16{", wa.name)
	for _, w := range wa.ary {
		fmt.Fprintf(out, "0x%04x, ", w)
	}
	fmt.Fprintf(out, "}\n\n")
}

func printContraction1(w io.Writer, wa *weightarray, incont []uca.Contraction, depth int) {
	trie := make(map[rune][]uca.Contraction)
	var leaf *uca.Contraction
	for _, cont := range incont {
		if depth < len(cont.Path) {
			r := cont.Path[depth]
			trie[r] = append(trie[r], cont)
		}
		if depth == len(cont.Path) {
			leaf = &cont
		}
	}

	if depth > 1 {
		fmt.Fprintf(w, "b%d := b%d[width%d:]\n", depth-1, depth-2, depth-1)
	}
	if depth > 0 {
		fmt.Fprintf(w, "cp%d, width%d := cs.DecodeRune(b%d)\n", depth, depth, depth-1)
	}

	fmt.Fprintf(w, "switch cp%d {\n", depth)

	for _, cp := range sortContractionTrie(trie) {
		fmt.Fprintf(w, "case %d:\n", cp)
		cnt := trie[cp]

		if len(cnt) == 1 && len(cnt[0].Path) == depth+1 {
			weights := wa.push(cnt[0].Weights)
			fmt.Fprintf(w, "return %s, b%d[width%d:], %d\n", weights, depth-1, depth, depth+1)
		} else {
			printContraction1(w, wa, cnt, depth+1)
		}
	}

	fmt.Fprintf(w, "}\n")

	if leaf != nil {
		weights := wa.push(incont[0].Weights)
		fmt.Fprintf(w, "return %s, b%d, %d\n", weights, depth-1, depth)
	}
}

func (out *output) printFastContractionsCtx(name string, allContractions []uca.Contraction) {
	trie := make(map[rune][]uca.Contraction)
	for _, cont := range allContractions {
		cp := cont.Path[0]
		trie[cp] = append(trie[cp], cont)
	}

	fmt.Fprintf(out.tables, "func (%s) Find(charset.Charset, rune, []byte) ([]uint16, []byte, int) {\n", name)
	fmt.Fprintf(out.tables, "return nil, nil, 0\n")
	fmt.Fprintf(out.tables, "}\n\n")

	fmt.Fprintf(out.tables, "var %s_weights = map[uint32][]uint16{\n", name)
	var cp0min, cp1min rune = 0xFFFF, 0xFFFF
	for _, cp1 := range sortContractionTrie(trie) {
		for _, cnt := range trie[cp1] {
			cp0 := cnt.Path[1]
			mask := uint32(cp1)<<16 | uint32(cp0)
			weights := fmt.Sprintf("%#v", cnt.Weights)
			fmt.Fprintf(out.tables, "0x%x: %s,\n", mask, strings.TrimPrefix(weights, "[]uint16"))

			if cp0 < cp0min {
				cp0min = cp0
			}
			if cp1 < cp1min {
				cp1min = cp1
			}
		}
	}
	fmt.Fprintf(out.tables, "}\n\n")

	fmt.Fprintf(out.tables, "func (%s) FindContextual(cp1, cp0 rune) []uint16 {\n", name)
	fmt.Fprintf(out.tables, "if cp0 < %d || cp1 < %d || cp0 > 0xFFFF || cp1 > 0xFFFF {\n", cp0min, cp1min)
	fmt.Fprintf(out.tables, "return nil\n")
	fmt.Fprintf(out.tables, "}\n")
	fmt.Fprintf(out.tables, "return %s_weights[uint32(cp1) << 16 | uint32(cp0)]\n", name)
	fmt.Fprintf(out.tables, "}\n\n")
}

func (out *output) printContractionsFast(name string, allContractions []uca.Contraction) {
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
				panic("mixed Contextual and non-contextual contractions")
			}
			if len(ctr.Path) != 2 {
				panic("Contextual contraction with Path != 2")
			}
		}
	}

	fmt.Fprintf(out.tables, "type %s struct{}\n\n", name)

	if contextual {
		out.printFastContractionsCtx(name, allContractions)
		return
	}

	var wa = &weightarray{name: name + "_weights"}

	fmt.Fprintf(out.tables, "func (%s) Find(cs charset.Charset, cp0 rune, b0 []byte) ([]uint16, []byte, int) {\n", name)
	printContraction1(out.tables, wa, allContractions, 0)
	fmt.Fprintf(out.tables, "return nil, nil, 0\n")
	fmt.Fprintf(out.tables, "}\n\n")

	fmt.Fprintf(out.tables, "func (%s) FindContextual(cp1, cp0 rune) []uint16 {\n", name)
	fmt.Fprintf(out.tables, "return nil\n")
	fmt.Fprintf(out.tables, "}\n\n")

	wa.print(out.tables)
}
