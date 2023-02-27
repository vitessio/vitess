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

package collations

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/mysql/collations/internal/uca"
)

type CollationWithContractions struct {
	Collation    Collation
	Contractions []uca.Contraction
	ContractFast uca.Contractor
	ContractTrie uca.Contractor
}

func findContractedCollations(t testing.TB, unique bool) (result []CollationWithContractions) {
	type collationMetadata struct {
		Contractions []uca.Contraction
	}

	var seen = make(map[string]bool)

	for _, collation := range testall() {
		var contract uca.Contractor
		if uca, ok := collation.(*Collation_utf8mb4_uca_0900); ok {
			contract = uca.contract
		}
		if uca, ok := collation.(*Collation_uca_legacy); ok {
			contract = uca.contract
		}
		if contract == nil {
			continue
		}

		rf, err := os.Open(fmt.Sprintf("testdata/mysqldata/%s.json", collation.Name()))
		if err != nil {
			t.Skipf("failed to open JSON metadata (%v). did you run colldump?", err)
		}

		var meta collationMetadata
		if err := json.NewDecoder(rf).Decode(&meta); err != nil {
			t.Fatal(err)
		}
		rf.Close()

		if unique {
			raw := fmt.Sprintf("%#v", meta.Contractions)
			if seen[raw] {
				continue
			}
			seen[raw] = true
		}

		for n := range meta.Contractions {
			ctr := &meta.Contractions[n]
			for i := 0; i < len(ctr.Weights)-3; i += 3 {
				if ctr.Weights[i] == 0x0 && ctr.Weights[i+1] == 0x0 && ctr.Weights[i+2] == 0x0 {
					ctr.Weights = ctr.Weights[:i]
					break
				}
			}
		}

		result = append(result, CollationWithContractions{
			Collation:    collation,
			Contractions: meta.Contractions,
			ContractFast: contract,
			ContractTrie: uca.NewTrieContractor(meta.Contractions),
		})
	}
	return
}

func testMatch(t *testing.T, name string, cnt uca.Contraction, result []uint16, remainder []byte, skip int) {
	assert.True(t, reflect.DeepEqual(cnt.Weights, result), "%s didn't match: expected %#v, got %#v", name, cnt.Weights, result)
	assert.Equal(t, 0, len(remainder), "%s bad remainder: %#v", name, remainder)
	assert.Equal(t, len(cnt.Path), skip, "%s bad skipped length %d for %#v", name, skip, cnt.Path)

}

func TestUCAContractions(t *testing.T) {
	for _, cwc := range findContractedCollations(t, false) {
		t.Run(cwc.Collation.Name(), func(t *testing.T) {
			for _, cnt := range cwc.Contractions {
				if cnt.Contextual {
					head := cnt.Path[0]
					tail := cnt.Path[1]

					result := cwc.ContractTrie.FindContextual(head, tail)
					testMatch(t, "ContractTrie", cnt, result, nil, 2)

					result = cwc.ContractFast.FindContextual(head, tail)
					testMatch(t, "ContractFast", cnt, result, nil, 2)
					continue
				}

				head := cnt.Path[0]
				tail := string(cnt.Path[1:])

				result, remainder, skip := cwc.ContractTrie.Find(charset.Charset_utf8mb4{}, head, []byte(tail))
				testMatch(t, "ContractTrie", cnt, result, remainder, skip)

				result, remainder, skip = cwc.ContractFast.Find(charset.Charset_utf8mb4{}, head, []byte(tail))
				testMatch(t, "ContractFast", cnt, result, remainder, skip)
			}
		})
	}
}

func benchmarkFind(b *testing.B, input []byte, contract uca.Contractor) {
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		in := input
		for len(in) > 0 {
			cp, width := utf8.DecodeRune(in)
			in = in[width:]
			_, _, _ = contract.Find(charset.Charset_utf8mb4{}, cp, in)
		}
	}
}

func benchmarkFindJA(b *testing.B, input []byte, contract uca.Contractor) {
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		prev := rune(0)
		in := input
		for len(in) > 0 {
			cp, width := utf8.DecodeRune(in)
			_ = contract.FindContextual(cp, prev)
			prev = cp
			in = in[width:]
		}
	}
}

type strgen struct {
	repertoire   map[rune]struct{}
	contractions []string
}

func newStrgen() *strgen {
	return &strgen{repertoire: make(map[rune]struct{})}
}

func (s *strgen) withASCII() *strgen {
	for r := rune(0); r < utf8.RuneSelf; r++ {
		s.repertoire[r] = struct{}{}
	}
	return s
}

func (s *strgen) withContractions(all []uca.Contraction) *strgen {
	for _, cnt := range all {
		for _, r := range cnt.Path {
			s.repertoire[r] = struct{}{}
		}

		if cnt.Contextual {
			s.contractions = append(s.contractions, string([]rune{cnt.Path[1], cnt.Path[0]}))
		} else {
			s.contractions = append(s.contractions, string(cnt.Path))
		}
	}
	return s
}

func (s *strgen) withText(in string) *strgen {
	for _, r := range in {
		s.repertoire[r] = struct{}{}
	}
	return s
}

func (s *strgen) generate(length int, freq float64) (out []byte) {
	var flat []rune
	for r := range s.repertoire {
		flat = append(flat, r)
	}
	sort.Slice(flat, func(i, j int) bool {
		return flat[i] < flat[j]
	})

	gen := rand.New(rand.NewSource(0xDEADBEEF))
	out = make([]byte, 0, length)
	for len(out) < length {
		if gen.Float64() < freq {
			cnt := s.contractions[rand.Intn(len(s.contractions))]
			out = append(out, cnt...)
		} else {
			cp := flat[rand.Intn(len(flat))]
			out = append(out, string(cp)...)
		}
	}
	return
}

func BenchmarkUCAContractions(b *testing.B) {
	for _, cwc := range findContractedCollations(b, true) {
		if cwc.Contractions[0].Contextual {
			continue
		}

		gen := newStrgen().withASCII().withContractions(cwc.Contractions)
		frequency := 0.05
		input := gen.generate(1024*32, 0.05)

		b.Run(fmt.Sprintf("%s-%.02f-fast", cwc.Collation.Name(), frequency), func(b *testing.B) {
			benchmarkFind(b, input, cwc.ContractFast)
		})

		b.Run(fmt.Sprintf("%s-%.02f-trie", cwc.Collation.Name(), frequency), func(b *testing.B) {
			benchmarkFind(b, input, cwc.ContractTrie)
		})
	}
}

func BenchmarkUCAContractionsJA(b *testing.B) {
	for _, cwc := range findContractedCollations(b, true) {
		if !cwc.Contractions[0].Contextual {
			continue
		}

		gen := newStrgen().withASCII().withText(JapaneseString).withText(JapaneseString2).withContractions(cwc.Contractions)
		frequency := 0.05
		input := gen.generate(1024*32, 0.05)

		b.Run(fmt.Sprintf("%s-%.02f-fast", cwc.Collation.Name(), frequency), func(b *testing.B) {
			benchmarkFindJA(b, input, cwc.ContractFast)
		})

		b.Run(fmt.Sprintf("%s-%.02f-trie", cwc.Collation.Name(), frequency), func(b *testing.B) {
			benchmarkFindJA(b, input, cwc.ContractTrie)
		})
	}
}
