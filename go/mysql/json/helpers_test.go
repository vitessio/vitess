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

package json

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/vt/vthash"
)

func repeatedArray(elements int) string {
	var buf strings.Builder
	buf.WriteByte('[')
	for i := range elements {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(strconv.Itoa(i))
	}
	buf.WriteByte(']')
	return buf.String()
}

func fractionalArray(elements int) string {
	var buf strings.Builder
	buf.WriteByte('[')
	for i := range elements {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString(".25")
	}
	buf.WriteByte(']')
	return buf.String()
}

func repeatedObject(members int) string {
	var buf strings.Builder
	buf.WriteByte('{')
	for i := range members {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(`"key`)
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString(`":`)
		buf.WriteString(strconv.Itoa(i))
	}
	buf.WriteByte('}')
	return buf.String()
}

func BenchmarkValueHash(b *testing.B) {
	documents := []struct {
		name string
		doc  string
	}{
		{"number", `1234.5678`},
		{"string", `"a reasonably ordinary string value"`},
		{"array/4", repeatedArray(4)},
		{"array/64", repeatedArray(64)},
		{"array/1024", repeatedArray(1024)},
		{"fractional array/64", fractionalArray(64)},
		{"fractional array/1024", fractionalArray(1024)},
		{"object/4", repeatedObject(4)},
		{"object/64", repeatedObject(64)},
		{"nested", `{"a":[1,2,3],"b":{"c":[4,5,6],"d":"seven"},"e":[[8],[9]]}`},
	}

	for _, doc := range documents {
		b.Run(doc.name, func(b *testing.B) {
			var p Parser
			v, err := p.ParseBytes([]byte(doc.doc))
			require.NoError(b, err)

			hasher := vthash.New()

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				hasher.Reset()
				v.Hash(&hasher)
				_ = hasher.Sum128()
			}
		})
	}
}

func hashNumberText(t *testing.T, num string) vthash.Hash {
	t.Helper()

	h := vthash.New()
	require.Truef(t, hashDecimalText(&h, num), "%q was not recognised as a number", num)
	return h.Sum128()
}

// numberSpellings returns many spellings of relatively few values: the same
// digits with the decimal point walked across them, with and without a written
// exponent, signed and unsigned. Values that coincide are the point of the
// corpus, since those are the pairs a fingerprint has to agree on.
func numberSpellings() []string {
	coefficients := []string{
		"0", "00", "1", "7", "10", "100", "1000", "123", "1230", "1002003",
		"9007199254740992", "9007199254740993", "999999999999999999999",
		"1000000000000000000000000000",
	}
	exponents := []string{"", "e0", "e1", "e-1", "e5", "e-5", "e+5", "e27", "e-27", "E3"}

	var spellings []string
	for _, coefficient := range coefficients {
		mantissas := []string{coefficient}
		for point := 1; point < len(coefficient); point++ {
			mantissas = append(mantissas, coefficient[:point]+"."+coefficient[point:])
		}
		mantissas = append(mantissas, "0."+coefficient, coefficient+".0", coefficient+".")

		for _, mantissa := range mantissas {
			for _, exponent := range exponents {
				spellings = append(spellings, mantissa+exponent, "-"+mantissa+exponent)
			}
		}
	}
	return spellings
}

// TestNumberHashMatchesDecimalComparison is the safety net under fingerprinting
// numbers from their text: comparison converts them to exact decimals instead,
// and the two must sort every pair of spellings into the same equality classes.
func TestNumberHashMatchesDecimalComparison(t *testing.T) {
	var (
		spellings []string
		values    []decimal.Decimal
		hashes    []vthash.Hash
		skipped   int
	)
	for _, spelling := range numberSpellings() {
		value, err := decimal.NewFromString(spelling)
		if err != nil {
			// A spelling with no decimal value has nothing to agree with:
			// comparison fails on it rather than calling it equal to anything.
			skipped++
			continue
		}
		spellings = append(spellings, spelling)
		values = append(values, value)
		hashes = append(hashes, hashNumberText(t, spelling))
	}
	t.Logf("comparing %d spellings pairwise, %d had no decimal value", len(spellings), skipped)

	for i, left := range spellings {
		for j, right := range spellings {
			equal := values[i].Cmp(values[j]) == 0
			require.Equalf(t, equal, hashes[i] == hashes[j],
				"%q and %q compare equal=%v but hash equal=%v (as decimals %s and %s)",
				left, right, equal, hashes[i] == hashes[j], values[i], values[j])
		}
	}
}

// TestNumberHashSpansUnparseableSpellings checks that spellings the decimal
// parser turns down still fingerprint by value. Comparison errors on them, so
// nothing can contradict the fingerprint, and treating a signed exponent as the
// number it spells beats treating it as its own value.
func TestNumberHashSpansUnparseableSpellings(t *testing.T) {
	_, err := decimal.NewFromString("1e+5")
	require.Error(t, err)

	require.Equal(t, hashNumberText(t, "1e5"), hashNumberText(t, "1e+5"))
	require.Equal(t, hashNumberText(t, "100000"), hashNumberText(t, "1e+5"))
}

// TestNumberHashSpellings checks that hashing a parsed number fingerprints its
// value and not the way it was written.
//
// hashDecimalText is covered directly by TestNumberHashMatchesDecimalComparison,
// but nothing exercised the path a document actually takes, from the parser
// through hashNumber. That is where a spelling can survive as far as the
// fingerprint: a zero-padded exponent once reached the canonicaliser verbatim
// and was turned away for being long, so 1e0000000000 and 1 fingerprinted
// apart while comparison called them equal.
func TestNumberHashSpellings(t *testing.T) {
	// Every spelling in a group is one value; no two groups are.
	groups := [][]string{
		{"1", "1.0", "1e0", "1e+0", "1e-0", "1e0000000000", "1e-0000000000", "1.000", "0.1e1"},
		{"10", "1e1", "1e+1", "1e0000000001", "10.0", "0.1e2"},
		{"0", "-0", "0.0", "0e0", "0e0000000000", "0e-0000000000"},
		{"-1", "-1.0", "-1e0", "-1e0000000000"},
		// Precision beyond the form a number is kept in is not part of its
		// value: the first three are one double, the fourth stayed an integer.
		{"9007199254740992", "9007199254740992.0", "9007199254740992.1", "9007199254740993.0"},
		{"9007199254740993"},
	}

	hashes := map[string]vthash.Hash{}
	for _, group := range groups {
		for _, spelling := range group {
			var p Parser
			v, err := p.Parse(spelling)
			require.NoErrorf(t, err, "%q", spelling)

			h := vthash.New()
			v.Hash(&h)
			hashes[spelling] = h.Sum128()
		}
	}

	for i, group := range groups {
		for _, spelling := range group {
			require.Equalf(t, hashes[group[0]], hashes[spelling],
				"%q and %q are one value but fingerprint apart", group[0], spelling)
		}
		for j, other := range groups {
			if i == j {
				continue
			}
			require.NotEqualf(t, hashes[group[0]], hashes[other[0]],
				"%q and %q are different values but fingerprint alike", group[0], other[0])
		}
	}
}

// TestNumberHashRejectsNonNumbers checks that text with no decimal value falls
// out of the fast path rather than being canonicalised into a collision.
func TestNumberHashRejectsNonNumbers(t *testing.T) {
	for _, num := range []string{"", "-", ".", "-.", "e5", "1e", "1e+", "1.2.3", "1e2e3", "0x10", " 1", "1 ", "+1", "1e1234567890123"} {
		t.Run(fmt.Sprintf("%q", num), func(t *testing.T) {
			h := vthash.New()
			require.False(t, hashDecimalText(&h, num))
		})
	}
}
