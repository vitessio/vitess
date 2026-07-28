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
	"strconv"
	"strings"
	"testing"
)

// numberArray builds a JSON array of n numbers, each spelled by digits.
func numberArray(n int, digits func(i int) string) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(digits(i))
	}
	sb.WriteByte(']')
	return sb.String()
}

// benchDocs covers the number shapes the parser takes different paths through.
//
// The first four are the everyday ones, where the cost is the scan itself. The
// last three reach the magnitude check, which a number only pays for when its
// digits and its exponent together could carry it past the largest double:
// once with an exponent large enough to ask the question of every element, once
// where the digits alone are enough to ask it, and once for a document the
// answer rejects.
var benchDocs = []struct {
	name     string
	doc      string
	rejected bool
}{
	{name: "int/1024", doc: numberArray(1024, func(i int) string { return strconv.Itoa(i * 7919) })},
	{name: "frac/1024", doc: numberArray(1024, func(i int) string { return strconv.Itoa(i) + "." + strconv.Itoa(i*7919) })},
	{name: "exp/1024", doc: numberArray(1024, func(i int) string { return strconv.Itoa(i) + "." + strconv.Itoa(i*7919) + "e" + strconv.Itoa(i%300) })},
	{name: "mixed-object", doc: `{"id":38141,"name":"a name","ok":true,"score":-12.5e3,"tags":["x","y"],"meta":null}`},

	{name: "checked/1024", doc: numberArray(1024, func(i int) string { return "1." + strconv.Itoa(i) + "e30" + strconv.Itoa(i%8) })},
	{name: "checked-long-fraction", doc: "0." + strings.Repeat("0", 400) + "1e-400"},
	{name: "rejected", doc: strings.Repeat("9", 400) + "e-100", rejected: true},
}

func BenchmarkParse(b *testing.B) {
	for _, tc := range benchDocs {
		b.Run(tc.name, func(b *testing.B) {
			var p Parser

			// A case that stops early measures the error path instead of the one
			// it was written for, and reads as a speed-up while doing it.
			if _, err := p.Parse(tc.doc); (err != nil) != tc.rejected {
				b.Fatalf("document does not take the path this case measures: err=%v", err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(tc.doc)))
			for b.Loop() {
				v, err := p.Parse(tc.doc)
				if err == nil && v == nil {
					b.Fatal("parsed to no value")
				}
			}
		})
	}
}
