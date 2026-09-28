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
// last three reach the magnitude check, which a number only pays for once it is
// written to more digits than its exponent leaves a double room for: once with
// an exponent that shrinks the room to ask the question of every element, once
// where the digits alone are past what a double has, and once for a document the
// answer rejects. reachesMagnitudeCheck holds them to that, since a case that
// falls short of the check goes on measuring the scan and reads as though the
// check were free.
var benchDocs = []struct {
	name     string
	doc      string
	checked  bool
	rejected bool
}{
	{name: "int/1024", doc: numberArray(1024, func(i int) string { return strconv.Itoa(i * 7919) })},
	{name: "frac/1024", doc: numberArray(1024, func(i int) string { return strconv.Itoa(i) + "." + strconv.Itoa(i*7919) })},
	{name: "exp/1024", doc: numberArray(1024, func(i int) string { return strconv.Itoa(i) + "." + strconv.Itoa(i*7919) + "e" + strconv.Itoa(i%300) })},
	{name: "mixed-object", doc: `{"id":38141,"name":"a name","ok":true,"score":-12.5e3,"tags":["x","y"],"meta":null}`},

	{name: "checked/1024", doc: numberArray(1024, func(i int) string { return "1" + strconv.Itoa(1000000000000000000+i) + "e289" }), checked: true},
	{name: "checked-long-fraction", doc: "1" + strings.Repeat("0", 308) + "." + strings.Repeat("5", 400), checked: true},
	{name: "rejected", doc: strings.Repeat("9", 400) + "e-100", checked: true, rejected: true},
}

// reachesMagnitudeCheck reports whether every number in doc is converted to find
// out whether a double can hold it, which is what the checked cases are for. A
// number written to fewer digits than a double has room for answers that question
// from its digits alone, and a case built out of those measures the scan it shares
// with every other case instead.
func reachesMagnitudeCheck(doc string) bool {
	numbers := 0
	for i := 0; i < len(doc); {
		if c := doc[i]; c != '-' && (c < '0' || c > '9') {
			i++
			continue
		}
		flen, exponent, ok := readFloat(doc[i:])
		if !ok {
			return false
		}
		if !mayExceedFloat64(doc[i:i+flen], exponent) {
			return false
		}
		numbers++
		i += flen
	}
	return numbers > 0
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
			if reachesMagnitudeCheck(tc.doc) != tc.checked {
				b.Fatalf("document reaches the magnitude check: %v, want %v", !tc.checked, tc.checked)
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
