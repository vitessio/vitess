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
)

func benchArray(n int, elem func(i int) string) string {
	var buf strings.Builder
	buf.WriteByte('[')
	for i := range n {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(elem(i))
	}
	buf.WriteByte(']')
	return buf.String()
}

// BenchmarkParse covers the shapes whose cost depends on how much the parser
// settles up front: numbers of each kind, and strings with and without escapes.
func BenchmarkParse(b *testing.B) {
	documents := []struct {
		name string
		doc  string
	}{
		{"integers", benchArray(1024, func(i int) string { return strconv.Itoa(i) })},
		{"fractions", benchArray(1024, func(i int) string { return strconv.Itoa(i) + ".25" })},
		{"exponents", benchArray(1024, func(i int) string { return strconv.Itoa(i) + "e3" })},
		{"big integers", benchArray(1024, func(i int) string { return fmt.Sprintf("922337203685477580%d", i%10) })},
		{"plain strings", benchArray(1024, func(i int) string { return fmt.Sprintf("%q", "value"+strconv.Itoa(i)) })},
		{"escaped strings", benchArray(1024, func(i int) string { return `"value\u0061` + strconv.Itoa(i) + `"` })},
		{"mixed object", `{"a":1,"b":2.5,"c":"str","d":[1,2,3],"e":{"f":4}}`},
	}

	for _, document := range documents {
		b.Run(document.name, func(b *testing.B) {
			raw := []byte(document.doc)
			var p Parser
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := p.ParseBytes(raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
