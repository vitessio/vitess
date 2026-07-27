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

	"github.com/stretchr/testify/require"

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
