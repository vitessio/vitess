/*
Copyright 2025 The Vitess Authors.

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

package endtoend

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

// 10 groups, 119 characters
const cValueTemplate = "###########-###########-###########-" +
	"###########-###########-###########-" +
	"###########-###########-###########-" +
	"###########"

// 5 groups, 59 characters
const padValueTemplate = "###########-###########-###########-" +
	"###########-###########"

func sysbenchRandom(template string) []byte {
	out := make([]byte, 0, len(template))
	for i := range template {
		switch template[i] {
		case '#':
			out = append(out, '0'+byte(rand.IntN(10)))
		default:
			out = append(out, template[i])
		}
	}
	return out
}

var oltpInitOnce sync.Once

// BenchmarkOLTP assesses `OLTP` queries execution.
// Recommended run options:
/*
export ver=v1 p=~/path && go test \
-run '^$' -bench '^BenchmarkOLTP' \
-benchtime 10s -count 5 -cpu 4 -benchmem \
-memprofile=$p/${ver}.mem.pprof -cpuprofile=$p/${ver}.cpu.pprof \
| tee $p/${ver}.txt
*/
func BenchmarkOLTP(b *testing.B) {
	const MaxRows = 10000
	const RangeSize = 100

	client := framework.NewClient()

	var query bytes.Buffer

	oltpInitOnce.Do(func() {
		b.Logf("seeding database for benchmark...")

		var rows = 1
		for i := 0; i < MaxRows/10; i++ {
			query.Reset()
			query.WriteString("insert into oltp_test(id, k, c, pad) values ")
			for j := 0; j < 10; j++ {
				if j > 0 {
					query.WriteString(", ")
				}
				_, _ = fmt.Fprintf(&query, "(%d, %d, '%s', '%s')", rows, rand.Int32N(0xFFFF), sysbenchRandom(cValueTemplate), sysbenchRandom(padValueTemplate))
				rows++
			}

			_, err := client.Execute(query.String(), nil)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.Logf("finshed (inserted %d rows)", rows)
	})

	b.Run("SimpleRanges", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := rand.IntN(MaxRows)

			query.Reset()
			_, _ = fmt.Fprintf(&query, "SELECT c FROM oltp_test WHERE id BETWEEN %d AND %d", id, id+rand.IntN(RangeSize)-1)
			_, err := client.Execute(query.String(), nil)
			if err != nil {
				b.Error(err)
			}
		}
	})

	b.Run("SumRanges", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := rand.IntN(MaxRows)

			query.Reset()
			_, _ = fmt.Fprintf(&query, "SELECT SUM(k) FROM oltp_test WHERE id BETWEEN %d AND %d", id, id+rand.IntN(RangeSize)-1)
			_, err := client.Execute(query.String(), nil)
			if err != nil {
				b.Error(err)
			}
		}
	})

	b.Run("OrderRanges", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := rand.IntN(MaxRows)

			query.Reset()
			_, _ = fmt.Fprintf(&query, "SELECT c FROM oltp_test WHERE id BETWEEN %d AND %d ORDER BY c", id, id+rand.IntN(RangeSize)-1)
			_, err := client.Execute(query.String(), nil)
			if err != nil {
				b.Error(err)
			}
		}
	})

	b.Run("DistinctRanges", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := rand.IntN(MaxRows)

			query.Reset()
			_, _ = fmt.Fprintf(&query, "SELECT DISTINCT c FROM oltp_test WHERE id BETWEEN %d AND %d ORDER BY c", id, id+rand.IntN(RangeSize)-1)
			_, err := client.Execute(query.String(), nil)
			if err != nil {
				b.Error(err)
			}
		}
	})
}
