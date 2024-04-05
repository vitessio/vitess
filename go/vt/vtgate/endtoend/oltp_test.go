package endtoend

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"vitess.io/vitess/go/mysql"
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

func BenchmarkOLTP(b *testing.B) {
	const MaxRows = 10000
	const RangeSize = 100

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	var query bytes.Buffer

	oltpInitOnce.Do(func() {
		b.Logf("seeding database for benchmark...")

		var rows int = 1
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

			_, err = conn.ExecuteFetch(query.String(), -1, false)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.Logf("finshed (inserted %d rows)", rows)
	})

	b.Run("SimpleRanges", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := rand.IntN(MaxRows)

			query.Reset()
			_, _ = fmt.Fprintf(&query, "SELECT c FROM oltp_test WHERE id BETWEEN %d AND %d", id, id+rand.IntN(RangeSize)-1)
			_, err := conn.ExecuteFetch(query.String(), 1000, false)
			if err != nil {
				b.Error(err)
			}
		}
	})

	b.Run("SumRanges", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := rand.IntN(MaxRows)

			query.Reset()
			_, _ = fmt.Fprintf(&query, "SELECT SUM(k) FROM oltp_test WHERE id BETWEEN %d AND %d", id, id+rand.IntN(RangeSize)-1)
			_, err := conn.ExecuteFetch(query.String(), 1000, false)
			if err != nil {
				b.Error(err)
			}
		}
	})

	b.Run("OrderRanges", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := rand.IntN(MaxRows)

			query.Reset()
			_, _ = fmt.Fprintf(&query, "SELECT c FROM oltp_test WHERE id BETWEEN %d AND %d ORDER BY c", id, id+rand.IntN(RangeSize)-1)
			_, err := conn.ExecuteFetch(query.String(), 1000, false)
			if err != nil {
				b.Error(err)
			}
		}
	})

	b.Run("DistinctRanges", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := rand.IntN(MaxRows)

			query.Reset()
			_, _ = fmt.Fprintf(&query, "SELECT DISTINCT c FROM oltp_test WHERE id BETWEEN %d AND %d ORDER BY c", id, id+rand.IntN(RangeSize)-1)
			_, err := conn.ExecuteFetch(query.String(), 1000, false)
			if err != nil {
				b.Error(err)
			}
		}
	})
}
