package endtoend

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	osExec "os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

var mirrorInitOnce sync.Once

func BenchmarkMirror(b *testing.B) {
	const numRows = 10000

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	var query bytes.Buffer

	mirrorInitOnce.Do(func() {
		b.Logf("seeding database for benchmark...")

		for i := 0; i < numRows; i++ {
			query.Reset()
			query.WriteString(fmt.Sprintf("INSERT INTO ks.mirror_tbl1(id) VALUES(%d)", i))
			_, err = conn.ExecuteFetch(query.String(), -1, false)
			if err != nil {
				b.Fatal(err)
			}

			query.Reset()
			query.WriteString(fmt.Sprintf("INSERT INTO ks2.mirror_tbl1(id) VALUES(%d)", i))
			_, err = conn.ExecuteFetch(query.String(), -1, false)
			if err != nil {
				b.Fatal(err)
			}

			query.Reset()
			query.WriteString(fmt.Sprintf("INSERT INTO ks.mirror_tbl2(id) VALUES(%d)", i))
			_, err = conn.ExecuteFetch(query.String(), -1, false)
			if err != nil {
				b.Fatal(err)
			}

			query.Reset()
			query.WriteString(fmt.Sprintf("INSERT INTO ks3.mirror_tbl2(id) VALUES(%d)", i))
			_, err = conn.ExecuteFetch(query.String(), -1, false)
			if err != nil {
				b.Fatal(err)
			}
		}

		b.Logf("finished (inserted %d rows)", numRows)
	})

	testCases := []struct {
		name string
		run  func(*testing.B, *rand.Rand)
	}{
		{
			name: "point select, { ks => ks1 }.tbl1",
			run: func(b *testing.B, rnd *rand.Rand) {
				for i := 0; i < b.N; i++ {
					id := rnd.Intn(numRows)
					query.Reset()
					_, _ = fmt.Fprintf(
						&query,
						"SELECT t1.id, t2.id FROM ks.mirror_tbl1 AS t1, ks.mirror_tbl2 AS t2 WHERE t1.id = %d AND t2.id = %d",
						id, id,
					)
					_, err := conn.ExecuteFetch(query.String(), 1, false)
					if err != nil {
						b.Error(err)
					}
				}
			},
		},
		{
			name: "point select, { ks => ks2 }.tbl1, { ks => ks3 }.tbl2",
			run: func(b *testing.B, rnd *rand.Rand) {
				for i := 0; i < b.N; i++ {
					id := rnd.Intn(numRows)
					query.Reset()
					_, _ = fmt.Fprintf(
						&query,
						"SELECT t1.id, t2.id FROM ks.mirror_tbl1 AS t1, ks.mirror_tbl2 AS t2 WHERE t1.id = %d AND t2.id = %d",
						id, id,
					)
					_, err := conn.ExecuteFetch(query.String(), 1, false)
					if err != nil {
						b.Error(err)
					}
				}
			},
		},
	}

	// Each time this BenchmarkMirror runs, use a different source of
	// random-ness. But use the same source of randomness across test cases and
	// mirror percentages sub-tests.
	randSeed := time.Now().UnixNano()

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("mirror 0%", func(b *testing.B) {
				mirrorTraffic(b, 0)
				b.ResetTimer()
				tc.run(b, rand.New(rand.NewSource(randSeed)))
			})

			b.Run("mirror 1%", func(b *testing.B) {
				mirrorTraffic(b, 1)
				b.ResetTimer()
				tc.run(b, rand.New(rand.NewSource(randSeed)))
			})

			b.Run("mirror 5%", func(b *testing.B) {
				mirrorTraffic(b, 5)
				b.ResetTimer()
				tc.run(b, rand.New(rand.NewSource(randSeed)))
			})

			b.Run("mirror 10%", func(b *testing.B) {
				mirrorTraffic(b, 10)
				b.ResetTimer()
				tc.run(b, rand.New(rand.NewSource(randSeed)))
			})

			b.Run("mirror 25%", func(b *testing.B) {
				mirrorTraffic(b, 25)
				b.ResetTimer()
				tc.run(b, rand.New(rand.NewSource(randSeed)))
			})

			b.Run("mirror 50%", func(b *testing.B) {
				mirrorTraffic(b, 50)
				b.ResetTimer()
				tc.run(b, rand.New(rand.NewSource(randSeed)))
			})

			b.Run("mirror 100%", func(b *testing.B) {
				mirrorTraffic(b, 100)
				b.ResetTimer()
				tc.run(b, rand.New(rand.NewSource(randSeed)))
			})
		})
	}
}

func mirrorTraffic(b *testing.B, percent float32) {
	server := fmt.Sprintf("localhost:%v", cluster.VTProcess().PortGrpc)
	rules := fmt.Sprintf(`{
		"rules": [
			{
				"from_table": "ks.mirror_tbl1",
				"to_table": "ks2.mirror_tbl1",
				"percent": %f
			},
			{
				"from_table": "ks.mirror_tbl2",
				"to_table": "ks3.mirror_tbl2",
				"percent": %f
			}
		]
	}`, percent, percent)
	_, err := osExec.Command("vtctldclient", "--server", server, "ApplyMirrorRules", "--rules", rules).CombinedOutput()
	require.NoError(b, err)
}
