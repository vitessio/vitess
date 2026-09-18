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

package vtgate

import (
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

// BenchmarkRouteExecuteOrderedScatter measures the large result shapes used to
// compare a full VTGate sort with a merge of shard-sorted result runs.
func BenchmarkRouteExecuteOrderedScatter(b *testing.B) {
	for _, shards := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			for _, rows := range []int{1024, 8192} {
				b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
					benchmarkRouteExecuteOrderedScatter(b, shards, rows)
				})
			}
		})
	}
}

// BenchmarkRouteExecuteOrderedScatterCrossover measures small through medium
// result shapes so an ordered scatter optimization does not move cost from
// large results to ordinary small results.
func BenchmarkRouteExecuteOrderedScatterCrossover(b *testing.B) {
	for _, shards := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			for _, rowsPerShard := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512} {
				b.Run(fmt.Sprintf("rows_per_shard=%d", rowsPerShard), func(b *testing.B) {
					benchmarkRouteExecuteOrderedScatter(b, shards, shards*rowsPerShard)
				})
			}
		})
	}
}

// BenchmarkRouteExecuteOrderedScatterSkew measures one large shard while the
// other shards hold only a few rows. It finds the amount of non-dominant work
// needed before merging beats repair of the nearly sorted flat result.
func BenchmarkRouteExecuteOrderedScatterSkew(b *testing.B) {
	for _, shards := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			for _, averageRows := range []int{5, 16, 256} {
				b.Run(fmt.Sprintf("average_rows_per_shard=%d", averageRows), func(b *testing.B) {
					for _, otherRows := range []int{1, 4, 16, 64} {
						if otherRows > averageRows {
							continue
						}
						b.Run(fmt.Sprintf("other_rows_per_shard=%d", otherRows), func(b *testing.B) {
							rows := make([]int, shards)
							for shard := range rows {
								rows[shard] = otherRows
							}
							rows[0] = shards*averageRows - (shards-1)*otherRows
							benchmarkRouteExecuteOrderedScatterRows(b, rows, true)
						})
					}
				})
			}
		})
	}
}

// BenchmarkRouteExecuteOrderedScatterGloballyOrdered measures runs whose shard
// order is already the final row order. It checks the boundary fast path that
// rebuilds response-order rows without a full sort or merge tree.
func BenchmarkRouteExecuteOrderedScatterGloballyOrdered(b *testing.B) {
	for _, shards := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			for _, rowsPerShard := range []int{16, 256} {
				b.Run(fmt.Sprintf("rows_per_shard=%d", rowsPerShard), func(b *testing.B) {
					rows := make([]int, shards)
					for shard := range rows {
						rows[shard] = rowsPerShard
					}
					benchmarkRouteExecuteOrderedScatterRows(b, rows, false)
				})
			}
		})
	}
}

func benchmarkRouteExecuteOrderedScatter(b *testing.B, shards, rows int) {
	rowsPerShard := rows / shards
	if rowsPerShard*shards != rows {
		b.Fatalf("cannot split %d rows evenly across %d shards", rows, shards)
	}
	rowsByShard := make([]int, shards)
	for shard := range rowsByShard {
		rowsByShard[shard] = rowsPerShard
	}
	benchmarkRouteExecuteOrderedScatterRows(b, rowsByShard, true)
}

func benchmarkRouteExecuteOrderedScatterRows(b *testing.B, rowsByShard []int, interleave bool) {
	ctx := b.Context()
	const cell = "aa"
	shards := len(rowsByShard)
	rows := 0
	for _, shardRows := range rowsByShard {
		rows += shardRows
	}

	u := createSandbox(KsTestUnsharded)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	u.VSchema = unshardedVSchema
	s.ShardSpec = orderedScatterShardSpec(shards)

	hc := discovery.NewFakeHealthCheck(nil)
	serv := newSandboxForCells(ctx, []string{cell})
	resolver := newTestResolver(ctx, hc, serv, cell)

	keyRanges, err := getAllShards(s.ShardSpec)
	if err != nil {
		b.Fatal(err)
	}
	if len(keyRanges) != shards {
		b.Fatalf("got %d shards, want %d", len(keyRanges), shards)
	}

	results := orderedScatterBenchmarkResults(rowsByShard, interleave)
	conns := make([]*sandboxconn.SandboxConn, 0, shards)
	for shardIdx, keyRange := range keyRanges {
		shard := key.KeyRangeString(keyRange)
		sbc := hc.AddTestTablet(cell, shard, 1, KsTestSharded, shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{results[shardIdx]})
		conns = append(conns, sbc)
	}

	executor := createExecutor(ctx, serv, cell, resolver)
	b.Cleanup(executor.Close)

	query := "select id from user order by id"
	session := &vtgatepb.Session{TargetString: "@primary"}
	result, err := executorExec(ctx, executor, session, query, nil)
	if err != nil {
		b.Fatal(err)
	}
	validateOrderedScatterBenchmarkResult(b, result, rows)

	for _, conn := range conns {
		queries := conn.GetQueries()
		if len(queries) != 1 {
			b.Fatalf("got %d shard queries, want 1", len(queries))
		}
		if !strings.Contains(strings.ToLower(queries[0].Sql), "order by") {
			b.Fatalf("shard query does not sort locally: %s", queries[0].Sql)
		}
	}

	// Reset consumed sandbox results and request logs outside the timer so the
	// measured path contains VTGate execution, not benchmark-fixture growth.
	for b.Loop() {
		b.StopTimer()
		for shard, conn := range conns {
			conn.SetResults([]*sqltypes.Result{results[shard]})
			conn.ClearQueries()
			conn.ClearOptions()
		}
		b.StartTimer()
		result, err = executorExec(ctx, executor, session, query, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != rows {
			b.Fatalf("got %d rows, want %d", len(result.Rows), rows)
		}
	}
}

func orderedScatterShardSpec(shards int) string {
	boundaries := make([]string, 0, shards+1)
	boundaries = append(boundaries, "")
	for shard := 1; shard < shards; shard++ {
		boundaries = append(boundaries, fmt.Sprintf("%02x", shard*256/shards))
	}
	boundaries = append(boundaries, "")
	return strings.Join(boundaries, "-")
}

func orderedScatterBenchmarkResults(rowsByShard []int, interleave bool) []*sqltypes.Result {
	results := make([]*sqltypes.Result, len(rowsByShard))
	for shard, rows := range rowsByShard {
		results[shard] = &sqltypes.Result{
			Fields: []*querypb.Field{{
				Name:    "id",
				Type:    sqltypes.Int64,
				Charset: collations.CollationBinaryID,
				Flags:   uint32(querypb.MySqlFlag_NUM_FLAG),
			}},
			Rows: make([][]sqltypes.Value, rows),
		}
	}
	value := int64(0)
	if !interleave {
		for _, result := range results {
			for row := range result.Rows {
				result.Rows[row] = []sqltypes.Value{sqltypes.NewInt64(value)}
				value++
			}
		}
		return results
	}
	for row := 0; ; row++ {
		added := false
		for _, result := range results {
			if row >= len(result.Rows) {
				continue
			}
			result.Rows[row] = []sqltypes.Value{sqltypes.NewInt64(value)}
			value++
			added = true
		}
		if !added {
			return results
		}
	}
}

func validateOrderedScatterBenchmarkResult(b *testing.B, result *sqltypes.Result, rows int) {
	b.Helper()
	if len(result.Rows) != rows {
		b.Fatalf("got %d rows, want %d", len(result.Rows), rows)
	}
	for row, values := range result.Rows {
		if len(values) != 1 {
			b.Fatalf("row %d has %d columns, want 1", row, len(values))
		}
		value, err := values[0].ToInt64()
		if err != nil {
			b.Fatalf("row %d is not an int64: %v", row, err)
		}
		if value != int64(row) {
			b.Fatalf("row %d has id %d, want %d", row, value, row)
		}
	}
}
