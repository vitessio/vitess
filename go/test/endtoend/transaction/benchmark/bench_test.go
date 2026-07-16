/*
Copyright 2024 The Vitess Authors.

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

package benchmark

import (
	"context"
	_ "embed"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	twopcutil "vitess.io/vitess/go/test/endtoend/transaction/twopc/utils"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

var (
	keyspaceName  = "ks"
	sidecarDBName = "vt_ks"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func startCluster(b *testing.B) mysql.ConnParams {
	b.Helper()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-40", "40-80", "80-c0", "c0-").
			WithReplicas(1).
			WithSchema(SchemaSQL).
			WithVSchema(VSchema).
			WithSidecarDBName(sidecarDBName).
			WithDurabilityPolicy(policy.DurabilitySemiSync),
	)
	require.NoError(b, err)

	cleanup, err := cluster.Start(b.Context())
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(b.Context()), time.Minute)
		defer cancel()
		if b.Failed() {
			cluster.DumpDiagnostics(ctx, b.Logf)
		}
		if err := cleanup(ctx); err != nil {
			b.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(b, err)

	return cluster.VTParams(b.Context(), "")
}

func setup(b *testing.B, vtParams mysql.ConnParams) *mysql.Conn {
	b.Helper()

	conn, err := mysql.Connect(b.Context(), &vtParams)
	require.NoError(b, err)
	twopcutil.ClearOutTable(b, vtParams, "test")
	b.Cleanup(func() {
		conn.Close()
		twopcutil.ClearOutTable(b, vtParams, "test")
	})

	return conn
}

// BenchmarkTwoPCCommit benchmarks the performance of a two-phase commit transaction
// with varying numbers of inserts.
// Recommended run options:
/*
export ver=v1 p=~/path && go test \
-run '^$' -bench '^BenchmarkTwoPCCommit' \
-benchtime 3s -count 6 -cpu 8
| tee $p/${ver}.txt
*/
func BenchmarkTwoPCCommit(b *testing.B) {
	b.StopTimer()
	vtParams := startCluster(b)

	// Pre-generate 100 random strings
	const sampleSize = 100
	randomStrings := generateRandomStrings(sampleSize, 25)

	// Define table-driven test cases
	testCases := []struct {
		name       string
		shardStart []int // Number of different shard involved per transaction
	}{
		{name: "SingleShard", shardStart: []int{1}},
		{name: "TwoShards", shardStart: []int{3, 4}},
		{name: "ThreeShards", shardStart: []int{2, 3, 4}},
		{name: "FourShards", shardStart: []int{4, 3, 2, 1}},
	}

	// Incremental id for inserts
	id := 1

	for _, tc := range testCases {
		for _, commitMode := range []string{"twopc", "multi"} {
			b.Run(commitMode+tc.name, func(b *testing.B) {
				b.StopTimer()
				conn := setup(b, vtParams)
				_, err := conn.ExecuteFetch("set transaction_mode = "+commitMode, 0, false)
				require.NoError(b, err)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err = conn.ExecuteFetch("begin", 0, false)
					require.NoError(b, err)

					randomMsg := randomStrings[id%sampleSize]
					// Perform the specified number of inserts to specific shards
					for _, val := range tc.shardStart {
						_, err = conn.ExecuteFetch(fmt.Sprintf("insert into test(id, msg) values(%d, '%s')", val+(4*id), randomMsg), 0, false)
						require.NoError(b, err)
					}
					id++

					_, err = conn.ExecuteFetch("commit", 0, false)
					require.NoError(b, err)
				}
				b.StopTimer()
			})
		}
	}
}

// generateRandomStrings generates 'n' random strings, each of length 'length'.
func generateRandomStrings(n, length int) []string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]string, n)
	for i := range result {
		b := make([]byte, length)
		for j := range b {
			b[j] = charset[rand.IntN(len(charset))]
		}
		result[i] = string(b)
	}
	return result
}
