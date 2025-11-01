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
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	twopcutil "vitess.io/vitess/go/test/endtoend/transaction/twopc/utils"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
	sidecarDBName   = "vt_ks"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:             keyspaceName,
			SchemaSQL:        SchemaSQL,
			VSchema:          VSchema,
			SidecarDBName:    sidecarDBName,
			DurabilityPolicy: policy.DurabilitySemiSync,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-40", "40-80", "80-c0", "c0-"}, 1, false); err != nil {
			return 1
		}

		// Start Vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}
		vtParams = clusterInstance.GetVTParams(keyspaceName)

		return m.Run()
	}()
	os.Exit(exitcode)
}

func start(b *testing.B) (*mysql.Conn, func()) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(b, err)
	cleanup(b)

	return conn, func() {
		conn.Close()
		cleanup(b)
	}
}

func cleanup(b *testing.B) {
	twopcutil.ClearOutTable(b, vtParams, "test")
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
			conn, _ := start(b)
			_, err := conn.ExecuteFetch("set transaction_mode = "+commitMode, 0, false)
			if err != nil {
				b.Fatal(err)
			}
			b.Run(commitMode+tc.name, func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err = conn.ExecuteFetch("begin", 0, false)
					if err != nil {
						b.Fatal(err)
					}

					randomMsg := randomStrings[id%sampleSize]
					// Perform the specified number of inserts to specific shards
					for _, val := range tc.shardStart {
						_, err = conn.ExecuteFetch(fmt.Sprintf("insert into test(id, msg) values(%d, '%s')", val+(4*id), randomMsg), 0, false)
						if err != nil {
							b.Fatal(err)
						}
					}
					id++

					_, err = conn.ExecuteFetch("commit", 0, false)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			conn.Close()
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
