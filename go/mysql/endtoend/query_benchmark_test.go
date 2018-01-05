/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endtoend

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	vttestpb "github.com/youtube/vitess/go/vt/proto/vttest"
	"github.com/youtube/vitess/go/vt/vttest"
)

// This file contains various long-running tests for mysql.

// BenchmarkWithRealDatabase runs a real MySQL database, and runs all kinds
// of benchmarks on it. To minimize overhead, we only run one database, and
// run all the benchmarks on it.
func BenchmarkWithRealDatabase(b *testing.B) {
	// Launch MySQL.
	// We need a Keyspace in the topology, so the DbName is set.
	// We need a Shard too, so the database 'vttest' is created.
	cfg := vttest.Config{
		Topology: &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{
				{
					Name: "vttest",
					Shards: []*vttestpb.Shard{
						{
							Name:           "0",
							DbNameOverride: "vttest",
						},
					},
				},
			},
		},
		OnlyMySQL: true,
	}
	if err := cfg.InitSchemas("vttest", "create table a(id int, name varchar(128), primary key(id))", nil); err != nil {
		b.Fatalf("InitSchemas failed: %v\n", err)
	}
	defer os.RemoveAll(cfg.SchemaDir)
	cluster := vttest.LocalCluster{
		Config: cfg,
	}
	if err := cluster.Setup(); err != nil {
		b.Fatalf("could not launch mysql: %v\n", err)
	}
	defer cluster.TearDown()
	params := cluster.MySQLConnParams()

	b.Run("Inserts", func(b *testing.B) {
		benchmarkInserts(b, &params)
	})
	b.Run("ParallelReads", func(b *testing.B) {
		benchmarkParallelReads(b, &params, 10)
	})
}

func benchmarkInserts(b *testing.B, params *mysql.ConnParams) {
	// Connect.
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, params)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	// Delete what we may already have in the database.
	if _, err := conn.ExecuteFetch("delete from a", 0, false); err != nil {
		b.Fatalf("delete failed: %v", err)
	}

	// Now reset timer.
	b.ResetTimer()

	// Do the insert.
	for i := 0; i < b.N; i++ {
		_, err := conn.ExecuteFetch(fmt.Sprintf("insert into a(id, name) values(%v, 'nice name %v')", i, i), 0, false)
		if err != nil {
			b.Fatalf("ExecuteFetch(%v) failed: %v", i, err)
		}
	}
}

func benchmarkParallelReads(b *testing.B, params *mysql.ConnParams, parallelCount int) {
	ctx := context.Background()
	wg := sync.WaitGroup{}
	for i := 0; i < parallelCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			conn, err := mysql.Connect(ctx, params)
			if err != nil {
				b.Fatal(err)
			}

			for j := 0; j < b.N; j++ {
				if _, err := conn.ExecuteFetch("select * from a", 10000, true); err != nil {
					b.Fatalf("ExecuteFetch(%v, %v) failed: %v", i, j, err)
				}
			}
			conn.Close()
		}(i)
	}
	wg.Wait()
}
