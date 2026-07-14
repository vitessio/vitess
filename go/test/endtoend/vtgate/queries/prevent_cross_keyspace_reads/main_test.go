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

package preventcrosskeyspacereads

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	ks1Name         = "ks1"
	ks2Name         = "ks2"

	//go:embed ks1_schema.sql
	ks1SchemaSQL string

	//go:embed ks2_schema.sql
	ks2SchemaSQL string

	//go:embed ks1_vschema.json
	ks1VSchema string

	//go:embed ks2_vschema.json
	ks2VSchema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			// ks1 has prevent_cross_keyspace_reads: true in vschema.
			vitesst.WithKeyspace(ks1Name).
				WithSchema(ks1SchemaSQL).
				WithVSchema(ks1VSchema),
			// ks2 has no cross-keyspace join restriction.
			vitesst.WithKeyspace(ks2Name).
				WithSchema(ks2SchemaSQL).
				WithVSchema(ks2VSchema),
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		clusterInstance = cluster
		vtParams = cluster.VTParams(ctx, "")
		return m.Run()
	}()
	os.Exit(exitCode)
}
