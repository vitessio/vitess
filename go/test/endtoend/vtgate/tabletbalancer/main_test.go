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

package tabletbalancer

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	cell1           = "zone1"
	cell2           = "zone2"
	keyspaceName    = "ks"

	//go:embed schema.sql
	schemaSQL string

	vSchema = `{
		"sharded": false,
		"tables": {
			"balancer_test": {}
		}
	}`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithCells(cell1, cell2),
			vitesst.WithKeyspace(keyspaceName).
				WithReplicas(5).
				WithSchema(schemaSQL).
				WithVSchema(vSchema),
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

		// we need to create an alias to make sure that we'll use both cells at all, even with cells_to_watch
		allCells := fmt.Sprintf("%s,%s", cell1, cell2)
		if err := cluster.Vtctld().ExecuteCommand(ctx, "AddCellsAlias",
			"--cells", allCells,
			"combined_cells"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}
