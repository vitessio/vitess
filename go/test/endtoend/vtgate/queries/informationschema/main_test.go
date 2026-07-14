/*
Copyright 2021 The Vitess Authors.

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

package informationschema

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
	mysqlParams     mysql.ConnParams
	keyspaceName    = "ks"

	//go:embed schema.sql
	schemaSQL string

	//go:embed vschema.json
	vschema      string
	routingRules = `
{"rules": [
  {
    "from_table": "ks.t1000",
	"to_tables": ["ks.t1"]
  }
]}
`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithVTGateArgs("--schema-change-signal"),
			vitesst.WithVTTabletArgs("--queryserver-config-schema-change-signal"),
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames("-80", "80-").
				WithSchema(schemaSQL).
				WithVSchema(vschema),
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

		if err := cluster.Vtctld().ExecuteCommand(ctx, "ApplyRoutingRules", "--rules", routingRules); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		if err := cluster.Vtctld().ExecuteCommand(ctx, "RebuildVSchemaGraph"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		clusterInstance = cluster
		vtParams = cluster.VTParams(ctx, "")

		conn, closer, err := vitesst.NewMySQL(ctx, cluster, keyspaceName, schemaSQL)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := closer(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "mysql teardown:", err)
			}
		}()
		mysqlParams = conn
		return m.Run()
	}()
	os.Exit(exitCode)
}
