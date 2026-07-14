/*
Copyright 2022 The Vitess Authors.

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

package misc

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
	keyspaceName    = "ks_misc"
	uks             = "uks"

	//go:embed uschema.sql
	uschemaSQL string

	//go:embed schema.sql
	schemaSQL string

	//go:embed vschema.json
	vschema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithVTTabletArgs(
				"--queryserver-config-max-result-size", "1000000",
				"--queryserver-config-query-timeout", "2s",
				"--queryserver-config-transaction-timeout", "3s",
				"--queryserver-config-query-pool-timeout", "2s",
			),
			vitesst.WithVTGateArgs("--query-timeout", "100"),
			vitesst.WithKeyspace(uks).
				WithSchema(uschemaSQL),
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

		clusterInstance = cluster
		vtParams = cluster.VTParams(ctx, "")

		// create mysql instance and connection parameters
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
