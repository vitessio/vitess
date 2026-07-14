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

package orderby

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
	keyspaceName    = "ks_orderby"

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
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames("-80", "80-").
				WithReplicas(1).
				WithRDOnly(1).
				WithSchema(schemaSQL).
				WithVSchema(vschema),
			vitesst.WithVTGateArgs("--schema-change-signal=false", "--enable-system-settings=true"),
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

		mysqlConn, closer, err := vitesst.NewMySQL(ctx, cluster, keyspaceName, schemaSQL)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := closer(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "mysql teardown:", err)
			}
		}()
		mysqlParams = mysqlConn
		return m.Run()
	}()
	os.Exit(exitCode)
}
