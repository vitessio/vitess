/*
Copyright 2020 The Vitess Authors.

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

package healthcheck

import (
	"context"
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
	keyspaceName    = "commerce"
	sqlSchema       = `create table product(
		sku varbinary(128),
			description varbinary(128),
			price bigint,
			primary key(sku)
		) ENGINE=InnoDB;
		create table customer(
			id bigint not null auto_increment,
			email varchar(128),
			primary key(id)
		) ENGINE=InnoDB;
		create table corder(
			order_id bigint not null auto_increment,
			customer_id bigint,
			sku varbinary(128),
			price bigint,
			primary key(order_id)
		) ENGINE=InnoDB;`

	vSchema = `{
						"tables": {
							"product": {},
							"customer": {},
							"corder": {}
						}
					}`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithVTTabletArgs("--health-check-interval", "1s", "--shutdown-grace-period", "3s"),
			vitesst.WithVTOrc(),
			vitesst.WithKeyspace(keyspaceName).
				WithReplicas(1).
				WithRDOnly(1).
				WithSchema(sqlSchema).
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
		vtParams = cluster.VTParams(ctx, "")
		return m.Run()
	}()
	os.Exit(exitCode)
}
