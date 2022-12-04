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

package global

import (
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	cell                  = "zone1"
	hostname              = "localhost"
	unshardedKeyspaceName = "uks"
	unshardedSQLSchema    = `
		CREATE TABLE t1(
			id BIGINT NOT NULL,
			PRIMARY KEY(id)
		) ENGINE=InnoDB;
	`
	unshardedVSchema = `
		{
			"sharded":false,
			"tables": {
				"t1": {}
			}
		}
	`
	shardedKeyspaceName = "sks"
	shardedSQLSchema    = `
		CREATE TABLE t2(
			id BIGINT NOT NULL,
			PRIMARY KEY(id)
		) ENGINE=InnoDB;
	`
	shardedVSchema = `
		{
			"sharded": true,
			"vindexes": {
				"hash": {
					"type": "hash"
				}
			},
			"tables": {
				"t2": {
					"columnVindexes": [
						{
							"column": "id",
							"name": "hash"
						}
					]
				}
			}
		}
	`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      unshardedKeyspaceName,
			SchemaSQL: unshardedSQLSchema,
			VSchema:   unshardedVSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*uKeyspace, 1, false); err != nil {
			return 1
		}

		sKeyspace := &cluster.Keyspace{
			Name:      shardedKeyspaceName,
			SchemaSQL: shardedSQLSchema,
			VSchema:   shardedVSchema,
		}
		if err := clusterInstance.StartKeyspace(*sKeyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = append(
			clusterInstance.VtGateExtraArgs,
			"--global-keyspace",
			"vt_global",
			"--global-keyspace",
			"uks",
		)
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}
