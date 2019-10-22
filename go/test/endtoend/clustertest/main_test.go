/*
Copyright 2019 The Vitess Authors.

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

package clustertest

import (
	"flag"
	"net/http"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	ClusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "commerce"
	Cell            = "zone1"
	SQLSchema       = `create table product( 
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

	VSchema = `{
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
		ClusterInstance = &cluster.LocalProcessCluster{Cell: Cell, Hostname: "localhost"}
		defer ClusterInstance.Teardown()

		// Start topo server
		err := ClusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SQLSchema,
			VSchema:   VSchema,
		}
		err = ClusterInstance.StartUnshardedKeyspace(*keyspace, 1, true)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = ClusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: ClusterInstance.Hostname,
			Port: ClusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func testURL(t *testing.T, url string, testCaseName string) {
	statusCode := getStatusForURL(url)
	if got, want := statusCode, 200; got != want {
		t.Errorf("select:\n%v want\n%v for %s", got, want, testCaseName)
	}
}

// getStatusForUrl returns the status code for the URL
func getStatusForURL(url string) int {
	resp, _ := http.Get(url)
	if resp != nil {
		return resp.StatusCode
	}
	return 0
}
