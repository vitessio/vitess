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
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	keyspaceName    = "commerce"
	cell            = "zone1"
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

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(1).
			WithRDOnly(1).
			WithSchema(sqlSchema).
			WithVSchema(vSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}

func testURL(t *testing.T, url string, testCaseName string) {
	statusCode := getStatusForURL(url)
	if got, want := statusCode, 200; got != want {
		assert.Equalf(t, want, got, "\nurl: %v\nstatus code: %v \nwant %v for %s", url, got, want, testCaseName)
	}
}

// getStatusForUrl returns the status code for the URL
func getStatusForURL(url string) int {
	resp, err := http.Get(url)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}
