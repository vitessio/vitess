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

package tablethealthcheckcache

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance       *vitesst.Cluster
	vtParams              mysql.ConnParams
	tabletRefreshInterval = 5 * time.Second
	keyspaceName          = "healthcheck_test_ks"
	cell                  = "healthcheck_test_cell"
	shards                = []string{"-80", "80-"}
	schemaSQL             = `
create table customer(
	customer_id bigint not null auto_increment,
	email varbinary(128),
	primary key(customer_id)
) ENGINE=InnoDB;
create table corder(
	order_id bigint not null auto_increment,
	customer_id bigint,
	sku varbinary(128),
	price bigint,
	primary key(order_id)
) ENGINE=InnoDB;
`

	vSchema = `
{
	"sharded": true,
	"vindexes": {
		"hash": {
			"type": "hash"
		}
	},
	"tables": {
		"customer": {
			"column_vindexes": [
				{
					"column": "customer_id",
					"name": "hash"
				}
			]
		},
		"corder": {
			"column_vindexes": [
				{
					"column": "customer_id",
					"name": "hash"
				}
			]
		}
	}
}
`
)

// TestMain sets up the vitess cluster for any subsequent tests
func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithCells(cell),
			vitesst.WithVTTabletArgs("--health-check-interval", "1s"),
			vitesst.WithVTGateArgs("--tablet-refresh-interval", tabletRefreshInterval.String()),
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames(shards...).
				WithReplicas(1).
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
		vtParams = cluster.VTParams(ctx, "")
		return m.Run()
	}()
	os.Exit(exitCode)
}

// TestHealthCheckCacheWithTabletChurn verifies that the tablet healthcheck cache has the correct number of records
// after many rounds of adding and removing tablets in quick succession. This verifies that we don't have any race
// conditions with these operations and their interactions with the cache.
func TestHealthCheckCacheWithTabletChurn(t *testing.T) {
	ctx := t.Context()
	tries := 5
	numShards := len(shards)
	// 1 for primary,replica
	expectedTabletHCcacheEntries := numShards * 2
	churnTabletType := "rdonly"

	// verify output of SHOW VITESS_TABLETS
	vtgateConn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer vtgateConn.Close()
	query := "show vitess_tablets"

	// starting with two shards, each with 1 primary and 1 replica tablet)
	// we'll be adding and removing a tablet of type churnTabletType
	qr, _ := vtgateConn.ExecuteFetch(query, 100, true)
	assert.Equal(t, expectedTabletHCcacheEntries, len(qr.Rows), "wrong number of tablet records in healthcheck cache, expected %d but had %d. Results: %v", expectedTabletHCcacheEntries, len(qr.Rows), qr.Rows)

	for range tries {
		tablet := addTablet(ctx, t, churnTabletType)
		expectedTabletHCcacheEntries++

		qr, _ := vtgateConn.ExecuteFetch(query, 100, true)
		assert.Equal(t, expectedTabletHCcacheEntries, len(qr.Rows), "wrong number of tablet records in healthcheck cache, expected %d but had %d. Results: %v", expectedTabletHCcacheEntries, len(qr.Rows), qr.Rows)

		deleteTablet(ctx, t, tablet)
		expectedTabletHCcacheEntries--

		// We need to sleep for at least vtgate's --tablet-refresh-interval to be sure we
		// have resynchronized the healthcheck cache with the topo server via the topology
		// watcher and pruned the deleted tablet from the healthcheck cache.
		time.Sleep(tabletRefreshInterval)

		qr, _ = vtgateConn.ExecuteFetch(query, 100, true)
		assert.Equal(t, expectedTabletHCcacheEntries, len(qr.Rows), "wrong number of tablet records in healthcheck cache, expected %d but had %d. Results: %v", expectedTabletHCcacheEntries, len(qr.Rows), qr.Rows)
	}

	// one final time, w/o the churning tablet
	qr, _ = vtgateConn.ExecuteFetch(query, 100, true)
	assert.Equal(t, expectedTabletHCcacheEntries, len(qr.Rows), "wrong number of tablet records in healthcheck cache, expected %d but had %d", expectedTabletHCcacheEntries, len(qr.Rows))
}

func addTablet(ctx context.Context, t *testing.T, tabletType string) *vitesst.Tablet {
	tablet, err := clusterInstance.AddTablet(ctx, "", keyspaceName, shards[0], tabletType)
	require.Nil(t, err)

	name := fmt.Sprintf("%s.%s.%s", keyspaceName, shards[0], tabletType)
	_, _, err = clusterInstance.VTGate().MakeAPICallRetry(ctx, "/debug/vars", 30*time.Second,
		func(status int, body string) bool {
			if status != 200 {
				return false
			}
			var vars map[string]any
			if err := json.Unmarshal([]byte(body), &vars); err != nil {
				return false
			}
			conns, ok := vars["HealthcheckConnections"].(map[string]any)
			if !ok {
				return false
			}
			count, ok := conns[name]
			return ok && fmt.Sprintf("%v", count) == "1"
		})
	require.Nil(t, err)

	t.Logf("Added tablet: %s", tablet.Alias())
	return tablet
}

func deleteTablet(ctx context.Context, t *testing.T, tablet *vitesst.Tablet) {
	err := tablet.Remove(ctx)
	require.Nil(t, err)

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "DeleteTablets", tablet.Alias())
	require.Nil(t, err)

	t.Logf("Deleted tablet: %s", tablet.Alias())
}
