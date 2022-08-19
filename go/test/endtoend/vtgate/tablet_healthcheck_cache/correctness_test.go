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
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
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
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
			VSchema:   vSchema,
		}
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, []string{"--health_check_interval", "1s"}...)
		err = clusterInstance.StartKeyspace(*keyspace, shards, 1, false)
		if err != nil {
			return 1
		}

		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, []string{"--tablet_refresh_interval", tabletRefreshInterval.String()}...)
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

// TestHealthCheckCacheWithTabletChurn verifies that the tablet healthcheck cache has the correct number of records
// after many rounds of adding and removing tablets in quick succession. This verifies that we don't have any race
// conditions with these operations and their interactions with the cache.
func TestHealthCheckCacheWithTabletChurn(t *testing.T) {
	ctx := context.Background()
	tries := 5
	numShards := len(shards)
	// 1 for primary,replica
	expectedTabletHCcacheEntries := numShards * 2
	churnTabletUID := 9999
	churnTabletType := "rdonly"

	// verify output of SHOW VITESS_TABLETS
	vtgateConn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer vtgateConn.Close()
	query := "show vitess_tablets"

	// starting with two shards, each with 1 primary and 1 replica tablet)
	// we'll be adding and removing a tablet of type churnTabletType with churnTabletUID
	qr, _ := vtgateConn.ExecuteFetch(query, 100, true)
	assert.Equal(t, expectedTabletHCcacheEntries, len(qr.Rows), "wrong number of tablet records in healthcheck cache, expected %d but had %d. Results: %v", expectedTabletHCcacheEntries, len(qr.Rows), qr.Rows)

	for i := 0; i < tries; i++ {
		tablet := addTablet(t, churnTabletUID, churnTabletType)
		expectedTabletHCcacheEntries++

		qr, _ := vtgateConn.ExecuteFetch(query, 100, true)
		assert.Equal(t, expectedTabletHCcacheEntries, len(qr.Rows), "wrong number of tablet records in healthcheck cache, expected %d but had %d. Results: %v", expectedTabletHCcacheEntries, len(qr.Rows), qr.Rows)

		deleteTablet(t, tablet)
		expectedTabletHCcacheEntries--

		// We need to sleep for at least vtgate's --tablet_refresh_interval to be sure we
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

func addTablet(t *testing.T, tabletUID int, tabletType string) *cluster.Vttablet {
	tablet := &cluster.Vttablet{
		TabletUID: tabletUID,
		Type:      tabletType,
		HTTPPort:  clusterInstance.GetAndReservePort(),
		GrpcPort:  clusterInstance.GetAndReservePort(),
		MySQLPort: clusterInstance.GetAndReservePort(),
		Alias:     fmt.Sprintf("%s-%010d", cell, tabletUID),
	}
	// Start Mysqlctl process
	tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstanceOptionalInit(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory, !clusterInstance.ReusingVTDATAROOT)
	proc, err := tablet.MysqlctlProcess.StartProcess()
	require.Nil(t, err)

	// Start vttablet process
	tablet.VttabletProcess = cluster.VttabletProcessInstance(
		tablet.HTTPPort,
		tablet.GrpcPort,
		tabletUID,
		cell,
		shards[0],
		keyspaceName,
		clusterInstance.VtctldProcess.Port,
		tablet.Type,
		clusterInstance.TopoProcess.Port,
		clusterInstance.Hostname,
		clusterInstance.TmpDirectory,
		clusterInstance.VtTabletExtraArgs,
		clusterInstance.EnableSemiSync,
		clusterInstance.DefaultCharset)

	// wait for mysqld to be ready
	err = proc.Wait()
	require.Nil(t, err)

	tablet.VttabletProcess.ServingStatus = ""
	err = tablet.VttabletProcess.Setup()
	require.Nil(t, err)

	serving := tablet.VttabletProcess.WaitForStatus("SERVING", time.Duration(60*time.Second))
	assert.Equal(t, serving, true, "Tablet did not become ready within a reasonable time")
	err = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.%s",
		tablet.VttabletProcess.Keyspace, tablet.VttabletProcess.Shard, tablet.Type), 1)
	require.Nil(t, err)

	t.Logf("Added tablet: %s", tablet.Alias)
	return tablet
}

func deleteTablet(t *testing.T, tablet *cluster.Vttablet) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func(tablet *cluster.Vttablet) {
		defer wg.Done()
		_ = tablet.VttabletProcess.TearDown()
		_ = tablet.MysqlctlProcess.Stop()
		tablet.MysqlctlProcess.CleanupFiles(tablet.TabletUID)
	}(tablet)
	wg.Wait()

	err := clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", tablet.Alias)
	require.Nil(t, err)

	t.Logf("Deleted tablet: %s", tablet.Alias)
}
