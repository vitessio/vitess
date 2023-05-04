/*
Copyright 2023 The Vitess Authors.

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

package tablethealthcheck

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams

	// tabletRefreshInterval is the interval at which the tablet health check will refresh.
	// This value is set to a high value to ensure that the vtgate does not attempt to refresh the tablet between the time a tablet is added and the time it is promoted.
	tabletRefreshInterval = time.Hour

	keyspaceName = "healthcheck_test_ks"
	cell         = "healthcheck_test_cell"
	shards       = []string{"-80", "80-"}
	schemaSQL    = `
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
		err = clusterInstance.StartKeyspace(*keyspace, shards, 0, false)
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

// TestHealthCheckExternallyReparentNewTablet ensures that calling TabletExternallyReparented on a new tablet will switch the primary tablet
// without having to wait for the tabletRefreshInterval.
func TestHealthCheckExternallyReparentNewTablet(t *testing.T) {
	ctx := context.Background()

	// verify output of `show vitess_tablets` and `INSERT` statement
	vtgateConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer vtgateConn.Close()

	// add a new tablet
	reparentTabletUID := 9999
	reparentTabletType := "rdonly"
	tablet := addTablet(t, reparentTabletUID, reparentTabletType)

	// promote the new tablet to the primary
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("TabletExternallyReparented", tablet.Alias)
	require.NoError(t, err)

	// update the new primary tablet to be read-write
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadWrite", tablet.Alias)
	require.NoError(t, err)

	// wait for the vtgate to finish updating the new primary tablet
	// While 1 second is enough time in most cases, we'll wait for 3 seconds just to be safe, especially if we're running on a slow machine.
	time.Sleep(3 * time.Second)

	// verify that the vtgate will recognize the new primary tablet
	qr, _ := vtgateConn.ExecuteFetch("show vitess_tablets", 100, true)
	require.Equal(t, 3, len(qr.Rows), "wrong number of tablet records in healthcheck, expected %d but had %d. Got result=%v", 3, len(qr.Rows), qr)
	require.Equal(t, "-80", qr.Rows[0][2].ToString())
	require.Equal(t, "PRIMARY", qr.Rows[0][3].ToString())
	require.Equal(t, "SERVING", qr.Rows[0][4].ToString())
	require.Equal(t, tabletAlias(reparentTabletUID), qr.Rows[0][5].ToString())

	// delete the old primary tablet
	// This will ensure that the vtgate will experience the primary connection error if the switch would have to wait for the `tabletRefreshInterval`.
	deleteTablet(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0])

	// verify that the vtgate will route the `INSERT` statement to the new primary tablet instead of the deleted tablet
	qr, err = vtgateConn.ExecuteFetch("insert into customer(customer_id, email) values(2, 'dummy1')", 100, true) // -80
	require.EqualValues(t, 1, qr.RowsAffected)
	require.NoError(t, err)
}

func tabletAlias(tabletUID int) string {
	return fmt.Sprintf("%s-%010d", cell, tabletUID)
}

func addTablet(t *testing.T, tabletUID int, tabletType string) *cluster.Vttablet {
	tablet := &cluster.Vttablet{
		TabletUID: tabletUID,
		Type:      tabletType,
		HTTPPort:  clusterInstance.GetAndReservePort(),
		GrpcPort:  clusterInstance.GetAndReservePort(),
		MySQLPort: clusterInstance.GetAndReservePort(),
		Alias:     tabletAlias(tabletUID),
	}
	// Start Mysqlctl process
	mysqlctlProcess, err := cluster.MysqlCtlProcessInstanceOptionalInit(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory, !clusterInstance.ReusingVTDATAROOT)
	require.NoError(t, err)
	tablet.MysqlctlProcess = *mysqlctlProcess
	proc, err := tablet.MysqlctlProcess.StartProcess()
	require.NoError(t, err)

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
		clusterInstance.DefaultCharset)

	// wait for mysqld to be ready
	err = proc.Wait()
	require.NoError(t, err)

	tablet.VttabletProcess.ServingStatus = ""
	err = tablet.VttabletProcess.Setup()
	require.NoError(t, err)

	serving := tablet.VttabletProcess.WaitForStatus("SERVING", time.Duration(60*time.Second))
	require.Equal(t, serving, true, "Tablet did not become ready within a reasonable time")

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
	require.NoError(t, err)

	t.Logf("Deleted tablet: %s", tablet.Alias)
}
