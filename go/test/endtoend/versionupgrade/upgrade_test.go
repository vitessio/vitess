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

/*

ABOUT THIS TEST
===============

This test plays part in testing an upgrade path from a previous version/tag. It takes a GitHub workflow file to complete the functionality.
What's in this file is the setting up of a cluster, sharded and unsharded keyspace, creating and populating some tables, then testing retrieval of data.
The twist here is that you can run this test over pre-existing vtdataroot, which means this test can reuse existing etcd, existing tables, existing mysql,
in which case it will not attempt to create keyspaces/schemas/tables, nor will it populate table data. Instead, it will only check for retrieval of data.

The game is to setup the cluster with a stable version (say `v8.0.0`), take it down (and preserve data), then setup a new cluster with a new version (namely the branch/PR head) and attempt to read the data.

Both executions must force some settings so that both reuse same directories, ports, etc. An invocation will look like:
go test ./go/test/endtoend/versionupgrade80/upgrade80_test.go --keep-data -force-vtdataroot /tmp/vtdataroot/vtroot_10901 --force-port-start 11900 --force-base-tablet-uid 1190

*/

package versionupgrade

import (
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	vtParams              mysql.ConnParams
	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	totalTableCount       = 4
	createTable           = `
		CREATE TABLE %s (
			id bigint(20) NOT NULL,
			msg varchar(64),
			PRIMARY KEY (id)
		) ENGINE=InnoDB;
		`
	insertIntoTable = `
		INSERT INTO %s (id, msg) VALUES (17, 'abc');
		`
	selectFromTable = `
		SELECT id, msg FROM %s LIMIT 1;
		`
)

// TestMain is the main entry point
func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		schemaChangeDirectory = path.Join("/tmp", fmt.Sprintf("schema_change_dir_%d", clusterInstance.GetAndReserveTabletUID()))
		defer os.RemoveAll(schemaChangeDirectory)
		defer clusterInstance.Teardown()

		if _, err := os.Stat(schemaChangeDirectory); os.IsNotExist(err) {
			_ = os.Mkdir(schemaChangeDirectory, 0700)
		}

		clusterInstance.VtctldExtraArgs = []string{
			"-schema_change_dir", schemaChangeDirectory,
			"-schema_change_controller", "local",
			"-schema_change_check_interval", "1"}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}

		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 2, true); err != nil {
			return 1, err
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 1, false); err != nil {
			return 1, err
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		// set the gateway we want to use
		vtgateInstance.GatewayImplementation = "tabletgateway"
		// Start vtgate
		if err := vtgateInstance.Setup(); err != nil {
			return 1, err
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}

func TestShards(t *testing.T) {
	defer cluster.PanicHandler(t)
	assert.Equal(t, 2, len(clusterInstance.Keyspaces[0].Shards))
}

func TestDeploySchema(t *testing.T) {
	defer cluster.PanicHandler(t)

	if clusterInstance.ReusingVTDATAROOT {
		// we assume data is already deployed
		return
	}
	// Create n tables, populate
	for i := 0; i < totalTableCount; i++ {
		tableName := fmt.Sprintf("vt_upgrade_test_%02d", i)

		{
			sqlQuery := fmt.Sprintf(createTable, tableName)
			_, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, sqlQuery, "")
			require.Nil(t, err)
		}
		for i := range clusterInstance.Keyspaces[0].Shards {
			sqlQuery := fmt.Sprintf(insertIntoTable, tableName)
			tablet := clusterInstance.Keyspaces[0].Shards[i].Vttablets[0]
			_, err := tablet.VttabletProcess.QueryTablet(sqlQuery, keyspaceName, true)
			require.Nil(t, err)
		}
	}

	checkTables(t, "", totalTableCount)
}

func TestTablesExist(t *testing.T) {
	defer cluster.PanicHandler(t)

	checkTables(t, "", totalTableCount)
}

// checkTables checks the number of tables in the first two shards.
func checkTables(t *testing.T, showTableName string, expectCount int) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], showTableName, expectCount)
	}
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *cluster.Vttablet, showTableName string, expectCount int) {
	query := fmt.Sprintf(`show tables like '%%%s%%';`, showTableName)
	queryResult, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	require.Nil(t, err)
	assert.Equal(t, expectCount, len(queryResult.Rows))
}

// TestTablesData checks the data in tables
func TestTablesData(t *testing.T) {
	// Create n tables, populate
	for i := 0; i < totalTableCount; i++ {
		tableName := fmt.Sprintf("vt_upgrade_test_%02d", i)

		for i := range clusterInstance.Keyspaces[0].Shards {
			sqlQuery := fmt.Sprintf(selectFromTable, tableName)
			tablet := clusterInstance.Keyspaces[0].Shards[i].Vttablets[0]
			queryResult, err := tablet.VttabletProcess.QueryTablet(sqlQuery, keyspaceName, true)
			require.Nil(t, err)
			require.NotNil(t, queryResult)
			row := queryResult.Named().Row()
			require.NotNil(t, row)
			require.Equal(t, int64(17), row.AsInt64("id", 0))
			require.Equal(t, "abc", row.AsString("msg", ""))
		}
	}
}
