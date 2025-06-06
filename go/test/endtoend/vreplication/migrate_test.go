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

package vreplication

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func insertInitialDataIntoExternalCluster(t *testing.T, conn *mysql.Conn) {
	t.Run("insertInitialData", func(t *testing.T) {
		fmt.Printf("Inserting initial data\n")
		execVtgateQuery(t, conn, "rating:0", "insert into review(rid, pid, review) values(1, 1, 'review1');")
		execVtgateQuery(t, conn, "rating:0", "insert into review(rid, pid, review) values(2, 1, 'review2');")
		execVtgateQuery(t, conn, "rating:0", "insert into review(rid, pid, review) values(3, 2, 'review3');")
		execVtgateQuery(t, conn, "rating:0", "insert into rating(gid, pid, rating) values(1, 1, 4);")
		execVtgateQuery(t, conn, "rating:0", "insert into rating(gid, pid, rating) values(2, 2, 5);")
	})
}

// TestMigrateUnsharded runs an e2e test for importing from an external cluster using the
// vtctldclient Mount and Migrate commands.We have an anti-pattern in Vitess: vt executables
// look for an environment variable VTDATAROOT for certain cluster parameters like the log
// directory when they are created. Until this test we just needed a single cluster for e2e
// tests. However now we need to create an external Vitess cluster. For this we need a
// different VTDATAROOT and hence the VTDATAROOT env variable gets overwritten. Each time
// we need to create vt processes in the "other" cluster we need to set the appropriate
// VTDATAROOT.
func TestMigrateUnsharded(t *testing.T) {
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()

	oldDefaultReplicas := defaultReplicas
	oldDefaultRdonly := defaultRdonly
	defaultReplicas = 0
	defaultRdonly = 0
	defer func() {
		defaultReplicas = oldDefaultReplicas
		defaultRdonly = oldDefaultRdonly
	}()

	defaultCell := vc.Cells[vc.CellNames[0]]
	_, err := vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0",
		initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, nil)
	require.NoError(t, err, "failed to create product keyspace")

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	insertInitialData(t)

	// create external cluster
	extCell := "extcell1"
	extCells := []string{extCell}
	extVc := NewVitessCluster(t, &clusterOptions{
		cells:         extCells,
		clusterConfig: externalClusterConfig,
	})
	defer extVc.TearDown()

	extCell2 := extVc.Cells[extCell]
	extVc.AddKeyspace(t, []*Cell{extCell2}, "rating", "0",
		initialExternalVSchema, initialExternalSchema, 0, 0, 1000, nil)
	extVtgate := extCell2.Vtgates[0]
	require.NotNil(t, extVtgate)

	verifyClusterHealth(t, extVc)
	extVtgateConn := getConnection(t, extVc.ClusterConfig.hostname, extVc.ClusterConfig.vtgateMySQLPort)
	insertInitialDataIntoExternalCluster(t, extVtgateConn)

	targetPrimary := vc.getPrimaryTablet(t, "product", "0")

	var output, expected string

	t.Run("mount external cluster", func(t *testing.T) {
		etcdHostPort := fmt.Sprintf("localhost:%d", extVc.ClusterConfig.topoPort)
		output, err := vc.VtctldClient.ExecuteCommandWithOutput("Mount", "register", "--name=ext1", "--topo-type=etcd2",
			"--topo-server", etcdHostPort, "--topo-root=/vitess/global")
		require.NoError(t, err, "Mount Register command failed with %s", output)

		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Mount", "list")
		require.NoError(t, err, "Mount List command failed with %s", output)

		names := gjson.Get(output, "names")
		require.Equal(t, 1, len(names.Array()))
		require.Equal(t, "ext1", names.Array()[0].String())
		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Mount", "show", "--name=ext1")
		require.NoError(t, err, "Mount command failed with %s\n", output)

		require.Equal(t, "etcd2", gjson.Get(output, "topo_type").String())
		require.Equal(t, etcdHostPort, gjson.Get(output, "topo_server").String())
		require.Equal(t, "/vitess/global", gjson.Get(output, "topo_root").String())
	})

	ksWorkflow := "product.e1"

	t.Run("migrate from external cluster", func(t *testing.T) {
		if output, err = vc.VtctldClient.ExecuteCommandWithOutput("Migrate",
			"--target-keyspace", "product", "--workflow", "e1",
			"create", "--source-keyspace", "rating", "--mount-name", "ext1", "--all-tables", "--cells=extcell1", "--tablet-types=primary,replica"); err != nil {
			t.Fatalf("Migrate command failed with %+v : %s\n", err, output)
		}
		waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
		expectNumberOfStreams(t, vtgateConn, "migrate", "e1", "product:0", 1)
		waitForRowCountInTablet(t, targetPrimary, "product", "rating", 2)
		waitForRowCountInTablet(t, targetPrimary, "product", "review", 3)
		execVtgateQuery(t, extVtgateConn, "rating", "insert into review(rid, pid, review) values(4, 1, 'review4');")
		execVtgateQuery(t, extVtgateConn, "rating", "insert into rating(gid, pid, rating) values(3, 1, 3);")
		waitForRowCountInTablet(t, targetPrimary, "product", "rating", 3)
		waitForRowCountInTablet(t, targetPrimary, "product", "review", 4)
		doVDiff(t, ksWorkflow, "extcell1")

		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Migrate",
			"--target-keyspace", "product", "--workflow", "e1", "show")
		require.NoError(t, err, "Migrate command failed with %s", output)

		wf := gjson.Get(output, "workflows").Array()[0]
		require.Equal(t, "e1", wf.Get("name").String())
		require.Equal(t, "Migrate", wf.Get("workflow_type").String())

		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Migrate",
			"--target-keyspace", "product", "--workflow", "e1", "status", "--format=json")
		require.NoError(t, err, "Migrate command failed with %s", output)

		require.Equal(t, "Running", gjson.Get(output, "shard_streams.product/0.streams.0.status").String())

		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Migrate",
			"--target-keyspace", "product", "--workflow", "e1", "complete")
		require.NoError(t, err, "Migrate command failed with %s", output)

		expectNumberOfStreams(t, vtgateConn, "migrate", "e1", "product:0", 0)
	})
	t.Run("cancel migrate workflow", func(t *testing.T) {
		execVtgateQuery(t, vtgateConn, "product", "drop table review,rating")
		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Migrate",
			"--target-keyspace", "product", "--workflow", "e1", "Create", "--source-keyspace", "rating",
			"--mount-name", "ext1", "--all-tables", "--auto-start=false", "--cells=extcell1")
		require.NoError(t, err, "Migrate command failed with %s", output)

		expectNumberOfStreams(t, vtgateConn, "migrate", "e1", "product:0", 1, binlogdatapb.VReplicationWorkflowState_Stopped.String())
		waitForRowCountInTablet(t, targetPrimary, "product", "rating", 0)
		waitForRowCountInTablet(t, targetPrimary, "product", "review", 0)
		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Migrate",
			"--target-keyspace", "product", "--workflow", "e1", "cancel")
		require.NoError(t, err, "Migrate command failed with %s", output)

		expectNumberOfStreams(t, vtgateConn, "migrate", "e1", "product:0", 0)
		var found bool
		found, err = checkIfTableExists(t, vc, "zone1-100", "review")
		require.NoError(t, err)
		require.False(t, found)
		found, err = checkIfTableExists(t, vc, "zone1-100", "rating")
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("unmount external cluster", func(t *testing.T) {
		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Mount", "unregister", "--name=ext1")
		require.NoError(t, err, "Mount command failed with %s\n", output)

		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Mount", "list")
		require.NoError(t, err, "Mount command failed with %+v : %s\n", output)
		expected = "{}\n"
		require.Equal(t, expected, output)

		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Mount", "show", "--name=ext1")
		require.Errorf(t, err, "there is no vitess cluster named ext1")
	})
}

// TestMigrateSharded adds a test for a sharded cluster to validate a fix for a bug where
// the target keyspace name doesn't match that of the source cluster. The test migrates
// from a cluster with keyspace customer to an "external" cluster with keyspace rating.
func TestMigrateSharded(t *testing.T) {
	t.Skip("This test is very flaky, works locally though")
	setSidecarDBName("_vt")
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	oldDefaultReplicas := defaultReplicas
	oldDefaultRdonly := defaultRdonly
	defaultReplicas = 0
	defaultRdonly = 0
	defer func() {
		defaultReplicas = oldDefaultReplicas
		defaultRdonly = oldDefaultRdonly
	}()

	vc = setupCluster(t)
	defer vc.TearDown()

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	setupCustomerKeyspace(t)
	createMoveTablesWorkflow(t, "customer,Lead,datze,customer2")
	tstWorkflowSwitchReadsAndWrites(t)
	tstWorkflowComplete(t)

	var err error
	// create external cluster
	extCell := "extcell1"
	extCells := []string{extCell}
	extVc := NewVitessCluster(t, &clusterOptions{
		cells:         extCells,
		clusterConfig: externalClusterConfig,
	})
	defer extVc.TearDown()

	setupExtKeyspace(t, extVc, "rating", extCell)
	err = cluster.WaitForHealthyShard(extVc.VtctldClient, "rating", "-80")
	require.NoError(t, err)
	err = cluster.WaitForHealthyShard(extVc.VtctldClient, "rating", "80-")
	require.NoError(t, err)
	verifyClusterHealth(t, extVc)
	extVtgateConn := getConnection(t, extVc.ClusterConfig.hostname, extVc.ClusterConfig.vtgateMySQLPort)
	defer extVtgateConn.Close()

	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_Migrate
	var output string
	if output, err = extVc.VtctldClient.ExecuteCommandWithOutput("Mount", "register", "--name=external", "--topo-type=etcd2",
		fmt.Sprintf("--topo-server=localhost:%d", vc.ClusterConfig.topoPort), "--topo-root=/vitess/global"); err != nil {
		require.FailNow(t, "Mount command failed with %+v : %s\n", err, output)
	}
	ksWorkflow := "rating.e1"
	if output, err = extVc.VtctldClient.ExecuteCommandWithOutput("Migrate",
		"--target-keyspace", "rating", "--workflow", "e1",
		"create", "--source-keyspace", "customer", "--mount-name", "external", "--all-tables", "--cells=zone1",
		"--tablet-types=primary"); err != nil {
		require.FailNow(t, "Migrate command failed with %+v : %s\n", err, output)
	}
	waitForWorkflowState(t, extVc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	// this is because currently doVtctldclientVDiff is using the global vc :-( and we want to run a diff on the extVc cluster
	vc = extVc
	doVtctldclientVDiff(t, "rating", "e1", "zone1", nil)
}

func setupExtKeyspace(t *testing.T, vc *VitessCluster, ksName, cellName string) {
	numReplicas := 1
	shards := []string{"-80", "80-"}
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells[cellName]}, ksName, strings.Join(shards, ","),
		customerVSchema, customerSchema, numReplicas, 0, 1200, nil); err != nil {
		t.Fatal(err)
	}
	vtgate := vc.Cells[cellName].Vtgates[0]
	for _, shard := range shards {
		err := cluster.WaitForHealthyShard(vc.VtctldClient, ksName, shard)
		require.NoError(t, err)
		require.NoError(t, vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", ksName, shard), numReplicas, waitTimeout))
	}
}
