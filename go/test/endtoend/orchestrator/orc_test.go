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

package orchestrator

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/topo/topoproto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func createCluster(t *testing.T, numReplicas int, numRdonly int) *cluster.LocalProcessCluster {
	keyspaceName := "ks"
	shardName := "0"
	keyspace := &cluster.Keyspace{Name: keyspaceName}
	shard0 := &cluster.Shard{Name: shardName}
	//dbName          = "vt_" + keyspaceName
	//username        = "vt_dba"
	hostname := "localhost"
	cell1 := "zone1"
	cell2 := "zone2"
	//	insertSQL       = "insert into vt_insert_test(id, msg) values (%d, 'test %d')"
	//	sqlSchema       = `
	//	create table vt_insert_test (
	//	id bigint,
	//	msg varchar(64),
	//	primary key (id)
	//	) Engine=InnoDB
	//	`
	tablets := []*cluster.Vttablet{}
	clusterInstance := cluster.NewCluster(cell1, hostname)

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err)

	// Adding another cell in the same cluster
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cell2)
	require.NoError(t, err)
	err = clusterInstance.VtctlProcess.AddCellInfo(cell2)
	require.NoError(t, err)

	// creating tablets by hand instead of using StartKeyspace because we don't want to call InitShardMaster
	uidBase := 100
	for i := 0; i < numReplicas; i++ {
		tablets = append(tablets, clusterInstance.NewVttabletInstance("replica", uidBase+i, cell1))
	}
	for i := 0; i < numRdonly; i++ {
		tablets = append(tablets, clusterInstance.NewVttabletInstance("rdonly", uidBase+numReplicas+i, cell1))
	}

	clusterInstance.VtTabletExtraArgs = []string{
		"-lock_tables_timeout", "5s",
		"-disable_active_reparents",
	}

	// Initialize Cluster
	shard0.Vttablets = tablets
	err = clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard0})
	require.NoError(t, err)

	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, tablet := range shard0.Vttablets {
		log.Infof("Starting MySql for tablet %v", tablet.Alias)
		proc, err := tablet.MysqlctlProcess.StartProcess()
		require.NoError(t, err)
		mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		err := proc.Wait()
		require.NoError(t, err)
	}

	return clusterInstance
}

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose master
// verify rdonly is not elected, only replica
// verify replication is setup
func TestMasterElection(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := createCluster(t, 1, 1)
	defer func() {
		clusterInstance.Teardown()
	}()
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	for _, tablet := range shard0.Vttablets {
		// Reset status, don't wait for the tablet status. We will check it later
		tablet.VttabletProcess.ServingStatus = ""

		// Start the tablet
		err := tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	defer func() {
		// Kill tablets
		killTablets(t, shard0)
	}()

	for _, tablet := range shard0.Vttablets {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}

	// Start orchestrator
	clusterInstance.OrcProcess = clusterInstance.NewOrcProcess(path.Join(os.Getenv("PWD"), "test_config.json"))
	err := clusterInstance.OrcProcess.Setup()
	require.NoError(t, err)

	//log.Exitf("error")
	checkMasterTablet(t, clusterInstance, shard0.Vttablets[0])
}

// 2. bring down master, let orc promote replica
func TestDownMaster(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := createCluster(t, 2, 0)
	defer func() {
		clusterInstance.Teardown()
	}()
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	for _, tablet := range shard0.Vttablets {
		// Reset status, don't wait for the tablet status. We will check it later
		tablet.VttabletProcess.ServingStatus = ""

		// Start the tablet
		err := tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	defer func() {
		// Kill tablets
		killTablets(t, shard0)
	}()

	for _, tablet := range shard0.Vttablets {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}

	// Start orchestrator
	clusterInstance.OrcProcess = clusterInstance.NewOrcProcess(path.Join(os.Getenv("PWD"), "test_config.json"))
	err := clusterInstance.OrcProcess.Setup()
	require.NoError(t, err)

	// find master from topo
	curMaster := shardMasterTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curMaster, "should have elected a master")

	// Make the current master agent and database unavailable.
	err = curMaster.VttabletProcess.TearDown()
	require.NoError(t, err)
	err = curMaster.MysqlctlProcess.Stop()
	require.NoError(t, err)

	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be master
		if tablet.Alias != curMaster.Alias {
			checkMasterTablet(t, clusterInstance, tablet)
			break
		}
	}
}

// Cases to test:
// 3. create cluster with 2 replicas let orc choose master
// Graceful reparent to other replica
// verify replication is setup correctly
func TestGracefulReparent(t *testing.T) {

}

func shardMasterTablet(t *testing.T, cluster *cluster.LocalProcessCluster, keyspace *cluster.Keyspace, shard *cluster.Shard) *cluster.Vttablet {
	start := time.Now()
	for {
		now := time.Now()
		if now.Sub(start) > time.Second*60 {
			assert.FailNow(t, "failed to elect master before timeout")
		}
		result, err := cluster.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", fmt.Sprintf("%s/%s", keyspace.Name, shard.Name))
		assert.Nil(t, err)

		var shardInfo topodatapb.Shard
		err = json2.Unmarshal([]byte(result), &shardInfo)
		assert.Nil(t, err)
		if shardInfo.MasterAlias == nil {
			log.Warningf("Shard %v/%v has no master yet, sleep for 1 second\n", keyspace.Name, shard.Name)
			time.Sleep(time.Second)
			continue
		}
		for _, tablet := range shard.Vttablets {
			if tablet.Alias == topoproto.TabletAliasString(shardInfo.MasterAlias) {
				return tablet
			}
		}
	}
}

// Makes sure the tablet type is master, and its health check agrees.
func checkMasterTablet(t *testing.T, cluster *cluster.LocalProcessCluster, tablet *cluster.Vttablet) {
	start := time.Now()
	for {
		now := time.Now()
		if now.Sub(start) > time.Second*60 {
			assert.FailNow(t, "failed to elect master before timeout")
		}
		result, err := cluster.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
		require.NoError(t, err)
		var tabletInfo topodatapb.Tablet
		err = json2.Unmarshal([]byte(result), &tabletInfo)
		require.NoError(t, err)

		if topodatapb.TabletType_MASTER != tabletInfo.GetType() {
			log.Warningf("Tablet %v is not master yet, sleep for 1 second\n", tablet.Alias)
			time.Sleep(time.Second)
			continue
		} else {
			// make sure the health stream is updated
			result, err = cluster.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", tablet.Alias)
			require.NoError(t, err)
			var streamHealthResponse querypb.StreamHealthResponse

			err = json2.Unmarshal([]byte(result), &streamHealthResponse)
			require.NoError(t, err)

			assert.True(t, streamHealthResponse.GetServing())
			tabletType := streamHealthResponse.GetTarget().GetTabletType()
			require.Equal(t, topodatapb.TabletType_MASTER, tabletType)
			break
		}
	}
}

//func checkInsertedValues(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, index int) error {
//	// wait until it gets the data
//	timeout := time.Now().Add(10 * time.Second)
//	for time.Now().Before(timeout) {
//		selectSQL := fmt.Sprintf("select msg from vt_insert_test where id=%d", index)
//		qr := runSQL(ctx, t, selectSQL, tablet)
//		if len(qr.Rows) == 1 {
//			return nil
//		}
//		time.Sleep(300 * time.Millisecond)
//	}
//	return fmt.Errorf("data is not yet replicated")
//}
//
//func validateTopology(t *testing.T, pingTablets bool) {
//	if pingTablets {
//		err := clusterInstance.VtctlclientProcess.ExecuteCommand("Validate", "-ping-tablets=true")
//		require.NoError(t, err)
//	} else {
//		err := clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
//		require.NoError(t, err)
//	}
//}

func killTablets(t *testing.T, shard *cluster.Shard) {
	for _, tablet := range shard.Vttablets {
		log.Infof("Calling TearDown on tablet %v", tablet.Alias)
		err := tablet.VttabletProcess.TearDown()
		require.NoError(t, err)
	}
}

//func getMysqlConnParam(tablet *cluster.Vttablet) mysql.ConnParams {
//	connParams := mysql.ConnParams{
//		Uname:      username,
//		DbName:     dbName,
//		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", tablet.TabletUID)),
//	}
//	return connParams
//}
//
//func runSQL(ctx context.Context, t *testing.T, sql string, tablet *cluster.Vttablet) *sqltypes.Result {
//	// Get Connection
//	tabletParams := getMysqlConnParam(tablet)
//	conn, err := mysql.Connect(ctx, &tabletParams)
//	require.Nil(t, err)
//	defer conn.Close()
//
//	// runSQL
//	return execute(t, conn, sql)
//}
//
//func execute(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
//	t.Helper()
//	qr, err := conn.ExecuteFetch(query, 1000, true)
//	require.Nil(t, err)
//	return qr
//}
