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

package vtgr

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"

	"github.com/stretchr/testify/require"
	"gotest.tools/assert"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// To run this test locally on MacOS, set hostname to localhost first:
// $ sudo scutil --set HostName localhost

func createCluster(t *testing.T, numReplicas int) *cluster.LocalProcessCluster {
	keyspaceName := "ks"
	shardName := "0"
	keyspace := &cluster.Keyspace{Name: keyspaceName}
	shard0 := &cluster.Shard{Name: shardName}
	hostname := "localhost"
	cell1 := "zone1"
	tablets := []*cluster.Vttablet{}
	clusterInstance := cluster.NewCluster(cell1, hostname)

	os.Setenv("EXTRA_MY_CNF", path.Join(os.Getenv("PWD"), "my.cnf"))

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err)

	uidBase := 100
	for i := 0; i < numReplicas; i++ {
		tablet := clusterInstance.NewVttabletInstance("replica", uidBase+i, cell1)
		tablets = append(tablets, tablet)
	}

	// Initialize Cluster
	shard0.Vttablets = tablets
	err = clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard0})
	require.NoError(t, err)

	// Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, tablet := range shard0.Vttablets {
		proc, err := tablet.MysqlctlProcess.StartProcess()
		require.NoError(t, err)
		mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		err := proc.Wait()
		require.NoError(t, err)
	}
	for _, tablet := range shard0.Vttablets {
		// Reset status, don't wait for the tablet status. We will check it later
		tablet.VttabletProcess.ServingStatus = ""
		tablet.VttabletProcess.DbFlavor = "MysqlGR"
		// If we enable backup the GR setup is a bit wacky
		tablet.VttabletProcess.SupportsBackup = false
		// Start the tablet
		err := tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	// Start vtgr - we deploy vtgr on the tablet node in the test
	baseGrPort := 33061
	for i, tablet := range shard0.Vttablets {
		tablet.VtgrProcess = clusterInstance.NewVtgrProcess(
			[]string{fmt.Sprintf("%s/%s", keyspaceName, shardName)},
			path.Join(os.Getenv("PWD"), "test_config.json"),
			baseGrPort+i,
		)
	}

	for _, tablet := range shard0.Vttablets {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"NOT_SERVING"})
		require.NoError(t, err)
	}
	return clusterInstance
}

func killTablets(t *testing.T, shard *cluster.Shard) {
	for _, tablet := range shard.Vttablets {
		if tablet.VtgrProcess != nil {
			err := tablet.VtgrProcess.TearDown()
			require.NoError(t, err)
		}
		err := tablet.VttabletProcess.TearDown()
		require.NoError(t, err)
	}
}

func TestBasicSetup(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := createCluster(t, 2)
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	defer func() {
		clusterInstance.Teardown()
		killTablets(t, shard0)
	}()
	for _, tablet := range shard0.Vttablets {
		// Until there is a primary, all tablets are replica and should all be NOT_SERVING status
		tab := getTablet(t, clusterInstance, tablet.Alias)
		assert.Equal(t, tab.Type.String(), "REPLICA")
		assert.Equal(t, tablet.VttabletProcess.GetTabletStatus(), "NOT_SERVING")
	}
	_, err := getPrimaryTablet(t, clusterInstance, keyspace.Name, shard0.Name)
	assert.ErrorContains(t, err, "timeout looking for primary tablet")

	tablet1 := shard0.Vttablets[0]
	query := `select count(*)
		from performance_schema.replication_group_members
		where MEMBER_STATE='ONLINE'`
	var count int
	err = getSQLResult(t, tablet1, query, func(values []sqltypes.Value) bool {
		cnt, err := values[0].ToInt64()
		if err != nil {
			return false
		}
		count = int(cnt)
		return true
	})
	require.NoError(t, err)
	require.NoError(t, err)
	// without vtgr, tablet process will not create a mysql group
	// and all the nodes are replicas type in NOT_SERVING state
	assert.Equal(t, 0, int(count))
}

func TestVTGRSetup(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := createCluster(t, 2)
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	defer func() {
		clusterInstance.Teardown()
		killTablets(t, shard0)
	}()
	for _, tablet := range shard0.Vttablets {
		// Until there is a primary, all tablets are replica and should all be NOT_SERVING status
		tab := getTablet(t, clusterInstance, tablet.Alias)
		assert.Equal(t, tab.Type.String(), "REPLICA")
		assert.Equal(t, tablet.VttabletProcess.GetTabletStatus(), "NOT_SERVING")
	}

	// start VTGR processes
	for _, tablet := range shard0.Vttablets {
		err := tablet.VtgrProcess.Start(tablet.Alias)
		require.NoError(t, err)
	}

	// VTGR will pick one tablet as the primary
	primaryAlias, err := getPrimaryTablet(t, clusterInstance, keyspace.Name, shard0.Name)
	require.NoError(t, err)
	require.NotEqual(t, nil, primaryAlias)

	tablet1 := shard0.Vttablets[0]
	query := `select count(*) 
		from performance_schema.replication_group_members 
		where MEMBER_STATE='ONLINE'`
	err = getSQLResult(t, tablet1, query, func(values []sqltypes.Value) bool {
		cnt, err := values[0].ToInt64()
		if err != nil {
			return false
		}
		// VTGR should bootstrap the group and put the replica into the group
		return cnt == 2
	})
	require.NoError(t, err)
}

func TestVTGRWrongPrimaryTablet(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := createCluster(t, 2)
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	defer func() {
		clusterInstance.Teardown()
		killTablets(t, shard0)
	}()
	for _, tablet := range shard0.Vttablets {
		// Until there is a primary, all tablets are replica and should all be NOT_SERVING status
		tab := getTablet(t, clusterInstance, tablet.Alias)
		assert.Equal(t, tab.Type.String(), "REPLICA")
		assert.Equal(t, tablet.VttabletProcess.GetTabletStatus(), "NOT_SERVING")
	}
	// start VTGR processes
	for _, tablet := range shard0.Vttablets {
		err := tablet.VtgrProcess.Start(tablet.Alias)
		require.NoError(t, err)
	}
	// VTGR will pick one tablet as the primary
	primaryAlias, err := getPrimaryTablet(t, clusterInstance, keyspace.Name, shard0.Name)
	require.NoError(t, err)
	require.NotEqual(t, nil, primaryAlias)
	tablet := shard0.Vttablets[0]
	query := `select member_id
		from performance_schema.replication_group_members
		where member_role='SECONDARY' and member_state='ONLINE'`
	var member string
	err = getSQLResult(t, tablet, query, func(values []sqltypes.Value) bool {
		member = values[0].ToString()
		return true
	})
	require.NoError(t, err)
	query = fmt.Sprintf(`select group_replication_set_as_primary('%s')`, member)
	_, err = tablet.VttabletProcess.QueryTabletWithDB(query, "")
	require.NoError(t, err)

	// Verify the mysql primary changed, and also the primary tablet changed as well
	query = fmt.Sprintf(`select member_role from performance_schema.replication_group_members where member_id='%s'`, member)
	err = getSQLResult(t, tablet, query, func(values []sqltypes.Value) bool {
		return values[0].ToString() == "PRIMARY"
	})
	require.NoError(t, err)
	err = verifyPrimaryChange(t, clusterInstance, keyspace.Name, shard0.Name, primaryAlias)
	require.NoError(t, err)
}

func TestVTGRFailover(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance := createCluster(t, 3)
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	defer func() {
		clusterInstance.Teardown()
		killTablets(t, shard0)
	}()
	for _, tablet := range shard0.Vttablets {
		// Until there is a primary, all tablets are replica and should all be NOT_SERVING status
		tab := getTablet(t, clusterInstance, tablet.Alias)
		assert.Equal(t, tab.Type.String(), "REPLICA")
		assert.Equal(t, tablet.VttabletProcess.GetTabletStatus(), "NOT_SERVING")
	}
	// start VTGR processes
	for _, tablet := range shard0.Vttablets {
		err := tablet.VtgrProcess.Start(tablet.Alias)
		require.NoError(t, err)
	}
	primaryAlias, err := getPrimaryTablet(t, clusterInstance, keyspace.Name, shard0.Name)
	require.NoError(t, err)
	// VTGR has init the cluster
	require.NotEqual(t, "", primaryAlias)
	primaryTablet := findTabletByAlias(shard0.Vttablets, primaryAlias)
	require.NotNil(t, primaryTablet)
	// Wait until there are two nodes in the group
	query := `select count(*) from
		performance_schema.replication_group_members
		where MEMBER_STATE='ONLINE'`
	err = getSQLResult(t, primaryTablet, query, func(values []sqltypes.Value) bool {
		return values[0].ToString() == "3"
	})
	require.NoError(t, err)

	// Now kill the primary
	// VTGR should move mysql primary to a different node and change failover primary tablet
	err = primaryTablet.VttabletProcess.TearDown()
	require.NoError(t, err)
	err = verifyPrimaryChange(t, clusterInstance, keyspace.Name, shard0.Name, primaryAlias)
	require.NoError(t, err)
	// now the primary has changed
	primaryAlias, err = getPrimaryTablet(t, clusterInstance, keyspace.Name, shard0.Name)
	require.NoError(t, err)
	// verify on the _new_ primary node, we are running the mysql primary as well
	primaryTablet = findTabletByAlias(shard0.Vttablets, primaryAlias)
	require.NotNil(t, primaryTablet)
	query = `SELECT count(*) FROM
		performance_schema.replication_group_members
		WHERE MEMBER_STATE='ONLINE' AND MEMBER_ROLE='PRIMARY' AND MEMBER_PORT=@@port`
	err = getSQLResult(t, primaryTablet, query, func(values []sqltypes.Value) bool {
		return values[0].ToString() == "1"
	})
	require.NoError(t, err)
}

func getTablet(t *testing.T, cluster *cluster.LocalProcessCluster, alias string) *topodatapb.Tablet {
	result, err := cluster.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", alias)
	require.NoError(t, err)
	var tabletInfo topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tabletInfo)
	require.NoError(t, err)
	return &tabletInfo
}

func findTabletByAlias(tablets []*cluster.Vttablet, alias *topodatapb.TabletAlias) *cluster.Vttablet {
	for _, tablet := range tablets {
		if tablet.Cell == alias.Cell && strings.HasSuffix(tablet.Alias, strconv.Itoa(int(alias.Uid))) {
			return tablet
		}
	}
	return nil
}

func verifyPrimaryChange(t *testing.T, cluster *cluster.LocalProcessCluster, ks, shard string, old *topodatapb.TabletAlias) error {
	timeToWait := time.Now().Add(180 * time.Second)
	for time.Now().Before(timeToWait) {
		time.Sleep(1 * time.Second)
		result, err := cluster.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", fmt.Sprintf("%s/%s", ks, shard))
		require.NoError(t, err)
		var shardInfo topodatapb.Shard
		err = json2.Unmarshal([]byte(result), &shardInfo)
		require.NoError(t, err)
		if shardInfo.PrimaryAlias.String() != old.String() {
			return nil
		}
	}
	return fmt.Errorf("fail to verify primary change")
}

func getPrimaryTablet(t *testing.T, cluster *cluster.LocalProcessCluster, ks, shard string) (*topodatapb.TabletAlias, error) {
	timeToWait := time.Now().Add(180 * time.Second)
	for time.Now().Before(timeToWait) {
		time.Sleep(1 * time.Second)
		result, err := cluster.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", fmt.Sprintf("%s/%s", ks, shard))
		require.NoError(t, err)
		var shardInfo topodatapb.Shard
		err = json2.Unmarshal([]byte(result), &shardInfo)
		require.NoError(t, err)
		if shardInfo.PrimaryAlias != nil {
			return shardInfo.PrimaryAlias, nil
		}
	}
	return nil, fmt.Errorf("timeout looking for primary tablet")
}

func getSQLResult(t *testing.T, tablet *cluster.Vttablet, query string, check func([]sqltypes.Value) bool) error {
	timeToWait := time.Now().Add(180 * time.Second)
	for time.Now().Before(timeToWait) {
		time.Sleep(1 * time.Second)
		qr, err := tablet.VttabletProcess.QueryTabletWithDB(query, "")
		require.NoError(t, err)
		if len(qr.Rows) == 1 && check(qr.Rows[0]) {
			return nil
		}
	}
	return fmt.Errorf("timeout waiting for sql result")
}
