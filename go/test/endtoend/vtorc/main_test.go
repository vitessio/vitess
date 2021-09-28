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

package vtorc

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/topo"
	_ "vitess.io/vitess/go/vt/topo/consultopo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/topo/k8stopo"
	_ "vitess.io/vitess/go/vt/topo/zk2topo"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/topo/topoproto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type cellInfo struct {
	cellName       string
	replicaTablets []*cluster.Vttablet
	rdonlyTablets  []*cluster.Vttablet
	// constants that should be set in TestMain
	numReplicas int
	numRdonly   int
	uidBase     int
}

var (
	clusterInstance *cluster.LocalProcessCluster
	ts              *topo.Server
	cellInfos       []*cellInfo
	lastUsedValue   = 100
)

const (
	keyspaceName = "ks"
	shardName    = "0"
	hostname     = "localhost"
	cell1        = "zone1"
	cell2        = "zone2"
)

// createClusterAndStartTopo starts the cluster and topology service
func createClusterAndStartTopo() error {
	clusterInstance = cluster.NewCluster(cell1, hostname)

	// Start topo server
	err := clusterInstance.StartTopo()
	if err != nil {
		return err
	}

	// Adding another cell in the same cluster
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cell2)
	if err != nil {
		return err
	}
	err = clusterInstance.VtctlProcess.AddCellInfo(cell2)
	if err != nil {
		return err
	}

	// create the vttablets
	err = createVttablets()
	if err != nil {
		return err
	}

	// create topo server connection
	ts, err = topo.OpenServer(*clusterInstance.TopoFlavorString(), clusterInstance.VtctlProcess.TopoGlobalAddress, clusterInstance.VtctlProcess.TopoGlobalRoot)
	return err
}

// createVttablets is used to create the vttablets for all the tests
func createVttablets() error {
	keyspace := &cluster.Keyspace{Name: keyspaceName}
	shard0 := &cluster.Shard{Name: shardName}

	// creating tablets by hand instead of using StartKeyspace because we don't want to call InitShardPrimary
	var tablets []*cluster.Vttablet
	for _, cellInfo := range cellInfos {
		for i := 0; i < cellInfo.numReplicas; i++ {
			vttabletInstance := clusterInstance.NewVttabletInstance("replica", cellInfo.uidBase, cellInfo.cellName)
			cellInfo.uidBase++
			tablets = append(tablets, vttabletInstance)
			cellInfo.replicaTablets = append(cellInfo.replicaTablets, vttabletInstance)
		}
		for i := 0; i < cellInfo.numRdonly; i++ {
			vttabletInstance := clusterInstance.NewVttabletInstance("rdonly", cellInfo.uidBase, cellInfo.cellName)
			cellInfo.uidBase++
			tablets = append(tablets, vttabletInstance)
			cellInfo.rdonlyTablets = append(cellInfo.rdonlyTablets, vttabletInstance)
		}
	}
	clusterInstance.VtTabletExtraArgs = []string{
		"-lock_tables_timeout", "5s",
		"-disable_active_reparents",
	}
	// Initialize Cluster
	shard0.Vttablets = tablets
	err := clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard0})
	if err != nil {
		return err
	}
	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, tablet := range shard0.Vttablets {
		log.Infof("Starting MySql for tablet %v", tablet.Alias)
		proc, err := tablet.MysqlctlProcess.StartProcess()
		if err != nil {
			return err
		}
		mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
	}
	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		err := proc.Wait()
		if err != nil {
			return err
		}
	}
	for _, tablet := range shard0.Vttablets {
		// Reset status, don't wait for the tablet status. We will check it later
		tablet.VttabletProcess.ServingStatus = ""
		// Start the tablet
		err := tablet.VttabletProcess.Setup()
		if err != nil {
			return err
		}
	}
	for _, tablet := range shard0.Vttablets {
		err := tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"})
		if err != nil {
			return err
		}
	}

	// we also need to wait for the tablet type to change from restore to replica, before we delete a tablet from the topology
	// otherwise it will notice that their is no record for the tablet in the topology when it tries to update its state and shutdown itself!
	for _, tablet := range shard0.Vttablets {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"replica", "rdonly"})
		if err != nil {
			return err
		}
	}

	return nil
}

// shutdownVttablets shuts down all the vttablets and removes them from the topology
func shutdownVttablets() error {
	// demote the primary tablet if there is
	err := demotePrimaryTablet()
	if err != nil {
		return err
	}

	for _, vttablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
		// we need to stop a vttablet only if it is not shutdown
		if !vttablet.VttabletProcess.IsShutdown() {
			// wait for primary tablet to demote. For all others, it will not wait
			err = vttablet.VttabletProcess.WaitForTabletTypes([]string{vttablet.Type})
			if err != nil {
				return err
			}
			// Stop the vttablets
			err := vttablet.VttabletProcess.TearDown()
			if err != nil {
				return err
			}
			// Remove the tablet record for this tablet
		}
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", vttablet.Alias)
		if err != nil {
			return err
		}
	}
	clusterInstance.Keyspaces[0].Shards[0].Vttablets = nil
	return nil
}

// demotePrimaryTablet demotes the primary tablet for our shard
func demotePrimaryTablet() (err error) {
	// lock the shard
	ctx, unlock, lockErr := ts.LockShard(context.Background(), keyspaceName, shardName, "demotePrimaryTablet-vtorc-endtoend-test")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// update the shard record's primary
	if _, err = ts.UpdateShardFields(ctx, keyspaceName, shardName, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = nil
		si.SetPrimaryTermStartTime(time.Now())
		return nil
	}); err != nil {
		return err
	}
	return
}

// startVtorc is used to start the orchestrator with the given extra arguments
func startVtorc(t *testing.T, orcExtraArgs []string, pathToConfig string) {
	// Start vtorc
	clusterInstance.VtorcProcess = clusterInstance.NewOrcProcess(pathToConfig)
	clusterInstance.VtorcProcess.ExtraArgs = orcExtraArgs
	err := clusterInstance.VtorcProcess.Setup()
	require.NoError(t, err)
}

// stopVtorc is used to stop the orchestrator
func stopVtorc(t *testing.T) {
	// Stop vtorc
	if clusterInstance.VtorcProcess != nil {
		err := clusterInstance.VtorcProcess.TearDown()
		require.NoError(t, err)
	}
	clusterInstance.VtorcProcess = nil
}

// setupVttabletsAndVtorc is used to setup the vttablets and start the orchestrator
func setupVttabletsAndVtorc(t *testing.T, numReplicasReqCell1, numRdonlyReqCell1 int, orcExtraArgs []string, configFileName string) {
	// stop vtorc if it is running
	stopVtorc(t)

	// remove all the vttablets so that each test can add the amount that they require
	err := shutdownVttablets()
	require.NoError(t, err)

	for _, cellInfo := range cellInfos {
		if cellInfo.cellName == cell1 {
			for _, tablet := range cellInfo.replicaTablets {
				if numReplicasReqCell1 == 0 {
					break
				}
				err = cleanAndStartVttablet(t, tablet)
				require.NoError(t, err)
				numReplicasReqCell1--
			}

			for _, tablet := range cellInfo.rdonlyTablets {
				if numRdonlyReqCell1 == 0 {
					break
				}
				err = cleanAndStartVttablet(t, tablet)
				require.NoError(t, err)
				numRdonlyReqCell1--
			}
		}
	}

	if numRdonlyReqCell1 > 0 || numReplicasReqCell1 > 0 {
		t.Fatalf("more than available tablets requested. Please increase the constants numReplicas or numRdonly")
	}

	// wait for the tablets to come up properly
	for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
		err := tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}
	for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"replica", "rdonly"})
		require.NoError(t, err)
	}

	pathToConfig := path.Join(os.Getenv("PWD"), configFileName)
	// start vtorc
	startVtorc(t, orcExtraArgs, pathToConfig)
}

func cleanAndStartVttablet(t *testing.T, vttablet *cluster.Vttablet) error {
	// remove the databases if they exist
	_, err := runSQL(t, "DROP DATABASE IF EXISTS vt_ks", vttablet, "")
	require.NoError(t, err)
	_, err = runSQL(t, "DROP DATABASE IF EXISTS _vt", vttablet, "")
	require.NoError(t, err)
	// stop the replication
	_, err = runSQL(t, "STOP SLAVE", vttablet, "")
	require.NoError(t, err)
	// reset the binlog
	_, err = runSQL(t, "RESET MASTER", vttablet, "")
	require.NoError(t, err)

	// start the vttablet
	err = vttablet.VttabletProcess.Setup()
	require.NoError(t, err)

	clusterInstance.Keyspaces[0].Shards[0].Vttablets = append(clusterInstance.Keyspaces[0].Shards[0].Vttablets, vttablet)
	return nil
}

func TestMain(m *testing.M) {
	// setup cellInfos before creating the cluster
	cellInfos = append(cellInfos, &cellInfo{
		cellName:    cell1,
		numReplicas: 12,
		numRdonly:   2,
		uidBase:     100,
	})
	cellInfos = append(cellInfos, &cellInfo{
		cellName:    cell2,
		numReplicas: 2,
		numRdonly:   0,
		uidBase:     200,
	})

	exitcode, err := func() (int, error) {
		err := createClusterAndStartTopo()
		if err != nil {
			return 1, err
		}

		return m.Run(), nil
	}()

	cluster.PanicHandler(nil)

	// stop vtorc first otherwise its logs get polluted
	// with instances being unreachable triggering unnecessary operations
	if clusterInstance.VtorcProcess != nil {
		_ = clusterInstance.VtorcProcess.TearDown()
	}

	for _, cellInfo := range cellInfos {
		killTablets(cellInfo.replicaTablets)
		killTablets(cellInfo.rdonlyTablets)
	}
	clusterInstance.Keyspaces[0].Shards[0].Vttablets = nil
	clusterInstance.Teardown()

	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

func shardPrimaryTablet(t *testing.T, cluster *cluster.LocalProcessCluster, keyspace *cluster.Keyspace, shard *cluster.Shard) *cluster.Vttablet {
	start := time.Now()
	for {
		now := time.Now()
		if now.Sub(start) > time.Second*60 {
			assert.FailNow(t, "failed to elect primary before timeout")
		}
		result, err := cluster.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", fmt.Sprintf("%s/%s", keyspace.Name, shard.Name))
		assert.Nil(t, err)

		var shardInfo topodatapb.Shard
		err = json2.Unmarshal([]byte(result), &shardInfo)
		assert.Nil(t, err)
		if shardInfo.PrimaryAlias == nil {
			log.Warningf("Shard %v/%v has no primary yet, sleep for 1 second\n", keyspace.Name, shard.Name)
			time.Sleep(time.Second)
			continue
		}
		for _, tablet := range shard.Vttablets {
			if tablet.Alias == topoproto.TabletAliasString(shardInfo.PrimaryAlias) {
				return tablet
			}
		}
	}
}

// Makes sure the tablet type is primary, and its health check agrees.
func checkPrimaryTablet(t *testing.T, cluster *cluster.LocalProcessCluster, tablet *cluster.Vttablet, checkServing bool) {
	start := time.Now()
	for {
		now := time.Now()
		if now.Sub(start) > time.Second*60 {
			//log.Exitf("error")
			assert.FailNow(t, "failed to elect primary before timeout")
		}
		result, err := cluster.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
		require.NoError(t, err)
		var tabletInfo topodatapb.Tablet
		err = json2.Unmarshal([]byte(result), &tabletInfo)
		require.NoError(t, err)

		if topodatapb.TabletType_PRIMARY != tabletInfo.GetType() {
			log.Warningf("Tablet %v is not primary yet, sleep for 1 second\n", tablet.Alias)
			time.Sleep(time.Second)
			continue
		}
		// make sure the health stream is updated
		result, err = cluster.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", tablet.Alias)
		require.NoError(t, err)
		var streamHealthResponse querypb.StreamHealthResponse

		err = json2.Unmarshal([]byte(result), &streamHealthResponse)
		require.NoError(t, err)
		if checkServing && !streamHealthResponse.GetServing() {
			log.Warningf("Tablet %v is not serving in health stream yet, sleep for 1 second\n", tablet.Alias)
			time.Sleep(time.Second)
			continue
		}
		tabletType := streamHealthResponse.GetTarget().GetTabletType()
		if tabletType != topodatapb.TabletType_PRIMARY {
			log.Warningf("Tablet %v is not primary in health stream yet, sleep for 1 second\n", tablet.Alias)
			time.Sleep(time.Second)
			continue
		}
		break
	}
}

func checkReplication(t *testing.T, clusterInstance *cluster.LocalProcessCluster, primary *cluster.Vttablet, replicas []*cluster.Vttablet, timeToWait time.Duration) {
	endTime := time.Now().Add(timeToWait)
	// create tables, insert data and make sure it is replicated correctly
	sqlSchema := `
		create table vt_ks.vt_insert_test (
		id bigint,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB
		`
	timeout := time.After(time.Until(endTime))
	for {
		select {
		case <-timeout:
			t.Fatal("timedout waiting for keyspace vt_ks to be created by schema engine")
			return
		default:
			_, err := runSQL(t, sqlSchema, primary, "")
			if err != nil {
				log.Warning("create table failed on primary, will retry")
				time.Sleep(100 * time.Millisecond)
				break
			}
			confirmReplication(t, primary, replicas, time.Until(endTime), lastUsedValue)
			lastUsedValue++
			validateTopology(t, clusterInstance, true, time.Until(endTime))
			return
		}
	}
}

// call this function only after check replication.
// it inserts more data into the table vt_insert_test and checks that it is replicated too
func verifyWritesSucceed(t *testing.T, primary *cluster.Vttablet, replicas []*cluster.Vttablet, timeToWait time.Duration) {
	confirmReplication(t, primary, replicas, timeToWait, lastUsedValue)
	lastUsedValue++
}

func confirmReplication(t *testing.T, primary *cluster.Vttablet, replicas []*cluster.Vttablet, timeToWait time.Duration, valueToInsert int) {
	log.Infof("Insert data into primary and check that it is replicated to replica")
	// insert data into the new primary, check the connected replica work
	insertSQL := fmt.Sprintf("insert into vt_insert_test(id, msg) values (%d, 'test %d')", valueToInsert, valueToInsert)
	_, err := runSQL(t, insertSQL, primary, "vt_ks")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	timeout := time.After(timeToWait)
	for {
		select {
		case <-timeout:
			t.Fatal("timedout waiting for replication, data not yet replicated")
			return
		default:
			err = nil
			for _, tab := range replicas {
				errInReplication := checkInsertedValues(t, tab, valueToInsert)
				if errInReplication != nil {
					err = errInReplication
				}
			}
			if err != nil {
				log.Warningf("waiting for replication - error received - %v, will retry", err)
				time.Sleep(300 * time.Millisecond)
				break
			}
			return
		}
	}
}

func checkInsertedValues(t *testing.T, tablet *cluster.Vttablet, index int) error {
	selectSQL := fmt.Sprintf("select msg from vt_ks.vt_insert_test where id=%d", index)
	qr, err := runSQL(t, selectSQL, tablet, "")
	// The error may be not nil, if the replication has not caught upto the point where the table exists.
	// We can safely skip this error and retry reading after wait
	if err == nil && len(qr.Rows) == 1 {
		return nil
	}
	return fmt.Errorf("data is not yet replicated")
}

func waitForReplicationToStop(t *testing.T, vttablet *cluster.Vttablet) error {
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timedout: waiting for primary to stop replication")
		default:
			res, err := runSQL(t, "SHOW SLAVE STATUS", vttablet, "")
			if err != nil {
				return err
			}
			if len(res.Rows) == 0 {
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func validateTopology(t *testing.T, cluster *cluster.LocalProcessCluster, pingTablets bool, timeToWait time.Duration) {
	ch := make(chan error)
	timeout := time.After(timeToWait)
	go func() {
		for {
			select {
			case <-timeout:
				ch <- fmt.Errorf("time out waiting for validation to pass")
				return
			default:
				var err error
				var output string
				if pingTablets {
					output, err = cluster.VtctlclientProcess.ExecuteCommandWithOutput("Validate", "-ping-tablets=true")
				} else {
					output, err = cluster.VtctlclientProcess.ExecuteCommandWithOutput("Validate")
				}
				if err != nil {
					log.Warningf("Validate failed, retrying, output - %s", output)
					time.Sleep(100 * time.Millisecond)
					break
				}
				ch <- nil
				return
			}
		}
	}()

	select {
	case err := <-ch:
		require.NoError(t, err)
		return
	case <-timeout:
		t.Fatal("time out waiting for validation to pass")
	}
}

func killTablets(vttablets []*cluster.Vttablet) {
	for _, tablet := range vttablets {
		log.Infof("Shutting down MySQL for %v", tablet.Alias)
		_ = tablet.MysqlctlProcess.Stop()
		log.Infof("Calling TearDown on tablet %v", tablet.Alias)
		_ = tablet.VttabletProcess.TearDown()
	}
}

func getMysqlConnParam(tablet *cluster.Vttablet, db string) mysql.ConnParams {
	connParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", tablet.TabletUID)),
	}
	if db != "" {
		connParams.DbName = db
	}
	return connParams
}

func runSQL(t *testing.T, sql string, tablet *cluster.Vttablet, db string) (*sqltypes.Result, error) {
	// Get Connection
	tabletParams := getMysqlConnParam(tablet, db)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.Nil(t, err)
	defer conn.Close()

	// runSQL
	return execute(t, conn, sql)
}

func execute(t *testing.T, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, 1000, true)
}

// startVttablet is used to start a vttablet from the given cell and type
func startVttablet(t *testing.T, cell string, isRdonly bool) *cluster.Vttablet {

	var tablet *cluster.Vttablet
	for _, cellInfo := range cellInfos {
		if cellInfo.cellName == cell {
			tabletsToUse := cellInfo.replicaTablets
			if isRdonly {
				tabletsToUse = cellInfo.rdonlyTablets
			}
			for _, vttablet := range tabletsToUse {
				if isVttabletInUse(vttablet) {
					continue
				}
				tablet = vttablet
				err := cleanAndStartVttablet(t, vttablet)
				require.NoError(t, err)
				break
			}
			break
		}
	}

	require.NotNil(t, tablet, "Could not start requested tablet")
	// wait for the tablets to come up properly
	err := tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"})
	require.NoError(t, err)
	err = tablet.VttabletProcess.WaitForTabletTypes([]string{"replica", "rdonly"})
	require.NoError(t, err)
	return tablet
}

func isVttabletInUse(tablet *cluster.Vttablet) bool {
	for _, vttablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
		if tablet == vttablet {
			return true
		}
	}
	return false
}

func permanentlyRemoveVttablet(tablet *cluster.Vttablet) {
	// remove the tablet from our global list
	for _, cellInfo := range cellInfos {
		for i, vttablet := range cellInfo.replicaTablets {
			if vttablet == tablet {
				// remove this tablet since its mysql has stopped
				cellInfo.replicaTablets = append(cellInfo.replicaTablets[:i], cellInfo.replicaTablets[i+1:]...)
				killTablets([]*cluster.Vttablet{tablet})
				return
			}
		}
		for i, vttablet := range cellInfo.rdonlyTablets {
			if vttablet == tablet {
				// remove this tablet since its mysql has stopped
				cellInfo.replicaTablets = append(cellInfo.replicaTablets[:i], cellInfo.replicaTablets[i+1:]...)
				killTablets([]*cluster.Vttablet{tablet})
				return
			}
		}
	}
}

func changePrivileges(t *testing.T, sql string, tablet *cluster.Vttablet, user string) {
	_, err := runSQL(t, "SET sql_log_bin = OFF;"+sql+";SET sql_log_bin = ON;", tablet, "")
	require.NoError(t, err)

	res, err := runSQL(t, fmt.Sprintf("SELECT id FROM INFORMATION_SCHEMA.PROCESSLIST WHERE user = '%s'", user), tablet, "")
	require.NoError(t, err)
	for _, row := range res.Rows {
		id, err := row[0].ToInt64()
		require.NoError(t, err)
		_, err = runSQL(t, fmt.Sprintf("kill %d", id), tablet, "")
		require.NoError(t, err)
	}
}

func resetPrimaryLogs(t *testing.T, curPrimary *cluster.Vttablet) {
	_, err := runSQL(t, "FLUSH BINARY LOGS", curPrimary, "")
	require.NoError(t, err)

	binLogsOutput, err := runSQL(t, "SHOW BINARY LOGS", curPrimary, "")
	require.NoError(t, err)
	require.True(t, len(binLogsOutput.Rows) >= 2, "there should be atlease 2 binlog files")

	lastLogFile := binLogsOutput.Rows[len(binLogsOutput.Rows)-1][0].ToString()

	// purge binary logs of the primary so that crossCellReplica cannot catch up
	_, err = runSQL(t, "PURGE BINARY LOGS TO '"+lastLogFile+"'", curPrimary, "")
	require.NoError(t, err)
}
