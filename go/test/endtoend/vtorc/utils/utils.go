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

package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	// Register topo implementations.
	_ "vitess.io/vitess/go/vt/topo/consultopo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/topo/zk2topo"
)

const (
	keyspaceName = "ks"
	shardName    = "0"
	Hostname     = "localhost"
	Cell1        = "zone1"
	Cell2        = "zone2"
)

// CellInfo stores the information regarding 1 cell including the tablets it contains
type CellInfo struct {
	CellName       string
	ReplicaTablets []*cluster.Vttablet
	RdonlyTablets  []*cluster.Vttablet
	// constants that should be set in TestMain
	NumReplicas int
	NumRdonly   int
	UIDBase     int
}

// VTOrcClusterInfo stores the information for a cluster. This is supposed to be used only for VTOrc tests.
type VTOrcClusterInfo struct {
	ClusterInstance *cluster.LocalProcessCluster
	Ts              *topo.Server
	CellInfos       []*CellInfo
	lastUsedValue   int
}

// CreateClusterAndStartTopo starts the cluster and topology service
func CreateClusterAndStartTopo(cellInfos []*CellInfo) (*VTOrcClusterInfo, error) {
	clusterInstance := cluster.NewCluster(Cell1, Hostname)

	// Start topo server
	err := clusterInstance.StartTopo()
	if err != nil {
		return nil, err
	}

	// Adding another cell in the same cluster
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+Cell2)
	if err != nil {
		return nil, err
	}
	err = clusterInstance.VtctlProcess.AddCellInfo(Cell2)
	if err != nil {
		return nil, err
	}

	// create the vttablets
	err = createVttablets(clusterInstance, cellInfos)
	if err != nil {
		return nil, err
	}

	// create topo server connection
	ts, err := topo.OpenServer(*clusterInstance.TopoFlavorString(), clusterInstance.VtctlProcess.TopoGlobalAddress, clusterInstance.VtctlProcess.TopoGlobalRoot)
	return &VTOrcClusterInfo{
		ClusterInstance: clusterInstance,
		Ts:              ts,
		CellInfos:       cellInfos,
		lastUsedValue:   100,
	}, err
}

// createVttablets is used to create the vttablets for all the tests
func createVttablets(clusterInstance *cluster.LocalProcessCluster, cellInfos []*CellInfo) error {
	keyspace := &cluster.Keyspace{Name: keyspaceName}
	shard0 := &cluster.Shard{Name: shardName}

	// creating tablets by hand instead of using StartKeyspace because we don't want to call InitShardPrimary
	var tablets []*cluster.Vttablet
	for _, cellInfo := range cellInfos {
		for i := 0; i < cellInfo.NumReplicas; i++ {
			vttabletInstance := clusterInstance.NewVttabletInstance("replica", cellInfo.UIDBase, cellInfo.CellName)
			cellInfo.UIDBase++
			tablets = append(tablets, vttabletInstance)
			cellInfo.ReplicaTablets = append(cellInfo.ReplicaTablets, vttabletInstance)
		}
		for i := 0; i < cellInfo.NumRdonly; i++ {
			vttabletInstance := clusterInstance.NewVttabletInstance("rdonly", cellInfo.UIDBase, cellInfo.CellName)
			cellInfo.UIDBase++
			tablets = append(tablets, vttabletInstance)
			cellInfo.RdonlyTablets = append(cellInfo.RdonlyTablets, vttabletInstance)
		}
	}
	clusterInstance.VtTabletExtraArgs = []string{
		"--lock_tables_timeout", "5s",
		"--disable_active_reparents",
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
func shutdownVttablets(clusterInfo *VTOrcClusterInfo) error {
	// reset the shard primary
	err := resetShardPrimary(clusterInfo.Ts)
	if err != nil {
		return err
	}

	for _, vttablet := range clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets {
		// we need to stop a vttablet only if it is not shutdown
		if !vttablet.VttabletProcess.IsShutdown() {
			// Stop the vttablets
			err := vttablet.VttabletProcess.TearDown()
			if err != nil {
				return err
			}
			// Remove the tablet record for this tablet
		}
		// Ignoring error here because some tests delete tablets themselves.
		_ = clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommand("DeleteTablets", vttablet.Alias)
	}
	clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets = nil
	return nil
}

// resetShardPrimary resets the shard's primary
func resetShardPrimary(ts *topo.Server) (err error) {
	// lock the shard
	ctx, unlock, lockErr := ts.LockShard(context.Background(), keyspaceName, shardName, "resetShardPrimary-vtorc-endtoend-test")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// update the shard record's primary
	if _, err = ts.UpdateShardFields(ctx, keyspaceName, shardName, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = nil
		return nil
	}); err != nil {
		return err
	}
	return
}

// StartVTOrcs is used to start the vtorcs with the given extra arguments
func StartVTOrcs(t *testing.T, clusterInfo *VTOrcClusterInfo, orcExtraArgs []string, config cluster.VTOrcConfiguration, count int) {
	t.Helper()
	// Start vtorc
	for i := 0; i < count; i++ {
		vtorcProcess := clusterInfo.ClusterInstance.NewVTOrcProcess(config)
		vtorcProcess.ExtraArgs = orcExtraArgs
		err := vtorcProcess.Setup()
		require.NoError(t, err)
		clusterInfo.ClusterInstance.VTOrcProcesses = append(clusterInfo.ClusterInstance.VTOrcProcesses, vtorcProcess)
	}
}

// StopVTOrcs is used to stop the vtorcs
func StopVTOrcs(t *testing.T, clusterInfo *VTOrcClusterInfo) {
	t.Helper()
	// Stop vtorc
	for _, vtorcProcess := range clusterInfo.ClusterInstance.VTOrcProcesses {
		if err := vtorcProcess.TearDown(); err != nil {
			log.Errorf("Error in vtorc teardown: %v", err)
		}
	}
	clusterInfo.ClusterInstance.VTOrcProcesses = nil
}

// SetupVttabletsAndVTOrcs is used to setup the vttablets and start the vtorcs
func SetupVttabletsAndVTOrcs(t *testing.T, clusterInfo *VTOrcClusterInfo, numReplicasReqCell1, numRdonlyReqCell1 int, orcExtraArgs []string, config cluster.VTOrcConfiguration, vtorcCount int, durability string) {
	// stop vtorc if it is running
	StopVTOrcs(t, clusterInfo)

	// remove all the vttablets so that each test can add the amount that they require
	err := shutdownVttablets(clusterInfo)
	require.NoError(t, err)

	for _, cellInfo := range clusterInfo.CellInfos {
		if cellInfo.CellName == Cell1 {
			for _, tablet := range cellInfo.ReplicaTablets {
				if numReplicasReqCell1 == 0 {
					break
				}
				cleanAndStartVttablet(t, clusterInfo, tablet)
				numReplicasReqCell1--
			}

			for _, tablet := range cellInfo.RdonlyTablets {
				if numRdonlyReqCell1 == 0 {
					break
				}
				cleanAndStartVttablet(t, clusterInfo, tablet)
				numRdonlyReqCell1--
			}
		}
	}

	if numRdonlyReqCell1 > 0 || numReplicasReqCell1 > 0 {
		t.Fatalf("more than available tablets requested. Please increase the constants numReplicas or numRdonly")
	}

	// wait for the tablets to come up properly
	for _, tablet := range clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets {
		err := tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}
	for _, tablet := range clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"replica", "rdonly"})
		require.NoError(t, err)
	}

	if durability == "" {
		durability = "none"
	}
	out, err := clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, fmt.Sprintf("--durability-policy=%s", durability))
	require.NoError(t, err, out)
	// VTOrc now uses shard record too, so we need to clear that as well for correct testing.
	_, err = clusterInfo.Ts.UpdateShardFields(context.Background(), keyspaceName, shardName, func(info *topo.ShardInfo) error {
		info.PrimaryTermStartTime = nil
		info.PrimaryAlias = nil
		return nil
	})
	require.NoError(t, err)

	// start vtorc
	StartVTOrcs(t, clusterInfo, orcExtraArgs, config, vtorcCount)
}

// cleanAndStartVttablet cleans the MySQL instance underneath for running a new test. It also starts the vttablet.
func cleanAndStartVttablet(t *testing.T, clusterInfo *VTOrcClusterInfo, vttablet *cluster.Vttablet) {
	t.Helper()
	// set super_read_only to false
	_, err := RunSQL(t, "SET GLOBAL super_read_only = OFF", vttablet, "")
	require.NoError(t, err)
	// remove the databases if they exist
	_, err = RunSQL(t, "DROP DATABASE IF EXISTS vt_ks", vttablet, "")
	require.NoError(t, err)
	_, err = RunSQL(t, "DROP DATABASE IF EXISTS _vt", vttablet, "")
	require.NoError(t, err)
	// stop the replication
	_, err = RunSQL(t, "STOP SLAVE", vttablet, "")
	require.NoError(t, err)
	// reset the binlog
	_, err = RunSQL(t, "RESET MASTER", vttablet, "")
	require.NoError(t, err)
	// set read-only to true
	_, err = RunSQL(t, "SET GLOBAL read_only = ON", vttablet, "")
	require.NoError(t, err)

	// start the vttablet
	err = vttablet.VttabletProcess.Setup()
	require.NoError(t, err)

	clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets = append(clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets, vttablet)
}

// ShardPrimaryTablet waits until a primary tablet has been elected for the given shard and returns it
func ShardPrimaryTablet(t *testing.T, clusterInfo *VTOrcClusterInfo, keyspace *cluster.Keyspace, shard *cluster.Shard) *cluster.Vttablet {
	start := time.Now()
	for {
		now := time.Now()
		if now.Sub(start) > time.Second*60 {
			assert.FailNow(t, "failed to elect primary before timeout")
		}
		si, err := clusterInfo.ClusterInstance.VtctldClientProcess.GetShard(keyspace.Name, shard.Name)
		require.NoError(t, err)

		if si.Shard.PrimaryAlias == nil {
			log.Warningf("Shard %v/%v has no primary yet, sleep for 1 second\n", keyspace.Name, shard.Name)
			time.Sleep(time.Second)
			continue
		}
		for _, tablet := range shard.Vttablets {
			if tablet.Alias == topoproto.TabletAliasString(si.Shard.PrimaryAlias) {
				return tablet
			}
		}
	}
}

// CheckPrimaryTablet waits until the specified tablet becomes the primary tablet
// Makes sure the tablet type is primary, and its health check agrees.
func CheckPrimaryTablet(t *testing.T, clusterInfo *VTOrcClusterInfo, tablet *cluster.Vttablet, checkServing bool) {
	start := time.Now()
	for {
		now := time.Now()
		if now.Sub(start) > time.Second*60 {
			//log.Exitf("error")
			assert.FailNow(t, "failed to elect primary before timeout")
		}
		tabletInfo, err := clusterInfo.ClusterInstance.VtctldClientProcess.GetTablet(tablet.Alias)
		require.NoError(t, err)
		if topodatapb.TabletType_PRIMARY != tabletInfo.GetType() {
			log.Warningf("Tablet %v is not primary yet, sleep for 1 second\n", tablet.Alias)
			time.Sleep(time.Second)
			continue
		}
		// make sure the health stream is updated
		shrs, err := clusterInfo.ClusterInstance.StreamTabletHealth(context.Background(), tablet, 1)
		require.NoError(t, err)

		streamHealthResponse := shrs[0]

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

// CheckReplication checks that the replication is setup correctly and writes succeed and are replicated on all the replicas
func CheckReplication(t *testing.T, clusterInfo *VTOrcClusterInfo, primary *cluster.Vttablet, replicas []*cluster.Vttablet, timeToWait time.Duration) {
	endTime := time.Now().Add(timeToWait)
	// create tables, insert data and make sure it is replicated correctly
	sqlSchema := `
		create table if not exists vt_ks.vt_insert_test (
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
			_, err := RunSQL(t, sqlSchema, primary, "")
			if err != nil {
				log.Warningf("create table failed on primary - %v, will retry", err)
				time.Sleep(100 * time.Millisecond)
				break
			}
			clusterInfo.lastUsedValue++
			confirmReplication(t, primary, replicas, time.Until(endTime), clusterInfo.lastUsedValue)
			validateTopology(t, clusterInfo, true, time.Until(endTime))
			return
		}
	}
}

// VerifyWritesSucceed inserts more data into the table vt_insert_test and checks that it is replicated too
// Call this function only after CheckReplication has been executed once, since that function creates the table that this function uses.
func VerifyWritesSucceed(t *testing.T, clusterInfo *VTOrcClusterInfo, primary *cluster.Vttablet, replicas []*cluster.Vttablet, timeToWait time.Duration) {
	t.Helper()
	clusterInfo.lastUsedValue++
	confirmReplication(t, primary, replicas, timeToWait, clusterInfo.lastUsedValue)
}

func confirmReplication(t *testing.T, primary *cluster.Vttablet, replicas []*cluster.Vttablet, timeToWait time.Duration, valueToInsert int) {
	t.Helper()
	log.Infof("Insert data into primary and check that it is replicated to replica")
	// insert data into the new primary, check the connected replica work
	insertSQL := fmt.Sprintf("insert into vt_insert_test(id, msg) values (%d, 'test %d')", valueToInsert, valueToInsert)
	_, err := RunSQL(t, insertSQL, primary, "vt_ks")
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

// CheckTabletUptoDate verifies that the tablet has all the writes so far
func CheckTabletUptoDate(t *testing.T, clusterInfo *VTOrcClusterInfo, tablet *cluster.Vttablet) {
	err := checkInsertedValues(t, tablet, clusterInfo.lastUsedValue)
	require.NoError(t, err)
}

func checkInsertedValues(t *testing.T, tablet *cluster.Vttablet, index int) error {
	selectSQL := fmt.Sprintf("select msg from vt_ks.vt_insert_test where id=%d", index)
	qr, err := RunSQL(t, selectSQL, tablet, "")
	// The error may be not nil, if the replication has not caught upto the point where the table exists.
	// We can safely skip this error and retry reading after wait
	if err == nil && len(qr.Rows) == 1 {
		return nil
	}
	return fmt.Errorf("data is not yet replicated")
}

// WaitForReplicationToStop waits for replication to stop on the given tablet
func WaitForReplicationToStop(t *testing.T, vttablet *cluster.Vttablet) error {
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timedout: waiting for primary to stop replication")
		default:
			res, err := RunSQL(t, "SHOW SLAVE STATUS", vttablet, "")
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

func validateTopology(t *testing.T, clusterInfo *VTOrcClusterInfo, pingTablets bool, timeToWait time.Duration) {
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
					output, err = clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("Validate", "--ping-tablets")
				} else {
					output, err = clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("Validate")
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

// KillTablets is used to kill the tablets
func KillTablets(vttablets []*cluster.Vttablet) {
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

// RunSQL is used to run a SQL statement on the given tablet
func RunSQL(t *testing.T, sql string, tablet *cluster.Vttablet, db string) (*sqltypes.Result, error) {
	// Get Connection
	tabletParams := getMysqlConnParam(tablet, db)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.Nil(t, err)
	defer conn.Close()

	// RunSQL
	return execute(t, conn, sql)
}

// RunSQLs is used to run a list of SQL statements on the given tablet
func RunSQLs(t *testing.T, sqls []string, tablet *cluster.Vttablet, db string) error {
	// Get Connection
	tabletParams := getMysqlConnParam(tablet, db)
	var timeoutDuration = time.Duration(5 * len(sqls))
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration*time.Second)
	defer cancel()
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.Nil(t, err)
	defer conn.Close()

	// Run SQLs
	for _, sql := range sqls {
		if _, err := execute(t, conn, sql); err != nil {
			return err
		}
	}
	return nil
}

func execute(t *testing.T, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, 1000, true)
}

// StartVttablet is used to start a vttablet from the given cell and type
func StartVttablet(t *testing.T, clusterInfo *VTOrcClusterInfo, cell string, isRdonly bool) *cluster.Vttablet {

	var tablet *cluster.Vttablet
	for _, cellInfo := range clusterInfo.CellInfos {
		if cellInfo.CellName == cell {
			tabletsToUse := cellInfo.ReplicaTablets
			if isRdonly {
				tabletsToUse = cellInfo.RdonlyTablets
			}
			for _, vttablet := range tabletsToUse {
				if isVttabletInUse(clusterInfo, vttablet) {
					continue
				}
				tablet = vttablet
				cleanAndStartVttablet(t, clusterInfo, vttablet)
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

func isVttabletInUse(clusterInfo *VTOrcClusterInfo, tablet *cluster.Vttablet) bool {
	for _, vttablet := range clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets {
		if tablet == vttablet {
			return true
		}
	}
	return false
}

// PermanentlyRemoveVttablet removes the tablet specified from the cluster. It makes it so that
// this vttablet or mysql instance are not reused for any other test.
func PermanentlyRemoveVttablet(clusterInfo *VTOrcClusterInfo, tablet *cluster.Vttablet) {
	// remove the tablet from our global list
	for _, cellInfo := range clusterInfo.CellInfos {
		for i, vttablet := range cellInfo.ReplicaTablets {
			if vttablet == tablet {
				// remove this tablet since its mysql has stopped
				cellInfo.ReplicaTablets = append(cellInfo.ReplicaTablets[:i], cellInfo.ReplicaTablets[i+1:]...)
				KillTablets([]*cluster.Vttablet{tablet})
				return
			}
		}
		for i, vttablet := range cellInfo.RdonlyTablets {
			if vttablet == tablet {
				// remove this tablet since its mysql has stopped
				cellInfo.RdonlyTablets = append(cellInfo.RdonlyTablets[:i], cellInfo.RdonlyTablets[i+1:]...)
				KillTablets([]*cluster.Vttablet{tablet})
				return
			}
		}
	}
}

// ResetPrimaryLogs is used reset the binary logs
func ResetPrimaryLogs(t *testing.T, curPrimary *cluster.Vttablet) {
	_, err := RunSQL(t, "FLUSH BINARY LOGS", curPrimary, "")
	require.NoError(t, err)

	binLogsOutput, err := RunSQL(t, "SHOW BINARY LOGS", curPrimary, "")
	require.NoError(t, err)
	require.True(t, len(binLogsOutput.Rows) >= 2, "there should be atlease 2 binlog files")

	lastLogFile := binLogsOutput.Rows[len(binLogsOutput.Rows)-1][0].ToString()

	_, err = RunSQL(t, "PURGE BINARY LOGS TO '"+lastLogFile+"'", curPrimary, "")
	require.NoError(t, err)
}

// CheckSourcePort is used to check that the replica has the given source port set in its MySQL instance
func CheckSourcePort(t *testing.T, replica *cluster.Vttablet, source *cluster.Vttablet, timeToWait time.Duration) {
	timeout := time.After(timeToWait)
	for {
		select {
		case <-timeout:
			t.Fatal("timedout waiting for correct primary to be setup")
			return
		default:
			res, err := RunSQL(t, "SHOW SLAVE STATUS", replica, "")
			require.NoError(t, err)

			if len(res.Rows) != 1 {
				log.Warningf("no replication status yet, will retry")
				break
			}

			for idx, field := range res.Fields {
				if strings.EqualFold(field.Name, "MASTER_PORT") || strings.EqualFold(field.Name, "SOURCE_PORT") {
					port, err := res.Rows[0][idx].ToInt64()
					require.NoError(t, err)
					if port == int64(source.MySQLPort) {
						return
					}
				}
			}
			log.Warningf("source port not set correctly yet, will retry")
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// MakeAPICall is used make an API call given the url. It returns the status and the body of the response received
func MakeAPICall(t *testing.T, vtorc *cluster.VTOrcProcess, url string) (status int, response string, err error) {
	t.Helper()
	status, response, err = vtorc.MakeAPICall(url)
	return status, response, err
}

// MakeAPICallRetry is used to make an API call and retry on the given condition.
// The function provided takes in the status and response and returns if we should continue to retry or not
func MakeAPICallRetry(t *testing.T, vtorc *cluster.VTOrcProcess, url string, retry func(int, string) bool) (status int, response string) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("timed out waiting for api to work")
			return
		default:
			status, response, _ := MakeAPICall(t, vtorc, url)
			if retry(status, response) {
				time.Sleep(1 * time.Second)
				break
			}
			return status, response
		}
	}
}

// SetupNewClusterSemiSync is used to setup a new cluster with semi-sync set.
// It creates a cluster with 4 tablets, one of which is a Replica
func SetupNewClusterSemiSync(t *testing.T) *VTOrcClusterInfo {
	var tablets []*cluster.Vttablet
	clusterInstance := cluster.NewCluster(Cell1, Hostname)
	keyspace := &cluster.Keyspace{Name: keyspaceName}
	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err, "Error starting topo: %v", err)

	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+Cell1)
	require.NoError(t, err, "Error managing topo: %v", err)

	for i := 0; i < 3; i++ {
		tablet := clusterInstance.NewVttabletInstance("replica", 100+i, Cell1)
		tablets = append(tablets, tablet)
	}
	tablet := clusterInstance.NewVttabletInstance("rdonly", 103, Cell1)
	tablets = append(tablets, tablet)

	shard := &cluster.Shard{Name: shardName}
	shard.Vttablets = tablets

	clusterInstance.VtTabletExtraArgs = []string{
		"--lock_tables_timeout", "5s",
		"--disable_active_reparents",
	}

	// Initialize Cluster
	err = clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard})
	require.NoError(t, err, "Cannot launch cluster: %v", err)

	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			log.Infof("Starting MySql for tablet %v", tablet.Alias)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				require.NoError(t, err, "Error starting start mysql: %v", err)
			}
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
		}
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		if err := proc.Wait(); err != nil {
			require.NoError(t, err, "Error starting mysql: %v", err)
		}
	}

	for _, tablet := range tablets {
		require.NoError(t, err)
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	for _, tablet := range tablets {
		err := tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}

	out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=semi_sync")
	require.NoError(t, err, out)

	// create topo server connection
	ts, err := topo.OpenServer(*clusterInstance.TopoFlavorString(), clusterInstance.VtctlProcess.TopoGlobalAddress, clusterInstance.VtctlProcess.TopoGlobalRoot)
	require.NoError(t, err)
	clusterInfo := &VTOrcClusterInfo{
		ClusterInstance: clusterInstance,
		Ts:              ts,
		CellInfos:       nil,
		lastUsedValue:   100,
	}
	return clusterInfo
}

// AddSemiSyncKeyspace is used to setup a new keyspace with semi-sync.
// It creates a keyspace with 3 tablets
func AddSemiSyncKeyspace(t *testing.T, clusterInfo *VTOrcClusterInfo) {
	var tablets []*cluster.Vttablet
	keyspaceSemiSyncName := "ks2"
	keyspace := &cluster.Keyspace{Name: keyspaceSemiSyncName}

	for i := 0; i < 3; i++ {
		tablet := clusterInfo.ClusterInstance.NewVttabletInstance("replica", 300+i, Cell1)
		tablets = append(tablets, tablet)
	}

	shard := &cluster.Shard{Name: shardName}
	shard.Vttablets = tablets

	oldVttabletArgs := clusterInfo.ClusterInstance.VtTabletExtraArgs
	defer func() {
		clusterInfo.ClusterInstance.VtTabletExtraArgs = oldVttabletArgs
	}()
	clusterInfo.ClusterInstance.VtTabletExtraArgs = []string{
		"--lock_tables_timeout", "5s",
		"--disable_active_reparents",
	}

	// Initialize Cluster
	err := clusterInfo.ClusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard})
	require.NoError(t, err, "Cannot launch cluster: %v", err)

	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInfo.ClusterInstance.Keyspaces[1].Shards {
		for _, tablet := range shard.Vttablets {
			log.Infof("Starting MySql for tablet %v", tablet.Alias)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				require.NoError(t, err, "Error starting start mysql: %v", err)
			}
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
		}
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		if err := proc.Wait(); err != nil {
			require.NoError(t, err, "Error starting mysql: %v", err)
		}
	}

	for _, tablet := range tablets {
		require.NoError(t, err)
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	for _, tablet := range tablets {
		err := tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"})
		require.NoError(t, err)
	}

	vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", clusterInfo.ClusterInstance.VtctldProcess.GrpcPort, clusterInfo.ClusterInstance.TmpDirectory)
	out, err := vtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceSemiSyncName, "--durability-policy=semi_sync")
	require.NoError(t, err, out)
}

// IsSemiSyncSetupCorrectly checks that the semi-sync is setup correctly on the given vttablet
func IsSemiSyncSetupCorrectly(t *testing.T, tablet *cluster.Vttablet, semiSyncVal string) bool {
	dbVar, err := tablet.VttabletProcess.GetDBVar("rpl_semi_sync_slave_enabled", "")
	require.NoError(t, err)
	return semiSyncVal == dbVar
}

// IsPrimarySemiSyncSetupCorrectly checks that the priamry side semi-sync is setup correctly on the given vttablet
func IsPrimarySemiSyncSetupCorrectly(t *testing.T, tablet *cluster.Vttablet, semiSyncVal string) bool {
	dbVar, err := tablet.VttabletProcess.GetDBVar("rpl_semi_sync_master_enabled", "")
	require.NoError(t, err)
	return semiSyncVal == dbVar
}

// WaitForReadOnlyValue waits for the read_only global variable to reach the provided value
func WaitForReadOnlyValue(t *testing.T, curPrimary *cluster.Vttablet, expectValue int64) (match bool) {
	timeout := 15 * time.Second
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		qr, err := RunSQL(t, "select @@global.read_only as read_only", curPrimary, "")
		require.NoError(t, err)
		require.NotNil(t, qr)
		row := qr.Named().Row()
		require.NotNil(t, row)
		readOnly, err := row.ToInt64("read_only")
		require.NoError(t, err)
		if readOnly == expectValue {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

// WaitForSuccessfulRecoveryCount waits until the given recovery name's count of successful runs matches the count expected
func WaitForSuccessfulRecoveryCount(t *testing.T, vtorcInstance *cluster.VTOrcProcess, recoveryName string, countExpected int) {
	t.Helper()
	timeout := 15 * time.Second
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		vars := vtorcInstance.GetVars()
		successfulRecoveriesMap := vars["SuccessfulRecoveries"].(map[string]interface{})
		successCount := getIntFromValue(successfulRecoveriesMap[recoveryName])
		if successCount == countExpected {
			return
		}
		time.Sleep(time.Second)
	}
	vars := vtorcInstance.GetVars()
	successfulRecoveriesMap := vars["SuccessfulRecoveries"].(map[string]interface{})
	successCount := getIntFromValue(successfulRecoveriesMap[recoveryName])
	assert.EqualValues(t, countExpected, successCount)
}

// WaitForSuccessfulPRSCount waits until the given keyspace-shard's count of successful prs runs matches the count expected.
func WaitForSuccessfulPRSCount(t *testing.T, vtorcInstance *cluster.VTOrcProcess, keyspace, shard string, countExpected int) {
	t.Helper()
	timeout := 15 * time.Second
	startTime := time.Now()
	mapKey := fmt.Sprintf("%v.%v.success", keyspace, shard)
	for time.Since(startTime) < timeout {
		vars := vtorcInstance.GetVars()
		prsCountsMap := vars["planned_reparent_counts"].(map[string]interface{})
		successCount := getIntFromValue(prsCountsMap[mapKey])
		if successCount == countExpected {
			return
		}
		time.Sleep(time.Second)
	}
	vars := vtorcInstance.GetVars()
	prsCountsMap := vars["planned_reparent_counts"].(map[string]interface{})
	successCount := getIntFromValue(prsCountsMap[mapKey])
	assert.EqualValues(t, countExpected, successCount)
}

// WaitForSuccessfulERSCount waits until the given keyspace-shard's count of successful ers runs matches the count expected.
func WaitForSuccessfulERSCount(t *testing.T, vtorcInstance *cluster.VTOrcProcess, keyspace, shard string, countExpected int) {
	t.Helper()
	timeout := 15 * time.Second
	startTime := time.Now()
	mapKey := fmt.Sprintf("%v.%v.success", keyspace, shard)
	for time.Since(startTime) < timeout {
		vars := vtorcInstance.GetVars()
		ersCountsMap := vars["emergency_reparent_counts"].(map[string]interface{})
		successCount := getIntFromValue(ersCountsMap[mapKey])
		if successCount == countExpected {
			return
		}
		time.Sleep(time.Second)
	}
	vars := vtorcInstance.GetVars()
	ersCountsMap := vars["emergency_reparent_counts"].(map[string]interface{})
	successCount := getIntFromValue(ersCountsMap[mapKey])
	assert.EqualValues(t, countExpected, successCount)
}

// getIntFromValue is a helper function to get an integer from the given value.
// If it is convertible to a float, then we round the number to the nearest integer.
// If the value is not numeric at all, we return 0.
func getIntFromValue(val any) int {
	value := reflect.ValueOf(val)
	if value.CanFloat() {
		return int(math.Round(value.Float()))
	}
	if value.CanInt() {
		return int(value.Int())
	}
	return 0
}

// WaitForDetectedProblems waits until the given analysis code, alias, keyspace and shard count matches the count expected.
func WaitForDetectedProblems(t *testing.T, vtorcInstance *cluster.VTOrcProcess, code, alias, ks, shard string, expect int) {
	t.Helper()
	key := strings.Join([]string{code, alias, ks, shard}, ".")
	timeout := 15 * time.Second
	startTime := time.Now()

	for time.Since(startTime) < timeout {
		vars := vtorcInstance.GetVars()
		problems := vars["DetectedProblems"].(map[string]interface{})
		actual := getIntFromValue(problems[key])
		if actual == expect {
			return
		}
		time.Sleep(time.Second)
	}

	vars := vtorcInstance.GetVars()
	problems := vars["DetectedProblems"].(map[string]interface{})
	actual, ok := problems[key]
	actual = getIntFromValue(actual)

	assert.True(t, ok,
		"The metric DetectedProblems[%s] should exist but does not (all problems: %+v)",
		key, problems,
	)

	assert.EqualValues(t, expect, actual,
		"The metric DetectedProblems[%s] should be %v but is %v (all problems: %+v)",
		key, expect, actual,
		problems,
	)
}

// WaitForTabletType waits for the tablet to reach a certain type.
func WaitForTabletType(t *testing.T, tablet *cluster.Vttablet, expectedTabletType string) {
	t.Helper()
	err := tablet.VttabletProcess.WaitForTabletTypes([]string{expectedTabletType})
	require.NoError(t, err)
}

// WaitForInstancePollSecondsExceededCount waits for 30 seconds and then queries api/aggregated-discovery-metrics.
// It expects to find minimum occurrence or exact count of `keyName` provided.
func WaitForInstancePollSecondsExceededCount(t *testing.T, vtorcInstance *cluster.VTOrcProcess, keyName string, minCountExpected float64, enforceEquality bool) {
	t.Helper()
	var sinceInSeconds = 30
	duration := time.Duration(sinceInSeconds)
	time.Sleep(duration * time.Second)

	statusCode, res, err := vtorcInstance.MakeAPICall("api/aggregated-discovery-metrics?seconds=" + strconv.Itoa(sinceInSeconds))
	if err != nil {
		assert.Fail(t, "Not able to call api/aggregated-discovery-metrics")
	}
	if statusCode == 200 {
		resultMap := make(map[string]any)
		err := json.Unmarshal([]byte(res), &resultMap)
		if err != nil {
			assert.Fail(t, "invalid response from api/aggregated-discovery-metrics")
		}
		successCount := resultMap[keyName]
		if iSuccessCount, ok := successCount.(float64); ok {
			if enforceEquality {
				assert.Equal(t, iSuccessCount, minCountExpected)
			} else {
				assert.GreaterOrEqual(t, iSuccessCount, minCountExpected)
			}
			return
		}
	}
	assert.Fail(t, "invalid response from api/aggregated-discovery-metrics")
}

// PrintVTOrcLogsOnFailure prints the VTOrc logs on failure of the test.
// This function is supposed to be called as the first defer command from the vtorc tests.
func PrintVTOrcLogsOnFailure(t *testing.T, clusterInstance *cluster.LocalProcessCluster) {
	// If the test has not failed, then we don't need to print anything.
	if !t.Failed() {
		return
	}

	log.Errorf("Printing VTOrc logs")
	for _, vtorc := range clusterInstance.VTOrcProcesses {
		if vtorc == nil || vtorc.LogFileName == "" {
			continue
		}
		filePath := path.Join(vtorc.LogDir, vtorc.LogFileName)
		log.Errorf("Printing file - %s", filePath)
		content, err := os.ReadFile(filePath)
		if err != nil {
			log.Errorf("Error while reading the file - %v", err)
		}
		log.Errorf("%s", string(content))
	}
}

// EnableGlobalRecoveries enables global recoveries for the given VTOrc.
func EnableGlobalRecoveries(t *testing.T, vtorc *cluster.VTOrcProcess) {
	status, resp, err := MakeAPICall(t, vtorc, "/api/enable-global-recoveries")
	require.NoError(t, err)
	assert.Equal(t, 200, status)
	assert.Equal(t, "Global recoveries enabled\n", resp)
}

// DisableGlobalRecoveries disables global recoveries for the given VTOrc.
func DisableGlobalRecoveries(t *testing.T, vtorc *cluster.VTOrcProcess) {
	status, resp, err := MakeAPICall(t, vtorc, "/api/disable-global-recoveries")
	require.NoError(t, err)
	assert.Equal(t, 200, status)
	assert.Equal(t, "Global recoveries disabled\n", resp)
}
