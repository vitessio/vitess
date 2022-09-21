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
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// This imports toposervers to register their implementations of TopoServer.
	_ "vitess.io/vitess/go/vt/topo/consultopo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/topo/k8stopo"
	_ "vitess.io/vitess/go/vt/topo/zk2topo"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
	ClusterInstance     *cluster.LocalProcessCluster
	Ts                  *topo.Server
	CellInfos           []*CellInfo
	VtctldClientProcess *cluster.VtctldClientProcess
	lastUsedValue       int
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

	// store the vtctldclient process
	vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", clusterInstance.VtctldProcess.GrpcPort, clusterInstance.TmpDirectory)

	// create topo server connection
	ts, err := topo.OpenServer(*clusterInstance.TopoFlavorString(), clusterInstance.VtctlProcess.TopoGlobalAddress, clusterInstance.VtctlProcess.TopoGlobalRoot)
	return &VTOrcClusterInfo{
		ClusterInstance:     clusterInstance,
		Ts:                  ts,
		CellInfos:           cellInfos,
		lastUsedValue:       100,
		VtctldClientProcess: vtctldClientProcess,
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

	time.Sleep(10 * time.Second)

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
	// demote the primary tablet if there is
	err := demotePrimaryTablet(clusterInfo.Ts)
	if err != nil {
		return err
	}

	for _, vttablet := range clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets {
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
		err = clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", vttablet.Alias)
		if err != nil {
			return err
		}
	}
	clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets = nil
	return nil
}

// demotePrimaryTablet demotes the primary tablet for our shard
func demotePrimaryTablet(ts *topo.Server) (err error) {
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
	out, err := clusterInfo.VtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, fmt.Sprintf("--durability-policy=%s", durability))
	require.NoError(t, err, out)

	// start vtorc
	StartVTOrcs(t, clusterInfo, orcExtraArgs, config, vtorcCount)
}

// cleanAndStartVttablet cleans the MySQL instance underneath for running a new test. It also starts the vttablet.
func cleanAndStartVttablet(t *testing.T, clusterInfo *VTOrcClusterInfo, vttablet *cluster.Vttablet) {
	t.Helper()
	// set super-read-only to false
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
		result, err := clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", fmt.Sprintf("%s/%s", keyspace.Name, shard.Name))
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
		result, err := clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
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
			confirmReplication(t, primary, replicas, time.Until(endTime), clusterInfo.lastUsedValue)
			clusterInfo.lastUsedValue++
			validateTopology(t, clusterInfo, true, time.Until(endTime))
			return
		}
	}
}

// VerifyWritesSucceed inserts more data into the table vt_insert_test and checks that it is replicated too
// Call this function only after CheckReplication has been executed once, since that function creates the table that this function uses.
func VerifyWritesSucceed(t *testing.T, clusterInfo *VTOrcClusterInfo, primary *cluster.Vttablet, replicas []*cluster.Vttablet, timeToWait time.Duration) {
	t.Helper()
	confirmReplication(t, primary, replicas, timeToWait, clusterInfo.lastUsedValue)
	clusterInfo.lastUsedValue++
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
					output, err = clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("Validate", "--", "--ping-tablets=true")
				} else {
					output, err = clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("Validate")
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
				cellInfo.ReplicaTablets = append(cellInfo.ReplicaTablets[:i], cellInfo.ReplicaTablets[i+1:]...)
				KillTablets([]*cluster.Vttablet{tablet})
				return
			}
		}
	}
}

// ChangePrivileges is used to change the privileges of the given user. These commands are executed such that they are not replicated
func ChangePrivileges(t *testing.T, sql string, tablet *cluster.Vttablet, user string) {
	_, err := RunSQL(t, "SET sql_log_bin = OFF;"+sql+";SET sql_log_bin = ON;", tablet, "")
	require.NoError(t, err)

	res, err := RunSQL(t, fmt.Sprintf("SELECT id FROM INFORMATION_SCHEMA.PROCESSLIST WHERE user = '%s'", user), tablet, "")
	require.NoError(t, err)
	for _, row := range res.Rows {
		id, err := row[0].ToInt64()
		require.NoError(t, err)
		_, err = RunSQL(t, fmt.Sprintf("kill %d", id), tablet, "")
		require.NoError(t, err)
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
func MakeAPICall(t *testing.T, url string) (status int, response string) {
	t.Helper()
	res, err := http.Get(url)
	require.NoError(t, err)
	bodyBytes, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	body := string(bodyBytes)
	return res.StatusCode, body
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

	vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", clusterInstance.VtctldProcess.GrpcPort, clusterInstance.TmpDirectory)

	out, err := vtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=semi_sync")
	require.NoError(t, err, out)

	// create topo server connection
	ts, err := topo.OpenServer(*clusterInstance.TopoFlavorString(), clusterInstance.VtctlProcess.TopoGlobalAddress, clusterInstance.VtctlProcess.TopoGlobalRoot)
	require.NoError(t, err)
	clusterInfo := &VTOrcClusterInfo{
		ClusterInstance:     clusterInstance,
		Ts:                  ts,
		CellInfos:           nil,
		lastUsedValue:       100,
		VtctldClientProcess: vtctldClientProcess,
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
