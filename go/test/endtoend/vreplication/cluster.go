package vreplication

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	debug = false // set to true to always use local env vtdataroot for local debugging

	originalVtdataroot    string
	vtdataroot            string
	mainClusterConfig     *ClusterConfig
	externalClusterConfig *ClusterConfig
)

// ClusterConfig defines the parameters like ports, tmpDir, tablet types which uniquely define a vitess cluster
type ClusterConfig struct {
	hostname            string
	topoPort            int
	vtctldPort          int
	vtctldGrpcPort      int
	vtdataroot          string
	tmpDir              string
	vtgatePort          int
	vtgateGrpcPort      int
	vtgateMySQLPort     int
	tabletTypes         string
	tabletPortBase      int
	tabletGrpcPortBase  int
	tabletMysqlPortBase int
}

// VitessCluster represents all components within the test cluster
type VitessCluster struct {
	ClusterConfig *ClusterConfig
	Name          string
	Cells         map[string]*Cell
	Topo          *cluster.TopoProcess
	Vtctld        *cluster.VtctldProcess
	Vtctl         *cluster.VtctlProcess
	VtctlClient   *cluster.VtctlClientProcess
}

// Cell represents a Vitess cell within the test cluster
type Cell struct {
	Name      string
	Keyspaces map[string]*Keyspace
	Vtgates   []*cluster.VtgateProcess
}

// Keyspace represents a Vitess keyspace contained by a cell within the test cluster
type Keyspace struct {
	Name    string
	Shards  map[string]*Shard
	VSchema string
	Schema  string
}

// Shard represents a Vitess shard in a keyspace
type Shard struct {
	Name      string
	IsSharded bool
	Tablets   map[string]*Tablet
}

// Tablet represents a vttablet within a shard
type Tablet struct {
	Name     string
	Vttablet *cluster.VttabletProcess
	DbServer *cluster.MysqlctlProcess
}

func setTempVtDataRoot() string {
	dirSuffix := 100000 + rand.Intn(999999-100000) // 6 digits
	if debug {
		vtdataroot = originalVtdataroot
	} else {
		vtdataroot = path.Join(originalVtdataroot, fmt.Sprintf("vreple2e_%d", dirSuffix))
	}
	if _, err := os.Stat(vtdataroot); os.IsNotExist(err) {
		os.Mkdir(vtdataroot, 0700)
	}
	_ = os.Setenv("VTDATAROOT", vtdataroot)
	fmt.Printf("VTDATAROOT is %s\n", vtdataroot)
	return vtdataroot
}

func getClusterConfig(idx int, dataRootDir string) *ClusterConfig {
	basePort := 15000
	etcdPort := 2379

	basePort += idx * 10000
	etcdPort += idx * 10000
	if _, err := os.Stat(dataRootDir); os.IsNotExist(err) {
		os.Mkdir(dataRootDir, 0700)
	}

	return &ClusterConfig{
		hostname:            "localhost",
		topoPort:            etcdPort,
		vtctldPort:          basePort,
		vtctldGrpcPort:      basePort + 999,
		tmpDir:              dataRootDir + "/tmp",
		vtgatePort:          basePort + 1,
		vtgateGrpcPort:      basePort + 991,
		vtgateMySQLPort:     basePort + 306,
		tabletTypes:         "master",
		vtdataroot:          dataRootDir,
		tabletPortBase:      basePort + 1000,
		tabletGrpcPortBase:  basePort + 1991,
		tabletMysqlPortBase: basePort + 1306,
	}
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
	originalVtdataroot = os.Getenv("VTDATAROOT")
	var mainVtDataRoot string
	if debug {
		mainVtDataRoot = originalVtdataroot
	} else {
		mainVtDataRoot = setTempVtDataRoot()
	}
	mainClusterConfig = getClusterConfig(0, mainVtDataRoot)
	externalClusterConfig = getClusterConfig(1, mainVtDataRoot+"/ext")
}

// NewVitessCluster starts a basic cluster with vtgate, vtctld and the topo
func NewVitessCluster(t *testing.T, name string, cellNames []string, clusterConfig *ClusterConfig) *VitessCluster {
	vc := &VitessCluster{Name: name, Cells: make(map[string]*Cell), ClusterConfig: clusterConfig}
	require.NotNil(t, vc)
	topo := cluster.TopoProcessInstance(vc.ClusterConfig.topoPort, vc.ClusterConfig.topoPort+1, vc.ClusterConfig.hostname, "etcd2", "global")

	require.NotNil(t, topo)
	require.Nil(t, topo.Setup("etcd2", nil))
	topo.ManageTopoDir("mkdir", "/vitess/global")
	vc.Topo = topo
	for _, cellName := range cellNames {
		topo.ManageTopoDir("mkdir", "/vitess/"+cellName)
	}

	vtctld := cluster.VtctldProcessInstance(vc.ClusterConfig.vtctldPort, vc.ClusterConfig.vtctldGrpcPort,
		vc.ClusterConfig.topoPort, vc.ClusterConfig.hostname, vc.ClusterConfig.tmpDir)
	vc.Vtctld = vtctld
	require.NotNil(t, vc.Vtctld)
	// use first cell as `-cell`
	vc.Vtctld.Setup(cellNames[0])

	vc.Vtctl = cluster.VtctlProcessInstance(vc.ClusterConfig.topoPort, vc.ClusterConfig.hostname)
	require.NotNil(t, vc.Vtctl)
	for _, cellName := range cellNames {
		vc.Vtctl.AddCellInfo(cellName)
		cell, err := vc.AddCell(t, cellName)
		require.NoError(t, err)
		require.NotNil(t, cell)
	}

	vc.VtctlClient = cluster.VtctlClientProcessInstance(vc.ClusterConfig.hostname, vc.Vtctld.GrpcPort, vc.ClusterConfig.tmpDir)
	require.NotNil(t, vc.VtctlClient)

	return vc
}

// AddKeyspace creates a keyspace with specified shard keys and number of replica/read-only tablets
func (vc *VitessCluster) AddKeyspace(t *testing.T, cells []*Cell, ksName string, shards string, vschema string, schema string, numReplicas int, numRdonly int, tabletIDBase int) (*Keyspace, error) {
	keyspace := &Keyspace{
		Name:   ksName,
		Shards: make(map[string]*Shard),
	}

	if err := vc.Vtctl.CreateKeyspace(keyspace.Name); err != nil {
		t.Fatalf(err.Error())
	}
	cellsToWatch := ""
	for i, cell := range cells {
		if i > 0 {
			cellsToWatch = cellsToWatch + ","
		}
		cell.Keyspaces[ksName] = keyspace
		cellsToWatch = cellsToWatch + cell.Name
	}
	require.NoError(t, vc.AddShards(t, cells, keyspace, shards, numReplicas, numRdonly, tabletIDBase))

	if schema != "" {
		if err := vc.VtctlClient.ApplySchema(ksName, schema); err != nil {
			t.Fatalf(err.Error())
		}
	}
	keyspace.Schema = schema
	if vschema != "" {
		if err := vc.VtctlClient.ApplyVSchema(ksName, vschema); err != nil {
			t.Fatalf(err.Error())
		}
	}
	keyspace.VSchema = vschema
	for _, cell := range cells {
		if len(cell.Vtgates) == 0 {
			fmt.Println("Starting vtgate")
			vc.StartVtgate(t, cell, cellsToWatch)
		}
	}
	_ = vc.VtctlClient.ExecuteCommand("RebuildKeyspaceGraph", ksName)
	return keyspace, nil
}

// AddTablet creates new tablet with specified attributes
func (vc *VitessCluster) AddTablet(t *testing.T, cell *Cell, keyspace *Keyspace, shard *Shard, tabletType string, tabletID int) (*Tablet, *exec.Cmd, error) {
	tablet := &Tablet{}

	vttablet := cluster.VttabletProcessInstance(
		vc.ClusterConfig.tabletPortBase+tabletID,
		vc.ClusterConfig.tabletGrpcPortBase+tabletID,
		tabletID,
		cell.Name,
		shard.Name,
		keyspace.Name,
		vc.ClusterConfig.vtctldPort,
		tabletType,
		vc.Topo.Port,
		vc.ClusterConfig.hostname,
		vc.ClusterConfig.tmpDir,
		[]string{
			"-queryserver-config-schema-reload-time", "5",
			"-enable-lag-throttler",
			"-heartbeat_enable",
			"-heartbeat_interval", "250ms",
		}, //FIXME: for multi-cell initial schema doesn't seem to load without "-queryserver-config-schema-reload-time"
		false)

	require.NotNil(t, vttablet)
	vttablet.SupportsBackup = false

	tablet.DbServer = cluster.MysqlCtlProcessInstance(tabletID, vc.ClusterConfig.tabletMysqlPortBase+tabletID, vc.ClusterConfig.tmpDir)
	require.NotNil(t, tablet.DbServer)
	tablet.DbServer.InitMysql = true
	proc, err := tablet.DbServer.StartProcess()
	if err != nil {
		t.Fatal(err.Error())
	}
	require.NotNil(t, proc)
	tablet.Name = fmt.Sprintf("%s-%d", cell.Name, tabletID)
	vttablet.Name = tablet.Name
	tablet.Vttablet = vttablet
	shard.Tablets[tablet.Name] = tablet

	return tablet, proc, nil
}

// AddShards creates shards given list of comma-separated keys with specified tablets in each shard
func (vc *VitessCluster) AddShards(t *testing.T, cells []*Cell, keyspace *Keyspace, names string, numReplicas int, numRdonly int, tabletIDBase int) error {
	arrNames := strings.Split(names, ",")
	fmt.Printf("Addshards got %d shards with %+v\n", len(arrNames), arrNames)
	isSharded := len(arrNames) > 1
	masterTabletUID := 0
	for ind, shardName := range arrNames {
		tabletID := tabletIDBase + ind*100
		tabletIndex := 0
		shard := &Shard{Name: shardName, IsSharded: isSharded, Tablets: make(map[string]*Tablet, 1)}
		if _, ok := keyspace.Shards[shardName]; ok {
			fmt.Printf("Shard %s already exists, not adding\n", shardName)
		} else {
			fmt.Printf("Adding Shard %s\n", shardName)
			if err := vc.VtctlClient.ExecuteCommand("CreateShard", keyspace.Name+"/"+shardName); err != nil {
				t.Fatalf("CreateShard command failed with %+v\n", err)
			}
			keyspace.Shards[shardName] = shard
		}
		for i, cell := range cells {
			dbProcesses := make([]*exec.Cmd, 0)
			tablets := make([]*Tablet, 0)
			if i == 0 {
				// only add master tablet for first cell, so first time CreateShard is called
				fmt.Println("Adding Master tablet")
				master, proc, err := vc.AddTablet(t, cell, keyspace, shard, "replica", tabletID+tabletIndex)
				require.NoError(t, err)
				require.NotNil(t, master)
				tabletIndex++
				master.Vttablet.VreplicationTabletType = "MASTER"
				tablets = append(tablets, master)
				dbProcesses = append(dbProcesses, proc)
				masterTabletUID = master.Vttablet.TabletUID
			}

			for i := 0; i < numReplicas; i++ {
				fmt.Println("Adding Replica tablet")
				tablet, proc, err := vc.AddTablet(t, cell, keyspace, shard, "replica", tabletID+tabletIndex)
				require.NoError(t, err)
				require.NotNil(t, tablet)
				tabletIndex++
				tablets = append(tablets, tablet)
				dbProcesses = append(dbProcesses, proc)
			}
			for i := 0; i < numRdonly; i++ {
				fmt.Println("Adding RdOnly tablet")
				tablet, proc, err := vc.AddTablet(t, cell, keyspace, shard, "rdonly", tabletID+tabletIndex)
				require.NoError(t, err)
				require.NotNil(t, tablet)
				tabletIndex++
				tablets = append(tablets, tablet)
				dbProcesses = append(dbProcesses, proc)
			}

			for ind, proc := range dbProcesses {
				fmt.Printf("Waiting for mysql process for tablet %s\n", tablets[ind].Name)
				if err := proc.Wait(); err != nil {
					t.Fatalf("%v :: Unable to start mysql server for %v", err, tablets[ind].Vttablet)
				}
			}
			for ind, tablet := range tablets {
				fmt.Printf("Creating vt_keyspace database for tablet %s\n", tablets[ind].Name)
				if _, err := tablet.Vttablet.QueryTablet(fmt.Sprintf("create database vt_%s", keyspace.Name),
					keyspace.Name, false); err != nil {
					t.Fatalf("Unable to start create database vt_%s for tablet %v", keyspace.Name, tablet.Vttablet)
				}
				fmt.Printf("Running Setup() for vttablet %s\n", tablets[ind].Name)
				if err := tablet.Vttablet.Setup(); err != nil {
					t.Fatalf(err.Error())
				}
			}
		}
		require.NotEqual(t, 0, masterTabletUID, "Should have created a master tablet")
		fmt.Printf("InitShardMaster for %d\n", masterTabletUID)
		require.NoError(t, vc.VtctlClient.InitShardMaster(keyspace.Name, shardName, cells[0].Name, masterTabletUID))
		fmt.Printf("Finished creating shard %s\n", shard.Name)
	}
	return nil
}

// DeleteShard deletes a shard
func (vc *VitessCluster) DeleteShard(t *testing.T, cellName string, ksName string, shardName string) {
	shard := vc.Cells[cellName].Keyspaces[ksName].Shards[shardName]
	require.NotNil(t, shard)
	for _, tab := range shard.Tablets {
		fmt.Printf("Shutting down tablet %s\n", tab.Name)
		tab.Vttablet.TearDown()
	}
	fmt.Printf("Deleting Shard %s\n", shardName)
	//TODO how can we avoid the use of even_if_serving?
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("DeleteShard", "-recursive", "-even_if_serving", ksName+"/"+shardName); err != nil {
		t.Fatalf("DeleteShard command failed with error %+v and output %s\n", err, output)
	}

}

// StartVtgate starts a vtgate process
func (vc *VitessCluster) StartVtgate(t *testing.T, cell *Cell, cellsToWatch string) {
	vtgate := cluster.VtgateProcessInstance(
		vc.ClusterConfig.vtgatePort,
		vc.ClusterConfig.vtgateGrpcPort,
		vc.ClusterConfig.vtgateMySQLPort,
		cell.Name,
		cellsToWatch,
		vc.ClusterConfig.hostname,
		vc.ClusterConfig.tabletTypes,
		vc.ClusterConfig.topoPort,
		vc.ClusterConfig.tmpDir,
		[]string{"-tablet_refresh_interval", "10ms"})
	require.NotNil(t, vtgate)
	if err := vtgate.Setup(); err != nil {
		t.Fatalf(err.Error())
	}
	cell.Vtgates = append(cell.Vtgates, vtgate)
}

// AddCell adds a new cell to the cluster
func (vc *VitessCluster) AddCell(t *testing.T, name string) (*Cell, error) {
	cell := &Cell{Name: name, Keyspaces: make(map[string]*Keyspace), Vtgates: make([]*cluster.VtgateProcess, 0)}
	vc.Cells[name] = cell
	return cell, nil
}

// TearDown brings down a cluster, deleting processes, removing topo keys
func (vc *VitessCluster) TearDown() {
	for _, cell := range vc.Cells {
		for _, vtgate := range cell.Vtgates {
			if err := vtgate.TearDown(); err != nil {
				log.Errorf("Error in vtgate teardown - %s", err.Error())
			}
		}
	}
	for _, cell := range vc.Cells {
		for _, keyspace := range cell.Keyspaces {
			for _, shard := range keyspace.Shards {
				for _, tablet := range shard.Tablets {
					if tablet.DbServer != nil && tablet.DbServer.TabletUID > 0 {
						if _, err := tablet.DbServer.StopProcess(); err != nil {
							log.Errorf("Error stopping mysql process: %s", err.Error())
						}
					}
					fmt.Printf("Stopping vttablet %s\n", tablet.Name)
					if err := tablet.Vttablet.TearDown(); err != nil {
						fmt.Printf("Stopped vttablet %s %s\n", tablet.Name, err.Error())
					}
				}
			}
		}
	}

	if err := vc.Vtctld.TearDown(); err != nil {
		fmt.Printf("Error stopping Vtctld:  %s\n", err.Error())
	}

	for _, cell := range vc.Cells {
		if err := vc.Topo.TearDown(cell.Name, originalVtdataroot, vtdataroot, false, "etcd2"); err != nil {
			fmt.Printf("Error in etcd teardown - %s\n", err.Error())
		}
	}
}

// WaitForVReplicationToCatchup waits for "workflow" to finish copying
func (vc *VitessCluster) WaitForVReplicationToCatchup(vttablet *cluster.VttabletProcess, workflow string, database string, duration time.Duration) error {
	queries := [3]string{
		fmt.Sprintf(`select count(*) from _vt.vreplication where workflow = "%s" and db_name = "%s" and pos = ''`, workflow, database),
		"select count(*) from information_schema.tables where table_schema='_vt' and table_name='copy_state' limit 1;",
		fmt.Sprintf(`select count(*) from _vt.copy_state where vrepl_id in (select id from _vt.vreplication where workflow = "%s" and db_name = "%s" )`, workflow, database),
	}
	results := [3]string{"[INT64(0)]", "[INT64(1)]", "[INT64(0)]"}
	var lastChecked time.Time
	for ind, query := range queries {
		waitDuration := 500 * time.Millisecond
		for duration > 0 {
			fmt.Printf("Executing query %s on %s\n", query, vttablet.Name)
			lastChecked = time.Now()
			qr, err := vc.execTabletQuery(vttablet, query)
			if err != nil {
				return err
			}
			if qr != nil && qr.Rows != nil && len(qr.Rows) > 0 && fmt.Sprintf("%v", qr.Rows[0]) == string(results[ind]) {
				break
			} else {
				fmt.Printf("In WaitForVReplicationToCatchup: %s %+v\n", query, qr.Rows)
			}
			time.Sleep(waitDuration)
			duration -= waitDuration
		}
		if duration <= 0 {
			fmt.Printf("WaitForVReplicationToCatchup timed out for workflow %s, keyspace %s\n", workflow, database)
			return errors.New("WaitForVReplicationToCatchup timed out")
		}
	}
	fmt.Printf("WaitForVReplicationToCatchup succeeded at %v\n", lastChecked)
	return nil
}

func (vc *VitessCluster) execTabletQuery(vttablet *cluster.VttabletProcess, query string) (*sqltypes.Result, error) {
	vtParams := mysql.ConnParams{
		UnixSocket: fmt.Sprintf("%s/mysql.sock", vttablet.Directory),
		Uname:      "vt_dba",
	}
	ctx := context.Background()
	var conn *mysql.Conn
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		return nil, err
	}
	qr, err := conn.ExecuteFetch(query, 1000, true)
	return qr, err
}

func (vc *VitessCluster) getVttabletsInKeyspace(t *testing.T, cell *Cell, ksName string, tabletType string) map[string]*cluster.VttabletProcess {
	keyspace := cell.Keyspaces[ksName]
	tablets := make(map[string]*cluster.VttabletProcess)
	for _, shard := range keyspace.Shards {
		for _, tablet := range shard.Tablets {
			if tablet.Vttablet.GetTabletStatus() == "SERVING" && strings.EqualFold(tablet.Vttablet.VreplicationTabletType, tabletType) {
				fmt.Printf("Serving status of tablet %s is %s, %s\n", tablet.Name, tablet.Vttablet.ServingStatus, tablet.Vttablet.GetTabletStatus())
				tablets[tablet.Name] = tablet.Vttablet
			}
		}
	}
	return tablets
}
