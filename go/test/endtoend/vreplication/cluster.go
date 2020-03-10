package vreplication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	_ "strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	vtdataroot string
)

var globalConfig = struct {
	hostname        string
	topoPort        int
	vtctldPort      int
	vtctldGrpcPort  int
	tmpDir          string
	vtgatePort      int
	vtgateGrpcPort  int
	vtgateMySQLPort int
	tabletTypes     string
}{"localhost", 2379, 15000, 15999, vtdataroot + "/tmp",
	15001, 15991, 15306, "MASTER,REPLICA"}

var (
	tabletPortBase      = 15000
	tabletGrpcPortBase  = 20000
	tabletMysqlPortBase = 25000
)

// VitessCluster represents all components within the test cluster
type VitessCluster struct {
	Name        string
	Cells       map[string]*Cell
	Topo        *cluster.TopoProcess
	Vtctld      *cluster.VtctldProcess
	Vtctl       *cluster.VtctlProcess
	VtctlClient *cluster.VtctlClientProcess
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

func initGlobals() {
	vtdataroot = os.Getenv("VTDATAROOT")
	globalConfig.tmpDir = vtdataroot + "/tmp"
}

// NewVitessCluster creates an entire VitessCluster for e2e testing
func NewVitessCluster(name string) (cluster *VitessCluster, err error) {
	return &VitessCluster{Name: name, Cells: make(map[string]*Cell)}, nil
}

// InitCluster creates the global processes needed for a cluster
func InitCluster(t *testing.T, cellName string) *VitessCluster {
	initGlobals()
	vc, _ := NewVitessCluster("Vdemo")
	assert.NotNil(t, vc)
	topo := cluster.TopoProcessInstance(globalConfig.topoPort, globalConfig.topoPort*10, globalConfig.hostname, "etcd2", "global")

	assert.NotNil(t, topo)
	assert.Nil(t, topo.Setup("etcd2", nil))
	topo.ManageTopoDir("mkdir", "/vitess/global")
	vc.Topo = topo
	topo.ManageTopoDir("mkdir", "/vitess/"+cellName)

	vtctld := cluster.VtctldProcessInstance(globalConfig.vtctldPort, globalConfig.vtctldGrpcPort,
		globalConfig.topoPort, globalConfig.hostname, globalConfig.tmpDir)
	vc.Vtctld = vtctld
	assert.NotNil(t, vc.Vtctld)
	vc.Vtctld.Setup(cellName)

	vc.Vtctl = cluster.VtctlProcessInstance(globalConfig.topoPort, globalConfig.hostname)
	assert.NotNil(t, vc.Vtctl)
	vc.Vtctl.AddCellInfo(cellName)
	cell, err := vc.AddCell(t, cellName)
	assert.Nil(t, err)
	assert.NotNil(t, cell)

	vc.VtctlClient = cluster.VtctlClientProcessInstance(globalConfig.hostname, vc.Vtctld.GrpcPort, globalConfig.tmpDir)
	assert.NotNil(t, vc.VtctlClient)

	return vc
}

// AddKeyspace creates a keyspace with specified shard keys and number of replica/read-only tablets
func (vc *VitessCluster) AddKeyspace(t *testing.T, cell *Cell, ksName string, shards string, vschema string, schema string, numReplicas int, numRdonly int, tabletIDBase int) (*Keyspace, error) {
	keyspace := &Keyspace{
		Name:   ksName,
		Shards: make(map[string]*Shard),
	}

	if err := vc.Vtctl.CreateKeyspace(keyspace.Name); err != nil {
		t.Fatalf(err.Error())
	}
	cell.Keyspaces[ksName] = keyspace
	if err := vc.AddShards(t, cell, keyspace, shards, numReplicas, numRdonly, tabletIDBase); err != nil {
		t.Fatalf(err.Error())
	}
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
	if len(cell.Vtgates) == 0 {
		fmt.Println("Starting vtgate")
		vc.StartVtgate(t, cell)
	}
	_ = vc.VtctlClient.ExecuteCommand("RebuildKeyspaceGraph", ksName)
	return keyspace, nil
}

// AddTablet creates new tablet with specified attributes
func (vc *VitessCluster) AddTablet(t *testing.T, cell *Cell, keyspace *Keyspace, shard *Shard, tabletType string, tabletID int) (*Tablet, *exec.Cmd, error) {
	tablet := &Tablet{}

	vttablet := cluster.VttabletProcessInstance(
		tabletPortBase+tabletID,
		tabletGrpcPortBase+tabletID,
		tabletID,
		cell.Name,
		shard.Name,
		keyspace.Name,
		globalConfig.vtctldPort,
		tabletType,
		vc.Topo.Port,
		globalConfig.hostname,
		globalConfig.tmpDir,
		nil,
		false)
	assert.NotNil(t, vttablet)
	//vttablet.ServingStatus = "SERVING"
	vttablet.SupportsBackup = false

	tablet.DbServer = cluster.MysqlCtlProcessInstance(tabletID, tabletMysqlPortBase+tabletID, globalConfig.tmpDir)
	assert.NotNil(t, tablet.DbServer)
	tablet.DbServer.InitMysql = true
	proc, err := tablet.DbServer.StartProcess()
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.NotNil(t, proc)
	tablet.Name = fmt.Sprintf("%s-%d", cell.Name, tabletID)
	vttablet.Name = tablet.Name
	tablet.Vttablet = vttablet
	shard.Tablets[tablet.Name] = tablet

	return tablet, proc, nil
}

// AddShards creates shards given list of comma-separated keys with specified tablets in each shard
func (vc *VitessCluster) AddShards(t *testing.T, cell *Cell, keyspace *Keyspace, names string, numReplicas int, numRdonly int, tabletIDBase int) error {
	arrNames := strings.Split(names, ",")
	fmt.Printf("Addshards got %d shards with %+v\n", len(arrNames), arrNames)
	isSharded := len(arrNames) > 1
	for ind, shardName := range arrNames {
		if _, ok := keyspace.Shards[shardName]; ok {
			fmt.Printf("Shard %s already exists, not adding\n", shardName)
			continue
		}
		tabletID := tabletIDBase + ind*100
		tabletIndex := 0
		dbProcesses := make([]*exec.Cmd, 0)
		tablets := make([]*Tablet, 0)

		fmt.Printf("Adding Shard %s\n", shardName)
		if err := vc.VtctlClient.ExecuteCommand("CreateShard", keyspace.Name+"/"+shardName); err != nil {
			t.Fatalf("CreateShard command failed with %+v\n", err)
		}

		shard := &Shard{Name: shardName, IsSharded: isSharded, Tablets: make(map[string]*Tablet, 1)}
		fmt.Println("Adding Master tablet")
		master, proc, err := vc.AddTablet(t, cell, keyspace, shard, "replica", tabletID+tabletIndex)
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, master)
		tabletIndex++
		master.Vttablet.VreplicationTabletType = "MASTER"
		tablets = append(tablets, master)
		dbProcesses = append(dbProcesses, proc)
		for i := 0; i < numReplicas; i++ {
			fmt.Println("Adding Replica tablet")
			tablet, proc, err := vc.AddTablet(t, cell, keyspace, shard, "replica", tabletID+tabletIndex)
			if err != nil {
				t.Fatalf(err.Error())
			}
			assert.NotNil(t, tablet)
			tabletIndex++
			tablets = append(tablets, tablet)
			dbProcesses = append(dbProcesses, proc)
		}
		for i := 0; i < numRdonly; i++ {
			fmt.Println("Adding RdOnly tablet")
			tablet, proc, err := vc.AddTablet(t, cell, keyspace, shard, "rdonly", tabletID+tabletIndex)
			if err != nil {
				t.Fatalf(err.Error())
			}
			assert.NotNil(t, tablet)
			tabletIndex++
			tablets = append(tablets, tablet)
			dbProcesses = append(dbProcesses, proc)
		}

		keyspace.Shards[shardName] = shard
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
		fmt.Printf("InitShardMaster for %d\n", master.Vttablet.TabletUID)
		err = vc.VtctlClient.InitShardMaster(keyspace.Name, shardName, cell.Name, master.Vttablet.TabletUID)
		if err != nil {
			t.Fatal(err.Error())
		}
		fmt.Printf("Finished creating shard %s\n", shard.Name)
	}
	return nil
}

func (vc *VitessCluster) DeleteShard(t *testing.T, cellName string, ksName string, shardName string) {
	shard := vc.Cells[cellName].Keyspaces[ksName].Shards[shardName]
	assert.NotNil(t, shard)
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
func (vc *VitessCluster) StartVtgate(t *testing.T, cell *Cell) {
	vtgate := cluster.VtgateProcessInstance(
		globalConfig.vtgatePort,
		globalConfig.vtgateGrpcPort,
		globalConfig.vtgateMySQLPort,
		cell.Name,
		cell.Name,
		globalConfig.hostname,
		globalConfig.tabletTypes,
		globalConfig.topoPort,
		globalConfig.tmpDir,
		[]string{"-tablet_refresh_interval", "10ms"})
	assert.NotNil(t, vtgate)
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
	var dbProcesses []*exec.Cmd
	for _, cell := range vc.Cells {
		for _, keyspace := range cell.Keyspaces {
			for _, shard := range keyspace.Shards {
				for _, tablet := range shard.Tablets {
					if tablet.DbServer != nil && tablet.DbServer.TabletUID > 0 {
						if proc, err := tablet.DbServer.StopProcess(); err != nil {
							log.Errorf("Error stopping mysql process: %s", err.Error())
						} else {
							dbProcesses = append(dbProcesses, proc)
						}
					}
					if err := tablet.Vttablet.TearDown(); err != nil {
						log.Errorf("Error stopping vttablet %s", err.Error())
					}
				}
			}
		}
	}

	for _, proc := range dbProcesses {
		if err := proc.Wait(); err != nil {
			log.Errorf("Error waiting for mysql to stop: %s", err.Error())
		}
	}

	if err := vc.Vtctld.TearDown(); err != nil {
		log.Errorf("Error stopping Vtctld:  %s", err.Error())
	}

	for _, cell := range vc.Cells {
		if err := vc.Topo.TearDown(cell.Name, vtdataroot, vtdataroot, false, "etcd2"); err != nil {
			log.Errorf("Error in etcd teardown - %s", err.Error())
		}
	}
}

// WaitForVReplicationToCatchup waits for "workflow" to finish copying
func (vc *VitessCluster) WaitForVReplicationToCatchup(vttablet *cluster.VttabletProcess, workflow string, database string, duration time.Duration) error {
	queries := [3]string{
		fmt.Sprintf(`select 1 from _vt.vreplication where workflow = "%s" and db_name = "%s" and pos != '' limit 1`, workflow, database),
		"select count(*) from information_schema.tables where table_schema='_vt' and table_name='copy_state' limit 1;",
		fmt.Sprintf(`select count(*) from _vt.copy_state where vrepl_id in (select id from _vt.vreplication where workflow = "%s" and db_name = "%s" )`, workflow, database),
	}
	results := [3]string{"[INT64(1)]", "[INT64(1)]", "[INT64(0)]"}
	for ind, query := range queries {
		waitDuration := 100 * time.Millisecond
		for duration > 0 {
			//fmt.Printf("Executing query %s on %s\n", query, vttablet.Name)
			qr, err := vc.execTabletQuery(vttablet, query)
			if err != nil {
				return err
			}
			if qr != nil && qr.Rows != nil && len(qr.Rows) > 0 && fmt.Sprintf("%v", qr.Rows[0]) == string(results[ind]) {
				break
			} else {
				fmt.Printf("In WaitForVReplicationToCatchup: %s\n", query)
			}
			time.Sleep(waitDuration)
			duration -= waitDuration
		}
		if duration <= 0 {
			fmt.Printf("WaitForVReplicationToCatchup timed out for workflow %s, keyspace %s\n", workflow, database)
			return errors.New("WaitForVReplicationToCatchup timed out")
		}
	}
	return nil
}

func (vc *VitessCluster) execTabletQuery(vttablet *cluster.VttabletProcess, query string) (*sqltypes.Result, error) {
	vtParams := mysql.ConnParams{
		UnixSocket: fmt.Sprintf("%s/mysql.sock", vttablet.Directory),
		Uname:      "vt_dba",
	}
	ctx := context.Background()
	if conn, err := mysql.Connect(ctx, &vtParams); err != nil {
		return nil, err
	} else {
		qr, err := conn.ExecuteFetch(query, 1000, true)
		return qr, err
	}
}

func (vc *VitessCluster) getVttabletsInKeyspace(t *testing.T, cell *Cell, ksName string, tabletType string) map[string]*cluster.VttabletProcess {
	keyspace := cell.Keyspaces[ksName]
	tablets := make(map[string]*cluster.VttabletProcess)
	for _, shard := range keyspace.Shards {
		for _, tablet := range shard.Tablets {
			if tablet.Vttablet.GetTabletStatus() == "SERVING" && strings.ToLower(tablet.Vttablet.VreplicationTabletType) == strings.ToLower(tabletType) {
				fmt.Printf("Serving status of tablet %s is %s, %s\n", tablet.Name, tablet.Vttablet.ServingStatus, tablet.Vttablet.GetTabletStatus())
				tablets[tablet.Name] = tablet.Vttablet
			}
		}
	}
	return tablets
}
