package vreplication

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"strings"
	_ "strings"
	"testing"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	vtdataroot string

)

var globalConfig = struct {
	hostname string
	topoPort int
	vtctldPort int
	vtctldGrpcPort int
	tmpDir string
	vtgatePort int
	vtgateGrpcPort int
	vtgateMySQLPort int
	tabletTypes string //TODO
} {"localhost", 2379, 15000, 15999, vtdataroot+"/tmp",
	15001, 15991, 15306, "MASTER,REPLICA"}


//TODO ports should be automatically (incrementally) allocated based on what is available?
var (
	tabletIndex = 1
	tabletSubIndex = 0
	tabletPortBase = 15000
	tabletGrpcPortBase = 16000
	tabletMysqlPortBase = 17000
)

type VitessCluster struct {
	Name string
	Cells map[string]*Cell
	Topo *cluster.EtcdProcess
	Vtctld *cluster.VtctldProcess
	Vtctl *cluster.VtctlProcess
	VtctlClient *cluster.VtctlClientProcess
}

type Cell struct {
	Name string
	Keyspaces map[string]*Keyspace
	Vtgates []*cluster.VtgateProcess
}

type Keyspace struct {
	Name string
	Shards map[string]*Shard
	VSchema string
	Schema string
}

type Shard struct {
	Name string
	IsSharded bool
	Tablets map[string]*Tablet
}

type Tablet struct {
	Name string
	Vttablet *cluster.VttabletProcess
	DbServer *cluster.MysqlctlProcess
}


func initGlobals() {
	vtdataroot = os.Getenv("VTDATAROOT")
	globalConfig.tmpDir = vtdataroot + "/tmp"
	//TODO init some vars (like ports, indexes) here rather than hardcoding
}

func NewVitessCluster (name string) (cluster *VitessCluster, err error) {
	return &VitessCluster{Name:name, Cells:make(map[string]*Cell)}, nil
}

func InitCluster(t *testing.T, cellName string) *VitessCluster {
	initGlobals()
	vc, _ := NewVitessCluster("Vdemo")
	assert.NotNil(t, vc)
	topo := cluster.EtcdProcessInstance(globalConfig.topoPort, globalConfig.topoPort*10, globalConfig.hostname, "global")
	assert.NotNil(t, topo)
	assert.Nil(t, topo.Setup())
	topo.ManageTopoDir("mkdir", "/vitess/global")
	vc.Topo = topo
	topo.ManageTopoDir("mkdir", "/vitess/"+cellName)

	vtctld := cluster.VtctldProcessInstance(globalConfig.vtctldPort, globalConfig.vtctldGrpcPort,
		globalConfig.topoPort, globalConfig.hostname, globalConfig.tmpDir)
	vc.Vtctld =vtctld
	assert.NotNil(t, vc.Vtctld)
	vc.Vtctld.Setup(cellName)

	vc.Vtctl = cluster.VtctlProcessInstance(globalConfig.topoPort, globalConfig.hostname)
	assert.NotNil(t, vc.Vtctl)
	vc.Vtctl.AddCellInfo(cellName)
	cell, err := vc.AddCell(t, cellName)
	assert.Nil(t, err); assert.NotNil(t, cell)

	vc.VtctlClient = cluster.VtctlClientProcessInstance(globalConfig.hostname, vc.Vtctld.GrpcPort, globalConfig.tmpDir)
	assert.NotNil(t, vc.VtctlClient)

	return vc
}

func (vc *VitessCluster) AddKeyspace(t *testing.T, cell *Cell, ksName string, shards string, vschema string, schema string, numReplicas int, numRdonly int) (*Keyspace, error) {
	keyspace := &Keyspace{
		Name:    ksName,
		Shards:  make(map[string]*Shard),
	}

	if err := vc.Vtctl.CreateKeyspace(keyspace.Name); err != nil {
		t.Fatalf(err.Error())
	}
	cell.Keyspaces[ksName] = keyspace
	if err := vc.AddShards(t, cell, keyspace, shards, numReplicas, numRdonly); err != nil {
		t.Fatalf(err.Error())
	}
	if err := vc.VtctlClient.ApplySchema(ksName, schema); err != nil {
		t.Fatalf(err.Error())
	}
	keyspace.Schema = schema
	if err := vc.VtctlClient.ApplyVSchema(ksName, vschema); err != nil {
		t.Fatalf(err.Error())
	}
	keyspace.VSchema = vschema
	fmt.Println("Starting vtgate")
	if len(cell.Vtgates) == 0 {
		vc.StartVtgate(t, cell)
		//cell.Vtgates[0].WaitForStatusOfTabletInShard() //TODO
	}
	return keyspace, nil
}

func (vc *VitessCluster) AddTablet(t *testing.T, cell *Cell, keyspace *Keyspace, shard *Shard, tabletType string) (*Tablet, *exec.Cmd, error) {
	tablet := &Tablet{}
	var tabletId int
	tabletId = tabletIndex*100 + tabletSubIndex
	tabletSubIndex += 1

	vttablet := cluster.VttabletProcessInstance(
		tabletPortBase+tabletId,
		tabletGrpcPortBase+tabletId,
		tabletId,
		cell.Name,
		shard.Name,
		keyspace.Name,
		globalConfig.vtctldPort,
		tabletType,
		vc.Topo.Port,
		globalConfig.hostname,
		globalConfig.tmpDir,
		nil,
		true)
	assert.NotNil(t, vttablet)
	//vttablet.ServingStatus = "SERVING"
	vttablet.SupportsBackup = false

	tablet.DbServer = cluster.MysqlCtlProcessInstance(tabletId, tabletMysqlPortBase+tabletId, globalConfig.tmpDir)
	assert.NotNil(t, tablet.DbServer)
	tablet.DbServer.InitMysql = true
	proc, err := tablet.DbServer.StartProcess()
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.NotNil(t, proc)
	tablet.Name = fmt.Sprintf("%s-%d", cell.Name, tabletId )
	tablet.Vttablet = vttablet
	shard.Tablets[tablet.Name] = tablet

	return tablet, proc, nil
}

func (vc *VitessCluster) AddShards(t *testing.T, cell *Cell, keyspace *Keyspace, names string, numReplicas int, numRdonly int) error {
	arrNames := strings.Split(names, ",")
	isSharded := len(arrNames) > 1
	dbProcesses := make([]*exec.Cmd, 0)
	tablets := make([]*Tablet, 0)
	for _, name := range arrNames {
		fmt.Printf("Adding Shard %s\n", name)

		shard := &Shard{Name: name, IsSharded: isSharded, Tablets: make(map[string]*Tablet, 1)}
		fmt.Println("Adding Master tablet")
		master, proc, err := vc.AddTablet(t, cell, keyspace, shard, "replica")
		master.Vttablet.VreplicationTabletType = "MASTER"
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, master)
		tablets = append(tablets, master)
		dbProcesses = append(dbProcesses, proc)
		if err != nil {
			t.Fatalf(err.Error())
		}
		for i := 0; i < numReplicas; i++ {
			fmt.Println("Adding Replica tablet")
			tablet, proc, err := vc.AddTablet(t, cell,  keyspace, shard, "replica")
			if err != nil {
				t.Fatalf(err.Error())
			}
			assert.NotNil(t, tablet)
			tablets = append(tablets, tablet)
			dbProcesses = append(dbProcesses, proc)
		}
		for i := 0; i < numRdonly; i++ {
			fmt.Println("Adding RdOnly tablet")
			tablet, proc, err := vc.AddTablet(t, cell,  keyspace, shard, "rdonly")
			if err != nil {
				t.Fatalf(err.Error())
			}
			assert.NotNil(t, tablet)
			tablets = append(tablets, tablet)
			dbProcesses = append(dbProcesses, proc)
		}

		keyspace.Shards[name] = shard
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
		err = vc.VtctlClient.InitShardMaster(keyspace.Name, name, cell.Name, master.Vttablet.TabletUID)
		if err != nil {
			t.Fatal(err.Error())
		}
		fmt.Printf("Finished creating shard %s\n", shard.Name)
	}
	return nil
}

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
		nil)
	assert.NotNil(t, vtgate)
	err := vtgate.Setup(); if err != nil {
		t.Fatalf(err.Error())
	}
	cell.Vtgates[0] = vtgate
}

func (vc *VitessCluster) AddCell(t *testing.T, name string) (*Cell, error) {
	cell := &Cell{Name: name, Keyspaces: make(map[string]*Keyspace), Vtgates:make([]*cluster.VtgateProcess,1)}
	vc.Cells[name] = cell
	return cell, nil
}

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
		if err := vc.Topo.TearDown(cell.Name, vtdataroot, vtdataroot, false); err != nil {
			log.Errorf("Error in etcd teardown - %s", err.Error())
		}
	}
}

func execQuery(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	assert.Nil(t, err)
	return qr
}