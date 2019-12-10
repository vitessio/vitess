/*
Copyright 2019 The Vitess Authors.

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

package cluster

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// DefaultCell : If no cell name is passed, then use following
const DefaultCell = "zone1"

var (
	keepData = flag.Bool("keep-data", false, "don't delete the per-test VTDATAROOT subfolders")
)

// LocalProcessCluster Testcases need to use this to iniate a cluster
type LocalProcessCluster struct {
	Keyspaces          []Keyspace
	Cell               string
	BaseTabletUID      int
	Hostname           string
	TopoPort           int
	TmpDirectory       string
	OriginalVTDATAROOT string
	CurrentVTDATAROOT  string

	VtgateMySQLPort int
	VtgateGrpcPort  int
	VtctldHTTPPort  int

	// standalone executable
	VtctlclientProcess VtctlClientProcess
	VtctlProcess       VtctlProcess

	// background executable processes
	TopoProcess     EtcdProcess
	VtctldProcess   VtctldProcess
	VtgateProcess   VtgateProcess
	VtworkerProcess VtworkerProcess

	nextPortForProcess int

	//Extra arguments for vtTablet
	VtTabletExtraArgs []string

	//Extra arguments for vtGate
	VtGateExtraArgs []string

	VtctldExtraArgs []string

	EnableSemiSync bool
}

// Keyspace : Cluster accepts keyspace to launch it
type Keyspace struct {
	Name      string
	SchemaSQL string
	VSchema   string
	Shards    []Shard
}

// Shard with associated vttablets
type Shard struct {
	Name      string
	Vttablets []Vttablet
}

// MasterTablet get the 1st tablet which is master
func (shard *Shard) MasterTablet() *Vttablet {
	return &shard.Vttablets[0]
}

// Rdonly get the last tablet which is rdonly
func (shard *Shard) Rdonly() *Vttablet {
	for idx, tablet := range shard.Vttablets {
		if tablet.Type == "rdonly" {
			return &shard.Vttablets[idx]
		}
	}
	return nil
}

// Replica get the last but one tablet which is replica
// Mostly we have either 3 tablet setup [master, replica, rdonly]
func (shard *Shard) Replica() *Vttablet {
	for idx, tablet := range shard.Vttablets {
		if tablet.Type == "replica" && idx > 0 {
			return &shard.Vttablets[idx]
		}
	}
	return nil
}

// Vttablet stores the properties needed to start a vttablet process
type Vttablet struct {
	Type      string
	TabletUID int
	HTTPPort  int
	GrpcPort  int
	MySQLPort int
	Alias     string

	// background executable processes
	MysqlctlProcess MysqlctlProcess
	VttabletProcess *VttabletProcess
}

// StartTopo starts topology server
func (cluster *LocalProcessCluster) StartTopo() (err error) {
	if cluster.Cell == "" {
		cluster.Cell = DefaultCell
	}
	cluster.TopoPort = cluster.GetAndReservePort()
	cluster.TmpDirectory = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/tmp_%d", cluster.GetAndReservePort()))
	cluster.TopoProcess = *EtcdProcessInstance(cluster.TopoPort, cluster.GetAndReservePort(), cluster.Hostname, "global")
	log.Info(fmt.Sprintf("Starting etcd server on port : %d", cluster.TopoPort))
	if err = cluster.TopoProcess.Setup(); err != nil {
		log.Error(err.Error())
		return
	}

	log.Info("Creating topo dirs")
	if err = cluster.TopoProcess.ManageTopoDir("mkdir", "/vitess/global"); err != nil {
		log.Error(err.Error())
		return
	}

	if err = cluster.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cluster.Cell); err != nil {
		log.Error(err.Error())
		return
	}

	log.Info("Adding cell info")
	cluster.VtctlProcess = *VtctlProcessInstance(cluster.TopoProcess.Port, cluster.Hostname)
	if err = cluster.VtctlProcess.AddCellInfo(cluster.Cell); err != nil {
		log.Error(err)
		return
	}

	cluster.VtctldProcess = *VtctldProcessInstance(cluster.GetAndReservePort(), cluster.GetAndReservePort(), cluster.TopoProcess.Port, cluster.Hostname, cluster.TmpDirectory)
	log.Info(fmt.Sprintf("Starting vtctld server on port : %d", cluster.VtctldProcess.Port))
	cluster.VtctldHTTPPort = cluster.VtctldProcess.Port
	if err = cluster.VtctldProcess.Setup(cluster.Cell, cluster.VtctldExtraArgs...); err != nil {

		log.Error(err.Error())
		return
	}

	cluster.VtctlclientProcess = *VtctlClientProcessInstance("localhost", cluster.VtctldProcess.GrpcPort, cluster.TmpDirectory)
	return
}

// StartUnshardedKeyspace starts unshared keyspace with shard name as "0"
func (cluster *LocalProcessCluster) StartUnshardedKeyspace(keyspace Keyspace, replicaCount int, rdonly bool) error {
	return cluster.StartKeyspace(keyspace, []string{"0"}, replicaCount, rdonly)
}

// StartKeyspace starts required number of shard and the corresponding tablets
// keyspace : struct containing keyspace name, Sqlschema to apply, VSchema to apply
// shardName : list of shard names
// replicaCount: total number of replicas excluding master and rdonly
// rdonly: whether readonly tablets needed
func (cluster *LocalProcessCluster) StartKeyspace(keyspace Keyspace, shardNames []string, replicaCount int, rdonly bool) (err error) {
	totalTabletsRequired := replicaCount + 1 // + 1 is for master
	if rdonly {
		totalTabletsRequired = totalTabletsRequired + 1 // + 1 for rdonly
	}

	log.Info("Starting keyspace : " + keyspace.Name)
	_ = cluster.VtctlProcess.CreateKeyspace(keyspace.Name)
	var mysqlctlProcessList []*exec.Cmd
	for _, shardName := range shardNames {
		shard := &Shard{
			Name: shardName,
		}
		log.Info("Starting shard : " + shardName)
		mysqlctlProcessList = []*exec.Cmd{}
		for i := 0; i < totalTabletsRequired; i++ {
			// instantiate vttablet object with reserved ports
			tabletUID := cluster.GetAndReserveTabletUID()
			tablet := &Vttablet{
				TabletUID: tabletUID,
				HTTPPort:  cluster.GetAndReservePort(),
				GrpcPort:  cluster.GetAndReservePort(),
				MySQLPort: cluster.GetAndReservePort(),
				Alias:     fmt.Sprintf("%s-%010d", cluster.Cell, tabletUID),
			}
			if i == 0 { // Make the first one as master
				tablet.Type = "master"
			} else if i == totalTabletsRequired-1 && rdonly { // Make the last one as rdonly if rdonly flag is passed
				tablet.Type = "rdonly"
			}
			// Start Mysqlctl process
			log.Info(fmt.Sprintf("Starting mysqlctl for table uid %d, mysql port %d", tablet.TabletUID, tablet.MySQLPort))
			tablet.MysqlctlProcess = *MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, cluster.TmpDirectory)
			if proc, err := tablet.MysqlctlProcess.StartProcess(); err != nil {
				log.Error(err.Error())
				return err
			} else {
				mysqlctlProcessList = append(mysqlctlProcessList, proc)
			}

			// start vttablet process
			tablet.VttabletProcess = VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				cluster.Cell,
				shardName,
				keyspace.Name,
				cluster.VtctldProcess.Port,
				tablet.Type,
				cluster.TopoProcess.Port,
				cluster.Hostname,
				cluster.TmpDirectory,
				cluster.VtTabletExtraArgs,
				cluster.EnableSemiSync)
			tablet.Alias = tablet.VttabletProcess.TabletPath
			shard.Vttablets = append(shard.Vttablets, *tablet)
		}

		// wait till all mysqlctl is instantiated
		for _, proc := range mysqlctlProcessList {
			if err = proc.Wait(); err != nil {
				log.Errorf("Unable to start mysql , error %v", err.Error())
				return err
			}
		}
		for _, tablet := range shard.Vttablets {
			if _, err = tablet.VttabletProcess.QueryTablet(fmt.Sprintf("create database vt_%s", keyspace.Name), keyspace.Name, false); err != nil {
				log.Error(err.Error())
				return
			}

			log.Info(fmt.Sprintf("Starting vttablet for tablet uid %d, grpc port %d", tablet.TabletUID, tablet.GrpcPort))

			if err = tablet.VttabletProcess.Setup(); err != nil {
				log.Error(err.Error())
				return
			}
		}

		// Make first tablet as master
		if err = cluster.VtctlclientProcess.InitShardMaster(keyspace.Name, shardName, cluster.Cell, shard.Vttablets[0].TabletUID); err != nil {
			log.Error(err.Error())
			return
		}
		keyspace.Shards = append(keyspace.Shards, *shard)
	}
	// if the keyspace is present then append the shard info
	existingKeyspace := false
	for idx, ks := range cluster.Keyspaces {
		if ks.Name == keyspace.Name {
			cluster.Keyspaces[idx].Shards = append(cluster.Keyspaces[idx].Shards, keyspace.Shards...)
			existingKeyspace = true
		}
	}
	if !existingKeyspace {
		cluster.Keyspaces = append(cluster.Keyspaces, keyspace)
	}

	// Apply Schema SQL
	if keyspace.SchemaSQL != "" {
		if err = cluster.VtctlclientProcess.ApplySchema(keyspace.Name, keyspace.SchemaSQL); err != nil {
			log.Error(err.Error())
			return
		}
	}

	//Apply VSchema
	if keyspace.VSchema != "" {
		if err = cluster.VtctlclientProcess.ApplyVSchema(keyspace.Name, keyspace.VSchema); err != nil {
			log.Error(err.Error())
			return
		}
	}

	log.Info("Done creating keyspace : " + keyspace.Name)
	return
}

// StartVtgate starts vtgate
func (cluster *LocalProcessCluster) StartVtgate() (err error) {
	vtgateInstance := *cluster.GetVtgateInstance()
	cluster.VtgateProcess = vtgateInstance
	log.Info(fmt.Sprintf("Starting vtgate on port %d", vtgateInstance.Port))
	log.Info(fmt.Sprintf("Vtgate started, connect to mysql using : mysql -h 127.0.0.1 -P %d", cluster.VtgateMySQLPort))
	return cluster.VtgateProcess.Setup()
}

// GetVtgateInstance returns an instance of vtgateprocess
func (cluster *LocalProcessCluster) GetVtgateInstance() *VtgateProcess {
	vtgateHTTPPort := cluster.GetAndReservePort()
	vtgateGrpcPort := cluster.GetAndReservePort()
	cluster.VtgateMySQLPort = cluster.GetAndReservePort()
	vtgateProcInstance := VtgateProcessInstance(
		vtgateHTTPPort,
		vtgateGrpcPort,
		cluster.VtgateMySQLPort,
		cluster.Cell,
		cluster.Cell,
		cluster.Hostname,
		"MASTER,REPLICA",
		cluster.TopoProcess.Port,
		cluster.TmpDirectory,
		cluster.VtGateExtraArgs)
	return vtgateProcInstance
}

// NewCluster instantiates a new cluster
func NewCluster(cell string, hostname string) *LocalProcessCluster {
	cluster := &LocalProcessCluster{Cell: cell, Hostname: hostname}
	cluster.OriginalVTDATAROOT = os.Getenv("VTDATAROOT")
	cluster.CurrentVTDATAROOT = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vtroot_%d", cluster.GetAndReservePort()))
	_ = createDirectory(cluster.CurrentVTDATAROOT, 0700)
	_ = os.Setenv("VTDATAROOT", cluster.CurrentVTDATAROOT)
	rand.Seed(time.Now().UTC().UnixNano())
	return cluster
}

// ReStartVtgate starts vtgate with updated configs
func (cluster *LocalProcessCluster) ReStartVtgate() (err error) {
	err = cluster.VtgateProcess.TearDown()
	if err != nil {
		log.Error(err.Error())
		return
	}
	err = cluster.StartVtgate()
	if err != nil {
		log.Error(err.Error())
		return
	}
	return err
}

// WaitForTabletsToHealthyInVtgate waits for all tablets in all shards to be healthy as per vtgate
func (cluster *LocalProcessCluster) WaitForTabletsToHealthyInVtgate() (err error) {
	var isRdOnlyPresent bool
	for _, keyspace := range cluster.Keyspaces {
		for _, shard := range keyspace.Shards {
			isRdOnlyPresent = false
			if err = cluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspace.Name, shard.Name)); err != nil {
				return err
			}
			if err = cluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspace.Name, shard.Name)); err != nil {
				return err
			}
			for _, tablet := range shard.Vttablets {
				if tablet.Type == "rdonly" {
					isRdOnlyPresent = true
				}
			}
			if isRdOnlyPresent {
				err = cluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspace.Name, shard.Name))
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Teardown brings down the cluster by invoking teardown for individual processes
func (cluster *LocalProcessCluster) Teardown() {
	if err := cluster.VtgateProcess.TearDown(); err != nil {
		log.Errorf("Error in vtgate teardown - %s", err.Error())
	}
	var mysqlctlProcessList []*exec.Cmd
	for _, keyspace := range cluster.Keyspaces {
		for _, shard := range keyspace.Shards {
			for _, tablet := range shard.Vttablets {
				if tablet.MysqlctlProcess.TabletUID > 0 {
					if proc, err := tablet.MysqlctlProcess.StopProcess(); err != nil {
						log.Errorf("Error in mysqlctl teardown - %s", err.Error())
					} else {
						mysqlctlProcessList = append(mysqlctlProcessList, proc)
					}
				}
				if err := tablet.VttabletProcess.TearDown(); err != nil {
					log.Errorf("Error in vttablet teardown - %s", err.Error())
				}
			}
		}
	}

	for _, proc := range mysqlctlProcessList {
		if err := proc.Wait(); err != nil {
			log.Errorf("Error in mysqlctl teardown wait - %s", err.Error())
		}
	}

	if err := cluster.VtctldProcess.TearDown(); err != nil {
		log.Errorf("Error in vtctld teardown - %s", err.Error())
	}

	if err := cluster.TopoProcess.TearDown(cluster.Cell, cluster.OriginalVTDATAROOT, cluster.CurrentVTDATAROOT, *keepData); err != nil {
		log.Errorf("Error in etcd teardown - %s", err.Error())
	}
}

// StartVtworker starts a vtworker
func (cluster *LocalProcessCluster) StartVtworker(cell string, extraArgs ...string) error {
	httpPort := cluster.GetAndReservePort()
	grpcPort := cluster.GetAndReservePort()
	log.Info(fmt.Sprintf("Starting vtworker on port %d", httpPort))
	cluster.VtworkerProcess = *VtworkerProcessInstance(
		httpPort,
		grpcPort,
		cluster.TopoPort,
		cluster.Hostname,
		cluster.TmpDirectory)
	cluster.VtworkerProcess.ExtraArgs = extraArgs
	return cluster.VtworkerProcess.Setup(cell)

}

// GetAndReservePort gives port for required process
func (cluster *LocalProcessCluster) GetAndReservePort() int {
	if cluster.nextPortForProcess == 0 {
		cluster.nextPortForProcess = getRandomNumber(20000, 15000)
	}
	cluster.nextPortForProcess = cluster.nextPortForProcess + 1
	return cluster.nextPortForProcess
}

// GetAndReserveTabletUID gives tablet uid
func (cluster *LocalProcessCluster) GetAndReserveTabletUID() int {
	if cluster.BaseTabletUID == 0 {
		cluster.BaseTabletUID = getRandomNumber(10000, 0)
	}
	cluster.BaseTabletUID = cluster.BaseTabletUID + 1
	return cluster.BaseTabletUID
}

func getRandomNumber(maxNumber int32, baseNumber int) int {
	return int(rand.Int31n(maxNumber)) + baseNumber
}

// GetVttabletInstance creates a new vttablet object
func (cluster *LocalProcessCluster) GetVttabletInstance(UID int) *Vttablet {
	if UID == 0 {
		UID = cluster.GetAndReserveTabletUID()
	}
	return &Vttablet{
		TabletUID: UID,
		HTTPPort:  cluster.GetAndReservePort(),
		GrpcPort:  cluster.GetAndReservePort(),
		MySQLPort: cluster.GetAndReservePort(),
		Type:      "replica",
		Alias:     fmt.Sprintf("%s-%010d", cluster.Cell, UID),
	}
}

// StartVttablet starts a new tablet
func (cluster *LocalProcessCluster) StartVttablet(tablet *Vttablet, servingStatus string,
	supportBackup bool, cell string, keyspaceName string, hostname string, shardName string) error {
	tablet.VttabletProcess = VttabletProcessInstance(
		tablet.HTTPPort,
		tablet.GrpcPort,
		tablet.TabletUID,
		cell,
		shardName,
		keyspaceName,
		cluster.VtctldProcess.Port,
		tablet.Type,
		cluster.TopoProcess.Port,
		hostname,
		cluster.TmpDirectory,
		cluster.VtTabletExtraArgs,
		cluster.EnableSemiSync)

	tablet.VttabletProcess.SupportsBackup = supportBackup
	tablet.VttabletProcess.ServingStatus = servingStatus
	return tablet.VttabletProcess.Setup()
}
