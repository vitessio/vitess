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
	"fmt"
	"math/rand"
	"os"
	"path"

	"vitess.io/vitess/go/vt/log"
)

// DefaultCell : If no cell name is passed, then use following
const DefaultCell = "zone1"

// LocalProcessCluster Testcases need to use this to iniate a cluster
type LocalProcessCluster struct {
	Keyspaces     []Keyspace
	Cell          string
	BaseTabletUID int
	Hostname      string
	TopoPort      int
	TmpDirectory  string

	VtgateMySQLPort int
	VtctldHTTPPort  int

	// standalone executable
	VtctlclientProcess VtctlClientProcess
	VtctlProcess       VtctlProcess

	// background executable processes
	topoProcess   EtcdProcess
	vtctldProcess VtctldProcess
	VtgateProcess VtgateProcess

	nextPortForProcess int
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

// Vttablet stores the properties needed to start a vttablet process
type Vttablet struct {
	Type      string
	TabletUID int
	HTTPPort  int
	GrpcPort  int
	MySQLPort int

	// background executable processes
	mysqlctlProcess MysqlctlProcess
	vttabletProcess VttabletProcess
}

// StartTopo starts topology server
func (cluster *LocalProcessCluster) StartTopo() (err error) {
	if cluster.Cell == "" {
		cluster.Cell = DefaultCell
	}
	cluster.TopoPort = cluster.GetAndReservePort()
	cluster.TmpDirectory = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/tmp_%d", cluster.GetAndReservePort()))
	cluster.topoProcess = *EtcdProcessInstance(cluster.TopoPort, cluster.GetAndReservePort(), cluster.Hostname, "global")
	log.Info(fmt.Sprintf("Starting etcd server on port : %d", cluster.TopoPort))
	if err = cluster.topoProcess.Setup(); err != nil {
		log.Error(err.Error())
		return
	}

	log.Info("Creating topo dirs")
	if err = cluster.topoProcess.ManageTopoDir("mkdir", "/vitess/global"); err != nil {
		log.Error(err.Error())
		return
	}

	if err = cluster.topoProcess.ManageTopoDir("mkdir", "/vitess/"+cluster.Cell); err != nil {
		log.Error(err.Error())
		return
	}

	log.Info("Adding cell info")
	cluster.VtctlProcess = *VtctlProcessInstance(cluster.topoProcess.Port, cluster.Hostname)
	if err = cluster.VtctlProcess.AddCellInfo(cluster.Cell); err != nil {
		log.Error(err)
		return
	}

	cluster.vtctldProcess = *VtctldProcessInstance(cluster.GetAndReservePort(), cluster.GetAndReservePort(), cluster.topoProcess.Port, cluster.Hostname, cluster.TmpDirectory)
	log.Info(fmt.Sprintf("Starting vtctld server on port : %d", cluster.vtctldProcess.Port))
	cluster.VtctldHTTPPort = cluster.vtctldProcess.Port
	if err = cluster.vtctldProcess.Setup(cluster.Cell); err != nil {
		log.Error(err.Error())
		return
	}

	cluster.VtctlclientProcess = *VtctlClientProcessInstance("localhost", cluster.vtctldProcess.GrpcPort, cluster.TmpDirectory)
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
	shards := make([]Shard, 0)
	log.Info("Starting keyspace : " + keyspace.Name)
	_ = cluster.VtctlProcess.CreateKeyspace(keyspace.Name)
	for _, shardName := range shardNames {
		shard := &Shard{
			Name: shardName,
		}
		log.Info("Starting shard : " + shardName)
		for i := 0; i < totalTabletsRequired; i++ {
			// instantiate vttable object with reserved ports
			tablet := &Vttablet{
				TabletUID: cluster.GetAndReserveTabletUID(),
				HTTPPort:  cluster.GetAndReservePort(),
				GrpcPort:  cluster.GetAndReservePort(),
				MySQLPort: cluster.GetAndReservePort(),
			}
			if i == 0 { // Make the first one as master
				tablet.Type = "master"
			} else if i == totalTabletsRequired-1 && rdonly { // Make the last one as rdonly if rdonly flag is passed
				tablet.Type = "rdonly"
			}
			// Start Mysqlctl process
			log.Info(fmt.Sprintf("Starting mysqlctl for table uid %d, mysql port %d", tablet.TabletUID, tablet.MySQLPort))
			tablet.mysqlctlProcess = *MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, cluster.TmpDirectory)
			if err = tablet.mysqlctlProcess.Start(); err != nil {
				log.Error(err.Error())
				return
			}

			// start vttablet process
			tablet.vttabletProcess = *VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				cluster.Cell,
				shardName,
				keyspace.Name,
				cluster.vtctldProcess.Port,
				tablet.Type,
				cluster.topoProcess.Port,
				cluster.Hostname,
				cluster.TmpDirectory)
			log.Info(fmt.Sprintf("Starting vttablet for tablet uid %d, grpc port %d", tablet.TabletUID, tablet.GrpcPort))

			if err = tablet.vttabletProcess.Setup(); err != nil {
				log.Error(err.Error())
				return
			}

			shard.Vttablets = append(shard.Vttablets, *tablet)
		}

		// Make first tablet as master
		if err = cluster.VtctlclientProcess.InitShardMaster(keyspace.Name, shardName, cluster.Cell, shard.Vttablets[0].TabletUID); err != nil {
			log.Error(err.Error())
			return
		}

		shards = append(shards, *shard)
	}
	keyspace.Shards = shards
	cluster.Keyspaces = append(cluster.Keyspaces, keyspace)

	// Apply Schema SQL
	if err = cluster.VtctlclientProcess.ApplySchema(keyspace.Name, keyspace.SchemaSQL); err != nil {
		log.Error(err.Error())
		return
	}

	//Apply VSchema
	if err = cluster.VtctlclientProcess.ApplyVSchema(keyspace.Name, keyspace.VSchema); err != nil {
		log.Error(err.Error())
		return
	}

	log.Info("Done creating keyspace : " + keyspace.Name)
	return
}

// StartVtgate starts vtgate
func (cluster *LocalProcessCluster) StartVtgate() (err error) {
	vtgateHTTPPort := cluster.GetAndReservePort()
	vtgateGrpcPort := cluster.GetAndReservePort()
	cluster.VtgateMySQLPort = cluster.GetAndReservePort()
	log.Info(fmt.Sprintf("Starting vtgate on port %d", vtgateHTTPPort))
	cluster.VtgateProcess = *VtgateProcessInstance(
		vtgateHTTPPort,
		vtgateGrpcPort,
		cluster.VtgateMySQLPort,
		cluster.Cell,
		cluster.Cell,
		"MASTER,REPLICA",
		cluster.topoProcess.Port,
		cluster.Hostname,
		cluster.TmpDirectory)

	log.Info(fmt.Sprintf("Vtgate started, connect to mysql using : mysql -h 127.0.0.1 -P %d", cluster.VtgateMySQLPort))
	return cluster.VtgateProcess.Setup()
}

// Teardown brings down the cluster by invoking teardown for individual processes
func (cluster *LocalProcessCluster) Teardown() (err error) {
	if err = cluster.VtgateProcess.TearDown(); err != nil {
		log.Error(err.Error())
		return
	}

	for _, keyspace := range cluster.Keyspaces {
		for _, shard := range keyspace.Shards {
			for _, tablet := range shard.Vttablets {
				if err = tablet.mysqlctlProcess.Stop(); err != nil {
					log.Error(err.Error())
					return
				}

				if err = tablet.vttabletProcess.TearDown(); err != nil {
					log.Error(err.Error())
					return
				}
			}
		}
	}

	if err = cluster.vtctldProcess.TearDown(); err != nil {
		log.Error(err.Error())
		return
	}

	if err = cluster.topoProcess.TearDown(cluster.Cell); err != nil {
		log.Error(err.Error())
		return
	}
	return err
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
