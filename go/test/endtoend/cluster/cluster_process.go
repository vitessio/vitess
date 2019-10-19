package cluster

import (
	"fmt"
	"math/rand"
)

// LocalProcessCluster Testcases need to use this to iniate a cluster
type LocalProcessCluster struct {
	Port          int
	Keyspaces     []Keyspace
	Cell          string
	BaseTabletUID int
	Hostname      string
	TopoPort      int

	VtgateMySQLPort int
	VtctldHTTPPort  int

	// standalone executable
	VtctlclientProcess VtctlClientProcess

	// background executable processes
	topoProcess   EtcdProcess
	vtctldProcess VtctldProcess
	vtgateProcess VtgateProcess
}

// Keyspace Cluster accepts keyspace to launch it
type Keyspace struct {
	Name      string
	SQLSchema string
	VSchema   string
	Shards    []Shard
}

// Shard Keyspace need Shard to manage its lifecycle
type Shard struct {
	Name      string
	Vttablets []Vttablet
}

// Vttablet Shard need vttablet to manage the setup and teardown
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
		cluster.Cell = "zone1"
	}
	cluster.TopoPort = cluster.GetAndReservePort()
	cluster.topoProcess = *EtcdProcessInstance(cluster.TopoPort, cluster.Hostname)
	println(fmt.Sprintf("Starting etcd server on port : %d", cluster.TopoPort))
	if err = cluster.topoProcess.Setup(); err != nil {
		println(err.Error())
		return
	}

	println("Creating topop dirs")
	if err = cluster.topoProcess.ManageTopoDir("mkdir", "/vitess/global"); err != nil {
		println(err.Error())
		return
	}

	if err = cluster.topoProcess.ManageTopoDir("mkdir", "/vitess/"+cluster.Cell); err != nil {
		println(err.Error())
		return
	}

	println("Adding cell info")
	vtcltlProcess := VtctlProcessInstance(cluster.topoProcess.Port, cluster.Hostname)
	if err = vtcltlProcess.AddCellInfo(cluster.Cell); err != nil {
		println(err)
		return
	}

	cluster.vtctldProcess = *VtctldProcessInstance(cluster.GetAndReservePort(), cluster.GetAndReservePort(), cluster.topoProcess.Port, cluster.Hostname)
	println(fmt.Sprintf("Starting vtctld server on port : %d", cluster.vtctldProcess.Port))
	cluster.VtctldHTTPPort = cluster.vtctldProcess.Port
	if err = cluster.vtctldProcess.Setup(cluster.Cell); err != nil {
		println(err.Error())
		return
	}

	cluster.VtctlclientProcess = *VtctlClientProcessInstance("localhost", cluster.vtctldProcess.GrpcPort)
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
	println("Starting keyspace : " + keyspace.Name)
	for _, shardName := range shardNames {
		shard := &Shard{
			Name: shardName,
		}
		println("Starting shard : " + shardName)
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
			println(fmt.Sprintf("Starting mysqlctl for table uid %d, mysql port %d", tablet.TabletUID, tablet.MySQLPort))
			tablet.mysqlctlProcess = *MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort)
			if err = tablet.mysqlctlProcess.Start(); err != nil {
				println(err.Error())
				return
			}

			// start vttablet process
			tablet.vttabletProcess = *VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				cluster.Cell,
				shardName,
				cluster.Hostname,
				keyspace.Name,
				cluster.vtctldProcess.Port,
				tablet.Type,
				cluster.topoProcess.Port,
				cluster.Hostname)
			println(fmt.Sprintf("Starting vttablet for tablet uid %d, grpc port %d", tablet.TabletUID, tablet.GrpcPort))

			if err = tablet.vttabletProcess.Setup(); err != nil {
				println(err.Error())
				return
			}

			shard.Vttablets = append(shard.Vttablets, *tablet)
		}

		// Make 1st tablet as master shard
		if err = cluster.VtctlclientProcess.InitShardMaster(keyspace.Name, shardName, cluster.Cell, shard.Vttablets[0].TabletUID); err != nil {
			println(err.Error())
			return
		}

		// Apply SQLSchema
		if err = cluster.VtctlclientProcess.ApplySchema(keyspace.Name, keyspace.SQLSchema); err != nil {
			println(err.Error())
			return
		}

		//Apply VSchema
		if err = cluster.VtctlclientProcess.ApplyVSchema(keyspace.Name, keyspace.VSchema); err != nil {
			println(err.Error())
			return
		}
		shards = append(shards, *shard)
	}
	keyspace.Shards = shards
	cluster.Keyspaces = append(cluster.Keyspaces, keyspace)
	println("Done creating keyspace : " + keyspace.Name)
	return
}

// StartVtgate starts vtgate
func (cluster *LocalProcessCluster) StartVtgate() (err error) {
	vtgateHTTPPort := cluster.GetAndReservePort()
	vtgateGrpcPort := cluster.GetAndReservePort()
	cluster.VtgateMySQLPort = cluster.GetAndReservePort()
	println(fmt.Sprintf("Starting vtgate on port %d", vtgateHTTPPort))
	cluster.vtgateProcess = *VtgateProcessInstance(
		vtgateHTTPPort,
		vtgateGrpcPort,
		cluster.VtgateMySQLPort,
		cluster.Cell,
		cluster.Cell,
		cluster.Hostname, "MASTER,REPLICA",
		cluster.topoProcess.Port,
		cluster.Hostname)

	println(fmt.Sprintf("Vtgate started, connect to mysql using : mysql -h 127.0.0.1 -P %d", cluster.VtgateMySQLPort))
	return cluster.vtgateProcess.Setup()
}

// Teardown brings down the cluster by invoking teardown for individual processes
func (cluster *LocalProcessCluster) Teardown() (err error) {
	if err = cluster.vtgateProcess.TearDown(); err != nil {
		println(err.Error())
		return
	}

	for _, keyspace := range cluster.Keyspaces {
		for _, shard := range keyspace.Shards {
			for _, tablet := range shard.Vttablets {
				if err = tablet.mysqlctlProcess.Stop(); err != nil {
					println(err.Error())
					return
				}

				if err = tablet.vttabletProcess.TearDown(); err != nil {
					println(err.Error())
					return
				}
			}
		}
	}

	if err = cluster.vtctldProcess.TearDown(); err != nil {
		println(err.Error())
		return
	}

	if err = cluster.topoProcess.TearDown(cluster.Cell); err != nil {
		println(err.Error())
		return
	}
	return err
}

// GetAndReservePort gives port for required process
func (cluster *LocalProcessCluster) GetAndReservePort() int {
	if cluster.Port == 0 {
		cluster.Port = getRandomNumber(20000, 15000)
	}
	cluster.Port = cluster.Port + 1
	return cluster.Port
}

// GetAndReserveTabletUID gives tablet uid
func (cluster *LocalProcessCluster) GetAndReserveTabletUID() int {
	if cluster.BaseTabletUID == 0 {
		cluster.BaseTabletUID = getRandomNumber(100, 0)
	}
	cluster.BaseTabletUID = cluster.BaseTabletUID + 1
	return cluster.BaseTabletUID
}

func getRandomNumber(maxNumber int32, baseNumber int) int {
	return int(rand.Int31n(maxNumber)) + baseNumber
}
