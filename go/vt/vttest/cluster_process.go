package vttest

import (
	"fmt"
	"math/rand"
)

var (
	// Hostname to use in other utility testcases
	Hostname = "localhost"
)

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

// Shard Keyspace need Shard to manage its lifecycle
type Shard struct {
	Name      string
	Vttablets []Vttablet
}

// Keyspace Cluster accepts keyspace to launch it
type Keyspace struct {
	Name      string
	SQLSchema string
	VSchema   string
	Shards    []Shard
}

// LocalProcessCluster Testcases need to use this to iniate a cluster
type LocalProcessCluster struct {
	Port          int
	Keyspaces     []Keyspace
	Cell          string
	BaseTabletUID int

	// background executable processes
	topoProcess   EtcdProcess
	vtctldProcess VtctldProcess
	vtgateProcess VtgateProcess

	// standalone executable
	vtctlclientProcess VtctlClientProcess
}

// StartTopo starts topology server
func (cluster *LocalProcessCluster) StartTopo() (err error) {
	if cluster.Cell == "" {
		cluster.Cell = "zone1"
	}
	etcdPort := cluster.GetAndReservePort()
	cluster.topoProcess = *EtcdProcessInstance(etcdPort)
	fmt.Printf("Starting etcd server on port : %d", etcdPort)
	if err = cluster.topoProcess.Setup(); err != nil {
		return
	}

	println("Creating topop dirs")
	err = cluster.topoProcess.ManageTopoDir("mkdir", "/vitess/global")
	if err != nil {
		return
	}

	err = cluster.topoProcess.ManageTopoDir("mkdir", "/vitess/"+cluster.Cell)
	if err != nil {
		return
	}

	println("Adding cell info")
	vtcltlProcess := VtctlProcessInstance(cluster.topoProcess.Port, Hostname)
	err = vtcltlProcess.AddCellInfo(cluster.Cell)

	cluster.vtctldProcess = *VtctldProcessInstance(cluster.GetAndReservePort(), cluster.GetAndReservePort(), cluster.topoProcess.Port, Hostname)
	fmt.Printf("Starting vtctld server on port : %d", cluster.vtctldProcess.Port)
	if err = cluster.vtctldProcess.Setup(cluster.Cell); err != nil {
		return
	}

	cluster.vtctlclientProcess = *VtctlClientProcessInstance("localhost", cluster.vtctldProcess.GrpcPort)
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
			fmt.Printf("Starting mysqlctl for table uid %d, mysql port %d", tablet.TabletUID, tablet.MySQLPort)
			tablet.mysqlctlProcess = *MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort)
			err = tablet.mysqlctlProcess.Start()
			if err != nil {
				return
			}

			// start vttablet process
			tablet.vttabletProcess = *VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				cluster.Cell,
				shardName,
				Hostname,
				keyspace.Name,
				cluster.vtctldProcess.Port,
				tablet.Type,
				cluster.topoProcess.Port,
				Hostname)
			fmt.Printf("Starting vttable for table uid %d, grpc port %d", tablet.TabletUID, tablet.GrpcPort)
			err = tablet.vttabletProcess.Setup()
			if err != nil {
				return
			}

			shard.Vttablets = append(shard.Vttablets, *tablet)
		}

		// Make 1st tablet as master shard
		err = cluster.vtctlclientProcess.InitShardMaster(keyspace.Name, shardName, cluster.Cell, shard.Vttablets[0].TabletUID)
		if err != nil {
			return
		}

		// Apply SQLSchema
		err = cluster.vtctlclientProcess.ApplySchema(keyspace.Name, keyspace.SQLSchema)
		if err != nil {
			return
		}

		//Apply VSchema
		err = cluster.vtctlclientProcess.ApplyVSchema(keyspace.Name, keyspace.VSchema)
		if err != nil {
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
	vtgateMysqlPort := cluster.GetAndReservePort()
	fmt.Printf("Starting vtgate on port %d", vtgateHTTPPort)
	cluster.vtgateProcess = *VtgateProcessInstance(
		vtgateHTTPPort,
		vtgateGrpcPort,
		vtgateMysqlPort,
		cluster.Cell,
		cluster.Cell,
		Hostname, "MASTER,REPLICA",
		cluster.topoProcess.Port,
		Hostname)

	fmt.Printf("Vtgate started, connect to mysql using : mysql -h 127.0.0.1 -P %d", vtgateMysqlPort)
	return cluster.vtgateProcess.Setup()
}

// Teardown brings down the cluster by invoking teardown for individual processes
func (cluster *LocalProcessCluster) Teardown() (err error) {
	err = cluster.vtgateProcess.TearDown()
	if err != nil {
		return
	}

	for _, keyspace := range cluster.Keyspaces {
		for _, shard := range keyspace.Shards {
			for _, tablet := range shard.Vttablets {
				err = tablet.mysqlctlProcess.Stop()
				if err != nil {
					return
				}

				err = tablet.vttabletProcess.TearDown()
				if err != nil {
					return
				}
			}
		}
	}

	err = cluster.vtctldProcess.TearDown()
	if err != nil {
		return
	}

	err = cluster.topoProcess.TearDown(cluster.Cell)
	return err
}

// GetAndReservePort gives port for required process
func (cluster *LocalProcessCluster) GetAndReservePort() int {
	if cluster.Port == 0 {
		cluster.Port = randomPort()
	}
	cluster.Port = cluster.Port + 1
	return cluster.Port
}

// GetAndReserveTabletUID gives tablet uid
func (cluster *LocalProcessCluster) GetAndReserveTabletUID() int {
	if cluster.BaseTabletUID == 0 {
		cluster.BaseTabletUID = rand.Intn(100)
	}
	cluster.BaseTabletUID = cluster.BaseTabletUID + 1
	return cluster.BaseTabletUID
}
