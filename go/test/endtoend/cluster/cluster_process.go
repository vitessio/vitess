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
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/filelock"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	// Ensure dialers are registered (needed by ExecOnTablet and ExecOnVTGate).
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

// DefaultCell : If no cell name is passed, then use following
const (
	DefaultCell      = "zone1"
	DefaultStartPort = 6700
)

var (
	keepData           = flag.Bool("keep-data", true, "don't delete the per-test VTDATAROOT subfolders")
	topoFlavor         = flag.String("topo-flavor", "etcd2", "choose a topo server from etcd2, zk2 or consul")
	isCoverage         = flag.Bool("is-coverage", false, "whether coverage is required")
	forceVTDATAROOT    = flag.String("force-vtdataroot", "", "force path for VTDATAROOT, which may already be populated")
	forcePortStart     = flag.Int("force-port-start", 0, "force assigning ports based on this seed")
	forceBaseTabletUID = flag.Int("force-base-tablet-uid", 0, "force assigning tablet ports based on this seed")
	partialKeyspace    = flag.Bool("partial-keyspace", false, "add a second keyspace for sharded tests and mark first shard as moved to this keyspace in the shard routing rules")

	// PerfTest controls whether to run the slower end-to-end tests that check the system's performance
	PerfTest = flag.Bool("perf-test", false, "include end-to-end performance tests")
)

// LocalProcessCluster Testcases need to use this to iniate a cluster
type LocalProcessCluster struct {
	Keyspaces          []Keyspace
	Cell               string
	DefaultCharset     string
	BaseTabletUID      int
	Hostname           string
	TopoFlavor         string
	TopoPort           int
	TmpDirectory       string
	OriginalVTDATAROOT string
	CurrentVTDATAROOT  string
	ReusingVTDATAROOT  bool

	VtgateMySQLPort int
	VtgateGrpcPort  int
	VtctldHTTPPort  int

	// major version numbers
	VtTabletMajorVersion int
	VtctlMajorVersion    int

	// standalone executable
	VtctlclientProcess  VtctlClientProcess
	VtctldClientProcess VtctldClientProcess
	VtctlProcess        VtctlProcess

	// background executable processes
	TopoProcess     TopoProcess
	VtctldProcess   VtctldProcess
	VtgateProcess   VtgateProcess
	VtbackupProcess VtbackupProcess
	VTOrcProcesses  []*VTOrcProcess

	nextPortForProcess int

	// Extra arguments for vtTablet
	VtTabletExtraArgs []string

	// Extra arguments for vtGate
	VtGateExtraArgs      []string
	VtGatePlannerVersion plancontext.PlannerVersion

	VtctldExtraArgs []string

	// mutex added to handle the parallel teardowns
	mx                *sync.Mutex
	teardownCompleted bool

	context.Context
	context.CancelFunc

	HasPartialKeyspaces bool
}

// Vttablet stores the properties needed to start a vttablet process
type Vttablet struct {
	Type      string
	TabletUID int
	HTTPPort  int
	GrpcPort  int
	MySQLPort int
	Alias     string
	Cell      string

	// background executable processes
	MysqlctlProcess  MysqlctlProcess
	MysqlctldProcess MysqlctldProcess
	VttabletProcess  *VttabletProcess
	VtgrProcess      *VtgrProcess
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
	Vttablets []*Vttablet
}

// PrimaryTablet get the 1st tablet which is always elected as primary
func (shard *Shard) PrimaryTablet() *Vttablet {
	return shard.Vttablets[0]
}

// Rdonly get the last tablet which is rdonly
func (shard *Shard) Rdonly() *Vttablet {
	for idx, tablet := range shard.Vttablets {
		if tablet.Type == "rdonly" {
			return shard.Vttablets[idx]
		}
	}
	return nil
}

// Replica get the last but one tablet which is replica
// Mostly we have either 3 tablet setup [primary, replica, rdonly]
func (shard *Shard) Replica() *Vttablet {
	for idx, tablet := range shard.Vttablets {
		if tablet.Type == "replica" && idx > 0 {
			return shard.Vttablets[idx]
		}
	}
	return nil
}

// CtrlCHandler handles the teardown for the ctrl-c.
func (cluster *LocalProcessCluster) CtrlCHandler() {
	cluster.Context, cluster.CancelFunc = context.WithCancel(context.Background())

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	select {
	case <-c:
		cluster.Teardown()
		os.Exit(0)
	case <-cluster.Done():
	}
}

// StartTopo starts topology server
func (cluster *LocalProcessCluster) StartTopo() (err error) {
	if cluster.Cell == "" {
		cluster.Cell = DefaultCell
	}

	topoFlavor = cluster.TopoFlavorString()
	cluster.TopoPort = cluster.GetAndReservePort()
	cluster.TmpDirectory = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/tmp_%d", cluster.GetAndReservePort()))
	cluster.TopoProcess = *TopoProcessInstance(cluster.TopoPort, cluster.GetAndReservePort(), cluster.Hostname, *topoFlavor, "global")

	log.Infof("Starting topo server %v on port: %d", *topoFlavor, cluster.TopoPort)
	if err = cluster.TopoProcess.Setup(*topoFlavor, cluster); err != nil {
		log.Error(err.Error())
		return
	}

	if *topoFlavor == "etcd2" {
		log.Info("Creating global and cell topo dirs")
		if err = cluster.TopoProcess.ManageTopoDir("mkdir", "/vitess/global"); err != nil {
			log.Error(err.Error())
			return
		}

		if err = cluster.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cluster.Cell); err != nil {
			log.Error(err.Error())
			return
		}
	}

	if !cluster.ReusingVTDATAROOT {
		cluster.VtctlProcess = *VtctlProcessInstance(cluster.TopoProcess.Port, cluster.Hostname)
		if err = cluster.VtctlProcess.AddCellInfo(cluster.Cell); err != nil {
			log.Error(err)
			return
		}
		cluster.VtctlProcess.LogDir = cluster.TmpDirectory
	}

	cluster.VtctldProcess = *VtctldProcessInstance(cluster.GetAndReservePort(), cluster.GetAndReservePort(),
		cluster.TopoProcess.Port, cluster.Hostname, cluster.TmpDirectory)
	log.Infof("Starting vtctld server on port: %d", cluster.VtctldProcess.Port)
	cluster.VtctldHTTPPort = cluster.VtctldProcess.Port
	if err = cluster.VtctldProcess.Setup(cluster.Cell, cluster.VtctldExtraArgs...); err != nil {
		log.Error(err.Error())
		return
	}

	cluster.VtctlclientProcess = *VtctlClientProcessInstance("localhost", cluster.VtctldProcess.GrpcPort, cluster.TmpDirectory)
	return
}

// StartVTOrc starts a VTOrc instance
func (cluster *LocalProcessCluster) StartVTOrc(keyspace string) error {
	// Start vtorc
	vtorcProcess := cluster.NewVTOrcProcess(VTOrcConfiguration{})
	err := vtorcProcess.Setup()
	if err != nil {
		log.Error(err.Error())
		return err
	}
	if keyspace != "" {
		vtorcProcess.ExtraArgs = append(vtorcProcess.ExtraArgs, fmt.Sprintf(`--clusters_to_watch="%s"`, keyspace))
	}
	cluster.VTOrcProcesses = append(cluster.VTOrcProcesses, vtorcProcess)
	return nil
}

// StartUnshardedKeyspace starts unshared keyspace with shard name as "0"
func (cluster *LocalProcessCluster) StartUnshardedKeyspace(keyspace Keyspace, replicaCount int, rdonly bool) error {
	return cluster.StartKeyspace(keyspace, []string{"0"}, replicaCount, rdonly)
}

func (cluster *LocalProcessCluster) startPartialKeyspace(keyspace Keyspace, shardNames []string, movedShard string, replicaCount int, rdonly bool, customizers ...any) (err error) {

	cluster.HasPartialKeyspaces = true
	routedKeyspace := &Keyspace{
		Name:      fmt.Sprintf("%s_routed", keyspace.Name),
		SchemaSQL: keyspace.SchemaSQL,
		VSchema:   keyspace.VSchema,
	}

	err = cluster.startKeyspace(*routedKeyspace, shardNames, replicaCount, rdonly, customizers...)
	if err != nil {
		return err
	}
	shardRoutingRulesTemplate := `{"rules":[{"from_keyspace":"%s","to_keyspace":"%s","shards":"%s"}]}`
	shardRoutingRules := fmt.Sprintf(shardRoutingRulesTemplate, keyspace.Name, routedKeyspace.Name, movedShard)
	cmd := exec.Command("vtctldclient", "--server",
		net.JoinHostPort("localhost", strconv.Itoa(cluster.VtctldProcess.GrpcPort)),
		"ApplyShardRoutingRules", "--rules", shardRoutingRules)
	_, err = cmd.Output()
	if err != nil {
		return err
	}
	return nil
}

func (cluster *LocalProcessCluster) StartKeyspace(keyspace Keyspace, shardNames []string, replicaCount int, rdonly bool, customizers ...any) (err error) {
	err = cluster.startKeyspace(keyspace, shardNames, replicaCount, rdonly, customizers...)
	if err != nil {
		return err
	}

	if *partialKeyspace && len(shardNames) > 1 {
		movedShard := shardNames[0]
		return cluster.startPartialKeyspace(keyspace, shardNames, movedShard, replicaCount, rdonly, customizers...)
	}
	return nil
}

// StartKeyspace starts required number of shard and the corresponding tablets
// keyspace : struct containing keyspace name, Sqlschema to apply, VSchema to apply
// shardName : list of shard names
// replicaCount: total number of replicas excluding shard primary and rdonly
// rdonly: whether readonly tablets needed
// customizers: functions like "func(*VttabletProcess)" that can modify settings of various objects
// after they're created.
func (cluster *LocalProcessCluster) startKeyspace(keyspace Keyspace, shardNames []string, replicaCount int, rdonly bool, customizers ...any) (err error) {
	totalTabletsRequired := replicaCount + 1 // + 1 is for primary
	if rdonly {
		totalTabletsRequired = totalTabletsRequired + 1 // + 1 for rdonly
	}

	log.Infof("Starting keyspace: %v", keyspace.Name)
	if !cluster.ReusingVTDATAROOT {
		_ = cluster.VtctlProcess.CreateKeyspace(keyspace.Name)
	}
	var mysqlctlProcessList []*exec.Cmd
	for _, shardName := range shardNames {
		shard := &Shard{
			Name: shardName,
		}
		log.Infof("Starting shard: %v", shardName)
		mysqlctlProcessList = []*exec.Cmd{}
		for i := 0; i < totalTabletsRequired; i++ {
			// instantiate vttablet object with reserved ports
			tabletUID := cluster.GetAndReserveTabletUID()
			tablet := &Vttablet{
				TabletUID: tabletUID,
				Type:      "replica",
				HTTPPort:  cluster.GetAndReservePort(),
				GrpcPort:  cluster.GetAndReservePort(),
				MySQLPort: cluster.GetAndReservePort(),
				Alias:     fmt.Sprintf("%s-%010d", cluster.Cell, tabletUID),
			}
			if i == 0 { // Make the first one as primary
				tablet.Type = "primary"
			} else if i == totalTabletsRequired-1 && rdonly { // Make the last one as rdonly if rdonly flag is passed
				tablet.Type = "rdonly"
			}
			// Start Mysqlctl process
			log.Infof("Starting mysqlctl for table uid %d, mysql port %d", tablet.TabletUID, tablet.MySQLPort)
			tablet.MysqlctlProcess = *MysqlCtlProcessInstanceOptionalInit(tablet.TabletUID, tablet.MySQLPort, cluster.TmpDirectory, !cluster.ReusingVTDATAROOT)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				log.Errorf("error starting mysqlctl process: %v, %v", tablet.MysqlctldProcess, err)
				return err
			}
			mysqlctlProcessList = append(mysqlctlProcessList, proc)

			// start vttablet process
			tablet.VttabletProcess = VttabletProcessInstance(
				tablet.HTTPPort,
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
				cluster.DefaultCharset)
			tablet.Alias = tablet.VttabletProcess.TabletPath
			if cluster.ReusingVTDATAROOT {
				tablet.VttabletProcess.ServingStatus = "SERVING"
			}
			shard.Vttablets = append(shard.Vttablets, tablet)
			// Apply customizations
			for _, customizer := range customizers {
				if f, ok := customizer.(func(*VttabletProcess)); ok {
					f(tablet.VttabletProcess)
				} else {
					return fmt.Errorf("type mismatch on customizer: %T", customizer)
				}
			}
		}

		// wait till all mysqlctl is instantiated
		for _, proc := range mysqlctlProcessList {
			if err = proc.Wait(); err != nil {
				log.Errorf("unable to start mysql process %v: %v", proc, err)
				return err
			}
		}
		for _, tablet := range shard.Vttablets {
			log.Infof("Starting vttablet for tablet uid %d, grpc port %d", tablet.TabletUID, tablet.GrpcPort)

			if err = tablet.VttabletProcess.Setup(); err != nil {
				log.Errorf("error starting vttablet for tablet uid %d, grpc port %d: %v", tablet.TabletUID, tablet.GrpcPort, err)
				return
			}
		}

		// Make first tablet as primary
		if err = cluster.VtctlclientProcess.InitializeShard(keyspace.Name, shardName, cluster.Cell, shard.Vttablets[0].TabletUID); err != nil {
			log.Errorf("error running InitializeShard on keyspace %v, shard %v: %v", keyspace.Name, shardName, err)
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
			log.Errorf("error applying schema: %v, %v", keyspace.SchemaSQL, err)
			return
		}
	}

	// Apply VSchema
	if keyspace.VSchema != "" {
		if err = cluster.VtctlclientProcess.ApplyVSchema(keyspace.Name, keyspace.VSchema); err != nil {
			log.Errorf("error applying vschema: %v, %v", keyspace.VSchema, err)
			return
		}
	}

	log.Infof("Done creating keyspace: %v ", keyspace.Name)

	err = cluster.StartVTOrc(keyspace.Name)
	if err != nil {
		log.Errorf("Error starting VTOrc - %v", err)
		return err
	}

	return
}

// StartUnshardedKeyspaceLegacy starts unshared keyspace with shard name as "0"
func (cluster *LocalProcessCluster) StartUnshardedKeyspaceLegacy(keyspace Keyspace, replicaCount int, rdonly bool) error {
	return cluster.StartKeyspaceLegacy(keyspace, []string{"0"}, replicaCount, rdonly)
}

// StartKeyspaceLegacy starts required number of shard and the corresponding tablets
// keyspace : struct containing keyspace name, Sqlschema to apply, VSchema to apply
// shardName : list of shard names
// replicaCount: total number of replicas excluding shard primary and rdonly
// rdonly: whether readonly tablets needed
// customizers: functions like "func(*VttabletProcess)" that can modify settings of various objects
// after they're created.
func (cluster *LocalProcessCluster) StartKeyspaceLegacy(keyspace Keyspace, shardNames []string, replicaCount int, rdonly bool, customizers ...any) (err error) {
	totalTabletsRequired := replicaCount + 1 // + 1 is for primary
	if rdonly {
		totalTabletsRequired = totalTabletsRequired + 1 // + 1 for rdonly
	}

	log.Infof("Starting keyspace: %v", keyspace.Name)
	if !cluster.ReusingVTDATAROOT {
		_ = cluster.VtctlProcess.CreateKeyspace(keyspace.Name)
	}
	var mysqlctlProcessList []*exec.Cmd
	for _, shardName := range shardNames {
		shard := &Shard{
			Name: shardName,
		}
		log.Infof("Starting shard: %v", shardName)
		mysqlctlProcessList = []*exec.Cmd{}
		for i := 0; i < totalTabletsRequired; i++ {
			// instantiate vttablet object with reserved ports
			tabletUID := cluster.GetAndReserveTabletUID()
			tablet := &Vttablet{
				TabletUID: tabletUID,
				Type:      "replica",
				HTTPPort:  cluster.GetAndReservePort(),
				GrpcPort:  cluster.GetAndReservePort(),
				MySQLPort: cluster.GetAndReservePort(),
				Alias:     fmt.Sprintf("%s-%010d", cluster.Cell, tabletUID),
			}
			if i == 0 { // Make the first one as primary
				tablet.Type = "primary"
			} else if i == totalTabletsRequired-1 && rdonly { // Make the last one as rdonly if rdonly flag is passed
				tablet.Type = "rdonly"
			}
			// Start Mysqlctl process
			log.Infof("Starting mysqlctl for table uid %d, mysql port %d", tablet.TabletUID, tablet.MySQLPort)
			tablet.MysqlctlProcess = *MysqlCtlProcessInstanceOptionalInit(tablet.TabletUID, tablet.MySQLPort, cluster.TmpDirectory, !cluster.ReusingVTDATAROOT)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				log.Errorf("error starting mysqlctl process: %v, %v", tablet.MysqlctldProcess, err)
				return err
			}
			mysqlctlProcessList = append(mysqlctlProcessList, proc)

			// start vttablet process
			tablet.VttabletProcess = VttabletProcessInstance(
				tablet.HTTPPort,
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
				cluster.DefaultCharset)
			tablet.Alias = tablet.VttabletProcess.TabletPath
			if cluster.ReusingVTDATAROOT {
				tablet.VttabletProcess.ServingStatus = "SERVING"
			}
			shard.Vttablets = append(shard.Vttablets, tablet)
			// Apply customizations
			for _, customizer := range customizers {
				if f, ok := customizer.(func(*VttabletProcess)); ok {
					f(tablet.VttabletProcess)
				} else {
					return fmt.Errorf("type mismatch on customizer: %T", customizer)
				}
			}
		}

		// wait till all mysqlctl is instantiated
		for _, proc := range mysqlctlProcessList {
			if err = proc.Wait(); err != nil {
				log.Errorf("unable to start mysql process %v: %v", proc, err)
				return err
			}
		}
		for _, tablet := range shard.Vttablets {
			if !cluster.ReusingVTDATAROOT {
				if _, err = tablet.VttabletProcess.QueryTablet(fmt.Sprintf("create database vt_%s", keyspace.Name), keyspace.Name, false); err != nil {
					log.Errorf("error creating database for keyspace %v: %v", keyspace.Name, err)
					return
				}
			}

			log.Infof("Starting vttablet for tablet uid %d, grpc port %d", tablet.TabletUID, tablet.GrpcPort)

			if err = tablet.VttabletProcess.Setup(); err != nil {
				log.Errorf("error starting vttablet for tablet uid %d, grpc port %d: %v", tablet.TabletUID, tablet.GrpcPort, err)
				return
			}
		}

		// Make first tablet as primary
		if err = cluster.VtctlclientProcess.InitShardPrimary(keyspace.Name, shardName, cluster.Cell, shard.Vttablets[0].TabletUID); err != nil {
			log.Errorf("error running ISM on keyspace %v, shard %v: %v", keyspace.Name, shardName, err)
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
			log.Errorf("error applying schema: %v, %v", keyspace.SchemaSQL, err)
			return
		}
	}

	// Apply VSchema
	if keyspace.VSchema != "" {
		if err = cluster.VtctlclientProcess.ApplyVSchema(keyspace.Name, keyspace.VSchema); err != nil {
			log.Errorf("error applying vschema: %v, %v", keyspace.VSchema, err)
			return
		}
	}

	log.Infof("Done creating keyspace: %v ", keyspace.Name)
	return
}

// SetupCluster creates the skeleton for a cluster by creating keyspace
// shards and initializing tablets and mysqlctl processes.
// This does not start any process and user have to explicitly start all
// the required services (ex topo, vtgate, mysql and vttablet)
func (cluster *LocalProcessCluster) SetupCluster(keyspace *Keyspace, shards []Shard) (err error) {
	log.Infof("Starting keyspace: %v", keyspace.Name)

	if !cluster.ReusingVTDATAROOT {
		// Create Keyspace
		err = cluster.VtctlProcess.CreateKeyspace(keyspace.Name)
		if err != nil {
			log.Error(err)
			return
		}
	}

	// Create shard
	for _, shard := range shards {
		for _, tablet := range shard.Vttablets {
			// Setup MysqlctlProcess
			tablet.MysqlctlProcess = *MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, cluster.TmpDirectory)
			// Setup VttabletProcess
			tablet.VttabletProcess = VttabletProcessInstance(
				tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				tablet.Cell,
				shard.Name,
				keyspace.Name,
				cluster.VtctldProcess.Port,
				tablet.Type,
				cluster.TopoProcess.Port,
				cluster.Hostname,
				cluster.TmpDirectory,
				cluster.VtTabletExtraArgs,
				cluster.DefaultCharset)
		}

		keyspace.Shards = append(keyspace.Shards, shard)
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
		cluster.Keyspaces = append(cluster.Keyspaces, *keyspace)
	}

	log.Infof("Done launching keyspace: %v", keyspace.Name)
	return err
}

// StartVtgate starts vtgate
func (cluster *LocalProcessCluster) StartVtgate() (err error) {
	if cluster.HasPartialKeyspaces {
		cluster.VtGateExtraArgs = append(cluster.VtGateExtraArgs, "--enable-partial-keyspace-migration")
	}
	vtgateInstance := *cluster.NewVtgateInstance()
	cluster.VtgateProcess = vtgateInstance
	log.Infof("Starting vtgate on port %d", vtgateInstance.Port)
	log.Infof("Vtgate started, connect to mysql using : mysql -h 127.0.0.1 -P %d", cluster.VtgateMySQLPort)
	return cluster.VtgateProcess.Setup()
}

// NewVtgateInstance returns an instance of vtgateprocess
func (cluster *LocalProcessCluster) NewVtgateInstance() *VtgateProcess {
	vtgateHTTPPort := cluster.GetAndReservePort()
	cluster.VtgateGrpcPort = cluster.GetAndReservePort()
	cluster.VtgateMySQLPort = cluster.GetAndReservePort()
	vtgateProcInstance := VtgateProcessInstance(
		vtgateHTTPPort,
		cluster.VtgateGrpcPort,
		cluster.VtgateMySQLPort,
		cluster.Cell,
		cluster.Cell,
		cluster.Hostname,
		"PRIMARY,REPLICA",
		cluster.TopoProcess.Port,
		cluster.TmpDirectory,
		cluster.VtGateExtraArgs,
		cluster.VtGatePlannerVersion)
	return vtgateProcInstance
}

// NewBareCluster instantiates a new cluster and does not assume existence of any of the vitess processes
func NewBareCluster(cell string, hostname string) *LocalProcessCluster {
	cluster := &LocalProcessCluster{Cell: cell, Hostname: hostname, mx: new(sync.Mutex), DefaultCharset: "utf8mb4"}
	go cluster.CtrlCHandler()

	cluster.OriginalVTDATAROOT = os.Getenv("VTDATAROOT")
	cluster.CurrentVTDATAROOT = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vtroot_%d", cluster.GetAndReservePort()))
	cluster.VtGatePlannerVersion = defaultVtGatePlannerVersion
	if *forceVTDATAROOT != "" {
		cluster.CurrentVTDATAROOT = *forceVTDATAROOT
	}
	if _, err := os.Stat(cluster.CurrentVTDATAROOT); err == nil {
		// path/to/whatever exists
		cluster.ReusingVTDATAROOT = true
	} else {
		_ = createDirectory(cluster.CurrentVTDATAROOT, 0700)
	}
	_ = os.Setenv("VTDATAROOT", cluster.CurrentVTDATAROOT)
	log.Infof("Created cluster on %s. ReusingVTDATAROOT=%v", cluster.CurrentVTDATAROOT, cluster.ReusingVTDATAROOT)

	rand.Seed(time.Now().UTC().UnixNano())
	return cluster
}

// NewCluster instantiates a new cluster
func NewCluster(cell string, hostname string) *LocalProcessCluster {
	cluster := NewBareCluster(cell, hostname)

	err := cluster.populateVersionInfo()
	if err != nil {
		log.Errorf("Error populating version information - %v", err)
	}
	return cluster
}

// populateVersionInfo is used to populate the version information for the binaries used to setup the cluster.
func (cluster *LocalProcessCluster) populateVersionInfo() error {
	var err error
	cluster.VtTabletMajorVersion, err = GetMajorVersion("vttablet")
	if err != nil {
		return err
	}
	cluster.VtctlMajorVersion, err = GetMajorVersion("vtctl")
	return err
}

func GetMajorVersion(binaryName string) (int, error) {
	version, err := exec.Command(binaryName, "--version").Output()
	if err != nil {
		return 0, err
	}
	versionRegex := regexp.MustCompile(`Version: ([0-9]+)\.([0-9]+)\.([0-9]+)`)
	v := versionRegex.FindStringSubmatch(string(version))
	if len(v) != 4 {
		return 0, fmt.Errorf("could not parse server version from: %s", version)
	}
	if err != nil {
		return 0, fmt.Errorf("could not parse server version from: %s", version)
	}
	return strconv.Atoi(v[1])
}

// RestartVtgate starts vtgate with updated configs
func (cluster *LocalProcessCluster) RestartVtgate() (err error) {
	err = cluster.VtgateProcess.TearDown()
	if err != nil {
		log.Errorf("error stopping vtgate %v: %v", cluster.VtgateProcess, err)
		return
	}
	err = cluster.StartVtgate()
	if err != nil {
		log.Errorf("error starting vtgate %v: %v", cluster.VtgateProcess, err)
		return
	}
	return err
}

// WaitForTabletsToHealthyInVtgate waits for all tablets in all shards to be seen as
// healthy and serving in vtgate.
// For each shard:
//   - It must have 1 (and only 1) healthy primary tablet so we always wait for that
//   - For replica and rdonly tablets, which are optional, we wait for as many as we
//     should have based on how the cluster was defined.
func (cluster *LocalProcessCluster) WaitForTabletsToHealthyInVtgate() (err error) {
	for _, keyspace := range cluster.Keyspaces {
		for _, shard := range keyspace.Shards {
			rdonlyTabletCount, replicaTabletCount := 0, 0
			for _, tablet := range shard.Vttablets {
				switch strings.ToLower(tablet.Type) {
				case "replica":
					replicaTabletCount++
				case "rdonly":
					rdonlyTabletCount++
				}
			}
			if err = cluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", keyspace.Name, shard.Name), 1); err != nil {
				return err
			}
			if replicaTabletCount > 0 {
				err = cluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspace.Name, shard.Name), replicaTabletCount)
			}
			if rdonlyTabletCount > 0 {
				err = cluster.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspace.Name, shard.Name), rdonlyTabletCount)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ExecOnTablet executes a query on the local cluster Vttablet and returns the
// result.
func (cluster *LocalProcessCluster) ExecOnTablet(ctx context.Context, vttablet *Vttablet, sql string, binds map[string]any, opts *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	bindvars, err := sqltypes.BuildBindVariables(binds)
	if err != nil {
		return nil, err
	}

	tablet, err := cluster.vtctlclientGetTablet(vttablet)
	if err != nil {
		return nil, err
	}

	conn, err := tabletconn.GetDialer()(tablet, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	txID, reservedID := 0, 0

	return conn.Execute(ctx, &querypb.Target{
		Keyspace:   tablet.Keyspace,
		Shard:      tablet.Shard,
		TabletType: tablet.Type,
	}, sql, bindvars, int64(txID), int64(reservedID), opts)
}

// ExecOnVTGate executes a query on a local cluster VTGate with the provided
// target, bindvars, and execute options.
func (cluster *LocalProcessCluster) ExecOnVTGate(ctx context.Context, addr string, target string, sql string, binds map[string]any, opts *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	bindvars, err := sqltypes.BuildBindVariables(binds)
	if err != nil {
		return nil, err
	}

	conn, err := vtgateconn.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}

	session := conn.Session(target, opts)
	defer conn.Close()

	return session.Execute(ctx, sql, bindvars)
}

// StreamTabletHealth invokes a HealthStream on a local cluster Vttablet and
// returns the responses. It returns an error if the stream ends with fewer than
// `count` responses.
func (cluster *LocalProcessCluster) StreamTabletHealth(ctx context.Context, vttablet *Vttablet, count int) (responses []*querypb.StreamHealthResponse, err error) {
	tablet, err := cluster.vtctlclientGetTablet(vttablet)
	if err != nil {
		return nil, err
	}

	conn, err := tabletconn.GetDialer()(tablet, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}

	i := 0
	err = conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		responses = append(responses, shr)

		i++
		if i >= count {
			return io.EOF
		}

		return nil
	})

	switch {
	case err != nil:
		return nil, err
	case len(responses) < count:
		return nil, errors.New("stream ended early")
	}

	return responses, nil
}

func (cluster *LocalProcessCluster) vtctlclientGetTablet(tablet *Vttablet) (*topodatapb.Tablet, error) {
	result, err := cluster.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", "--", tablet.Alias)
	if err != nil {
		return nil, err
	}

	var ti topodatapb.Tablet
	if err := json2.Unmarshal([]byte(result), &ti); err != nil {
		return nil, err
	}

	return &ti, nil
}

// Teardown brings down the cluster by invoking teardown for individual processes
func (cluster *LocalProcessCluster) Teardown() {
	PanicHandler(nil)
	cluster.mx.Lock()
	defer cluster.mx.Unlock()
	if cluster.teardownCompleted {
		return
	}
	if cluster.CancelFunc != nil {
		cluster.CancelFunc()
	}
	if err := cluster.VtgateProcess.TearDown(); err != nil {
		log.Errorf("Error in vtgate teardown: %v", err)
	}

	for _, vtorcProcess := range cluster.VTOrcProcesses {
		if err := vtorcProcess.TearDown(); err != nil {
			log.Errorf("Error in vtorc teardown: %v", err)
		}
	}

	var mysqlctlProcessList []*exec.Cmd
	var mysqlctlTabletUIDs []int
	for _, keyspace := range cluster.Keyspaces {
		for _, shard := range keyspace.Shards {
			for _, tablet := range shard.Vttablets {
				if tablet.MysqlctlProcess.TabletUID > 0 {
					if proc, err := tablet.MysqlctlProcess.StopProcess(); err != nil {
						log.Errorf("Error in mysqlctl teardown: %v", err)
					} else {
						mysqlctlProcessList = append(mysqlctlProcessList, proc)
						mysqlctlTabletUIDs = append(mysqlctlTabletUIDs, tablet.MysqlctlProcess.TabletUID)
					}
				}
				if tablet.MysqlctldProcess.TabletUID > 0 {
					if err := tablet.MysqlctldProcess.Stop(); err != nil {
						log.Errorf("Error in mysqlctl teardown: %v", err)
					}
				}

				if err := tablet.VttabletProcess.TearDown(); err != nil {
					log.Errorf("Error in vttablet teardown: %v", err)
				}
			}
		}
	}

	// On the CI it was noticed that MySQL shutdown hangs sometimes and
	// on local investigation it was waiting on SEMI_SYNC acks for an internal command
	// of Vitess even after closing the socket file.
	// To prevent this process for hanging for 5 minutes, we will add a 30-second timeout.
	cluster.waitForMySQLProcessToExit(mysqlctlProcessList, mysqlctlTabletUIDs)

	if err := cluster.VtctldProcess.TearDown(); err != nil {
		log.Errorf("Error in vtctld teardown: %v", err)
	}

	if err := cluster.TopoProcess.TearDown(cluster.Cell, cluster.OriginalVTDATAROOT, cluster.CurrentVTDATAROOT, *keepData, *topoFlavor); err != nil {
		log.Errorf("Error in topo server teardown: %v", err)
	}

	// reset the VTDATAROOT path.
	os.Setenv("VTDATAROOT", cluster.OriginalVTDATAROOT)

	cluster.teardownCompleted = true
}

func (cluster *LocalProcessCluster) waitForMySQLProcessToExit(mysqlctlProcessList []*exec.Cmd, mysqlctlTabletUIDs []int) {
	wg := sync.WaitGroup{}
	for i, cmd := range mysqlctlProcessList {
		wg.Add(1)
		go func(cmd *exec.Cmd, tabletUID int) {
			defer func() {
				wg.Done()
			}()
			exit := make(chan error)
			go func() {
				exit <- cmd.Wait()
			}()
			select {
			case <-time.After(30 * time.Second):
				break
			case err := <-exit:
				if err == nil {
					return
				}
				log.Errorf("Error in mysqlctl teardown wait: %v", err)
				break
			}
			pidFile := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.pid", tabletUID))
			pidBytes, err := os.ReadFile(pidFile)
			if err != nil {
				// We can't read the file which means the PID file does not exist
				// The server must have stopped
				return
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			if err != nil {
				log.Errorf("Error in conversion to integer: %v", err)
				return
			}
			err = syscall.Kill(pid, syscall.SIGKILL)
			if err != nil {
				log.Errorf("Error in killing process: %v", err)
			}
		}(cmd, mysqlctlTabletUIDs[i])
	}
	wg.Wait()
}

// StartVtbackup starts a vtbackup
func (cluster *LocalProcessCluster) StartVtbackup(newInitDBFile string, initialBackup bool,
	keyspace string, shard string, cell string, extraArgs ...string) error {
	log.Info("Starting vtbackup")
	cluster.VtbackupProcess = *VtbackupProcessInstance(
		cluster.GetAndReserveTabletUID(),
		cluster.GetAndReservePort(),
		newInitDBFile,
		keyspace,
		shard,
		cell,
		cluster.Hostname,
		cluster.TmpDirectory,
		cluster.TopoPort,
		initialBackup)
	cluster.VtbackupProcess.ExtraArgs = extraArgs
	return cluster.VtbackupProcess.Setup()

}

// GetAndReservePort gives port for required process
func (cluster *LocalProcessCluster) GetAndReservePort() int {
	if cluster.nextPortForProcess == 0 {
		if *forcePortStart > 0 {
			cluster.nextPortForProcess = *forcePortStart
		} else {
			cluster.nextPortForProcess = getPort()
		}
	}
	for {
		cluster.nextPortForProcess = cluster.nextPortForProcess + 1
		log.Infof("Attempting to reserve port: %v", cluster.nextPortForProcess)
		ln, err := net.Listen("tcp", fmt.Sprintf(":%v", cluster.nextPortForProcess))

		if err != nil {
			log.Errorf("Can't listen on port %v: %s, trying next port", cluster.nextPortForProcess, err)
			continue
		}

		log.Infof("Port %v is available, reserving..", cluster.nextPortForProcess)
		ln.Close()
		break
	}
	return cluster.nextPortForProcess
}

// portFileTimeout determines when we see the content of a port file as
// stale. After this time, we assume we can start with the default base
// port again.
const portFileTimeout = 1 * time.Hour

// getPort checks if we have recent used port info in /tmp/todaytime.port
// If no, then use a random port and save that port + 200 in the above file
// If yes, then return that port, and save port + 200 in the same file
// here, assumptions is 200 ports might be consumed for all tests in a package
func getPort() int {
	portFile, err := os.OpenFile(path.Join(os.TempDir(), "endtoend.port"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	filelock.Lock(portFile)
	defer filelock.Unlock(portFile)

	fileInfo, err := portFile.Stat()
	if err != nil {
		panic(err)
	}

	portBytes, err := io.ReadAll(portFile)
	if err != nil {
		panic(err)
	}

	var port int
	if len(portBytes) == 0 || time.Now().After(fileInfo.ModTime().Add(portFileTimeout)) {
		port = getVtStartPort()
	} else {
		parsedPort, err := strconv.ParseInt(string(portBytes), 10, 64)
		if err != nil {
			panic(err)
		}
		port = int(parsedPort)
	}

	portFile.Truncate(0)
	portFile.Seek(0, 0)
	portFile.WriteString(fmt.Sprintf("%v", port+200))
	portFile.Close()
	return port
}

// GetAndReserveTabletUID gives tablet uid
func (cluster *LocalProcessCluster) GetAndReserveTabletUID() int {
	if cluster.BaseTabletUID == 0 {
		if *forceBaseTabletUID > 0 {
			cluster.BaseTabletUID = *forceBaseTabletUID
		} else {
			cluster.BaseTabletUID = getRandomNumber(10000, 0)
		}
	}
	cluster.BaseTabletUID = cluster.BaseTabletUID + 1
	return cluster.BaseTabletUID
}

func getRandomNumber(maxNumber int32, baseNumber int) int {
	return int(rand.Int31n(maxNumber)) + baseNumber
}

func getVtStartPort() int {
	osVtPort := os.Getenv("VTPORTSTART")
	if osVtPort != "" {
		cport, err := strconv.Atoi(osVtPort)
		if err == nil {
			return cport
		}
	}
	return DefaultStartPort
}

// NewVttabletInstance creates a new vttablet object
func (cluster *LocalProcessCluster) NewVttabletInstance(tabletType string, UID int, cell string) *Vttablet {
	if UID == 0 {
		UID = cluster.GetAndReserveTabletUID()
	}
	if cell == "" {
		cell = cluster.Cell
	}
	return &Vttablet{
		TabletUID: UID,
		HTTPPort:  cluster.GetAndReservePort(),
		GrpcPort:  cluster.GetAndReservePort(),
		MySQLPort: cluster.GetAndReservePort(),
		Type:      tabletType,
		Cell:      cell,
		Alias:     fmt.Sprintf("%s-%010d", cell, UID),
	}
}

// NewVTOrcProcess creates a new VTOrcProcess object
func (cluster *LocalProcessCluster) NewVTOrcProcess(config VTOrcConfiguration) *VTOrcProcess {
	base := VtctlProcessInstance(cluster.TopoProcess.Port, cluster.Hostname)
	base.Binary = "vtorc"
	return &VTOrcProcess{
		VtctlProcess: *base,
		LogDir:       cluster.TmpDirectory,
		Config:       config,
		WebPort:      cluster.GetAndReservePort(),
		Port:         cluster.GetAndReservePort(),
	}
}

// NewVtgrProcess creates a new VtgrProcess object
func (cluster *LocalProcessCluster) NewVtgrProcess(clusters []string, config string, grPort int) *VtgrProcess {
	base := VtctlProcessInstance(cluster.TopoProcess.Port, cluster.Hostname)
	base.Binary = "vtgr"
	return &VtgrProcess{
		VtctlProcess: *base,
		LogDir:       cluster.TmpDirectory,
		clusters:     clusters,
		config:       config,
		grPort:       grPort,
	}
}

// VtprocessInstanceFromVttablet creates a new vttablet object
func (cluster *LocalProcessCluster) VtprocessInstanceFromVttablet(tablet *Vttablet, shardName string, ksName string) *VttabletProcess {
	return VttabletProcessInstance(
		tablet.HTTPPort,
		tablet.GrpcPort,
		tablet.TabletUID,
		cluster.Cell,
		shardName,
		ksName,
		cluster.VtctldProcess.Port,
		tablet.Type,
		cluster.TopoProcess.Port,
		cluster.Hostname,
		cluster.TmpDirectory,
		cluster.VtTabletExtraArgs,
		cluster.DefaultCharset)
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
		cluster.DefaultCharset)

	tablet.VttabletProcess.SupportsBackup = supportBackup
	tablet.VttabletProcess.ServingStatus = servingStatus
	return tablet.VttabletProcess.Setup()
}

// TopoFlavorString returns the topo flavor
func (cluster *LocalProcessCluster) TopoFlavorString() *string {
	if cluster.TopoFlavor != "" {
		return &cluster.TopoFlavor
	}
	return topoFlavor
}

func getCoveragePath(fileName string) string {
	covDir := os.Getenv("COV_DIR")
	if covDir == "" {
		covDir = os.TempDir()
	}
	return path.Join(covDir, fileName)
}

// PrintMysqlctlLogFiles prints all the log files associated with the mysqlctl binary
func (cluster *LocalProcessCluster) PrintMysqlctlLogFiles() {
	logDir := cluster.TmpDirectory
	files, _ := os.ReadDir(logDir)
	for _, fileInfo := range files {
		if !fileInfo.IsDir() && strings.Contains(fileInfo.Name(), "mysqlctl") {
			log.Errorf("Printing the log file - " + fileInfo.Name())
			logOut, _ := os.ReadFile(path.Join(logDir, fileInfo.Name()))
			log.Errorf(string(logOut))
		}
	}
}

// GetVTParams returns mysql.ConnParams with host and port only for regular tests enabling global routing,
// and also dbname if we are testing for a cluster with a partially moved keyspace
func (cluster *LocalProcessCluster) GetVTParams(dbname string) mysql.ConnParams {
	params := mysql.ConnParams{
		Host: cluster.Hostname,
		Port: cluster.VtgateMySQLPort,
	}
	if cluster.HasPartialKeyspaces {
		params.DbName = dbname
	}
	return params
}

// DisableVTOrcRecoveries stops all VTOrcs from running any recoveries
func (cluster *LocalProcessCluster) DisableVTOrcRecoveries(t *testing.T) {
	for _, vtorc := range cluster.VTOrcProcesses {
		vtorc.DisableGlobalRecoveries(t)
	}
}

// EnableVTOrcRecoveries allows all VTOrcs to run any recoveries
func (cluster *LocalProcessCluster) EnableVTOrcRecoveries(t *testing.T) {
	for _, vtorc := range cluster.VTOrcProcesses {
		vtorc.EnableGlobalRecoveries(t)
	}
}
