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

// vttestserver allows users to spawn a self-contained Vitess server for local testing/CI.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttest"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

type topoFlags struct {
	cells     []string
	keyspaces []string
	shards    []string
	replicas  int
	rdonly    int
}

var (
	basePort  int
	config    vttest.Config
	doSeed    bool
	mycnf     string
	protoTopo string
	seed      vttest.SeedConfig
	topo      topoFlags
)

func registerFlags(fs *pflag.FlagSet) {
	fs.IntVar(&basePort, "port", 0,
		"Port to use for vtcombo. If this is 0, a random port will be chosen.")

	fs.StringVar(&protoTopo, "proto_topo", "",
		"Define the fake cluster topology as a compact text format encoded"+
			" vttest proto. See vttest.proto for more information.")

	fs.StringVar(&config.SchemaDir, "schema_dir", "",
		"Directory for initial schema files. Within this dir,"+
			" there should be a subdir for each keyspace. Within"+
			" each keyspace dir, each file is executed as SQL"+
			" after the database is created on each shard."+
			" If the directory contains a vschema.json file, it"+
			" will be used as the vschema for the V3 API.")

	fs.StringVar(&config.DefaultSchemaDir, "default_schema_dir", "",
		"Default directory for initial schema files. If no schema is found"+
			" in schema_dir, default to this location.")

	fs.StringVar(&config.DataDir, "data_dir", "",
		"Directory where the data files will be placed, defaults to a random "+
			"directory under /vt/vtdataroot")

	fs.BoolVar(&config.OnlyMySQL, "mysql_only", false,
		"If this flag is set only mysql is initialized."+
			" The rest of the vitess components are not started."+
			" Also, the output specifies the mysql unix socket"+
			" instead of the vtgate port.")

	fs.BoolVar(&config.PersistentMode, "persistent_mode", false,
		"If this flag is set, the MySQL data directory is not cleaned up"+
			" when LocalCluster.TearDown() is called. This is useful for running"+
			" vttestserver as a database container in local developer environments. Note"+
			" that db migration files (--schema_dir option) and seeding of"+
			" random data (--initialize_with_random_data option) will only run during"+
			" cluster startup if the data directory does not already exist. "+
			" Changes to VSchema are persisted across cluster restarts using a simple"+
			" watcher if the --data_dir argument is specified.")

	fs.BoolVar(&doSeed, "initialize_with_random_data", false,
		"If this flag is each table-shard will be initialized"+
			" with random data. See also the 'rng_seed' and 'min_shard_size'"+
			" and 'max_shard_size' flags.")

	fs.IntVar(&seed.RngSeed, "rng_seed", 123,
		"The random number generator seed to use when initializing"+
			" with random data (see also --initialize_with_random_data)."+
			" Multiple runs with the same seed will result with the same"+
			" initial data.")

	fs.IntVar(&seed.MinSize, "min_table_shard_size", 1000,
		"The minimum number of initial rows in a table shard. Ignored if"+
			"--initialize_with_random_data is false. The actual number is chosen"+
			" randomly.")

	fs.IntVar(&seed.MaxSize, "max_table_shard_size", 10000,
		"The maximum number of initial rows in a table shard. Ignored if"+
			"--initialize_with_random_data is false. The actual number is chosen"+
			" randomly")

	fs.Float64Var(&seed.NullProbability, "null_probability", 0.1,
		"The probability to initialize a field with 'NULL' "+
			" if --initialize_with_random_data is true. Only applies to fields"+
			" that can contain NULL values.")

	fs.StringVar(&config.MySQLBindHost, "mysql_bind_host", "localhost",
		"which host to bind vtgate mysql listener to")

	fs.StringVar(&mycnf, "extra_my_cnf", "",
		"extra files to add to the config, separated by ':'")

	fs.StringSliceVar(&topo.cells, "cells", []string{"test"}, "Comma separated list of cells")
	fs.StringSliceVar(&topo.keyspaces, "keyspaces", []string{"test_keyspace"},
		"Comma separated list of keyspaces")
	fs.StringSliceVar(&topo.shards, "num_shards", []string{"2"},
		"Comma separated shard count (one per keyspace)")
	fs.IntVar(&topo.replicas, "replica_count", 2,
		"Replica tablets per shard (includes primary)")
	fs.IntVar(&topo.rdonly, "rdonly_count", 1,
		"Rdonly tablets per shard")

	fs.StringVar(&config.Charset, "charset", "utf8mb4", "MySQL charset")

	fs.StringVar(&config.PlannerVersion, "planner-version", "", "Sets the default planner to use when the session has not changed it. Valid values are: Gen4, Gen4Greedy, Gen4Left2Right")

	fs.StringVar(&config.SnapshotFile, "snapshot_file", "",
		"A MySQL DB snapshot file")

	fs.BoolVar(&config.EnableSystemSettings, "enable_system_settings", true, "This will enable the system settings to be changed per session at the database connection level")

	fs.StringVar(&config.TransactionMode, "transaction_mode", "MULTI", "Transaction mode MULTI (default), SINGLE or TWOPC ")
	fs.Float64Var(&config.TransactionTimeout, "queryserver-config-transaction-timeout", 0, "query server transaction timeout (in seconds), a transaction will be killed if it takes longer than this value")

	fs.StringVar(&config.TabletHostName, "tablet_hostname", "localhost", "The hostname to use for the tablet otherwise it will be derived from OS' hostname")

	fs.StringVar(&config.VSchemaDDLAuthorizedUsers, "vschema_ddl_authorized_users", "", "Comma separated list of users authorized to execute vschema ddl operations via vtgate")

	fs.StringVar(&config.ForeignKeyMode, "foreign_key_mode", "allow", "This is to provide how to handle foreign key constraint in create/alter table. Valid values are: allow, disallow")
	fs.BoolVar(&config.EnableOnlineDDL, "enable_online_ddl", true, "Allow users to submit, review and control Online DDL")
	fs.BoolVar(&config.EnableDirectDDL, "enable_direct_ddl", true, "Allow users to submit direct DDL statements")

	// flags for using an actual topo implementation for vtcombo instead of in-memory topo. useful for test setup where an external topo server is shared across multiple vtcombo processes or other components
	fs.StringVar(&config.ExternalTopoImplementation, "external_topo_implementation", "", "the topology implementation to use for vtcombo process")
	fs.StringVar(&config.ExternalTopoGlobalServerAddress, "external_topo_global_server_address", "", "the address of the global topology server for vtcombo process")
	fs.StringVar(&config.ExternalTopoGlobalRoot, "external_topo_global_root", "", "the path of the global topology data in the global topology server for vtcombo process")

	fs.DurationVar(&config.VtgateTabletRefreshInterval, "tablet_refresh_interval", 10*time.Second, "Interval at which vtgate refreshes tablet information from topology server.")
	acl.RegisterFlags(fs)
}

func init() {
	servenv.OnParseFor("vttestserver", registerFlags)
}

func (t *topoFlags) buildTopology() (*vttestpb.VTTestTopology, error) {
	topo := &vttestpb.VTTestTopology{}
	topo.Cells = t.cells

	keyspaces := t.keyspaces
	shardCounts := t.shards
	if len(keyspaces) != len(shardCounts) {
		return nil, fmt.Errorf("--keyspaces must be same length as --shards")
	}

	for i := range keyspaces {
		name := keyspaces[i]
		numshards, err := strconv.ParseInt(shardCounts[i], 10, 32)
		if err != nil {
			return nil, err
		}

		ks := &vttestpb.Keyspace{
			Name:         name,
			ReplicaCount: int32(t.replicas),
			RdonlyCount:  int32(t.rdonly),
		}

		for _, shardname := range vttest.GetShardNames(int(numshards)) {
			ks.Shards = append(ks.Shards, &vttestpb.Shard{
				Name: shardname,
			})
		}

		topo.Keyspaces = append(topo.Keyspaces, ks)
	}

	return topo, nil
}

// Annoying, but in unit tests, parseFlags gets called multiple times per process
// (anytime startCluster is called), so we need to guard against the second test
// to run failing with, for example:
//
//	flag redefined: log_rotate_max_size
var flagsOnce sync.Once

func parseFlags() (env vttest.Environment, err error) {
	flagsOnce.Do(func() {
		servenv.RegisterFlags()
		servenv.RegisterGRPCServerFlags()
		servenv.RegisterGRPCServerAuthFlags()
		servenv.RegisterServiceMapFlag()
	})

	servenv.ParseFlags("vttestserver")

	if basePort != 0 {
		if config.DataDir == "" {
			env, err = vttest.NewLocalTestEnv("", basePort)
			if err != nil {
				return
			}
		} else {
			env, err = vttest.NewLocalTestEnvWithDirectory("", basePort, config.DataDir)
			if err != nil {
				return
			}
		}
	}

	if protoTopo == "" {
		config.Topology, err = topo.buildTopology()
		if err != nil {
			return
		}
	} else {
		var topology vttestpb.VTTestTopology
		err = prototext.Unmarshal([]byte(protoTopo), &topology)
		if err != nil {
			return
		}
		if len(topology.Cells) == 0 {
			topology.Cells = append(topology.Cells, "test")
		}
		config.Topology = &topology
	}

	if doSeed {
		config.Seed = &seed
	}

	if mycnf != "" {
		config.ExtraMyCnf = strings.Split(mycnf, ":")
	}

	return
}

func main() {
	cluster, err := runCluster()
	servenv.Init()
	if err != nil {
		log.Fatal(err)
	}
	defer cluster.TearDown()

	kvconf := cluster.JSONConfig()
	if err := json.NewEncoder(os.Stdout).Encode(kvconf); err != nil {
		log.Fatal(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}

func runCluster() (vttest.LocalCluster, error) {
	env, err := parseFlags()
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("Starting local cluster...")
	log.Infof("config: %#v", config)
	cluster := vttest.LocalCluster{
		Config: config,
		Env:    env,
	}
	err = cluster.Setup()
	if err != nil {
		return cluster, err
	}

	log.Info("Local cluster started.")

	return cluster, nil
}
