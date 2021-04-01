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

// vttestserver is a native Go implementation of `run_local_server.py`.
// It allows users to spawn a self-contained Vitess server for local testing/CI
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttest"
)

type topoFlags struct {
	cells     string
	keyspaces string
	shards    string
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

func init() {
	flag.IntVar(&basePort, "port", 0,
		"Port to use for vtcombo. If this is 0, a random port will be chosen.")

	flag.StringVar(&protoTopo, "proto_topo", "",
		"Define the fake cluster topology as a compact text format encoded"+
			" vttest proto. See vttest.proto for more information.")

	flag.StringVar(&config.SchemaDir, "schema_dir", "",
		"Directory for initial schema files. Within this dir,"+
			" there should be a subdir for each keyspace. Within"+
			" each keyspace dir, each file is executed as SQL"+
			" after the database is created on each shard."+
			" If the directory contains a vschema.json file, it"+
			" will be used as the vschema for the V3 API.")

	flag.StringVar(&config.DefaultSchemaDir, "default_schema_dir", "",
		"Default directory for initial schema files. If no schema is found"+
			" in schema_dir, default to this location.")

	flag.StringVar(&config.DataDir, "data_dir", "",
		"Directory where the data files will be placed, defaults to a random "+
			"directory under /vt/vtdataroot")

	flag.BoolVar(&config.OnlyMySQL, "mysql_only", false,
		"If this flag is set only mysql is initialized."+
			" The rest of the vitess components are not started."+
			" Also, the output specifies the mysql unix socket"+
			" instead of the vtgate port.")

	flag.BoolVar(&config.PersistentMode, "persistent_mode", false,
		"If this flag is set, the MySQL data directory is not cleaned up"+
			" when LocalCluster.TearDown() is called. This is useful for running"+
			" vttestserver as a database container in local developer environments. Note"+
			" that db migration files (-schema_dir option) and seeding of"+
			" random data (-initialize_with_random_data option) will only run during"+
			" cluster startup if the data directory does not already exist. vschema"+
			" migrations are run every time the cluster starts, since persistence"+
			" for the topology server has not been implemented yet")

	flag.BoolVar(&doSeed, "initialize_with_random_data", false,
		"If this flag is each table-shard will be initialized"+
			" with random data. See also the 'rng_seed' and 'min_shard_size'"+
			" and 'max_shard_size' flags.")

	flag.IntVar(&seed.RngSeed, "rng_seed", 123,
		"The random number generator seed to use when initializing"+
			" with random data (see also --initialize_with_random_data)."+
			" Multiple runs with the same seed will result with the same"+
			" initial data.")

	flag.IntVar(&seed.MinSize, "min_table_shard_size", 1000,
		"The minimum number of initial rows in a table shard. Ignored if"+
			"--initialize_with_random_data is false. The actual number is chosen"+
			" randomly.")

	flag.IntVar(&seed.MaxSize, "max_table_shard_size", 10000,
		"The maximum number of initial rows in a table shard. Ignored if"+
			"--initialize_with_random_data is false. The actual number is chosen"+
			" randomly")

	flag.Float64Var(&seed.NullProbability, "null_probability", 0.1,
		"The probability to initialize a field with 'NULL' "+
			" if --initialize_with_random_data is true. Only applies to fields"+
			" that can contain NULL values.")

	flag.StringVar(&config.MySQLBindHost, "mysql_bind_host", "localhost",
		"which host to bind vtgate mysql listener to")

	flag.StringVar(&mycnf, "extra_my_cnf", "",
		"extra files to add to the config, separated by ':'")

	flag.StringVar(&topo.cells, "cells", "test", "Comma separated list of cells")
	flag.StringVar(&topo.keyspaces, "keyspaces", "test_keyspace",
		"Comma separated list of keyspaces")
	flag.StringVar(&topo.shards, "num_shards", "2",
		"Comma separated shard count (one per keyspace)")
	flag.IntVar(&topo.replicas, "replica_count", 2,
		"Replica tablets per shard (includes master)")
	flag.IntVar(&topo.rdonly, "rdonly_count", 1,
		"Rdonly tablets per shard")

	flag.StringVar(&config.Charset, "charset", "utf8", "MySQL charset")
	flag.StringVar(&config.SnapshotFile, "snapshot_file", "",
		"A MySQL DB snapshot file")

	flag.StringVar(&config.TransactionMode, "transaction_mode", "MULTI", "Transaction mode MULTI (default), SINGLE or TWOPC ")
	flag.Float64Var(&config.TransactionTimeout, "queryserver-config-transaction-timeout", 0, "query server transaction timeout (in seconds), a transaction will be killed if it takes longer than this value")

	flag.StringVar(&config.TabletHostName, "tablet_hostname", "localhost", "The hostname to use for the tablet otherwise it will be derived from OS' hostname")

	flag.BoolVar(&config.InitWorkflowManager, "workflow_manager_init", false, "Enable workflow manager")

	flag.StringVar(&config.VSchemaDDLAuthorizedUsers, "vschema_ddl_authorized_users", "", "Comma separated list of users authorized to execute vschema ddl operations via vtgate")
}

func (t *topoFlags) buildTopology() (*vttestpb.VTTestTopology, error) {
	topo := &vttestpb.VTTestTopology{}
	topo.Cells = strings.Split(t.cells, ",")

	keyspaces := strings.Split(t.keyspaces, ",")
	shardCounts := strings.Split(t.shards, ",")
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

func parseFlags() (env vttest.Environment, err error) {
	flag.Parse()

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
		err = proto.UnmarshalText(protoTopo, &topology)
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
