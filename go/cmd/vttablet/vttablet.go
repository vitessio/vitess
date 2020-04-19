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

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"
	"io/ioutil"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/yaml2"
)

var (
	enforceTableACLConfig        = flag.Bool("enforce-tableacl-config", false, "if this flag is true, vttablet will fail to start if a valid tableacl config does not exist")
	tableACLConfig               = flag.String("table-acl-config", "", "path to table access checker config file; send SIGHUP to reload this file")
	tableACLConfigReloadInterval = flag.Duration("table-acl-config-reload-interval", 0, "Ticker to reload ACLs")
	tabletPath                   = flag.String("tablet-path", "", "tablet alias")
	tabletConfig                 = flag.String("tablet_config", "", "YAML file config for tablet")

	agent *tabletmanager.ActionAgent
)

func init() {
	servenv.RegisterDefaultFlags()
}

func main() {
	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()

	servenv.ParseFlags("vttablet")

	config := tabletenv.NewCurrentConfig()
	if err := config.Verify(); err != nil {
		log.Exitf("invalid config: %v", err)
	}

	tabletenv.Init()
	if *tabletConfig != "" {
		bytes, err := ioutil.ReadFile(*tabletConfig)
		if err != nil {
			log.Exitf("error reading config file %s: %v", *tabletConfig, err)
		}
		if err := yaml2.Unmarshal(bytes, config); err != nil {
			log.Exitf("error parsing config file %s: %v", bytes, err)
		}
	}
	gotBytes, _ := yaml2.Marshal(config)
	log.Infof("Loaded config file %s successfully:\n%s", *tabletConfig, gotBytes)

	servenv.Init()

	if *tabletPath == "" {
		log.Exit("-tablet-path required")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(*tabletPath)
	if err != nil {
		log.Exitf("failed to parse -tablet-path: %v", err)
	}

	var mycnf *mysqlctl.Mycnf
	var socketFile string
	// If no connection parameters were specified, load the mycnf file
	// and use the socket from it. If connection parameters were specified,
	// we assume that the mysql is not local, and we skip loading mycnf.
	// This also means that backup and restore will not be allowed.
	if !config.DB.HasGlobalSettings() {
		var err error
		if mycnf, err = mysqlctl.NewMycnfFromFlags(tabletAlias.Uid); err != nil {
			log.Exitf("mycnf read failed: %v", err)
		}
		socketFile = mycnf.SocketFile
	} else {
		log.Info("connection parameters were specified. Not loading my.cnf.")
	}

	// If connection parameters were specified, socketFile will be empty.
	// Otherwise, the socketFile (read from mycnf) will be used to initialize
	// dbconfigs.
	config.DB = config.DB.Init(socketFile)

	if *tableACLConfig != "" {
		// To override default simpleacl, other ACL plugins must set themselves to be default ACL factory
		tableacl.Register("simpleacl", &simpleacl.Factory{})
	} else if *enforceTableACLConfig {
		log.Exit("table acl config has to be specified with table-acl-config flag because enforce-tableacl-config is set.")
	}

	// creates and registers the query service
	ts := topo.Open()
	qsc := tabletserver.NewTabletServer("", config, ts, *tabletAlias)
	servenv.OnRun(func() {
		qsc.Register()
		addStatusParts(qsc)
	})
	servenv.OnClose(func() {
		// We now leave the queryservice running during lameduck,
		// so stop it in OnClose(), after lameduck is over.
		qsc.StopService()
	})

	qsc.InitACL(*tableACLConfig, *enforceTableACLConfig, *tableACLConfigReloadInterval)

	// Create mysqld and register the health reporter (needs to be done
	// before initializing the agent, so the initial health check
	// done by the agent has the right reporter)
	mysqld := mysqlctl.NewMysqld(config.DB)
	servenv.OnClose(mysqld.Close)

	// Depends on both query and updateStream.
	gRPCPort := int32(0)
	if servenv.GRPCPort != nil {
		gRPCPort = int32(*servenv.GRPCPort)
	}
	agent, err = tabletmanager.NewActionAgent(context.Background(), ts, mysqld, qsc, tabletAlias, config.DB, mycnf, int32(*servenv.Port), gRPCPort)
	if err != nil {
		log.Exitf("NewActionAgent() failed: %v", err)
	}

	servenv.OnClose(func() {
		// Close the agent so that our topo entry gets pruned properly and any
		// background goroutines that use the topo connection are stopped.
		agent.Close()

		// We will still use the topo server during lameduck period
		// to update our state, so closing it in OnClose()
		ts.Close()
	})

	servenv.RunDefault()
}
