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
	"bytes"
	"flag"
	"io/ioutil"

	"context"

	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/onlineddl"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/yaml2"

	rice "github.com/GeertJohan/go.rice"
)

var (
	enforceTableACLConfig        = flag.Bool("enforce-tableacl-config", false, "if this flag is true, vttablet will fail to start if a valid tableacl config does not exist")
	tableACLConfig               = flag.String("table-acl-config", "", "path to table access checker config file; send SIGHUP to reload this file")
	tableACLConfigReloadInterval = flag.Duration("table-acl-config-reload-interval", 0, "Ticker to reload ACLs. Duration flag, format e.g.: 30s. Default: do not reload")
	tabletPath                   = flag.String("tablet-path", "", "tablet alias")
	tabletConfig                 = flag.String("tablet_config", "", "YAML file config for tablet")

	tm *tabletmanager.TabletManager
)

func init() {
	servenv.RegisterDefaultFlags()
}

func main() {
	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()

	servenv.ParseFlags("vttablet")
	servenv.Init()

	if *tabletPath == "" {
		log.Exit("-tablet-path required")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(*tabletPath)
	if err != nil {
		log.Exitf("failed to parse -tablet-path: %v", err)
	}

	// config and mycnf initializations are intertwined.
	config, mycnf := initConfig(tabletAlias)

	ts := topo.Open()
	qsc := createTabletServer(config, ts, tabletAlias)

	mysqld := mysqlctl.NewMysqld(config.DB)
	servenv.OnClose(mysqld.Close)

	if err := extractOnlineDDL(); err != nil {
		log.Exitf("failed to extract online DDL binaries: %v", err)
	}

	// Initialize and start tm.
	gRPCPort := int32(0)
	if servenv.GRPCPort != nil {
		gRPCPort = int32(*servenv.GRPCPort)
	}
	tablet, err := tabletmanager.BuildTabletFromInput(tabletAlias, int32(*servenv.Port), gRPCPort)
	if err != nil {
		log.Exitf("failed to parse -tablet-path: %v", err)
	}
	tm = &tabletmanager.TabletManager{
		BatchCtx:            context.Background(),
		TopoServer:          ts,
		Cnf:                 mycnf,
		MysqlDaemon:         mysqld,
		DBConfigs:           config.DB.Clone(),
		QueryServiceControl: qsc,
		UpdateStream:        binlog.NewUpdateStream(ts, tablet.Keyspace, tabletAlias.Cell, qsc.SchemaEngine()),
		VREngine:            vreplication.NewEngine(config, ts, tabletAlias.Cell, mysqld, qsc.LagThrottler()),
	}
	if err := tm.Start(tablet, config.Healthcheck.IntervalSeconds.Get()); err != nil {
		log.Exitf("failed to parse -tablet-path or initialize DB credentials: %v", err)
	}
	servenv.OnClose(func() {
		// Close the tm so that our topo entry gets pruned properly and any
		// background goroutines that use the topo connection are stopped.
		tm.Close()

		// tm uses ts. So, it should be closed after tm.
		ts.Close()
	})

	servenv.RunDefault()
}

func initConfig(tabletAlias *topodatapb.TabletAlias) (*tabletenv.TabletConfig, *mysqlctl.Mycnf) {
	tabletenv.Init()
	// Load current config after tabletenv.Init, because it changes it.
	config := tabletenv.NewCurrentConfig()
	if err := config.Verify(); err != nil {
		log.Exitf("invalid config: %v", err)
	}

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
	config.DB.InitWithSocket(socketFile)
	for _, cfg := range config.ExternalConnections {
		cfg.InitWithSocket("")
	}
	return config, mycnf
}

// extractOnlineDDL extracts the gh-ost binary from this executable. gh-ost is appended
// to vttablet executable by `make build` and via ricebox
func extractOnlineDDL() error {
	riceBox, err := rice.FindBox("../../../resources/bin")
	if err != nil {
		return err
	}

	if binaryFileName, isOverride := onlineddl.GhostBinaryFileName(); !isOverride {
		// there is no path override for gh-ost. We're expected to auto-extract gh-ost.
		ghostBinary, err := riceBox.Bytes("gh-ost")
		if err != nil {
			return err
		}
		if err := ioutil.WriteFile(binaryFileName, ghostBinary, 0755); err != nil {
			// One possibility of failure is that gh-ost is up and running. In that case,
			// let's pause and check if the running gh-ost is exact same binary as the one we wish to extract.
			foundBytes, _ := ioutil.ReadFile(binaryFileName)
			if bytes.Equal(ghostBinary, foundBytes) {
				// OK, it's the same binary, there is no need to extract the file anyway
				return nil
			}
			return err
		}
	}

	return nil
}

func createTabletServer(config *tabletenv.TabletConfig, ts *topo.Server, tabletAlias *topodatapb.TabletAlias) *tabletserver.TabletServer {
	if *tableACLConfig != "" {
		// To override default simpleacl, other ACL plugins must set themselves to be default ACL factory
		tableacl.Register("simpleacl", &simpleacl.Factory{})
	} else if *enforceTableACLConfig {
		log.Exit("table acl config has to be specified with table-acl-config flag because enforce-tableacl-config is set.")
	}
	// creates and registers the query service
	qsc := tabletserver.NewTabletServer("", config, ts, *tabletAlias)
	servenv.OnRun(func() {
		qsc.Register()
		addStatusParts(qsc)
	})
	servenv.OnClose(qsc.StopService)
	qsc.InitACL(*tableACLConfig, *enforceTableACLConfig, *tableACLConfigReloadInterval)
	return qsc
}
