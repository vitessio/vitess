/*
Copyright 2017 Google Inc.

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

// Package vtqueryserver is a standalone version of the tablet server that
// only implements the queryservice interface without any of the topology,
// replication management, or other features of the full vttablet.
package vtqueryserver

import (
	"flag"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlproxy"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	mysqlProxy *mysqlproxy.Proxy
	target     = querypb.Target{
		TabletType: topodatapb.TabletType_MASTER,
		Keyspace:   "",
	}

	targetKeyspace   = flag.String("target", "", "Target database name")
	normalizeQueries = flag.Bool("normalize_queries", true, "Rewrite queries with bind vars. Turn this off if the app itself sends normalized queries with bind vars.")
	allowUnsafeDMLs  = flag.Bool("allow_unsafe_dmls", false, "Allow passthrough DML statements when running with statement-based replication")
)

func initProxy(dbcfgs *dbconfigs.DBConfigs) (*tabletserver.TabletServer, error) {
	target.Keyspace = *targetKeyspace
	log.Infof("initalizing vtqueryserver.Proxy for target %s", target.Keyspace)

	// creates and registers the query service
	qs := tabletserver.NewTabletServerWithNilTopoServer(tabletenv.Config)
	qs.SetAllowUnsafeDMLs(*allowUnsafeDMLs)
	mysqlProxy = mysqlproxy.NewProxy(&target, qs, *normalizeQueries)

	err := qs.StartService(target, *dbcfgs)
	if err != nil {
		return nil, err
	}

	return qs, nil
}

// Init initializes the proxy
func Init(dbcfgs *dbconfigs.DBConfigs) error {
	qs, err := initProxy(dbcfgs)
	if err != nil {
		return err
	}

	servenv.OnRun(func() {
		qs.Register()
		addStatusParts(qs)
	})

	servenv.OnClose(func() {
		// We now leave the queryservice running during lameduck,
		// so stop it in OnClose(), after lameduck is over.
		qs.StopService()
	})

	return nil
}
