// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"
	"fmt"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

var (
	tabletPath     = flag.String("tablet-path", "", "tablet alias or path to zk node representing the tablet")
	enableRowcache = flag.Bool("enable-rowcache", false, "enable rowcacche")
	overridesFile  = flag.String("schema-override", "", "schema overrides file")
	tableAclConfig = flag.String("table-acl-config", "", "path to table access checker config file")
	lockTimeout    = flag.Duration("lock_timeout", actionnode.DefaultLockTimeout, "lock time for wrangler/topo operations")

	agent *tabletmanager.ActionAgent
)

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterDefaultSecureFlags()
	servenv.InitServiceMapForBsonRpcService("tabletmanager")
	servenv.InitServiceMapForBsonRpcService("queryservice")
	servenv.InitServiceMapForBsonRpcService("updatestream")
}

// tabletParamToTabletAlias takes either an old style ZK tablet path or a
// new style tablet alias as a string, and returns a TabletAlias.
func tabletParamToTabletAlias(param string) (topo.TabletAlias, error) {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style string tablet alias
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 6 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[3] != "vt" || zkPathParts[4] != "tablets" {
			return topo.TabletAlias{}, fmt.Errorf("invalid tablet path: %v", param)
		}
		param = zkPathParts[2] + "-" + zkPathParts[5]
	}
	result, err := topo.ParseTabletAliasString(param)
	if err != nil {
		return topo.TabletAlias{}, fmt.Errorf("invalid tablet alias %v: %v", param, err)
	}
	return result, nil
}

func main() {
	defer exit.Recover()

	flags := dbconfigs.AppConfig | dbconfigs.DbaConfig |
		dbconfigs.FilteredConfig | dbconfigs.ReplConfig
	dbconfigs.RegisterFlags(flags)
	mysqlctl.RegisterFlags()
	flag.Parse()
	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Errorf("vttablet doesn't take any positional arguments")
		exit.Return(1)
	}

	servenv.Init()

	if *tabletPath == "" {
		log.Errorf("tabletPath required")
		exit.Return(1)
	}
	tabletAlias, err := tabletParamToTabletAlias(*tabletPath)
	if err != nil {
		log.Error(err)
		exit.Return(1)
	}

	mycnf, err := mysqlctl.NewMycnfFromFlags(tabletAlias.Uid)
	if err != nil {
		log.Errorf("mycnf read failed: %v", err)
		exit.Return(1)
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, flags)
	if err != nil {
		log.Warning(err)
	}
	dbcfgs.App.EnableRowcache = *enableRowcache

	if *tableAclConfig != "" {
		tableacl.Init(*tableAclConfig)
	}
	tabletserver.InitQueryService()
	binlog.RegisterUpdateStreamService(mycnf)

	// Depends on both query and updateStream.
	agent, err = tabletmanager.NewActionAgent(context.Background(), tabletAlias, dbcfgs, mycnf, *servenv.Port, *servenv.SecurePort, *overridesFile, *lockTimeout)
	if err != nil {
		log.Error(err)
		exit.Return(1)
	}

	tabletmanager.HttpHandleSnapshots(mycnf, tabletAlias.Uid)
	servenv.OnTerm(func() {
		tabletserver.DisallowQueries()
		binlog.DisableUpdateStreamService()
		agent.Stop()
	})
	servenv.OnClose(func() {
		// We will still use the topo server during lameduck period
		// to update our state, so closing it in OnClose()
		topo.CloseServers()
	})
	servenv.RunDefault()
}
