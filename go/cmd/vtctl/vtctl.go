// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/vitess/go/vt/naming"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	wr "code.google.com/p/vitess/go/vt/wrangler"
	"code.google.com/p/vitess/go/zk"
	"code.google.com/p/vitess/go/relog"
	"launchpad.net/gozk/zookeeper"
)

var usage = `
Commands:

Tablets:
  InitTablet <zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> <zk parent alias>

  ScrapTablet <zk tablet path>
    -force writes the scrap state in to zk, no questions asked, if a tablet is offline.

  SetReadOnly [<zk tablet path> | <zk shard/tablet path>]

  SetReadWrite [<zk tablet path> | <zk shard/tablet path>]

  DemoteMaster <zk tablet path>

  ChangeType <zk tablet path> <db type>
    Change the db type for this tablet if possible. this is mostly for arranging
    replicas - it will not convert a master.
    NOTE: This will automatically update the serving graph.

  Ping <zk tablet path>
    check that the agent is awake and responding - can be blocked by other in-flight
    operations.


Shards:
  RebuildShard <zk shard path>
    rebuild the shard, this may trigger an update to all connected clients

  ReparentShard <zk shard path> <zk tablet path>
    specify which shard to reparent and which tablet should be the new master


Generic:
  PurgeActions <zk action path>
    remove all actions - be careful, this is powerful cleanup magic

  WaitForAction <zk action path>
    watch an action node, printing updates, until the action is complete

  Resolve <keyspace>.<shard>.<db type>
    read a list of addresses that can answer this query

  Validate <zk vt path>
    validate that all nodes in this cell are consistent with the global replication graph

  ExportZkns <zk vt path>
    export the serving graph entries to the legacy zkns format

  ListIdle <zk vt path>
    list all idle tablet paths

  ListScrap <zk vt path>
    list all scrap tablet paths
`

var noWaitForAction = flag.Bool("no-wait", false,
	"don't wait for action completion, detach")
var waitTime = flag.Duration("wait-time", 24*time.Hour, "time to wait on an action")
var force = flag.Bool("force", false, "force action")
var verbose = flag.Bool("verbose", false, "verbose logging")
var pingTablets = flag.Bool("ping-tablets", false, "ping all tablets during validate")
var logLevel = flag.String("log.level", "WARNING", "set log level")
var stdin *bufio.Reader

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)

	}
	stdin = bufio.NewReader(os.Stdin)
}

func confirm(prompt string) bool {
	if *force {
		return true
	}
	fmt.Fprintf(os.Stderr, prompt+" [NO/yes] ")

	line, _ := stdin.ReadString('\n')
	return strings.ToLower(strings.TrimSpace(line)) == "yes"
}

func initTablet(zconn zk.Conn, path, hostname, mysqlPort, vtPort, keyspace, shardId, tabletType, parentAlias string, update bool) error {
	tm.MustBeTabletPath(path)

	pathParts := strings.Split(path, "/")
	cell := zk.ZkCellFromZkPath(path)
	uid, err := strconv.Atoi(pathParts[len(pathParts)-1])
	if err != nil {
		panic(err)
	}

	parent := tm.TabletAlias{}
	if parentAlias != "" {
		parent.Cell, parent.Uid = tm.ParseTabletReplicationPath(parentAlias)
	}

	tablet := tm.NewTablet(cell, uint(uid), parent, fmt.Sprintf("%v:%v", hostname, vtPort), fmt.Sprintf("%v:%v", hostname, mysqlPort), keyspace, shardId, tm.TabletType(tabletType))
	err = tm.CreateTablet(zconn, path, tablet)
	if err != nil {
		if zkErr, ok := err.(*zookeeper.Error); ok && zkErr.Code == zookeeper.ZNODEEXISTS {
			if update {
				oldTablet, err := tm.ReadTablet(zconn, path)
				if err != nil {
					relog.Warning("failed reading tablet %v: %v", path, err)
				} else {
					if oldTablet.Keyspace == tablet.Keyspace && oldTablet.Shard == tablet.Shard {
						*(oldTablet.Tablet) = *tablet
						err := tm.UpdateTablet(zconn, path, oldTablet)
						if err != nil {
							relog.Warning("failed reading tablet %v: %v", path, err)
						} else {
							return nil
						}
					}
				}
			}
			if *force {
				zk.DeleteRecursive(zconn, path, -1)
				err = tm.CreateTablet(zconn, path, tablet)
			}
		}
	}

	return err
}

func purgeActions(zconn zk.Conn, zkActionPath string) error {
	if path.Base(zkActionPath) != "action" {
		panic(fmt.Errorf("not action path: %v", zkActionPath))
	}

	children, _, err := zconn.Children(zkActionPath)
	if err != nil {
		return err
	}
	for _, child := range children {
		err = zk.DeleteRecursive(zconn, path.Join(zkActionPath, child), -1)
		if err != nil {
			return err
		}
	}
	return nil
}

func changeType(zconn zk.Conn, ai *tm.ActionInitiator, zkTabletPath, dbType string) error {
	if *force {
		return tm.ChangeType(zconn, zkTabletPath, tm.TabletType(dbType))
	} else {
		actionPath, err := ai.ChangeType(zkTabletPath, tm.TabletType(dbType))
		if err != nil {
			return err
		}

		// You don't have a choice - you must wait for completion before rebuilding.
		err = ai.WaitForCompletion(actionPath, *waitTime)
		if err != nil {
			return err
		}
	}

	tabletInfo, err := tm.ReadTablet(zconn, zkTabletPath)
	if err != nil {
		relog.Warning("%v: %v", zkTabletPath, err)
	}
	err = tm.RebuildShard(zconn, tabletInfo.ShardPath())
	return err
}

func getTabletMap(zconn zk.Conn, tabletPaths []string) map[string]*tm.TabletInfo {
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	tabletMap := make(map[string]*tm.TabletInfo)

	for _, path := range tabletPaths {
		tabletPath := path
		wg.Add(1)
		go func() {
			tabletInfo, err := tm.ReadTablet(zconn, tabletPath)
			if err != nil {
				relog.Warning("%v: %v", tabletPath, err)
			} else {
				mutex.Lock()
				tabletMap[tabletPath] = tabletInfo
				mutex.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()

	mutex.Lock()
	defer mutex.Unlock()
	return tabletMap
}

func listScrap(zconn zk.Conn, zkVtPath string) error {
	zkTabletsPath := path.Join(zkVtPath, "tablets")
	children, _, err := zconn.Children(zkTabletsPath)
	if err != nil {
		return err
	}

	sort.Strings(children)
	tabletPaths := make([]string, len(children))
	for i, child := range children {
		tabletPaths[i] = path.Join(zkTabletsPath, child)
	}

	tabletMap := getTabletMap(zconn, tabletPaths)

	for _, tabletPath := range tabletPaths {
		tabletInfo, ok := tabletMap[tabletPath]
		if ok && tabletInfo.Type == tm.TYPE_SCRAP {
			fmt.Println(tabletPath)
		}
	}

	return nil
}

func listIdle(zconn zk.Conn, zkVtPath string) error {
	zkTabletsPath := path.Join(zkVtPath, "tablets")
	children, _, err := zconn.Children(zkTabletsPath)
	if err != nil {
		return err
	}

	sort.Strings(children)
	tabletPaths := make([]string, len(children))
	for i, child := range children {
		tabletPaths[i] = path.Join(zkTabletsPath, child)
	}

	tabletMap := getTabletMap(zconn, tabletPaths)

	for _, tabletPath := range tabletPaths {
		tabletInfo, ok := tabletMap[tabletPath]
		if ok && tabletInfo.Type == tm.TYPE_IDLE {
			fmt.Println(tabletPath)
		}
	}

	return nil
}

func validateZk(zconn zk.Conn, ai *tm.ActionInitiator, zkVtPath string) error {
	// FIXME(msolomon) validate the replication view
	zkTabletsPath := path.Join(zkVtPath, "tablets")
	tabletUids, _, err := zconn.Children(zkTabletsPath)
	if err != nil {
		return err
	}
	someErrors := false
	for _, tabletUid := range tabletUids {
		tabletPath := path.Join(zkTabletsPath, tabletUid)
		relog.Info("checking tablet %v", tabletPath)
		err = tm.Validate(zconn, tabletPath, "")
		if err != nil {
			someErrors = true
			relog.Error("%v: %v", tabletPath, err)
		}
	}

	zkKeyspacesPath := path.Join("/zk/global/vt/keyspaces")
	keyspaces, _, err := zconn.Children(zkKeyspacesPath)
	if err != nil {
		return err
	}
	for _, keyspace := range keyspaces {
		zkShardsPath := path.Join(zkKeyspacesPath, keyspace, "shards")
		shards, _, err := zconn.Children(zkShardsPath)
		if err != nil {
			return err
		}
		for _, shard := range shards {
			zkShardPath := path.Join(zkShardsPath, shard)
			shardInfo, err := tm.ReadShard(zconn, zkShardPath)
			if err != nil {
				return err
			}
			aliases, err := tm.FindAllTabletAliasesInShard(zconn, shardInfo)
			if err != nil {
				return err
			}
			var masterAlias tm.TabletAlias
			shardTablets := make([]string, 0, 16)
			for _, alias := range aliases {
				shardTablets = append(shardTablets, tm.TabletPathForAlias(alias))
			}

			tabletMap := getTabletMap(zconn, shardTablets)
			for _, alias := range aliases {
				zkTabletPath := tm.TabletPathForAlias(alias)

				tabletInfo, ok := tabletMap[zkTabletPath]
				if !ok {
					continue
				}
				if tabletInfo.Parent.Uid == tm.NO_TABLET {
					if masterAlias.Cell != "" {
						someErrors = true
						relog.Error("%v: already have a master %v", zkTabletPath, masterAlias)
					} else {
						masterAlias = alias
					}
				}
			}

			// Need the master for this loop.
			for _, alias := range aliases {
				zkTabletPath := tm.TabletPathForAlias(alias)
				zkTabletReplicationPath := zkShardPath + "/" + masterAlias.String()
				if alias != masterAlias {
					zkTabletReplicationPath += "/" + alias.String()
				}

				err = tm.Validate(zconn, zkTabletPath, zkTabletReplicationPath)
				if err != nil {
					someErrors = true
					relog.Error("%v: %v", zkTabletReplicationPath, err)
				}
			}

			if !*pingTablets {
				continue
			}

			pingPaths := make([]string, 0, 128)
			for _, alias := range aliases {
				zkTabletPath := tm.TabletPathForAlias(alias)
				tabletInfo := tabletMap[zkTabletPath]
				zkTabletPid := path.Join(zkTabletPath, "pid")
				_, _, err := zconn.Get(zkTabletPid)
				if err != nil {
					someErrors = true
					relog.Error("no pid node %v: %v %v", zkTabletPid, err, tabletInfo.Hostname())
					continue
				}

				actionPath, err := ai.Ping(zkTabletPath)
				if err != nil {
					someErrors = true
					relog.Error("%v: %v %v", actionPath, err, tabletInfo.Hostname())
				} else {
					pingPaths = append(pingPaths, actionPath)
				}
			}
			// FIXME(msolomon) this should be parallel
			for _, actionPath := range pingPaths {
				err := ai.WaitForCompletion(actionPath, *waitTime)
				if err != nil {
					someErrors = true
					relog.Error("%v: %v", actionPath, err)
				}
			}
		}
	}

	if someErrors {
		return fmt.Errorf("some validation errors - see log")
	}
	return nil
}

func main() {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			relog.Fatal("%v", relog.NewPanicError(panicErr.(error)).String())
		}
	}()

	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	logger := relog.New(os.Stderr, "vtctl ",
		log.Ldate|log.Lmicroseconds|log.Lshortfile,
		relog.LogNameToLogLevel(*logLevel))
	relog.SetLogger(logger)

	zconn := zk.NewMetaConn(5e9)
	defer zconn.Close()

	ai := tm.NewActionInitiator(zconn)
	wrangler := wr.NewWrangler(zconn, ai)
	var actionPath string
	var err error

	switch args[0] {
	case "InitTablet":
		if len(args) != 9 {
			relog.Fatal("action %v requires 8 args", args[0])
		}
		err = initTablet(zconn, args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], false)
	case "UpdateTablet":
		if len(args) != 9 {
			relog.Fatal("action %v requires 8 args", args[0])
		}
		err = initTablet(zconn, args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], true)
	case "Ping":
		if len(args) != 2 {
			relog.Fatal("action %v requires args", args[0])
		}
		actionPath, err = ai.Ping(args[1])
	case tm.TABLET_ACTION_SET_RDONLY:
		if len(args) != 2 {
			relog.Fatal("action %v requires args", args[0])
		}
		actionPath, err = ai.SetReadOnly(args[1])
	case tm.TABLET_ACTION_SET_RDWR:
		if len(args) != 2 {
			relog.Fatal("action %v requires args", args[0])
		}
		actionPath, err = ai.SetReadWrite(args[1])
	case "ChangeType":
		if len(args) != 3 {
			relog.Fatal("action %v requires <zk tablet path> <db type>", args[0])
		}
		err = changeType(zconn, ai, args[1], args[2])
	case "DemoteMaster":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk tablet path>", args[0])
		}
		actionPath, err = ai.DemoteMaster(args[1])
	case "PurgeActions":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		err = purgeActions(zconn, args[1])
	case "RebuildShard":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		err = tm.RebuildShard(zconn, args[1])
	case "ReparentShard":
		if len(args) != 3 {
			relog.Fatal("action %v requires <zk shard path> <zk tablet path>", args[0])
		}
		actionPath, err = wrangler.ReparentShard(args[1], args[2], *force)
	case "ExportZkns":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt root path>", args[0])
		}
		err = exportZkns(zconn, args[1])
	case "Resolve":
		if len(args) != 2 {
			relog.Fatal("action %v requires <keyspace>.<shard>.<db type>:<port name>", args[0])
		}
		parts := strings.Split(args[1], ":")
		if len(parts) != 2 {
			relog.Fatal("action %v requires <keyspace>.<shard>.<db type>:<port name>", args[0])
		}
		namedPort := parts[1]

		parts = strings.Split(parts[0], ".")
		if len(parts) != 3 {
			relog.Fatal("action %v requires <keyspace>.<shard>.<db type>:<port name>", args[0])
		}

		addrs, lookupErr := naming.LookupVtName(zconn, "", parts[0], parts[1], parts[2], namedPort)
		if lookupErr == nil {
			for _, addr := range addrs {
				fmt.Printf("%v:%v\n", addr.Target, addr.Port)
			}
		} else {
			err = lookupErr
		}
	case "ScrapTablet":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk tablet path>", args[0])
		}
		if *force {
			err = tm.Scrap(zconn, args[1], *force)
		} else {
			actionPath, err = ai.Scrap(args[1])
		}
	case "Validate":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt path>", args[0])
		}
		err = validateZk(zconn, ai, args[1])
	case "ListScrap":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt path>", args[0])
		}
		err = listScrap(zconn, args[1])
	case "ListIdle":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt path>", args[0])
		}
		err = listIdle(zconn, args[1])
	case "WaitForAction":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk action path>", args[0])
		}
		actionPath = args[1]
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %#v\n\n", args[0])
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		relog.Fatal("action failed: %v %v", args[0], err)
	}
	if actionPath != "" {
		relog.Info("action created: %v", actionPath)
		if !*noWaitForAction {
			err := ai.WaitForCompletion(actionPath, *waitTime)
			if err != nil {
				relog.Fatal(err.Error())
			} else {
				relog.Info("action completed: %v", actionPath)
			}
		}
	}
}
