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

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/naming"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	wr "code.google.com/p/vitess/go/vt/wrangler"
	"code.google.com/p/vitess/go/zk"
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

  Sleep <zk tablet path> <duration>
    block the action queue for the specified duration (mostly for testing)

  Snapshot <zk tablet path>
    Stop mysqld and copy compressed data aside.

  Restore <zk src tablet path> <zk dst tablet path>
    Copy the latest snaphot from the source tablet and restart replication.
    NOTE: This does not wait for replication to catch up. The destination
    tablet must be "idle" to begin with. It will transition to "spare" once
    the restore is complete.

  Clone <zk src tablet path> <zk dst tablet path>
    This performs Snapshot and then Restore.  The advantage of having
    separate actions is that one snapshot can be used for many restores.


Shards:
  RebuildShard <zk shard path>
    rebuild the shard, this may trigger an update to all connected clients

  ReparentShard <zk shard path> <zk tablet path>
    specify which shard to reparent and which tablet should be the new master


Keyspaces:
  CreateKeyspace <zk keyspaces path>/<name> <shard count>
    e.g. CreateKeyspace /zk/global/vt/keyspaces/my_keyspace 4

  RebuildKeyspace <zk keyspace path>
    rebuild the shard, this may trigger an update to all connected clients


Generic:
  PurgeActions <zk action path>
    remove all actions - be careful, this is powerful cleanup magic

  WaitForAction <zk action path>
    watch an action node, printing updates, until the action is complete

  Resolve <keyspace>.<shard>.<db type>
    read a list of addresses that can answer this query

  Validate <zk keyspaces path> (/zk/global/vt/keyspaces)
    validate all nodes reachable from global replication graph are consistent

  ExportZkns <zk local vt path> (/zk/<cell>/vt)
    export the serving graph entries to the legacy zkns format

  ListIdle <zk local vt path>
    list all idle tablet paths

  ListScrap <zk local vt path>
    list all scrap tablet paths

  DumpTablets <zk local vt path>
    list all tablets in an awk-friendly way
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
		fmt.Fprintf(os.Stdout, "Usage of %s:\n", os.Args[0])
		// FIXME(msolomon) PrintDefaults needs to go to stdout
		flag.PrintDefaults()
		fmt.Fprintf(os.Stdout, usage)
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

// this is a placeholder implementation. right now very little information
// is needed for a keyspace.
func createKeyspace(zconn zk.Conn, path string) error {
	tm.MustBeKeyspacePath(path)
	actionPath := tm.KeyspaceActionPath(path)
	_, err := zk.CreateRecursive(zconn, actionPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			if !*force {
				relog.Fatal("keyspace already exists: %v", path)
			}
		} else {
			relog.Fatal("error creating keyspace: %v %v", path, err)
		}
	}

	return nil
}

func getMasterAlias(zconn zk.Conn, zkShardPath string) (string, error) {
	children, _, err := zconn.Children(zkShardPath)
	if err != nil {
		return "", err
	}
	if len(children) > 2 {
		return "", fmt.Errorf("master search failed: %v", zkShardPath)
	}
	for _, child := range children {
		if child == "action" {
			continue
		}
		return path.Join(zkShardPath, child), nil
	}

	panic("unreachable")
}

func initTablet(zconn zk.Conn, zkPath, hostname, mysqlPort, vtPort, keyspace, shardId, tabletType, parentAlias string, update bool) error {
	tm.MustBeTabletPath(zkPath)

	cell := zk.ZkCellFromZkPath(zkPath)
	pathParts := strings.Split(zkPath, "/")
	uid, err := strconv.Atoi(pathParts[len(pathParts)-1])
	if err != nil {
		return err
	}

	parent := tm.TabletAlias{}
	if parentAlias == "" && tm.TabletType(tabletType) != tm.TYPE_MASTER && tm.TabletType(tabletType) != tm.TYPE_IDLE {
		vtRoot := path.Join("/zk/global", tm.VtSubtree(zkPath))
		parentAlias, err = getMasterAlias(zconn, tm.ShardPath(vtRoot, keyspace, shardId))
		if err != nil {
			return err
		}
	}
	if parentAlias != "" {
		parent.Cell, parent.Uid = tm.ParseTabletReplicationPath(parentAlias)
	}

	tablet := tm.NewTablet(cell, uint(uid), parent, fmt.Sprintf("%v:%v", hostname, vtPort), fmt.Sprintf("%v:%v", hostname, mysqlPort), keyspace, shardId, tm.TabletType(tabletType))
	err = tm.CreateTablet(zconn, zkPath, tablet)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			if update {
				oldTablet, err := tm.ReadTablet(zconn, zkPath)
				if err != nil {
					relog.Warning("failed reading tablet %v: %v", zkPath, err)
				} else {
					if oldTablet.Keyspace == tablet.Keyspace && oldTablet.Shard == tablet.Shard {
						*(oldTablet.Tablet) = *tablet
						err := tm.UpdateTablet(zconn, zkPath, oldTablet)
						if err != nil {
							relog.Warning("failed reading tablet %v: %v", zkPath, err)
						} else {
							return nil
						}
					}
				}
			}
			if *force {
				zk.DeleteRecursive(zconn, zkPath, -1)
				err = tm.CreateTablet(zconn, zkPath, tablet)
			}
		}
	}

	return err
}

func changeType(zconn zk.Conn, ai *tm.ActionInitiator, wrangler *wr.Wrangler, zkTabletPath, dbType string) error {
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
		return err
	}

	if _, err := wrangler.RebuildShard(tabletInfo.ShardPath()); err != nil {
		return err
	}

	if _, err := wrangler.RebuildKeyspace(tabletInfo.KeyspacePath()); err != nil {
		return err
	}

	return nil
}

func snapshot(zconn zk.Conn, ai *tm.ActionInitiator, wrangler *wr.Wrangler, zkTabletPath string) error {
	ti, err := tm.ReadTablet(zconn, zkTabletPath)
	if err != nil {
		return err
	}

	rebuildRequired := ti.Tablet.IsServingType()
	originalType := ti.Tablet.Type

	actionPath, err := ai.ChangeType(zkTabletPath, tm.TYPE_BACKUP)
	if err != nil {
		return err
	}

	err = ai.WaitForCompletion(actionPath, *waitTime)
	if err != nil {
		if *force && ti.Tablet.Type == tm.TYPE_MASTER {
			relog.Info("force change type master -> backup: %v", zkTabletPath)
			// There is a legitimate reason to force in the case of a single
			// master.
			ti.Tablet.Type = tm.TYPE_BACKUP
			err = tm.UpdateTablet(zconn, zkTabletPath, ti)
			if err != nil {
				return err
			}
			// Remove the failed action, nothing will proceed until this is removed.
			if err = zconn.Delete(actionPath, -1); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if rebuildRequired {
		if _, err := wrangler.RebuildShard(ti.ShardPath()); err != nil {
			return err
		}

		if _, err := wrangler.RebuildKeyspace(ti.KeyspacePath()); err != nil {
			return err
		}
	}

	actionPath, err = ai.Snapshot(zkTabletPath)
	if err != nil {
		return err
	}
	err = ai.WaitForCompletion(actionPath, *waitTime)
	if err != nil {
		return err
	}

	// Restore type
	relog.Info("change type after snapshot: %v %v", zkTabletPath, originalType)
	actionPath, err = ai.ChangeType(zkTabletPath, originalType)
	if err != nil {
		return err
	}
	err = ai.WaitForCompletion(actionPath, *waitTime)
	if err != nil {
		if *force && ti.Tablet.Parent.Uid == tm.NO_TABLET {
			ti.Tablet.Type = tm.TYPE_MASTER
			relog.Info("force change type backup -> master: %v", zkTabletPath)
			err = tm.UpdateTablet(zconn, zkTabletPath, ti)
			if err != nil {
				return err
			}
			// Remove the failed action, nothing will proceed until this is removed.
			if err = zconn.Delete(actionPath, -1); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if rebuildRequired {
		if _, err := wrangler.RebuildShard(ti.ShardPath()); err != nil {
			return err
		}

		if _, err := wrangler.RebuildKeyspace(ti.KeyspacePath()); err != nil {
			return err
		}
	}

	return nil
}

func restore(zconn zk.Conn, ai *tm.ActionInitiator, wrangler *wr.Wrangler, zkSrcTabletPath, zkDstTabletPath string) error {
	actionPath, err := ai.ChangeType(zkDstTabletPath, tm.TYPE_RESTORE)
	if err != nil {
		return err
	}
	err = ai.WaitForCompletion(actionPath, *waitTime)
	if err != nil {
		return err
	}

	actionPath, err = ai.Restore(zkDstTabletPath, zkSrcTabletPath)
	if err != nil {
		return err
	}
	err = ai.WaitForCompletion(actionPath, *waitTime)
	if err != nil {
		return err
	}

	// Restore moves us into the replication graph as a spare, but
	// no serving consequences, so no rebuild required.
	return nil
}

func clone(zconn zk.Conn, ai *tm.ActionInitiator, wrangler *wr.Wrangler, zkSrcTabletPath, zkDstTabletPath string) error {
	if err := snapshot(zconn, ai, wrangler, zkSrcTabletPath); err != nil {
		return fmt.Errorf("snapshot err: %v", err)
	}
	if err := restore(zconn, ai, wrangler, zkSrcTabletPath, zkDstTabletPath); err != nil {
		return fmt.Errorf("restore err: %v", err)
	}
	return nil
}

func getTabletMap(zconn zk.Conn, tabletPaths []string) map[string]*tm.TabletInfo {
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	tabletMap := make(map[string]*tm.TabletInfo)

	for _, path := range tabletPaths {
		tabletPath := path
		wg.Add(1)
		go func() {
			defer wg.Done()
			tabletInfo, err := tm.ReadTablet(zconn, tabletPath)
			if err != nil {
				relog.Warning("%v: %v", tabletPath, err)
			} else {
				mutex.Lock()
				tabletMap[tabletPath] = tabletInfo
				mutex.Unlock()
			}
		}()
	}

	wg.Wait()

	mutex.Lock()
	defer mutex.Unlock()
	return tabletMap
}

// return a sorted list of tablets
func getAllTablets(zconn zk.Conn, zkVtPath string) ([]*tm.TabletInfo, error) {
	zkTabletsPath := path.Join(zkVtPath, "tablets")
	children, _, err := zconn.Children(zkTabletsPath)
	if err != nil {
		return nil, err
	}

	sort.Strings(children)
	tabletPaths := make([]string, len(children))
	for i, child := range children {
		tabletPaths[i] = path.Join(zkTabletsPath, child)
	}

	tabletMap := getTabletMap(zconn, tabletPaths)
	tablets := make([]*tm.TabletInfo, 0, len(tabletPaths))
	for _, tabletPath := range tabletPaths {
		tabletInfo, ok := tabletMap[tabletPath]
		if !ok {
			relog.Warning("failed to load tablet %v", tabletPath)
		}
		tablets = append(tablets, tabletInfo)
	}

	return tablets, nil
}

func listTabletsByType(zconn zk.Conn, zkVtPath string, dbType tm.TabletType) error {
	tablets, err := getAllTablets(zconn, zkVtPath)
	if err != nil {
		return err
	}
	for _, tablet := range tablets {
		if tablet.Type == dbType {
			fmt.Println(tablet.Path())
		}
	}

	return nil
}

func dumpTablets(zconn zk.Conn, zkVtPath string) error {
	tablets, err := getAllTablets(zconn, zkVtPath)
	if err != nil {
		return err
	}
	for _, tablet := range tablets {
		fmt.Printf("%v %v %v %v %v\n", tablet.Path(), tablet.Keyspace, tablet.Shard, tablet.Type, tablet.Addr)
	}

	return nil
}

func listScrap(zconn zk.Conn, zkVtPath string) error {
	return listTabletsByType(zconn, zkVtPath, tm.TYPE_SCRAP)
}

func listIdle(zconn zk.Conn, zkVtPath string) error {
	return listTabletsByType(zconn, zkVtPath, tm.TYPE_IDLE)
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
	case "CreateKeyspace":
		if len(args) != 2 {
			relog.Fatal("action %v requires 1 arg", args[0])
		}
		err = createKeyspace(zconn, args[1])
	case "InitTablet":
		if len(args) < 8 {
			relog.Fatal("action %v requires 7 or 8 args", args[0])
		}
		parentAlias := ""
		if len(args) == 9 {
			parentAlias = args[8]
		}
		err = initTablet(zconn, args[1], args[2], args[3], args[4], args[5], args[6], args[7], parentAlias, false)
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
	case "Sleep":
		if len(args) != 3 {
			relog.Fatal("action %v requires 2 args", args[0])
		}
		duration, err := time.ParseDuration(args[2])
		if err == nil {
			actionPath, err = ai.Sleep(args[1], duration)
		}
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
		err = changeType(zconn, ai, wrangler, args[1], args[2])
	case "DemoteMaster":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk tablet path>", args[0])
		}
		actionPath, err = ai.DemoteMaster(args[1])
	case "Clone":
		if len(args) != 3 {
			relog.Fatal("action %v requires <zk src tablet path> <zk dst tablet path>", args[0])
		}
		err = clone(zconn, ai, wrangler, args[1], args[2])
	case "Restore":
		if len(args) != 3 {
			relog.Fatal("action %v requires <zk src tablet path> <zk dst tablet path>", args[0])
		}
		err = restore(zconn, ai, wrangler, args[1], args[2])
	case "Snapshot":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk src tablet path>", args[0])
		}
		err = snapshot(zconn, ai, wrangler, args[1])
	case "PurgeActions":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		err = tm.PurgeActions(zconn, args[1])
	case "RebuildShard":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		actionPath, err = wrangler.RebuildShard(args[1])
	case "RebuildKeyspace":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk keyspace path>", args[0])
		}
		actionPath, err = wrangler.RebuildKeyspace(args[1])
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
			relog.Fatal("action %v requires <zk keyspaces path>", args[0])
		}
		err = validateZk(zconn, args[1])
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
	case "DumpTablets":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt path>", args[0])
		}
		err = dumpTablets(zconn, args[1])
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
		if *noWaitForAction {
			fmt.Println(actionPath)
		} else {
			err := ai.WaitForCompletion(actionPath, *waitTime)
			if err != nil {
				relog.Fatal(err.Error())
			} else {
				relog.Info("action completed: %v", actionPath)
			}
		}
	}
}
