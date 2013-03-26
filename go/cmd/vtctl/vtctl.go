// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"database/sql/driver"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log/syslog"
	"os"
	"os/signal"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/sync2"
	"code.google.com/p/vitess/go/tb"
	"code.google.com/p/vitess/go/vt/client2"
	hk "code.google.com/p/vitess/go/vt/hook"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/naming"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	wr "code.google.com/p/vitess/go/vt/wrangler"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

var noWaitForAction = flag.Bool("no-wait", false, "don't wait for action completion, detach")
var waitTime = flag.Duration("wait-time", 24*time.Hour, "time to wait on an action")
var lockWaitTimeout = flag.Duration("lock-wait-timeout", 0, "time to wait for a lock before starting an action")
var logLevel = flag.String("log.level", "INFO", "set log level")
var logfile = flag.String("logfile", "/vt/logs/vtctl.log", "log file")

type command struct {
	name   string
	method func(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error)
	params string
	help   string // if help is empty, won't list the command
}

type commandGroup struct {
	name     string
	commands []command
}

var commands = []commandGroup{
	commandGroup{
		"Tablets", []command{
			command{"InitTablet", commandInitTablet,
				"[-force] [-db-name-override=<db name>] [-key-start=<key start>] [-key-end=<key end>] <zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> [<zk parent alias>]",
				"Initializes a tablet in zookeeper."},
			command{"UpdateTablet", commandUpdateTablet,
				"[-force] [-db-name-override=<db name>] [-key-start=<key start>] [-key-end=<key end>] <zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> <zk parent alias>",
				"DEPRECATED (use ChangeSlaveType or other operations instead).\n" +
					"Updates a tablet in zookeeper."},
			command{"ScrapTablet", commandScrapTablet,
				"[-force] [-skip-rebuild] <zk tablet path>",
				"Scraps a tablet."},
			command{"SetReadOnly", commandSetReadOnly,
				"[<zk tablet path> | <zk shard/tablet path>]",
				"Sets the tablet or shard as ReadOnly."},
			command{"SetReadWrite", commandSetReadWrite,
				"[<zk tablet path> | <zk shard/tablet path>]",
				"Sets the tablet or shard as ReadWrite."},
			command{"DemoteMaster", commandDemoteMaster,
				"<zk tablet path>",
				"Demotes a master tablet."},
			command{"ChangeSlaveType", commandChangeSlaveType,
				"[-force] [-dry-run] <zk tablet path> <db type>",
				"Change the db type for this tablet if possible. This is mostly for arranging replicas - it will not convert a master.\n" +
					"NOTE: This will automatically update the serving graph.\n" +
					"Valid <db type>:\n" +
					"  " + strings.Join(tm.SlaveTabletTypeStrings, " ") + "\n"},
			command{"ChangeType", commandChangeSlaveType,
				"[-force] <zk tablet path> <db type>",
				"DEPRECATED (use ChangeSlaveType instead).\n" +
					"Change the db type for this tablet if possible. This is mostly for arranging replicas - it will not convert a master.\n" +
					"NOTE: This will automatically update the serving graph."},
			command{"Ping", commandPing,
				"<zk tablet path>",
				"Check that the agent is awake and responding - can be blocked by other in-flight operations."},
			command{"Query", commandQuery,
				"<zk tablet path> [<user> <password>] <query>",
				"Send a SQL query to a tablet."},
			command{"Sleep", commandSleep,
				"<zk tablet path> <duration>",
				"Block the action queue for the specified duration (mostly for testing)."},
			command{"Snapshot", commandSnapshot,
				"[-force] [-server-mode] [-concurrency=4] <zk tablet path>",
				"Stop mysqld and copy compressed data aside."},
			command{"SnapshotSourceEnd", commandSnapshotSourceEnd,
				"[-slave-start] [-read-write] <zk tablet path> <original tablet type>",
				"Restarts Mysql and restore original server type."},
			command{"Restore", commandRestore,
				"[-fetch-concurrency=3] [-fetch-retry-count=3] [-dont-wait-for-slave-start] <zk src tablet path> <src manifest file> <zk dst tablet path> [<zk new master path>]",
				"Copy the given snaphot from the source tablet and restart replication to the new master path (or uses the <src tablet path> if not specified). If <src manifest file> is 'default', uses the default value.\n" +
					"NOTE: This does not wait for replication to catch up. The destination tablet must be 'idle' to begin with. It will transition to 'spare' once the restore is complete."},
			command{"Clone", commandClone,
				"[-force] [-concurrency=4] [-fetch-concurrency=3] [-fetch-retry-count=3] [-server-mode] <zk src tablet path> <zk dst tablet path>",
				"This performs Snapshot and then Restore.  The advantage of having separate actions is that one snapshot can be used for many restores."},
			command{"ReparentTablet", commandReparentTablet,
				"<zk tablet path>",
				"Reparent a tablet to the current master in the shard. This only works if the current slave position matches the last known reparent action."},
			command{"PartialSnapshot", commandPartialSnapshot,
				"[-force] [-concurrency=4] <zk tablet path> <key name> <start key> <end key>",
				"Locks mysqld and copy compressed data aside."},
			command{"MultiSnapshot", commandMultiSnapshot,
				"[-force] [-concurrency=4] [-skip-slave-restart] [-maximum-file-size=1073741824] -spec='-' -tables='' <zk tablet path> <key name>",
				"Locks mysqld and copy compressed data aside."},
			command{"MultiRestore", commandMultiRestore,
				"[-force] [-concurrency=4] [-fetch-concurrency=4] [-fetch-retry-count=3] [-write-bin-logs] <destination zk path> <source zk path>...",
				"Restores a snapshot from multiple hosts."},
			command{"PartialRestore", commandPartialRestore,
				"[-fetch-concurrency=3] [-fetch-retry-count=3] <zk src tablet path> <src manifest file> <zk dst tablet path> [<zk new master path>]",
				"Copy the given partial snaphot from the source tablet and starts partial replication to the new master path (or uses the src tablet path if not specified).\n" +
					"NOTE: This does not wait for replication to catch up. The destination tablet must be 'idle' to begin with. It will transition to 'spare' once the restore is complete."},
			command{"PartialClone", commandPartialClone,
				"[-force] [-concurrency=4] [-fetch-concurrency=3] [-fetch-retry-count=3] <zk src tablet path> <zk dst tablet path> <key name> <start key> <end key>",
				"This performs PartialSnapshot and then PartialRestore.  The advantage of having separate actions is that one partial snapshot can be used for many restores."},
			command{"ExecuteHook", commandExecuteHook,
				"<zk tablet path> <hook name> [<param1=value1> <param2=value2> ...]",
				"This runs the specified hook on the given tablet."},
		},
	},
	commandGroup{
		"Shards", []command{
			command{"RebuildShardGraph", commandRebuildShardGraph,
				"<zk shard path> ... (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>)",
				"Rebuild the replication graph and shard serving data in zk. This may trigger an update to all connected clients."},
			command{"ReparentShard", commandReparentShard,
				"[-force] [-leave-master-read-only] <zk shard path> <zk tablet path>",
				"Specify which shard to reparent and which tablet should be the new master."},
			command{"ValidateShard", commandValidateShard,
				"[-ping-tablets] <zk shard path> (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>)",
				"Validate all nodes reachable from this shard are consistent."},
			command{"ShardReplicationPositions", commandShardReplicationPositions,
				"<zk shard path> (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>)",
				"Show slave status on all machines in the shard graph."},
			command{"ListShardTablets", commandListShardTablets,
				"<zk shard path> (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>)",
				"List all tablets in a given shard."},
			command{"ListShardActions", commandListShardActions,
				"<zk shard path> (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>)",
				"List all active actions in a given shard."},
		},
	},
	commandGroup{
		"Keyspaces", []command{
			command{"CreateKeyspace", commandCreateKeyspace,
				"[-force] <zk keyspaces path>/<name>",
				"e.g. CreateKeyspace /zk/global/vt/keyspaces/my_keyspace"},
			command{"RebuildKeyspaceGraph", commandRebuildKeyspaceGraph,
				"<zk keyspace path> ... (/zk/global/vt/keyspaces/<keyspace>)",
				"Rebuild the serving data for all shards in this keyspace. This may trigger an update to all connected clients."},
			command{"ValidateKeyspace", commandValidateKeyspace,
				"[-ping-tablets] <zk keyspace path> (/zk/global/vt/keyspaces/<keyspace>)",
				"Validate all nodes reachable from this keyspace are consistent."},
		},
	},
	commandGroup{
		"Generic", []command{
			command{"PurgeActions", commandPurgeActions,
				"<zk action path> ... (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>/action)",
				"Remove all actions - be careful, this is powerful cleanup magic."},
			command{"StaleActions", commandStaleActions,
				"[-max-staleness=<duration> -purge] <zk action path> ... (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>/action)",
				"List any queued actions that are considered stale."},
			command{"PruneActionLogs", commandPruneActionLogs,
				"[-keep-count=<count to keep>] <zk actionlog path> ...",
				"e.g. PruneActionLogs -keep-count=10 /zk/global/vt/keyspaces/my_keyspace/shards/0/actionlog\n" +
					"Removes older actionlog entries until at most <count to keep> are left."},
			command{"WaitForAction", commandWaitForAction,
				"<zk action path> (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>/action/<action id>)",
				"Watch an action node, printing updates, until the action is complete."},
			command{"Resolve", commandResolve,
				"<keyspace>.<shard>.<db type>:<port name>",
				"Read a list of addresses that can answer this query. The port name is usually _mysql or _vtocc."},
			command{"Validate", commandValidate,
				"[-ping-tablets] <zk keyspaces path> (/zk/global/vt/keyspaces)",
				"Validate all nodes reachable from global replication graph and all tablets in all discoverable cells are consistent."},
			command{"ExportZkns", commandExportZkns,
				"<zk local vt path>  (/zk/<cell>/vt)",
				"DEPRECATED\n" +
					"Export the serving graph entries to the legacy zkns format."},
			command{"ExportZknsForKeyspace", commandExportZknsForKeyspace,
				"<zk global keyspace path> (/zk/global/vt/keyspaces/<keyspace>)",
				"Export the serving graph entries to the legacy zkns format."},
			command{"RebuildReplicationGraph", commandRebuildReplicationGraph,
				"zk-vt-paths=<zk local vt path>,... keyspaces=<keyspace>,...",
				"This takes the Thor's hammer approach of recovery and should only be used in emergencies.  /zk/cell/vt/tablets/... are the canonical source of data for the system. This function use that canonical data to recover the replication graph, at which point further auditing with Validate can reveal any remaining issues."},
			command{"ListIdle", commandListIdle,
				"<zk local vt path> (/zk/<cell>/vt)",
				"DEPRECATED (use ListAllTablets + awk)\n" +
					"List all idle tablet paths."},
			command{"ListScrap", commandListScrap,
				"<zk local vt path>  (/zk/<cell>/vt)",
				"DEPRECATED (use ListAllTablets + awk)\n" +
					"List all scrap tablet paths."},
			command{"ListAllTablets", commandListAllTablets,
				"<zk local vt path>  (/zk/<cell>/vt)",
				"List all tablets in an awk-friendly way."},
			command{"ListTablets", commandListTablets,
				"<zk tablet path> ...  (/zk/<cell>/vt/tablets/<tablet uid> ...)",
				"List specified tablets in an awk-friendly way."},
		},
	},
	commandGroup{
		"Schema (beta)", []command{
			command{"GetSchema", commandGetSchema,
				"[-tables=<table1>,<table2>,...] [-include-views] <zk tablet path>",
				"Display the full schema for a tablet, or just the schema for the provided tables."},
			command{"ValidateSchemaShard", commandValidateSchemaShard,
				"<zk shard path>",
				"Validate the master schema matches all the slaves."},
			command{"ValidateSchemaKeyspace", commandValidateSchemaKeyspace,
				"<zk keyspace path>",
				"Validate the master schema from shard 0 matches all the other tablets in the keyspace."},
			command{"PreflightSchema", commandPreflightSchema,
				"{-sql=<sql> || -sql-file=<filename>} <zk tablet path>",
				"Apply the schema change to a temporary database to gather before and after schema and validate the change. The sql can be inlined or read from a file."},
			command{"ApplySchema", commandApplySchema,
				"[-force] {-sql=<sql> || -sql-file=<filename>} [-skip-preflight] [-stop-replication] <zk tablet path>",
				"Apply the schema change to the specified tablet (allowing replication by default). The sql can be inlined or read from a file. Note this doesn't change any tablet state (doesn't go into 'schema' type)."},
			command{"ApplySchemaShard", commandApplySchemaShard,
				"[-force] {-sql=<sql> || -sql-file=<filename>} [-simple] [-new-parent=<zk tablet path>] <zk shard path>",
				"Apply the schema change to the specified shard. If simple is specified, we just apply on the live master. Otherwise we will need to do the shell game. So we will apply the schema change to every single slave. if new_parent is set, we will also reparent (otherwise the master won't be touched at all). Using the force flag will cause a bunch of checks to be ignored, use with care."},
			command{"ApplySchemaKeyspace", commandApplySchemaKeyspace,
				"[-force] {-sql=<sql> || -sql-file=<filename>} [-simple] <zk keyspace path>",
				"Apply the schema change to the specified keyspace. If simple is specified, we just apply on the live masters. Otherwise we will need to do the shell game on each shard. So we will apply the schema change to every single slave (running in parallel on all shards, but on one host at a time in a given shard). We will not reparent at the end, so the masters won't be touched at all. Using the force flag will cause a bunch of checks to be ignored, use with care."},
		},
	},
}

var stdin *bufio.Reader

func init() {
	// FIXME(msolomon) need to send all of this to stdout
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [global parameters] command [command parameters]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nThe global optional parameters are:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nThe commands are listed below, sorted by group. Use '%s <command> -h' for more help.\n\n", os.Args[0])
		for _, group := range commands {
			fmt.Fprintf(os.Stderr, "%s:\n", group.name)
			for _, cmd := range group.commands {
				if strings.HasPrefix(cmd.help, "DEPRECATED") {
					continue
				}
				fmt.Fprintf(os.Stderr, "  %s %s\n", cmd.name, cmd.params)
			}
			fmt.Fprintf(os.Stderr, "\n")
		}
	}
	stdin = bufio.NewReader(os.Stdin)
}

func confirm(prompt string, force bool) bool {
	if force {
		return true
	}
	fmt.Fprintf(os.Stderr, prompt+" [NO/yes] ")
	line, _ := stdin.ReadString('\n')
	return strings.ToLower(strings.TrimSpace(line)) == "yes"
}

// this is a placeholder implementation. right now very little information
// is needed for a keyspace.
func createKeyspace(zconn zk.Conn, keyspacePath string, force bool) error {
	if err := tm.IsKeyspacePath(keyspacePath); err != nil {
		return err
	}
	pathList := make([]string, 0, 3)
	p, err := tm.KeyspaceActionPath(keyspacePath)
	if err != nil {
		return err
	}
	pathList = append(pathList, p)
	p, err = tm.KeyspaceActionLogPath(keyspacePath)
	if err != nil {
		return err
	}
	pathList = append(pathList, p)
	pathList = append(pathList, path.Join(keyspacePath, "shards"))

	for _, zkPath := range pathList {
		_, err := zk.CreateRecursive(zconn, zkPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				if !force {
					relog.Fatal("path already exists: %v", zkPath)
				}
			} else {
				relog.Fatal("error creating keyspace: %v %v", keyspacePath, err)
			}
		}
	}

	return nil
}

func getMasterAlias(zconn zk.Conn, zkShardPath string) (string, error) {
	// FIXME(msolomon) just read the shard node data instead - that is tearing resistant.
	children, _, err := zconn.Children(zkShardPath)
	if err != nil {
		return "", err
	}
	result := ""
	for _, child := range children {
		if child == "action" || child == "actionlog" {
			continue
		}
		if result != "" {
			return "", fmt.Errorf("master search failed: %v", zkShardPath)
		}
		result = path.Join(zkShardPath, child)
	}

	return result, nil
}

func initTablet(wrangler *wr.Wrangler, zkPath, hostname, mysqlPort, port, keyspace, shardId, tabletType, parentAlias, dbNameOverride, keyStart, keyEnd string, force, update bool) error {
	zconn := wrangler.ZkConn()
	if err := tm.IsTabletPath(zkPath); err != nil {
		return err
	}
	cell, err := zk.ZkCellFromZkPath(zkPath)
	if err != nil {
		return err
	}
	pathParts := strings.Split(zkPath, "/")
	uid, err := tm.ParseUid(pathParts[len(pathParts)-1])
	if err != nil {
		return err
	}

	// if keyStart or keyEnd is set, check the shard name is
	// keyStart-keyEnd
	if keyStart != "" || keyEnd != "" {
		if shardId != keyStart+"-"+keyEnd {
			return fmt.Errorf("Invalid shardId, was expecting '%v-%v' but got '%v'", keyStart, keyEnd, shardId)
		}
	}

	parent := tm.TabletAlias{}
	if parentAlias == "" && tm.TabletType(tabletType) != tm.TYPE_MASTER && tm.TabletType(tabletType) != tm.TYPE_IDLE {
		vtSubStree, err := tm.VtSubtree(zkPath)
		if err != nil {
			return err
		}
		vtRoot := path.Join("/zk/global", vtSubStree)
		parentAlias, err = getMasterAlias(zconn, tm.ShardPath(vtRoot, keyspace, shardId))
		if err != nil {
			return err
		}
	}
	if parentAlias != "" {
		parent.Cell, parent.Uid, err = tm.ParseTabletReplicationPath(parentAlias)
		if err != nil {
			return err
		}
	}

	tablet, err := tm.NewTablet(cell, uid, parent, fmt.Sprintf("%v:%v", hostname, port), fmt.Sprintf("%v:%v", hostname, mysqlPort), keyspace, shardId, tm.TabletType(tabletType))
	if err != nil {
		return err
	}
	tablet.DbNameOverride = dbNameOverride

	if keyStart != "" {
		tablet.KeyRange.Start, err = key.HexKeyspaceId(keyStart).Unhex()
		if err != nil {
			return err
		}
	}
	if keyEnd != "" {
		tablet.KeyRange.End, err = key.HexKeyspaceId(keyEnd).Unhex()
		if err != nil {
			return err
		}
	}

	err = tm.CreateTablet(zconn, zkPath, tablet)
	if err != nil && zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		// Try to update nicely, but if it fails fall back to force behavior.
		if update {
			oldTablet, err := tm.ReadTablet(zconn, zkPath)
			if err != nil {
				relog.Warning("failed reading tablet %v: %v", zkPath, err)
			} else {
				if oldTablet.Keyspace == tablet.Keyspace && oldTablet.Shard == tablet.Shard {
					*(oldTablet.Tablet) = *tablet
					err := tm.UpdateTablet(zconn, zkPath, oldTablet)
					if err != nil {
						relog.Warning("failed updating tablet %v: %v", zkPath, err)
					} else {
						return nil
					}
				}
			}
		}
		if force {
			_, err = wrangler.Scrap(zkPath, force, false)
			if err != nil {
				relog.Error("failed scrapping tablet %v: %v", zkPath, err)
				return err
			}
			err = zk.DeleteRecursive(zconn, zkPath, -1)
			if err != nil {
				relog.Error("failed deleting tablet %v: %v", zkPath, err)
			}
			err = tm.CreateTablet(zconn, zkPath, tablet)
		}
	}
	return err
}

func fmtTabletAwkable(ti *tm.TabletInfo) string {
	keyspace := ti.Keyspace
	shard := ti.Shard
	if keyspace == "" {
		keyspace = "<null>"
	}
	if shard == "" {
		shard = "<null>"
	}
	return fmt.Sprintf("%v %v %v %v %v %v", ti.Path(), keyspace, shard, ti.Type, ti.Addr, ti.MysqlAddr)
}

func listTabletsByType(zconn zk.Conn, zkVtPath string, dbType tm.TabletType) error {
	tablets, err := wr.GetAllTablets(zconn, zkVtPath)
	if err != nil {
		return err
	}
	for _, tablet := range tablets {
		if tablet.Type == dbType {
			fmt.Println(fmtTabletAwkable(tablet))
		}
	}
	return nil
}

func getActions(zconn zk.Conn, actionPath string) ([]*tm.ActionNode, error) {
	actions, _, err := zconn.Children(actionPath)
	if err != nil {
		return nil, fmt.Errorf("getActions failed: %v %v", actionPath, err)
	}
	sort.Strings(actions)
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	nodes := make([]*tm.ActionNode, 0, len(actions))
	for _, action := range actions {
		wg.Add(1)
		go func(action string) {
			defer wg.Done()
			actionNodePath := path.Join(actionPath, action)
			data, _, err := zconn.Get(actionNodePath)
			if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
				relog.Warning("getActions: %v %v", actionNodePath, err)
				return
			}
			actionNode, err := tm.ActionNodeFromJson(data, actionNodePath)
			if err != nil {
				relog.Warning("getActions: %v %v", actionNodePath, err)
				return
			}
			mu.Lock()
			nodes = append(nodes, actionNode)
			mu.Unlock()
		}(action)
	}
	wg.Wait()

	return nodes, nil
}

func listActionsByShard(zconn zk.Conn, zkShardPath string) error {
	// print the shard action nodes
	shardActionPath, err := tm.ShardActionPath(zkShardPath)
	if err != nil {
		return err
	}
	shardActionNodes, err := getActions(zconn, shardActionPath)
	if err != nil {
		return err
	}
	for _, shardAction := range shardActionNodes {
		fmt.Println(fmtAction(shardAction))
	}

	// get and print the tablet action nodes
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	actionMap := make(map[string]*tm.ActionNode)

	f := func(actionPath string) {
		defer wg.Done()
		actionNodes, err := getActions(zconn, actionPath)
		if err != nil {
			relog.Warning("listActionsByShard %v", err)
			return
		}
		mu.Lock()
		for _, node := range actionNodes {
			actionMap[node.Path()] = node
		}
		mu.Unlock()
	}

	tabletPaths, err := tabletPathsForShard(zconn, zkShardPath)
	if err != nil {
		return err
	}
	for _, tabletPath := range tabletPaths {
		actionPath, err := tm.TabletActionPath(tabletPath)
		if err != nil {
			relog.Warning("listActionsByShard %v", err)
		} else {
			wg.Add(1)
			go f(actionPath)
		}
	}

	wg.Wait()
	mu.Lock()
	defer mu.Unlock()

	keys := wr.CopyMapKeys(actionMap, []string{}).([]string)
	sort.Strings(keys)
	for _, key := range keys {
		action := actionMap[key]
		if action == nil {
			relog.Warning("nil action: %v", key)
		} else {
			fmt.Println(fmtAction(action))
		}
	}
	return nil
}

func fmtAction(action *tm.ActionNode) string {
	state := string(action.State)
	// FIXME(msolomon) The default state should really just have the value "queued".
	if action.State == tm.ACTION_STATE_QUEUED {
		state = "queued"
	}
	return fmt.Sprintf("%v %v %v %v %v", action.Path(), action.Action, state, action.ActionGuid, action.Error)
}

func listTabletsByShard(zconn zk.Conn, zkShardPath string) error {
	tabletPaths, err := tabletPathsForShard(zconn, zkShardPath)
	if err != nil {
		return err
	}
	return dumpTablets(zconn, tabletPaths)
}

func listTabletsByAliases(zconn zk.Conn, tabletAliases []string) error {
	tabletPaths := make([]string, len(tabletAliases))
	for i, tabletAlias := range tabletAliases {
		aliasParts := strings.Split(tabletAlias, "-")
		uid, _ := strconv.ParseUint(aliasParts[1], 10, 32)
		alias := tm.TabletAlias{aliasParts[0], uint32(uid)}
		tabletPaths[i] = tm.TabletPathForAlias(alias)
	}
	return dumpTablets(zconn, tabletPaths)
}

func tabletPathsForShard(zconn zk.Conn, zkShardPath string) ([]string, error) {
	tabletAliases, err := tm.FindAllTabletAliasesInShard(zconn, zkShardPath)
	if err != nil {
		return nil, err
	}
	tabletPaths := make([]string, len(tabletAliases))
	for i, alias := range tabletAliases {
		tabletPaths[i] = tm.TabletPathForAlias(alias)
	}
	return tabletPaths, nil
}

func dumpAllTablets(zconn zk.Conn, zkVtPath string) error {
	tablets, err := wr.GetAllTablets(zconn, zkVtPath)
	if err != nil {
		return err
	}
	for _, ti := range tablets {
		fmt.Println(fmtTabletAwkable(ti))
	}
	return nil
}

func dumpTablets(zconn zk.Conn, zkTabletPaths []string) error {
	tabletMap, err := wr.GetTabletMap(zconn, zkTabletPaths)
	if err != nil {
		return err
	}
	for _, tabletPath := range zkTabletPaths {
		ti, ok := tabletMap[tabletPath]
		if !ok {
			relog.Warning("failed to load tablet %v", tabletPath)
		} else {
			fmt.Println(fmtTabletAwkable(ti))
		}
	}
	return nil
}

func kquery(zconn zk.Conn, zkKeyspacePath, user, password, query string) error {
	sconn, err := client2.Dial(zconn, zkKeyspacePath, "master", false, 5*time.Second, user, password)
	if err != nil {
		return err
	}
	rows, err := sconn.QueryBind(query, nil)
	if err != nil {
		return err
	}
	cols := rows.Columns()
	fmt.Println(strings.Join(cols, "\t"))

	rowIndex := 0
	row := make([]driver.Value, len(cols))
	rowStrs := make([]string, len(cols)+1)
	for rows.Next(row) == nil {
		for i, value := range row {
			switch value.(type) {
			case []byte:
				rowStrs[i] = fmt.Sprintf("%q", value)
			default:
				rowStrs[i] = fmt.Sprintf("%v", value)
			}
		}

		fmt.Println(strings.Join(rowStrs, "\t"))
		rowIndex++
	}
	return nil
}

// parseParams parses an array of strings in the form of a=b
// into a map.
func parseParams(args []string) map[string]string {
	params := make(map[string]string)
	for _, arg := range args {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) == 1 {
			params[parts[0]] = ""
		} else {
			params[parts[0]] = parts[1]
		}
	}
	return params
}

// getFileParam returns a string containing either flag is not "",
// or the content of the file named flagFile
func getFileParam(flag, flagFile, name string) string {
	if flag != "" {
		if flagFile != "" {
			relog.Fatal("action requires only one of " + name + " or " + name + "-file")
		}
		return flag
	}

	if flagFile == "" {
		relog.Fatal("action requires one of " + name + " or " + name + "-file")
	}
	data, err := ioutil.ReadFile(flagFile)
	if err != nil {
		relog.Fatal("Cannot read file %v: %v", flagFile, err)
	}
	return string(data)
}

func commandInitTablet(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	dbNameOverride := subFlags.String("db-name-override", "", "override the name of the db used by vttablet")
	force := subFlags.Bool("force", false, "will overwrite the node if it already exists")
	keyStart := subFlags.String("key-start", "", "start of the key range")
	keyEnd := subFlags.String("key-end", "", "end of the key range")
	subFlags.Parse(args)
	if subFlags.NArg() != 7 && subFlags.NArg() != 8 {
		relog.Fatal("action InitTablet requires <zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> [<zk parent alias>]")
	}

	parentAlias := ""
	if subFlags.NArg() == 8 {
		parentAlias = subFlags.Arg(7)
	}
	return "", initTablet(wrangler, subFlags.Arg(0), subFlags.Arg(1), subFlags.Arg(2), subFlags.Arg(3), subFlags.Arg(4), subFlags.Arg(5), subFlags.Arg(6), parentAlias, *dbNameOverride, *keyStart, *keyEnd, *force, false)
}

func commandUpdateTablet(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	dbNameOverride := subFlags.String("db-name-override", "", "override the name of the db used by vttablet")
	force := subFlags.Bool("force", false, "will overwrite the node if it already exists")
	keyStart := subFlags.String("key-start", "", "start of the key range")
	keyEnd := subFlags.String("key-end", "", "end of the key range")
	subFlags.Parse(args)
	if subFlags.NArg() != 8 {
		relog.Fatal("action UpdateTablet requires <zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> <zk parent alias>")
	}

	return "", initTablet(wrangler, subFlags.Arg(0), subFlags.Arg(1), subFlags.Arg(2), subFlags.Arg(3), subFlags.Arg(4), subFlags.Arg(5), subFlags.Arg(6), subFlags.Arg(7), *dbNameOverride, *keyStart, *keyEnd, *force, true)
}

func commandScrapTablet(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "writes the scrap state in to zk, no questions asked, if a tablet is offline")
	skipRebuild := subFlags.Bool("skip-rebuild", false, "do not rebuild the shard and keyspace graph after scrapping")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ScrapTablet requires <zk tablet path>")
	}

	return wrangler.Scrap(subFlags.Arg(0), *force, *skipRebuild)
}

func commandSetReadOnly(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action SetReadOnly requires args")
	}
	return wrangler.ActionInitiator().SetReadOnly(subFlags.Arg(0))
}

func commandSetReadWrite(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action SetReadWrite requires args")
	}
	return wrangler.ActionInitiator().SetReadWrite(subFlags.Arg(0))
}

func commandDemoteMaster(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action DemoteMaster requires <zk tablet path>")
	}
	return wrangler.ActionInitiator().DemoteMaster(subFlags.Arg(0))
}

func commandChangeSlaveType(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will change the type in zookeeper, and not run hooks")
	dryRun := subFlags.Bool("dry-run", false, "just list the proposed change")

	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action ChangeSlaveType requires <zk tablet path> <db type>")
	}

	zkTabletPath := subFlags.Arg(0)
	newType := tm.TabletType(subFlags.Arg(1))
	if *dryRun {
		ti, err := tm.ReadTablet(wrangler.ZkConn(), zkTabletPath)
		if err != nil {
			relog.Fatal("failed reading tablet %v: %v", zkTabletPath, err)
		}
		if !tm.IsTrivialTypeChange(ti.Type, newType) || !tm.IsValidTypeChange(ti.Type, newType) {
			relog.Fatal("invalid type transition %v: %v -> %v", zkTabletPath, ti.Type, newType)
		}
		fmt.Printf("- %v\n", fmtTabletAwkable(ti))
		ti.Type = newType
		fmt.Printf("+ %v\n", fmtTabletAwkable(ti))
		return "", nil
	}
	return "", wrangler.ChangeType(zkTabletPath, newType, *force)
}

func commandPing(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action Ping requires <zk tablet path>")
	}
	return wrangler.ActionInitiator().Ping(subFlags.Arg(0))
}

func commandQuery(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 2 && subFlags.NArg() != 4 {
		relog.Fatal("action Query requires 2 or 4 args")
	}
	if subFlags.NArg() == 2 {
		return "", kquery(wrangler.ZkConn(), subFlags.Arg(0), "", "", subFlags.Arg(1))
	}

	return "", kquery(wrangler.ZkConn(), subFlags.Arg(0), subFlags.Arg(1), subFlags.Arg(2), subFlags.Arg(3))
}

func commandSleep(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action Sleep requires 2 args")
	}
	duration, err := time.ParseDuration(subFlags.Arg(1))
	if err != nil {
		return "", err
	}
	return wrangler.ActionInitiator().Sleep(subFlags.Arg(0), duration)
}

func commandSnapshotSourceEnd(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	slaveStartRequired := subFlags.Bool("slave-start", false, "will restart replication")
	readWrite := subFlags.Bool("read-write", false, "will make the server read-write")
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action SnapshotSourceEnd requires <zk tablet path> <original server type>")
	}

	return "", wrangler.SnapshotSourceEnd(subFlags.Arg(0), *slaveStartRequired, !(*readWrite), tm.TabletType(subFlags.Arg(1)))
}

func commandSnapshot(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	serverMode := subFlags.Bool("server-mode", false, "will symlink the data files and leave mysqld stopped")
	concurrency := subFlags.Int("concurrency", 4, "how many compression/checksum jobs to run simultaneously")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action Snapshot requires <zk src tablet path>")
	}

	filename, zkParentPath, slaveStartRequired, readOnly, originalType, err := wrangler.Snapshot(subFlags.Arg(0), *force, *concurrency, *serverMode)
	if err == nil {
		relog.Info("Manifest: %v", filename)
		relog.Info("ParentPath: %v", zkParentPath)
		if *serverMode {
			relog.Info("SlaveStartRequired: %v", slaveStartRequired)
			relog.Info("ReadOnly: %v", readOnly)
			relog.Info("OriginalType: %v", originalType)
		}
	}
	return "", err
}

func commandRestore(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	dontWaitForSlaveStart := subFlags.Bool("dont-wait-for-slave-start", false, "won't wait for replication to start (useful when restoring from snapshot source that is the replication master)")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	subFlags.Parse(args)
	if subFlags.NArg() != 3 && subFlags.NArg() != 4 {
		relog.Fatal("action Restore requires <zk src tablet path> <src manifest path> <zk dst tablet path> [<zk new master path>]")
	}
	zkParentPath := subFlags.Arg(0)
	if subFlags.NArg() == 4 {
		zkParentPath = subFlags.Arg(3)
	}
	return "", wrangler.Restore(subFlags.Arg(0), subFlags.Arg(1), subFlags.Arg(2), zkParentPath, *fetchConcurrency, *fetchRetryCount, false, *dontWaitForSlaveStart)
}

func commandClone(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 4, "how many compression/checksum jobs to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	serverMode := subFlags.Bool("server-mode", false, "will keep the snapshot server offline to serve DB files directly")
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action Clone requires <zk src tablet path> <zk dst tablet path>")
	}

	return "", wrangler.Clone(subFlags.Arg(0), subFlags.Arg(1), *force, *concurrency, *fetchConcurrency, *fetchRetryCount, *serverMode)
}

func commandReparentTablet(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ReparentTablet requires <zk tablet path>")
	}
	return "", wrangler.ReparentTablet(subFlags.Arg(0))
}

func commandPartialSnapshot(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 4, "how many compression jobs to run simultaneously")
	subFlags.Parse(args)
	if subFlags.NArg() != 4 {
		relog.Fatal("action PartialSnapshot requires <zk src tablet path> <key name> <start key> <end key>")
	}

	filename, zkParentPath, err := wrangler.PartialSnapshot(subFlags.Arg(0), subFlags.Arg(1), key.HexKeyspaceId(subFlags.Arg(2)), key.HexKeyspaceId(subFlags.Arg(3)), *force, *concurrency)
	if err == nil {
		relog.Info("Manifest: %v", filename)
		relog.Info("ParentPath: %v", zkParentPath)
	}
	return "", err
}

func commandMultiRestore(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (status string, err error) {
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	concurrency := subFlags.Int("concurrency", 4, "how many concurrent jobs to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 4, "how many files to fetch simultaneously")
	writeBinLogs := subFlags.Bool("write-bin-logs", false, "write the data into the mysql binary logs")
	subFlags.Parse(args)

	if subFlags.NArg() < 2 {
		relog.Fatal("MultiRestore requires <destination zk path> <source zk path>... %v", args)
	}
	destination := subFlags.Arg(0)
	sources := subFlags.Args()[1:]

	err = wrangler.RestoreFromMultiSnapshot(destination, sources, *concurrency, *fetchConcurrency, *fetchRetryCount, *writeBinLogs)
	return
}

func commandMultiSnapshot(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 4, "how many compression jobs to run simultaneously")
	spec := subFlags.String("spec", "-", "shard specification")
	tablesString := subFlags.String("tables", "", "dump only this comma separated list of tables")
	skipSlaveRestart := subFlags.Bool("skip-slave-restart", false, "after the snapshot is done, do not restart slave replication")
	maximumFilesize := subFlags.Uint64("maximum-file-size", 1*1024*1024*1024, "the maximum size for an uncompressed data file")
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action PartialSnapshot requires <zk src tablet path> <key name>")
	}

	shards, err := key.ParseShardingSpec(*spec)
	if err != nil {
		relog.Fatal("multisnapshot failed: %v", err)
	}
	var tables []string
	if *tablesString != "" {
		tables = strings.Split(*tablesString, ",")
	}

	filenames, zkParentPath, err := wrangler.MultiSnapshot(shards, subFlags.Arg(0), subFlags.Arg(1), *concurrency, tables, *force, *skipSlaveRestart, *maximumFilesize)

	if err == nil {
		relog.Info("manifest locations: %v", filenames)
		relog.Info("ParentPath: %v", zkParentPath)
	}
	return "", err
}

func commandPartialRestore(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	subFlags.Parse(args)
	if subFlags.NArg() != 3 && subFlags.NArg() != 4 {
		relog.Fatal("action PartialRestore requires <zk src tablet path> <src manifest path> <zk dst tablet path> [<zk new master path>]")
	}
	zkParentPath := subFlags.Arg(0)
	if subFlags.NArg() == 4 {
		zkParentPath = subFlags.Arg(3)
	}
	return "", wrangler.PartialRestore(subFlags.Arg(0), subFlags.Arg(1), subFlags.Arg(2), zkParentPath, *fetchConcurrency, *fetchRetryCount)
}

func commandPartialClone(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 4, "how many compression jobs to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	subFlags.Parse(args)
	if subFlags.NArg() != 5 {
		relog.Fatal("action PartialClone requires <zk src tablet path> <zk dst tablet path> <key name> <start key> <end key>")
	}

	return "", wrangler.PartialClone(subFlags.Arg(0), subFlags.Arg(1), subFlags.Arg(2), key.HexKeyspaceId(subFlags.Arg(3)), key.HexKeyspaceId(subFlags.Arg(4)), *force, *concurrency, *fetchConcurrency, *fetchRetryCount)
}

func commandExecuteHook(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() < 2 {
		relog.Fatal("action ExecuteHook requires <zk tablet path> <hook name>")
	}

	hook := &hk.Hook{Name: subFlags.Arg(1), Parameters: parseParams(subFlags.Args()[2:])}
	hr, err := wrangler.ExecuteHook(subFlags.Arg(0), hook)
	if err == nil {
		relog.Info(hr.String())
	}
	return "", err
}

func commandRebuildShardGraph(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action RebuildShardGraph requires at least one <zk shard path>")
	}

	zkPaths, err := zk.ResolveWildcards(wrangler.ZkConn(), subFlags.Args())
	if err != nil {
		return "", err
	}
	if len(zkPaths) == 0 {
		return "", nil
	}

	for _, zkPath := range zkPaths {
		if err := wrangler.RebuildShardGraph(zkPath); err != nil {
			return "", err
		}
	}
	return "", nil
}

func commandReparentShard(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	leaveMasterReadOnly := subFlags.Bool("leave-master-read-only", false, "leaves the master read-only after reparenting")
	force := subFlags.Bool("force", false, "will force the reparent even if the master is already correct")
	subFlags.Parse(args)

	if subFlags.NArg() != 2 {
		relog.Fatal("action ReparentShard requires <zk shard path> <zk tablet path>")
	}
	return "", wrangler.ReparentShard(subFlags.Arg(0), subFlags.Arg(1), *leaveMasterReadOnly, *force)
}

func commandValidateShard(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	pingTablets := subFlags.Bool("ping-tablets", true, "ping all tablets during validate")
	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateShard requires <zk shard path>")
	}
	return "", wrangler.ValidateShard(subFlags.Arg(0), *pingTablets)
}

func commandShardReplicationPositions(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ShardReplicationPositions requires <zk shard path>")
	}
	tablets, positions, err := wrangler.ShardReplicationPositions(subFlags.Arg(0))
	if tablets == nil {
		return "", err
	}

	lines := make([]string, 0, 24)
	for _, rt := range sortReplicatingTablets(tablets, positions) {
		pos := rt.ReplicationPosition
		ti := rt.TabletInfo
		if pos == nil {
			lines = append(lines, fmtTabletAwkable(ti)+" <err> <err> <err>")
		} else {
			lines = append(lines, fmtTabletAwkable(ti)+fmt.Sprintf(" %v:%010d %v:%010d %v", pos.MasterLogFile, pos.MasterLogPosition, pos.MasterLogFileIo, pos.MasterLogPositionIo, pos.SecondsBehindMaster))
		}
	}
	for _, l := range lines {
		fmt.Println(l)
	}
	return "", nil
}

func commandListShardTablets(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListShardTablets requires <zk shard path>")
	}
	return "", listTabletsByShard(wrangler.ZkConn(), subFlags.Arg(0))
}

func commandListShardActions(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListShardActions requires <zk shard path>")
	}
	return "", listActionsByShard(wrangler.ZkConn(), subFlags.Arg(0))
}

func commandCreateKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will keep going even if the keyspace already exists")
	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		relog.Fatal("action CreateKeyspace requires 1 arg")
	}
	return "", createKeyspace(wrangler.ZkConn(), subFlags.Arg(0), *force)
}

func commandRebuildKeyspaceGraph(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action RebuildKeyspaceGraph requires at least one <zk keyspace path>")
	}

	zkPaths, err := zk.ResolveWildcards(wrangler.ZkConn(), subFlags.Args())
	if err != nil {
		return "", err
	}
	if len(zkPaths) == 0 {
		return "", nil
	}

	for _, zkPath := range zkPaths {
		if err := wrangler.RebuildKeyspaceGraph(zkPath); err != nil {
			return "", err
		}
	}
	return "", nil
}

func commandValidateKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	pingTablets := subFlags.Bool("ping-tablets", false, "ping all tablets during validate")
	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateKeyspace requires <zk keyspace path>")
	}
	return "", wrangler.ValidateKeyspace(subFlags.Arg(0), *pingTablets)
}

func commandPurgeActions(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action PurgeActions requires <zk action path>")
	}
	for _, zkActionPath := range subFlags.Args() {
		err := tm.PurgeActions(wrangler.ZkConn(), zkActionPath)
		if err != nil {
			return "", err
		}
	}
	return "", nil
}

func commandStaleActions(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	maxStaleness := subFlags.Duration("max-staleness", 5*time.Minute, "how long since the last modification before an action considered stale")
	purge := subFlags.Bool("purge", false, "purge stale actions")
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action StaleActions requires <zk action path>")
	}

	zkPaths, err := zk.ResolveWildcards(wrangler.ZkConn(), subFlags.Args())
	if err != nil {
		return "", err
	}
	var errCount sync2.AtomicInt32
	wg := sync.WaitGroup{}
	for _, apath := range zkPaths {
		wg.Add(1)
		go func(zkActionPath string) {
			defer wg.Done()
			staleActions, err := tm.StaleActions(wrangler.ZkConn(), zkActionPath, *maxStaleness)
			if err != nil {
				errCount.Add(1)
				relog.Error("can't check stale actions: %v %v", zkActionPath, err)
				return
			}
			for _, action := range staleActions {
				fmt.Println(fmtAction(action))
			}
			if *purge && len(staleActions) > 0 {
				err := tm.PurgeActions(wrangler.ZkConn(), zkActionPath)
				if err != nil {
					errCount.Add(1)
					relog.Error("can't purge stale actions: %v %v", zkActionPath, err)
					return
				}
			}
		}(apath)
	}
	wg.Wait()
	if errCount.Get() > 0 {
		return "", fmt.Errorf("some errors occurred, check the log")
	}
	return "", nil
}

func commandPruneActionLogs(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	keepCount := subFlags.Int("keep-count", 10, "count to keep")
	subFlags.Parse(args)

	if subFlags.NArg() == 0 {
		relog.Fatal("action PruneActionLogs requires <zk action log path> ...")
	}

	paths, err := zk.ResolveWildcards(wrangler.ZkConn(), subFlags.Args())
	if err != nil {
		return "", err
	}

	var errCount sync2.AtomicInt32
	wg := sync.WaitGroup{}
	for _, zkActionLogPath := range paths {
		wg.Add(1)
		go func(zkActionLogPath string) {
			defer wg.Done()
			purgedCount, err := tm.PruneActionLogs(wrangler.ZkConn(), zkActionLogPath, *keepCount)
			if err == nil {
				relog.Debug("%v pruned %v", zkActionLogPath, purgedCount)
			} else {
				relog.Error("%v pruning failed: %v", zkActionLogPath, err)
				errCount.Add(1)
			}
		}(zkActionLogPath)
	}
	wg.Wait()
	if errCount.Get() > 0 {
		return "", fmt.Errorf("some errors occurred, check the log")
	}
	return "", nil
}

func commandWaitForAction(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action WaitForAction requires <zk action path>")
	}
	return subFlags.Arg(0), nil
}

func commandResolve(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action Resolve requires <keyspace>.<shard>.<db type>:<port name>")
	}
	parts := strings.Split(subFlags.Arg(0), ":")
	if len(parts) != 2 {
		relog.Fatal("action Resolve requires <keyspace>.<shard>.<db type>:<port name>")
	}
	namedPort := parts[1]

	parts = strings.Split(parts[0], ".")
	if len(parts) != 3 {
		relog.Fatal("action Resolve requires <keyspace>.<shard>.<db type>:<port name>")
	}

	addrs, err := naming.LookupVtName(wrangler.ZkConn(), "", parts[0], parts[1], parts[2], namedPort)
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		fmt.Printf("%v:%v\n", addr.Target, addr.Port)
	}
	return "", nil
}

func commandValidate(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	pingTablets := subFlags.Bool("ping-tablets", false, "ping all tablets during validate")
	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		relog.Fatal("action Validate requires <zk keyspaces path>")
	}
	return "", wrangler.Validate(subFlags.Arg(0), *pingTablets)
}

func commandExportZkns(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ExportZkns requires <zk vt root path>")
	}
	return "", wrangler.ExportZkns(subFlags.Arg(0))
}

func commandExportZknsForKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ExportZknsForKeyspace requires <zk vt root path>")
	}
	return "", wrangler.ExportZknsForKeyspace(subFlags.Arg(0))
}

func commandRebuildReplicationGraph(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	// This is sort of a nuclear option.
	subFlags.Parse(args)
	if subFlags.NArg() < 2 {
		relog.Fatal("action RebuildReplicationGraph requires zk-vt-paths=<zk vt path>,... keyspaces=<keyspace>,...")
	}

	params := parseParams(args)
	var keyspaces, zkVtPaths []string
	if _, ok := params["zk-vt-paths"]; ok {
		var err error
		zkVtPaths, err = zk.ResolveWildcards(wrangler.ZkConn(), strings.Split(params["zk-vt-paths"], ","))
		if err != nil {
			return "", err
		}
	}
	if _, ok := params["keyspaces"]; ok {
		keyspaces = strings.Split(params["keyspaces"], ",")
	}
	// RebuildReplicationGraph zk-vt-paths=/zk/test_nj/vt,/zk/test_ny/vt keyspaces=test_keyspace
	return "", wrangler.RebuildReplicationGraph(zkVtPaths, keyspaces)
}

func commandListIdle(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListIdle requires <zk vt path>")
	}
	return "", listTabletsByType(wrangler.ZkConn(), subFlags.Arg(0), tm.TYPE_IDLE)
}

func commandListScrap(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListScrap requires <zk vt path>")
	}
	return "", listTabletsByType(wrangler.ZkConn(), subFlags.Arg(0), tm.TYPE_SCRAP)
}

func commandListAllTablets(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListAllTablets requires <zk vt path>")
	}

	return "", dumpAllTablets(wrangler.ZkConn(), subFlags.Arg(0))
}

func commandListTablets(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action ListTablets requires <zk tablet path> ...")
	}

	// If these are not zk paths, assume they are tablet aliases.
	if strings.HasPrefix(subFlags.Arg(0), "/zk") {
		return "", dumpTablets(wrangler.ZkConn(), subFlags.Args())
	}
	zkPaths, err := zk.ResolveWildcards(wrangler.ZkConn(), subFlags.Args())
	if err != nil {
		return "", err
	}
	return "", listTabletsByAliases(wrangler.ZkConn(), zkPaths)
}

func commandGetSchema(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	tables := subFlags.String("tables", "", "comma separated tables to gather schema information for")
	includeViews := subFlags.Bool("include-views", false, "include the views in the output")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action GetSchema requires <zk tablet path>")
	}
	var tableArray []string
	if *tables != "" {
		tableArray = strings.Split(*tables, ",")
	}

	sd, err := wrangler.GetSchema(subFlags.Arg(0), tableArray, *includeViews)
	if err == nil {
		relog.Info(sd.String())
	}
	return "", err
}

func commandValidateSchemaShard(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateSchemaShard requires <zk shard path>")
	}

	return "", wrangler.ValidateSchemaShard(subFlags.Arg(0))
}

func commandValidateSchemaKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateSchemaKeyspace requires <zk keyspace path>")
	}

	return "", wrangler.ValidateSchemaKeyspace(subFlags.Arg(0))
}

func commandPreflightSchema(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	sql := subFlags.String("sql", "", "sql command")
	sqlFile := subFlags.String("sql-file", "", "file containing the sql commands")
	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		relog.Fatal("action PreflightSchema requires <zk tablet path>")
	}
	change := getFileParam(*sql, *sqlFile, "sql")
	scr, err := wrangler.PreflightSchema(subFlags.Arg(0), change)
	if err == nil {
		relog.Info(scr.String())
	}
	return "", err
}

func commandApplySchema(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will apply the schema even if preflight schema doesn't match")
	sql := subFlags.String("sql", "", "sql command")
	sqlFile := subFlags.String("sql-file", "", "file containing the sql commands")
	skipPreflight := subFlags.Bool("skip-preflight", false, "do not preflight the schema (use with care)")
	stopReplication := subFlags.Bool("stop-replication", false, "stop replication before applying schema")
	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		relog.Fatal("action ApplySchema requires <zk tablet path>")
	}
	change := getFileParam(*sql, *sqlFile, "sql")

	sc := &mysqlctl.SchemaChange{}
	sc.Sql = change
	sc.AllowReplication = !(*stopReplication)

	// do the preflight to get before and after schema
	if !(*skipPreflight) {
		scr, err := wrangler.PreflightSchema(subFlags.Arg(0), sc.Sql)
		if err != nil {
			relog.Fatal("preflight failed: %v", err)
		}
		relog.Info("Preflight: " + scr.String())
		sc.BeforeSchema = scr.BeforeSchema
		sc.AfterSchema = scr.AfterSchema
		sc.Force = *force
	}

	scr, err := wrangler.ApplySchema(subFlags.Arg(0), sc)
	if err == nil {
		relog.Info(scr.String())
	}
	return "", err
}

func commandApplySchemaShard(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will apply the schema even if preflight schema doesn't match")
	sql := subFlags.String("sql", "", "sql command")
	sqlFile := subFlags.String("sql-file", "", "file containing the sql commands")
	simple := subFlags.Bool("simple", false, "just apply change on master and let replication do the rest")
	newParent := subFlags.String("new-parent", "", "will reparent to this tablet after the change")
	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		relog.Fatal("action ApplySchemaShard requires <zk shard path>")
	}
	change := getFileParam(*sql, *sqlFile, "sql")

	if (*simple) && (*newParent != "") {
		relog.Fatal("new_parent for action ApplySchemaShard can only be specified for complex schema upgrades")
	}

	scr, err := wrangler.ApplySchemaShard(subFlags.Arg(0), change, *newParent, *simple, *force)
	if err == nil {
		relog.Info(scr.String())
	}
	return "", err
}

func commandApplySchemaKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will apply the schema even if preflight schema doesn't match")
	sql := subFlags.String("sql", "", "sql command")
	sqlFile := subFlags.String("sql-file", "", "file containing the sql commands")
	simple := subFlags.Bool("simple", false, "just apply change on master and let replication do the rest")
	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		relog.Fatal("action ApplySchemaKeyspace requires <zk keyspace path>")
	}
	change := getFileParam(*sql, *sqlFile, "sql")

	scr, err := wrangler.ApplySchemaKeyspace(subFlags.Arg(0), change, *simple, *force)
	if err == nil {
		relog.Info(scr.String())
	}
	return "", err
}

// signal handling, centralized here
func installSignalHandlers() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		// we got a signal, notify our modules:
		// - tm will interrupt anything waiting on a tablet action
		// - wr will interrupt anything waiting on a shard or
		//   keyspace lock
		tm.SignalInterrupt()
		wr.SignalInterrupt()
	}()
}

func main() {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			relog.Fatal("panic: %v", tb.Errorf("%v", panicErr))
		}
	}()

	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}
	action := args[0]
	installSignalHandlers()

	logPrefix := "vtctl "
	logLevel, err := relog.LogNameToLogLevel(*logLevel)
	if err != nil {
		relog.Fatal("%v", err)
	}
	relog.SetLevel(logLevel)

	startMsg := fmt.Sprintf("USER=%v SUDO_USER=%v %v", os.Getenv("USER"), os.Getenv("SUDO_USER"), strings.Join(os.Args, " "))

	if log, err := os.OpenFile(*logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		// Use a temp logger to keep a consistent trail of events in the log
		// without polluting stderr in the averge case.
		fileLogger := relog.New(log, logPrefix, logLevel)
		fileLogger.Info(startMsg)
		// Redefine the default logger to keep events in both places.
		relog.SetOutput(io.MultiWriter(log, os.Stderr))
	} else {
		relog.Warning("cannot write to provided logfile: %v", err)
	}

	if syslogger, err := syslog.New(syslog.LOG_INFO, logPrefix); err == nil {
		syslogger.Info(startMsg)
	} else {
		relog.Warning("cannot connect to syslog: %v", err)
	}

	zconn := zk.NewMetaConn(false)
	defer zconn.Close()

	wrangler := wr.NewWrangler(zconn, *waitTime, *lockWaitTimeout)
	var actionPath string

	found := false
	for _, group := range commands {
		for _, cmd := range group.commands {
			if cmd.name == action {
				subFlags := flag.NewFlagSet(action, flag.ExitOnError)
				subFlags.Usage = func() {
					fmt.Fprintf(os.Stderr, "Usage: %s %s %s\n\n", os.Args[0], cmd.name, cmd.params)
					fmt.Fprintf(os.Stderr, "%s\n\n", cmd.help)
					subFlags.PrintDefaults()
				}

				actionPath, err = cmd.method(wrangler, subFlags, args[1:])
				found = true
			}
		}
	}
	if !found {
		fmt.Fprintf(os.Stderr, "Unknown command %#v\n\n", action)
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		relog.Fatal("action failed: %v %v", action, err)
	}
	if actionPath != "" {
		if *noWaitForAction {
			fmt.Println(actionPath)
		} else {
			err := wrangler.ActionInitiator().WaitForCompletion(actionPath, *waitTime)
			if err != nil {
				relog.Fatal(err.Error())
			} else {
				relog.Info("action completed: %v", actionPath)
			}
		}
	}
}

type rTablet struct {
	*tm.TabletInfo
	*mysqlctl.ReplicationPosition
}

type rTablets []*rTablet

func (rts rTablets) Len() int { return len(rts) }

func (rts rTablets) Swap(i, j int) { rts[i], rts[j] = rts[j], rts[i] }

// Sort for tablet replication.
// master first, then i/o position, then sql position
func (rts rTablets) Less(i, j int) bool {
	// NOTE: Swap order of unpack to reverse sort
	l, r := rts[j], rts[i]
	// l or r ReplicationPosition would be nil if we failed to get
	// the position (put them at the beginning of the list)
	if l.ReplicationPosition == nil {
		return r.ReplicationPosition != nil
	}
	if r.ReplicationPosition == nil {
		return false
	}
	var lTypeMaster, rTypeMaster int
	if l.Type == tm.TYPE_MASTER {
		lTypeMaster = 1
	}
	if r.Type == tm.TYPE_MASTER {
		rTypeMaster = 1
	}
	if lTypeMaster < rTypeMaster {
		return true
	}
	if lTypeMaster == rTypeMaster {
		if l.MapKeyIo() < r.MapKeyIo() {
			return true
		}
		if l.MapKeyIo() == r.MapKeyIo() {
			if l.MapKey() < r.MapKey() {
				return true
			}
		}
	}
	return false
}

func sortReplicatingTablets(tablets []*tm.TabletInfo, positions []*mysqlctl.ReplicationPosition) []*rTablet {
	rtablets := make([]*rTablet, len(tablets))
	for i, pos := range positions {
		rtablets[i] = &rTablet{tablets[i], pos}
	}
	sort.Sort(rTablets(rtablets))
	return rtablets
}
