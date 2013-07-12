// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log/syslog"
	"os"
	"os/signal"
	"path"
	"sort"
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
	"code.google.com/p/vitess/go/vt/zktopo"
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
				"[-force] [-db-name-override=<db name>] <tablet alias|zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> [<parent alias|zk parent alias>]",
				"Initializes a tablet in zookeeper."},
			command{"UpdateTablet", commandUpdateTablet,
				"[-force] [-db-name-override=<db name>] <zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> <zk parent alias>",
				"DEPRECATED (use ChangeSlaveType or other operations instead).\n" +
					"Updates a tablet in zookeeper."},
			command{"ScrapTablet", commandScrapTablet,
				"[-force] [-skip-rebuild] <tablet alias|zk tablet path>",
				"Scraps a tablet."},
			command{"SetReadOnly", commandSetReadOnly,
				"[<tablet alias|zk tablet path>]",
				"Sets the tablet as ReadOnly."},
			command{"SetReadWrite", commandSetReadWrite,
				"[<tablet alias|zk tablet path>]",
				"Sets the tablet as ReadWrite."},
			command{"DemoteMaster", commandDemoteMaster,
				"<tablet alias|zk tablet path>",
				"Demotes a master tablet."},
			command{"ChangeSlaveType", commandChangeSlaveType,
				"[-force] [-dry-run] <tablet alias|zk tablet path> <db type>",
				"Change the db type for this tablet if possible. This is mostly for arranging replicas - it will not convert a master.\n" +
					"NOTE: This will automatically update the serving graph.\n" +
					"Valid <db type>:\n" +
					"  " + strings.Join(naming.SlaveTabletTypeStrings, " ") + "\n"},
			command{"ChangeType", commandChangeSlaveType,
				"[-force] <tablet alias|zk tablet path> <db type>",
				"DEPRECATED (use ChangeSlaveType instead).\n" +
					"Change the db type for this tablet if possible. This is mostly for arranging replicas - it will not convert a master.\n" +
					"NOTE: This will automatically update the serving graph."},
			command{"Ping", commandPing,
				"<tablet alias|zk tablet path>",
				"Check that the agent is awake and responding - can be blocked by other in-flight operations."},
			command{"RpcPing", commandRpcPing,
				"<tablet alias|zk tablet path>",
				"Check that the agent is awake and responding to RPCs."},
			command{"Query", commandQuery,
				"<cell> <keyspace> [<user> <password>] <query>",
				"Send a SQL query to a tablet."},
			command{"Sleep", commandSleep,
				"<tablet alias|zk tablet path> <duration>",
				"Block the action queue for the specified duration (mostly for testing)."},
			command{"Snapshot", commandSnapshot,
				"[-force] [-server-mode] [-concurrency=4] <tablet alias|zk tablet path>",
				"Stop mysqld and copy compressed data aside."},
			command{"SnapshotSourceEnd", commandSnapshotSourceEnd,
				"[-slave-start] [-read-write] <tablet alias|zk tablet path> <original tablet type>",
				"Restarts Mysql and restore original server type."},
			command{"Restore", commandRestore,
				"[-fetch-concurrency=3] [-fetch-retry-count=3] [-dont-wait-for-slave-start] <src tablet alias|zk src tablet path> <src manifest file> <dst tablet alias|zk dst tablet path> [<zk new master path>]",
				"Copy the given snaphot from the source tablet and restart replication to the new master path (or uses the <src tablet path> if not specified). If <src manifest file> is 'default', uses the default value.\n" +
					"NOTE: This does not wait for replication to catch up. The destination tablet must be 'idle' to begin with. It will transition to 'spare' once the restore is complete."},
			command{"Clone", commandClone,
				"[-force] [-concurrency=4] [-fetch-concurrency=3] [-fetch-retry-count=3] [-server-mode] <src tablet alias|zk src tablet path> <dst tablet alias|zk dst tablet path> ...",
				"This performs Snapshot and then Restore on all the targets in parallel. The advantage of having separate actions is that one snapshot can be used for many restores, and it's then easier to spread them over time."},
			command{"ReparentTablet", commandReparentTablet,
				"<tablet alias|zk tablet path>",
				"Reparent a tablet to the current master in the shard. This only works if the current slave position matches the last known reparent action."},
			command{"PartialSnapshot", commandPartialSnapshot,
				"[-force] [-concurrency=4] <tablet alias|zk tablet path> <key name> <start key> <end key>",
				"Locks mysqld and copy compressed data aside."},
			command{"MultiSnapshot", commandMultiSnapshot,
				"[-force] [-concurrency=8] [-skip-slave-restart] [-maximum-file-size=134217728] -spec='-' -tables='' <tablet alias|zk tablet path> <key name>",
				"Locks mysqld and copy compressed data aside."},
			command{"MultiRestore", commandMultiRestore,
				"[-force] [-concurrency=4] [-fetch-concurrency=4] [-insert-table-concurrency=4] [-fetch-retry-count=3] [-strategy=] <dst tablet alias|destination zk path> <source zk path>...",
				"Restores a snapshot from multiple hosts."},
			command{"PartialRestore", commandPartialRestore,
				"[-fetch-concurrency=3] [-fetch-retry-count=3] <src tablet alias|zk src tablet path> <src manifest file> <dst tablet alias|zk dst tablet path> [<zk new master path>]",
				"Copy the given partial snaphot from the source tablet and starts partial replication to the new master path (or uses the src tablet path if not specified).\n" +
					"NOTE: This does not wait for replication to catch up. The destination tablet must be 'idle' to begin with. It will transition to 'spare' once the restore is complete."},
			command{"PartialClone", commandPartialClone,
				"[-force] [-concurrency=4] [-fetch-concurrency=3] [-fetch-retry-count=3] <src tablet alias|zk src tablet path> <dst tablet alias|zk dst tablet path> <key name> <start key> <end key>",
				"This performs PartialSnapshot and then PartialRestore.  The advantage of having separate actions is that one partial snapshot can be used for many restores."},
			command{"ExecuteHook", commandExecuteHook,
				"<tablet alias|zk tablet path> <hook name> [<param1=value1> <param2=value2> ...]",
				"This runs the specified hook on the given tablet."},
		},
	},
	commandGroup{
		"Shards", []command{
			command{"RebuildShardGraph", commandRebuildShardGraph,
				"[-cells=a,b] <zk shard path> ... (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>)",
				"Rebuild the replication graph and shard serving data in zk. This may trigger an update to all connected clients."},
			command{"ReparentShard", commandReparentShard,
				"[-force] [-leave-master-read-only] <keyspace/shard|zk shard path> <tablet alias|zk tablet path>",
				"Specify which shard to reparent and which tablet should be the new master."},
			command{"ShardExternallyReparented", commandShardExternallyReparented,
				"[-scrap-stragglers] <keyspace/shard|zk shard path> <tablet alias|zk tablet path>",
				"Changes metadata to acknowledge a shard master change performed by an external tool."},
			command{"ValidateShard", commandValidateShard,
				"[-ping-tablets] <keyspace/shard|zk shard path>",
				"Validate all nodes reachable from this shard are consistent."},
			command{"ShardReplicationPositions", commandShardReplicationPositions,
				"<keyspace/shard|zk shard path>",
				"Show slave status on all machines in the shard graph."},
			command{"ListShardTablets", commandListShardTablets,
				"<keyspace/shard|zk shard path>)",
				"List all tablets in a given shard."},
			command{"ListShardActions", commandListShardActions,
				"<keyspace/shard|zk shard path>",
				"(requires Zookeeper TopologyServer)\n" +
					"List all active actions in a given shard."},
		},
	},
	commandGroup{
		"Keyspaces", []command{
			command{"CreateKeyspace", commandCreateKeyspace,
				"[-force] <keyspace name|zk keyspace path>",
				"Creates the given keyspace"},
			command{"RebuildKeyspaceGraph", commandRebuildKeyspaceGraph,
				"[-cells=a,b] <zk keyspace path> ... (/zk/global/vt/keyspaces/<keyspace>)",
				"Rebuild the serving data for all shards in this keyspace. This may trigger an update to all connected clients."},
			command{"ValidateKeyspace", commandValidateKeyspace,
				"[-ping-tablets] <keyspace name|zk keyspace path>",
				"Validate all nodes reachable from this keyspace are consistent."},
		},
	},
	commandGroup{
		"Generic", []command{
			command{"PurgeActions", commandPurgeActions,
				"<zk action path> ... (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>/action)",
				"(requires Zookeeper TopologyServer)\n" +
					"Remove all actions - be careful, this is powerful cleanup magic."},
			command{"StaleActions", commandStaleActions,
				"[-max-staleness=<duration> -purge] <zk action path> ... (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>/action)",
				"(requires Zookeeper TopologyServer)\n" +
					"List any queued actions that are considered stale."},
			command{"PruneActionLogs", commandPruneActionLogs,
				"[-keep-count=<count to keep>] <zk actionlog path> ...",
				"(requires Zookeeper TopologyServer)\n" +
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
				"<cell name|zk local vt path>",
				"(requires Zookeeper TopologyServer)\n" +
					"Export the serving graph entries to the zkns format."},
			command{"ExportZknsForKeyspace", commandExportZknsForKeyspace,
				"<keyspace|zk global keyspace path>",
				"(requires Zookeeper TopologyServer)\n" +
					"Export the serving graph entries to the zkns format."},
			command{"RebuildReplicationGraph", commandRebuildReplicationGraph,
				"<cell1|zk local vt path1>,<cell2|zk local vt path2>... <keyspace1>,<keyspace2>,...",
				"This takes the Thor's hammer approach of recovery and should only be used in emergencies.  cell1,cell2,... are the canonical source of data for the system. This function uses that canonical data to recover the replication graph, at which point further auditing with Validate can reveal any remaining issues."},
			command{"ListIdle", commandListIdle,
				"<cell name|zk local vt path> (/zk/<cell>/vt)",
				"DEPRECATED (use ListAllTablets + awk)\n" +
					"List all idle tablet paths."},
			command{"ListScrap", commandListScrap,
				"<cell name|zk local vt path>  (/zk/<cell>/vt)",
				"DEPRECATED (use ListAllTablets + awk)\n" +
					"List all scrap tablet paths."},
			command{"ListAllTablets", commandListAllTablets,
				"<cell name|zk local vt path>",
				"List all tablets in an awk-friendly way."},
			command{"ListTablets", commandListTablets,
				"<tablet alias|zk tablet path> ...",
				"List specified tablets in an awk-friendly way."},
		},
	},
	commandGroup{
		"Schema, Version, Permissions", []command{
			command{"GetSchema", commandGetSchema,
				"[-tables=<table1>,<table2>,...] [-include-views] <tablet alias|zk tablet path>",
				"Display the full schema for a tablet, or just the schema for the provided tables."},
			command{"ValidateSchemaShard", commandValidateSchemaShard,
				"[-include-views] <keyspace/shard|zk shard path>",
				"Validate the master schema matches all the slaves."},
			command{"ValidateSchemaKeyspace", commandValidateSchemaKeyspace,
				"[-include-views] <keyspace name|zk keyspace path>",
				"Validate the master schema from shard 0 matches all the other tablets in the keyspace."},
			command{"PreflightSchema", commandPreflightSchema,
				"{-sql=<sql> || -sql-file=<filename>} <tablet alias|zk tablet path>",
				"Apply the schema change to a temporary database to gather before and after schema and validate the change. The sql can be inlined or read from a file."},
			command{"ApplySchema", commandApplySchema,
				"[-force] {-sql=<sql> || -sql-file=<filename>} [-skip-preflight] [-stop-replication] <tablet alias|zk tablet path>",
				"Apply the schema change to the specified tablet (allowing replication by default). The sql can be inlined or read from a file. Note this doesn't change any tablet state (doesn't go into 'schema' type)."},
			command{"ApplySchemaShard", commandApplySchemaShard,
				"[-force] {-sql=<sql> || -sql-file=<filename>} [-simple] [-new-parent=<zk tablet path>] <keyspace/shard|zk shard path>",
				"Apply the schema change to the specified shard. If simple is specified, we just apply on the live master. Otherwise we will need to do the shell game. So we will apply the schema change to every single slave. if new_parent is set, we will also reparent (otherwise the master won't be touched at all). Using the force flag will cause a bunch of checks to be ignored, use with care."},
			command{"ApplySchemaKeyspace", commandApplySchemaKeyspace,
				"[-force] {-sql=<sql> || -sql-file=<filename>} [-simple] <keyspace|zk keyspace path>",
				"Apply the schema change to the specified keyspace. If simple is specified, we just apply on the live masters. Otherwise we will need to do the shell game on each shard. So we will apply the schema change to every single slave (running in parallel on all shards, but on one host at a time in a given shard). We will not reparent at the end, so the masters won't be touched at all. Using the force flag will cause a bunch of checks to be ignored, use with care."},

			command{"ValidateVersionShard", commandValidateVersionShard,
				"<keyspace/shard|zk shard path>",
				"Validate the master version matches all the slaves."},
			command{"ValidateVersionKeyspace", commandValidateVersionKeyspace,
				"<keyspace name|zk keyspace path>",
				"Validate the master version from shard 0 matches all the other tablets in the keyspace."},

			command{"GetPermissions", commandGetPermissions,
				"<tablet alias|zk tablet path>",
				"Display the permissions for a tablet."},
			command{"ValidatePermissionsShard", commandValidatePermissionsShard,
				"<keyspace/shard|zk shard path>",
				"Validate the master permissions match all the slaves."},
			command{"ValidatePermissionsKeyspace", commandValidatePermissionsKeyspace,
				"<keyspace name|zk keyspace path>",
				"Validate the master permissions from shard 0 match all the other tablets in the keyspace."},
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

func listTabletsByType(ts naming.TopologyServer, cell string, dbType naming.TabletType) error {
	tablets, err := wr.GetAllTablets(ts, cell)
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

func listActionsByShard(ts naming.TopologyServer, keyspace, shard string) error {
	// only works with ZkTopologyServer
	zkts, ok := ts.(*zktopo.ZkTopologyServer)
	if !ok {
		return fmt.Errorf("listActionsByShard only works with ZkTopologyServer")
	}

	// print the shard action nodes
	shardActionPath := zkts.ShardActionPath(keyspace, shard)
	shardActionNodes, err := getActions(zkts.GetZConn(), shardActionPath)
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
		actionNodes, err := getActions(zkts.GetZConn(), actionPath)
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

	tabletAliases, err := tm.FindAllTabletAliasesInShard(ts, keyspace, shard)
	if err != nil {
		return err
	}
	for _, tabletAlias := range tabletAliases {
		tabletPath := tm.TabletPathForAlias(tabletAlias)
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

func listTabletsByShard(ts naming.TopologyServer, keyspace, shard string) error {
	tabletAliases, err := tm.FindAllTabletAliasesInShard(ts, keyspace, shard)
	if err != nil {
		return err
	}
	return dumpTablets(ts, tabletAliases)
}

func dumpAllTablets(ts naming.TopologyServer, zkVtPath string) error {
	tablets, err := wr.GetAllTablets(ts, zkVtPath)
	if err != nil {
		return err
	}
	for _, ti := range tablets {
		fmt.Println(fmtTabletAwkable(ti))
	}
	return nil
}

func dumpTablets(ts naming.TopologyServer, tabletAliases []naming.TabletAlias) error {
	tabletMap, err := wr.GetTabletMap(ts, tabletAliases)
	if err != nil {
		return err
	}
	for _, tabletAlias := range tabletAliases {
		ti, ok := tabletMap[tabletAlias]
		if !ok {
			relog.Warning("failed to load tablet %v", tabletAlias)
		} else {
			fmt.Println(fmtTabletAwkable(ti))
		}
	}
	return nil
}

func kquery(ts naming.TopologyServer, cell, keyspace, user, password, query string) error {
	sconn, err := client2.Dial(ts, cell, keyspace, "master", false, 5*time.Second, user, password)
	if err != nil {
		return err
	}
	rows, err := sconn.Exec(query, nil)
	if err != nil {
		return err
	}
	cols := rows.Columns()
	fmt.Println(strings.Join(cols, "\t"))

	rowStrs := make([]string, len(cols)+1)
	for row := rows.Next(); row != nil; row = rows.Next() {
		for i, value := range row {
			switch value.(type) {
			case []byte:
				rowStrs[i] = fmt.Sprintf("%q", value)
			default:
				rowStrs[i] = fmt.Sprintf("%v", value)
			}
		}

		fmt.Println(strings.Join(rowStrs, "\t"))
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

func keyspaceParamToKeyspace(param string) string {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style string keyspace
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 6 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[2] != "global" || zkPathParts[3] != "vt" || zkPathParts[4] != "keyspaces" {
			relog.Fatal("Invalid keyspace path: %v", param)
		}
		return zkPathParts[5]
	}
	return param
}

func shardParamToKeyspaceShard(param string) (string, string) {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 8 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[2] != "global" || zkPathParts[3] != "vt" || zkPathParts[4] != "keyspaces" || zkPathParts[6] != "shards" {
			relog.Fatal("Invalid shard path: %v", param)
		}
		return zkPathParts[5], zkPathParts[7]
	}
	zkPathParts := strings.Split(param, "/")
	if len(zkPathParts) != 2 {
		relog.Fatal("Invalid shard path: %v", param)
	}
	return zkPathParts[0], zkPathParts[1]
}

// tabletParamToTabletAlias takes either an old style ZK tablet path or a
// new style tablet alias as a string, and returns a TabletAlias.
func tabletParamToTabletAlias(param string) naming.TabletAlias {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style string tablet alias
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 6 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[3] != "vt" || zkPathParts[4] != "tablets" {
			relog.Fatal("Invalid tablet path: %v", param)
		}
		param = zkPathParts[2] + "-" + zkPathParts[5]
	}
	result, err := naming.ParseTabletAliasString(param)
	if err != nil {
		relog.Fatal("Invalid tablet alias %v: %v", param, err)
	}
	return result
}

// tabletRepParamToTabletAlias takes either an old style ZK tablet replication
// path or a new style tablet alias as a string, and returns a
// TabletAlias.
func tabletRepParamToTabletAlias(param string) naming.TabletAlias {
	if param[0] == '/' {
		// old zookeeper replication path, e.g.
		// /zk/global/vt/keyspaces/ruser/shards/10-20/nyc-0000200278
		// convert to new-style string tablet alias
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 9 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[2] != "global" || zkPathParts[3] != "vt" || zkPathParts[4] != "keyspaces" || zkPathParts[6] != "shards" {
			relog.Fatal("Invalid tablet replication path: %v", param)
		}
		param = zkPathParts[8]
	}
	result, err := naming.ParseTabletAliasString(param)
	if err != nil {
		relog.Fatal("Invalid tablet alias %v: %v", param, err)
	}
	return result
}

// vtPathToCell takes either an old style ZK vt path /zk/<cell>/vt or
// a new style cell and returns the cell name
func vtPathToCell(param string) string {
	if param[0] == '/' {
		// old zookeeper replication path like /zk/<cell>/vt
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 4 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[3] != "vt" {
			relog.Fatal("Invalid vt path: %v", param)
		}
		return zkPathParts[2]
	}
	return param
}

func resolveWildcards(wrangler *wr.Wrangler, args []string) ([]string, error) {
	zkts, ok := wrangler.TopologyServer().(*zktopo.ZkTopologyServer)
	if !ok {
		return args, nil
	}
	return zk.ResolveWildcards(zkts.GetZConn(), args)
}

func commandInitTablet(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	dbNameOverride := subFlags.String("db-name-override", "", "override the name of the db used by vttablet")
	force := subFlags.Bool("force", false, "will overwrite the node if it already exists")
	subFlags.Parse(args)
	if subFlags.NArg() != 7 && subFlags.NArg() != 8 {
		relog.Fatal("action InitTablet requires <tablet alias|zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> [<parent alias|zk parent alias>]")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	parentAlias := naming.TabletAlias{}
	if subFlags.NArg() == 8 {
		parentAlias = tabletRepParamToTabletAlias(subFlags.Arg(7))
	}
	return "", wrangler.InitTablet(tabletAlias, subFlags.Arg(1), subFlags.Arg(2), subFlags.Arg(3), subFlags.Arg(4), subFlags.Arg(5), subFlags.Arg(6), parentAlias, *dbNameOverride, *force, false)
}

func commandUpdateTablet(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	dbNameOverride := subFlags.String("db-name-override", "", "override the name of the db used by vttablet")
	force := subFlags.Bool("force", false, "will overwrite the node if it already exists")
	subFlags.Parse(args)
	if subFlags.NArg() != 8 {
		relog.Fatal("action UpdateTablet requires <tablet alias|zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> <parent alias|zk parent alias>")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	parentAlias := tabletRepParamToTabletAlias(subFlags.Arg(7))
	return "", wrangler.InitTablet(tabletAlias, subFlags.Arg(1), subFlags.Arg(2), subFlags.Arg(3), subFlags.Arg(4), subFlags.Arg(5), subFlags.Arg(6), parentAlias, *dbNameOverride, *force, true)
}

func commandScrapTablet(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "writes the scrap state in to zk, no questions asked, if a tablet is offline")
	skipRebuild := subFlags.Bool("skip-rebuild", false, "do not rebuild the shard and keyspace graph after scrapping")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ScrapTablet requires <tablet alias|zk tablet path>")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return wrangler.Scrap(tabletAlias, *force, *skipRebuild)
}

func commandSetReadOnly(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action SetReadOnly requires <tablet alias|zk tablet path>")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return wrangler.ActionInitiator().SetReadOnly(tabletAlias)
}

func commandSetReadWrite(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action SetReadWrite requires <tablet alias|zk tablet path>")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return wrangler.ActionInitiator().SetReadWrite(tabletAlias)
}

func commandDemoteMaster(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action DemoteMaster requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return wrangler.ActionInitiator().DemoteMaster(tabletAlias)
}

func commandChangeSlaveType(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will change the type in zookeeper, and not run hooks")
	dryRun := subFlags.Bool("dry-run", false, "just list the proposed change")

	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action ChangeSlaveType requires <zk tablet path> <db type>")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	newType := naming.TabletType(subFlags.Arg(1))
	if *dryRun {
		ti, err := tm.ReadTablet(wrangler.TopologyServer(), tabletAlias)
		if err != nil {
			relog.Fatal("failed reading tablet %v: %v", tabletAlias, err)
		}
		if !naming.IsTrivialTypeChange(ti.Type, newType) || !naming.IsValidTypeChange(ti.Type, newType) {
			relog.Fatal("invalid type transition %v: %v -> %v", tabletAlias, ti.Type, newType)
		}
		fmt.Printf("- %v\n", fmtTabletAwkable(ti))
		ti.Type = newType
		fmt.Printf("+ %v\n", fmtTabletAwkable(ti))
		return "", nil
	}
	return "", wrangler.ChangeType(tabletAlias, newType, *force)
}

func commandPing(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action Ping requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return wrangler.ActionInitiator().Ping(tabletAlias)
}

func commandRpcPing(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action Ping requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return "", wrangler.ActionInitiator().RpcPing(tabletAlias, *waitTime)
}

func commandQuery(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 3 && subFlags.NArg() != 5 {
		relog.Fatal("action Query requires 3 or 5 args")
	}
	if subFlags.NArg() == 3 {
		return "", kquery(wrangler.TopologyServer(), subFlags.Arg(0), subFlags.Arg(1), "", "", subFlags.Arg(2))
	}

	return "", kquery(wrangler.TopologyServer(), subFlags.Arg(0), subFlags.Arg(1), subFlags.Arg(2), subFlags.Arg(3), subFlags.Arg(4))
}

func commandSleep(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action Sleep requires <tablet alias|zk tablet path> <duration>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	duration, err := time.ParseDuration(subFlags.Arg(1))
	if err != nil {
		return "", err
	}
	return wrangler.ActionInitiator().Sleep(tabletAlias, duration)
}

func commandSnapshotSourceEnd(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	slaveStartRequired := subFlags.Bool("slave-start", false, "will restart replication")
	readWrite := subFlags.Bool("read-write", false, "will make the server read-write")
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action SnapshotSourceEnd requires <tablet alias|zk tablet path> <original server type>")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return "", wrangler.SnapshotSourceEnd(tabletAlias, *slaveStartRequired, !(*readWrite), naming.TabletType(subFlags.Arg(1)))
}

func commandSnapshot(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	serverMode := subFlags.Bool("server-mode", false, "will symlink the data files and leave mysqld stopped")
	concurrency := subFlags.Int("concurrency", 4, "how many compression/checksum jobs to run simultaneously")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action Snapshot requires <tablet alias|zk src tablet path>")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	filename, parentAlias, slaveStartRequired, readOnly, originalType, err := wrangler.Snapshot(tabletAlias, *force, *concurrency, *serverMode)
	if err == nil {
		relog.Info("Manifest: %v", filename)
		relog.Info("ParentAlias: %v", parentAlias)
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
		relog.Fatal("action Restore requires <src tablet alias|zk src tablet path> <src manifest path> <dst tablet alias|zk dst tablet path> [<zk new master path>]")
	}
	srcTabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	dstTabletAlias := tabletParamToTabletAlias(subFlags.Arg(2))
	parentAlias := srcTabletAlias
	if subFlags.NArg() == 4 {
		parentAlias = tabletParamToTabletAlias(subFlags.Arg(3))
	}
	return "", wrangler.Restore(srcTabletAlias, subFlags.Arg(1), dstTabletAlias, parentAlias, *fetchConcurrency, *fetchRetryCount, false, *dontWaitForSlaveStart)
}

func commandClone(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 4, "how many compression/checksum jobs to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	serverMode := subFlags.Bool("server-mode", false, "will keep the snapshot server offline to serve DB files directly")
	subFlags.Parse(args)
	if subFlags.NArg() < 2 {
		relog.Fatal("action Clone requires <src tablet alias|zk src tablet path> <dst tablet alias|zk dst tablet path> ...")
	}

	srcTabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	dstTabletAliases := make([]naming.TabletAlias, subFlags.NArg()-1)
	for i := 1; i < subFlags.NArg(); i++ {
		dstTabletAliases[i-1] = tabletParamToTabletAlias(subFlags.Arg(i))
	}
	return "", wrangler.Clone(srcTabletAlias, dstTabletAliases, *force, *concurrency, *fetchConcurrency, *fetchRetryCount, *serverMode)
}

func commandReparentTablet(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ReparentTablet requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return "", wrangler.ReparentTablet(tabletAlias)
}

func commandPartialSnapshot(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 4, "how many compression jobs to run simultaneously")
	subFlags.Parse(args)
	if subFlags.NArg() != 4 {
		relog.Fatal("action PartialSnapshot requires <src tablet alias|zk src tablet path> <key name> <start key> <end key>")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	filename, parentAlias, err := wrangler.PartialSnapshot(tabletAlias, subFlags.Arg(1), key.HexKeyspaceId(subFlags.Arg(2)), key.HexKeyspaceId(subFlags.Arg(3)), *force, *concurrency)
	if err == nil {
		relog.Info("Manifest: %v", filename)
		relog.Info("ParentAlias: %v", parentAlias)
	}
	return "", err
}

func commandMultiRestore(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (status string, err error) {
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	concurrency := subFlags.Int("concurrency", 8, "how many concurrent jobs to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 4, "how many files to fetch simultaneously")
	insertTableConcurrency := subFlags.Int("insert-table-concurrency", 4, "how many myisam tables to load into a single destination table simultaneously")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, use 'mysqlctl multirestore -help' for more info")
	subFlags.Parse(args)

	if subFlags.NArg() < 2 {
		relog.Fatal("MultiRestore requires <dst tablet alias|destination zk path> <source zk path>... %v", args)
	}
	destination := tabletParamToTabletAlias(subFlags.Arg(0))
	sources := make([]naming.TabletAlias, subFlags.NArg()-1)
	for i := 1; i < subFlags.NArg(); i++ {
		sources[i-1] = tabletParamToTabletAlias(subFlags.Arg(i))
	}
	err = wrangler.RestoreFromMultiSnapshot(destination, sources, *concurrency, *fetchConcurrency, *insertTableConcurrency, *fetchRetryCount, *strategy)
	return
}

func commandMultiSnapshot(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 8, "how many compression jobs to run simultaneously")
	spec := subFlags.String("spec", "-", "shard specification")
	tablesString := subFlags.String("tables", "", "dump only this comma separated list of tables")
	skipSlaveRestart := subFlags.Bool("skip-slave-restart", false, "after the snapshot is done, do not restart slave replication")
	maximumFilesize := subFlags.Uint64("maximum-file-size", 128*1024*1024, "the maximum size for an uncompressed data file")
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action PartialSnapshot requires <src tablet alias|zk src tablet path> <key name>")
	}

	shards, err := key.ParseShardingSpec(*spec)
	if err != nil {
		relog.Fatal("multisnapshot failed: %v", err)
	}
	var tables []string
	if *tablesString != "" {
		tables = strings.Split(*tablesString, ",")
	}

	source := tabletParamToTabletAlias(subFlags.Arg(0))
	filenames, parentAlias, err := wrangler.MultiSnapshot(shards, source, subFlags.Arg(1), *concurrency, tables, *force, *skipSlaveRestart, *maximumFilesize)

	if err == nil {
		relog.Info("manifest locations: %v", filenames)
		relog.Info("ParentAlias: %v", parentAlias)
	}
	return "", err
}

func commandPartialRestore(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	subFlags.Parse(args)
	if subFlags.NArg() != 3 && subFlags.NArg() != 4 {
		relog.Fatal("action PartialRestore requires <src tablet alias|zk src tablet path> <src manifest path> <dst tablet alias|zk dst tablet path> [<zk new master path>]")
	}
	source := tabletParamToTabletAlias(subFlags.Arg(0))
	destination := tabletParamToTabletAlias(subFlags.Arg(2))
	parentAlias := source
	if subFlags.NArg() == 4 {
		parentAlias = tabletParamToTabletAlias(subFlags.Arg(3))
	}
	return "", wrangler.PartialRestore(source, subFlags.Arg(1), destination, parentAlias, *fetchConcurrency, *fetchRetryCount)
}

func commandPartialClone(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 4, "how many compression jobs to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	subFlags.Parse(args)
	if subFlags.NArg() != 5 {
		relog.Fatal("action PartialClone requires <src tablet alias|zk src tablet path> <dst tablet alias|zk dst tablet path> <key name> <start key> <end key>")
	}

	source := tabletParamToTabletAlias(subFlags.Arg(0))
	destination := tabletParamToTabletAlias(subFlags.Arg(1))
	return "", wrangler.PartialClone(source, destination, subFlags.Arg(2), key.HexKeyspaceId(subFlags.Arg(3)), key.HexKeyspaceId(subFlags.Arg(4)), *force, *concurrency, *fetchConcurrency, *fetchRetryCount)
}

func commandExecuteHook(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() < 2 {
		relog.Fatal("action ExecuteHook requires <tablet alias|zk tablet path> <hook name>")
	}

	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	hook := &hk.Hook{Name: subFlags.Arg(1), Parameters: parseParams(subFlags.Args()[2:])}
	hr, err := wrangler.ExecuteHook(tabletAlias, hook)
	if err == nil {
		relog.Info(hr.String())
	}
	return "", err
}

func commandRebuildShardGraph(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	cells := subFlags.String("cells", "", "comma separated list of cells to update")
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action RebuildShardGraph requires at least one <zk shard path>")
	}

	var cellArray []string
	if *cells != "" {
		cellArray = strings.Split(*cells, ",")
	}

	zkPaths, err := resolveWildcards(wrangler, subFlags.Args())
	if err != nil {
		return "", err
	}
	if len(zkPaths) == 0 {
		return "", nil
	}

	for _, zkPath := range zkPaths {
		keyspace, shard := shardParamToKeyspaceShard(zkPath)
		if err := wrangler.RebuildShardGraph(keyspace, shard, cellArray); err != nil {
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
		relog.Fatal("action ReparentShard requires <keyspace/shard|zk shard path> <tablet alias|zk tablet path>")
	}

	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(1))
	return "", wrangler.ReparentShard(keyspace, shard, tabletAlias, *leaveMasterReadOnly, *force)
}

func commandShardExternallyReparented(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	scrapStragglers := subFlags.Bool("scrap-stragglers", false, "will scrap the hosts that haven't been reparented")
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		relog.Fatal("action ShardExternallyReparented requires <keyspace/shard|zk shard path> <tablet alias|zk tablet path>")
	}

	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(1))
	return "", wrangler.ShardExternallyReparented(keyspace, shard, tabletAlias, *scrapStragglers)
}

func commandValidateShard(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	pingTablets := subFlags.Bool("ping-tablets", true, "ping all tablets during validate")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return "", wrangler.ValidateShard(keyspace, shard, *pingTablets)
}

func commandShardReplicationPositions(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ShardReplicationPositions requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	tablets, positions, err := wrangler.ShardReplicationPositions(keyspace, shard)
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
		relog.Fatal("action ListShardTablets requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return "", listTabletsByShard(wrangler.TopologyServer(), keyspace, shard)
}

func commandListShardActions(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListShardActions requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return "", listActionsByShard(wrangler.TopologyServer(), keyspace, shard)
}

func commandCreateKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	force := subFlags.Bool("force", false, "will keep going even if the keyspace already exists")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action CreateKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace := keyspaceParamToKeyspace(subFlags.Arg(0))
	err := wrangler.TopologyServer().CreateKeyspace(keyspace)
	if *force && err == naming.ErrNodeExists {
		relog.Info("keyspace %v already exists (ignoring error with -force)", keyspace)
		err = nil
	}
	return "", err
}

func commandRebuildKeyspaceGraph(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	cells := subFlags.String("cells", "", "comma separated list of cells to update")
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action RebuildKeyspaceGraph requires at least one <zk keyspace path>")
	}

	var cellArray []string
	if *cells != "" {
		cellArray = strings.Split(*cells, ",")
	}

	zkPaths, err := resolveWildcards(wrangler, subFlags.Args())
	if err != nil {
		return "", err
	}
	if len(zkPaths) == 0 {
		return "", nil
	}

	for _, zkPath := range zkPaths {
		keyspace := keyspaceParamToKeyspace(zkPath)
		if err := wrangler.RebuildKeyspaceGraph(keyspace, cellArray); err != nil {
			return "", err
		}
	}
	return "", nil
}

func commandValidateKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	pingTablets := subFlags.Bool("ping-tablets", false, "ping all tablets during validate")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace := keyspaceParamToKeyspace(subFlags.Arg(0))
	return "", wrangler.ValidateKeyspace(keyspace, *pingTablets)
}

func commandPurgeActions(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action PurgeActions requires <zk action path> ...")
	}
	zkts, ok := wrangler.TopologyServer().(*zktopo.ZkTopologyServer)
	if !ok {
		return "", fmt.Errorf("PurgeActions requires a ZkTopologyServer")
	}
	zkActionPaths, err := resolveWildcards(wrangler, subFlags.Args())
	if err != nil {
		return "", err
	}
	for _, zkActionPath := range zkActionPaths {
		err := zkts.PurgeActions(zkActionPath, tm.ActionNodeCanBePurged)
		if err != nil {
			return "", err
		}
	}
	return "", nil
}

func staleActions(zkts *zktopo.ZkTopologyServer, zkActionPath string, maxStaleness time.Duration) ([]*tm.ActionNode, error) {
	// get the stale strings
	actionNodes, err := zkts.StaleActions(zkActionPath, maxStaleness, tm.ActionNodeIsStale)
	if err != nil {
		return nil, err
	}

	// convert to ActionNode
	staleActions := make([]*tm.ActionNode, len(actionNodes))
	for i, actionNodeStr := range actionNodes {
		actionNode, err := tm.ActionNodeFromJson(actionNodeStr, "")
		if err != nil {
			return nil, err
		}
		staleActions[i] = actionNode
	}

	return staleActions, nil
}

func commandStaleActions(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	maxStaleness := subFlags.Duration("max-staleness", 5*time.Minute, "how long since the last modification before an action considered stale")
	purge := subFlags.Bool("purge", false, "purge stale actions")
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action StaleActions requires <zk action path>")
	}
	zkts, ok := wrangler.TopologyServer().(*zktopo.ZkTopologyServer)
	if !ok {
		return "", fmt.Errorf("StaleActions requires a ZkTopologyServer")
	}
	zkPaths, err := resolveWildcards(wrangler, subFlags.Args())
	if err != nil {
		return "", err
	}
	var errCount sync2.AtomicInt32
	wg := sync.WaitGroup{}
	for _, apath := range zkPaths {
		wg.Add(1)
		go func(zkActionPath string) {
			defer wg.Done()
			staleActions, err := staleActions(zkts, zkActionPath, *maxStaleness)
			if err != nil {
				errCount.Add(1)
				relog.Error("can't check stale actions: %v %v", zkActionPath, err)
				return
			}
			for _, action := range staleActions {
				fmt.Println(fmtAction(action))
			}
			if *purge && len(staleActions) > 0 {
				err := zkts.PurgeActions(zkActionPath, tm.ActionNodeCanBePurged)
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

	paths, err := resolveWildcards(wrangler, subFlags.Args())
	if err != nil {
		return "", err
	}

	zkts, ok := wrangler.TopologyServer().(*zktopo.ZkTopologyServer)
	if !ok {
		return "", fmt.Errorf("PruneActionLogs requires a ZkTopologyServer")
	}

	var errCount sync2.AtomicInt32
	wg := sync.WaitGroup{}
	for _, zkActionLogPath := range paths {
		wg.Add(1)
		go func(zkActionLogPath string) {
			defer wg.Done()
			purgedCount, err := zkts.PruneActionLogs(zkActionLogPath, *keepCount)
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

	addrs, err := naming.LookupVtName(wrangler.TopologyServer(), "local", parts[0], parts[1], naming.TabletType(parts[2]), namedPort)
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
		relog.Fatal("action ExportZkns requires <cell name|zk vt root path>")
	}
	cell := vtPathToCell(subFlags.Arg(0))
	return "", wrangler.ExportZkns(cell)
}

func commandExportZknsForKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ExportZknsForKeyspace requires <keyspace|zk global keyspace path>")
	}
	keyspace := keyspaceParamToKeyspace(subFlags.Arg(0))
	return "", wrangler.ExportZknsForKeyspace(keyspace)
}

func commandRebuildReplicationGraph(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	// This is sort of a nuclear option.
	subFlags.Parse(args)
	if subFlags.NArg() < 2 {
		relog.Fatal("action RebuildReplicationGraph requires <cell1>,<cell2>,... <keyspace1>,<keyspace2>...")
	}

	cellParams := strings.Split(subFlags.Arg(0), ",")
	resolvedCells, err := resolveWildcards(wrangler, cellParams)
	if err != nil {
		return "", err
	}
	cells := make([]string, 0, len(cellParams))
	for _, cell := range resolvedCells {
		cells = append(cells, vtPathToCell(cell))
	}

	keyspaceParams := strings.Split(subFlags.Arg(1), ",")
	resolvedKeyspaces, err := resolveWildcards(wrangler, keyspaceParams)
	if err != nil {
		return "", err
	}
	keyspaces := make([]string, 0, len(keyspaceParams))
	for _, keyspace := range resolvedKeyspaces {
		keyspaces = append(keyspaces, keyspaceParamToKeyspace(keyspace))
	}

	return "", wrangler.RebuildReplicationGraph(cells, keyspaces)
}

func commandListIdle(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListIdle requires <cell name|zk vt path>")
	}
	cell := vtPathToCell(subFlags.Arg(0))
	return "", listTabletsByType(wrangler.TopologyServer(), cell, naming.TYPE_IDLE)
}

func commandListScrap(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListScrap requires <cell name|zk vt path>")
	}
	cell := vtPathToCell(subFlags.Arg(0))
	return "", listTabletsByType(wrangler.TopologyServer(), cell, naming.TYPE_SCRAP)
}

func commandListAllTablets(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListAllTablets requires <cell name|zk vt path>")
	}

	cell := vtPathToCell(subFlags.Arg(0))
	return "", dumpAllTablets(wrangler.TopologyServer(), cell)
}

func commandListTablets(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action ListTablets requires <tablet alias|zk tablet path> ...")
	}

	zkPaths, err := resolveWildcards(wrangler, subFlags.Args())
	if err != nil {
		return "", err
	}
	aliases := make([]naming.TabletAlias, len(zkPaths))
	for i, zkPath := range zkPaths {
		aliases[i] = tabletParamToTabletAlias(zkPath)
	}
	return "", dumpTablets(wrangler.TopologyServer(), aliases)
}

func commandGetSchema(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	tables := subFlags.String("tables", "", "comma separated tables to gather schema information for")
	includeViews := subFlags.Bool("include-views", false, "include views in the output")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action GetSchema requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	var tableArray []string
	if *tables != "" {
		tableArray = strings.Split(*tables, ",")
	}

	sd, err := wrangler.GetSchema(tabletAlias, tableArray, *includeViews)
	if err == nil {
		relog.Info("%v", sd.String()) // they can contain %
	}
	return "", err
}

func commandValidateSchemaShard(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	includeViews := subFlags.Bool("include-views", false, "include views in the validation")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateSchemaShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return "", wrangler.ValidateSchemaShard(keyspace, shard, *includeViews)
}

func commandValidateSchemaKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	includeViews := subFlags.Bool("include-views", false, "include views in the validation")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateSchemaKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace := keyspaceParamToKeyspace(subFlags.Arg(0))
	return "", wrangler.ValidateSchemaKeyspace(keyspace, *includeViews)
}

func commandPreflightSchema(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	sql := subFlags.String("sql", "", "sql command")
	sqlFile := subFlags.String("sql-file", "", "file containing the sql commands")
	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		relog.Fatal("action PreflightSchema requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	change := getFileParam(*sql, *sqlFile, "sql")
	scr, err := wrangler.PreflightSchema(tabletAlias, change)
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
		relog.Fatal("action ApplySchema requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	change := getFileParam(*sql, *sqlFile, "sql")

	sc := &mysqlctl.SchemaChange{}
	sc.Sql = change
	sc.AllowReplication = !(*stopReplication)

	// do the preflight to get before and after schema
	if !(*skipPreflight) {
		scr, err := wrangler.PreflightSchema(tabletAlias, sc.Sql)
		if err != nil {
			relog.Fatal("preflight failed: %v", err)
		}
		relog.Info("Preflight: " + scr.String())
		sc.BeforeSchema = scr.BeforeSchema
		sc.AfterSchema = scr.AfterSchema
		sc.Force = *force
	}

	scr, err := wrangler.ApplySchema(tabletAlias, sc)
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
		relog.Fatal("action ApplySchemaShard requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	change := getFileParam(*sql, *sqlFile, "sql")
	var newParentAlias naming.TabletAlias
	if *newParent != "" {
		newParentAlias = tabletParamToTabletAlias(*newParent)
	}

	if (*simple) && (*newParent != "") {
		relog.Fatal("new_parent for action ApplySchemaShard can only be specified for complex schema upgrades")
	}

	scr, err := wrangler.ApplySchemaShard(keyspace, shard, change, newParentAlias, *simple, *force)
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
		relog.Fatal("action ApplySchemaKeyspace requires <keyspace|zk keyspace path>")
	}

	keyspace := keyspaceParamToKeyspace(subFlags.Arg(0))
	change := getFileParam(*sql, *sqlFile, "sql")
	scr, err := wrangler.ApplySchemaKeyspace(keyspace, change, *simple, *force)
	if err == nil {
		relog.Info(scr.String())
	}
	return "", err
}

func commandValidateVersionShard(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateVersionShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return "", wrangler.ValidateVersionShard(keyspace, shard)
}

func commandValidateVersionKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidateVersionKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace := keyspaceParamToKeyspace(subFlags.Arg(0))
	return "", wrangler.ValidateVersionKeyspace(keyspace)
}

func commandGetPermissions(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action GetPermissions requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	p, err := wrangler.GetPermissions(tabletAlias)
	if err == nil {
		relog.Info("%v", p.String()) // they can contain '%'
	}
	return "", err
}

func commandValidatePermissionsShard(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidatePermissionsShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return "", wrangler.ValidatePermissionsShard(keyspace, shard)
}

func commandValidatePermissionsKeyspace(wrangler *wr.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ValidatePermissionsKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace := keyspaceParamToKeyspace(subFlags.Arg(0))
	return "", wrangler.ValidatePermissionsKeyspace(keyspace)
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

	topoServer := naming.GetTopologyServer()
	defer naming.CloseTopologyServers()

	wrangler := wr.NewWrangler(topoServer, *waitTime, *lockWaitTimeout)
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
	if l.Type == naming.TYPE_MASTER {
		lTypeMaster = 1
	}
	if r.Type == naming.TYPE_MASTER {
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
