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
	"log"
	"log/syslog"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/client2"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/naming"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	wr "code.google.com/p/vitess/go/vt/wrangler"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

var usage = `
Commands:

Tablets:
  InitTablet <zk tablet path> <hostname> <mysql port> <vt port> <keyspace> <shard id> <tablet type> [<zk parent alias>]
  InitTablet zk_tablet_path=<zk tablet path> hostname=<hostname> mysql_port=<mysql port> port=<vt port> tablet_type=<tablet type> [keyspace=<keyspace>] [shard_id=<shard id>] [zk_parent_alias=<zk parent alias>] [key_start=<start>] [key_end=<end>]

  ScrapTablet <zk tablet path>
    -force writes the scrap state in to zk, no questions asked, if a tablet is offline.

  SetReadOnly [<zk tablet path> | <zk shard/tablet path>]

  SetReadWrite [<zk tablet path> | <zk shard/tablet path>]

  DemoteMaster <zk tablet path>

  ChangeSlaveType <zk tablet path> <db type>
    Change the db type for this tablet if possible. this is mostly for arranging
    replicas - it will not convert a master.
    NOTE: This will automatically update the serving graph.

  Ping <zk tablet path>
    check that the agent is awake and responding - can be blocked by other in-flight
    operations.

  Query <zk tablet path> [<user> <password>] <query>

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

  ReparentTablet <zk tablet path>
    Reparent a tablet to the current master in the shard. This only works
    if the current slave position matches the last known reparent action.

  PartialSnapshot <zk tablet path> <key name> <start key> <end key>
    Halt mysqld and copy compressed data aside.

  PartialRestore <zk src tablet path> <zk dst tablet path>
    Copy the latest partial snaphot from the source tablet and starts partial
    replication.
    NOTE: This does not wait for replication to catch up. The destination
    tablet must be "idle" to begin with. It will transition to "spare" once
    the restore is complete.

  PartialClone <zk src tablet path> <zk dst tablet path> <key name> <start key> <end key>
    This performs PartialSnapshot and then PartialRestore.  The
    advantage of having separate actions is that one partial snapshot can be
    used for many restores.

  ExecuteHook <zk tablet path> <hook name> [<param1=value1> <param2=value2> ...]
    This runs the specified hook on the given tablet.

Shards:
  RebuildShardGraph <zk shard path>
    Rebuild the replication graph and shard serving data in zk.
    This may trigger an update to all connected clients

  ReparentShard <zk shard path> <zk tablet path>
    specify which shard to reparent and which tablet should be the new master
    -leave-master-read-only: skip the flip to read-write mode

  ValidateShard <zk shard path> (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>)
    validate all nodes reachable from this shard are consistent

  ShardReplicationPositions <zk shard path> (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>)
    Show slave status on all machines in the shard graph.


Keyspaces:
  CreateKeyspace <zk keyspaces path>/<name> <shard count>
    e.g. CreateKeyspace /zk/global/vt/keyspaces/my_keyspace 4

  RebuildKeyspaceGraph <zk keyspace path>
    Rebuild the serving data for all shards in this keyspace.
    This may trigger an update to all connected clients

  ValidateKeyspace <zk keyspace path> (/zk/global/vt/keyspaces/<keyspace>)
    validate all nodes reachable from this keyspace are consistent


Generic:
  PurgeActions <zk action path>
    remove all actions - be careful, this is powerful cleanup magic

  WaitForAction <zk action path>
    watch an action node, printing updates, until the action is complete

  Resolve <keyspace>.<shard>.<db type>
    read a list of addresses that can answer this query

  Validate <zk keyspaces path> (/zk/global/vt/keyspaces)
    validate all nodes reachable from global replication graph and all
    tablets in all discoverable cells are consistent

  ExportZkns <zk local vt path>  (/zk/<cell>/vt)
    export the serving graph entries to the legacy zkns format

  ExportZknsForKeyspace <zk global keyspace path> (/zk/global/vt/keyspaces/<keyspace>)
    export the serving graph entries to the legacy zkns format

  RebuildReplicationGraph zk-vt-paths=<zk local vt path>,... keyspaces=<keyspace>,...
    This takes the Thor's hammer approach of recovery and should only
    be used in emergencies.  /zk/cell/vt/tablets/* are the canonical
    source of data for the system. This function use that canonical
    data to recover the replication graph, at which point further
    auditing with Validate can reveal any remaining issues.

  ListIdle <zk local vt path>
    list all idle tablet paths

  ListScrap <zk local vt path>
    list all scrap tablet paths

  ListShardTablets <zk shard path>
    list all tablets paths in a given shard

  ListAllTablets <zk local vt path>
    list all tablets in an awk-friendly way

  ListTablets <zk tablet path> ...
    list specified tablets in an awk-friendly way


Schema:
  GetSchema <zk tablet path>
    display the full schema for a tablet

  ValidateSchemaShard <zk shard path>
    validate the master schema matches all the slaves.

  ValidateSchemaKeyspace <zk keyspace path>
    validate the master schema from shard 0 matches all the other tablets in the keyspace.

  ApplySchema zk_tablet_path=<zk tablet path> {sql=<sql> || sql_file=<filename>} [use_db=<db name>] [allow_replication=false]
    apply the schema change to the specific tablet (not using _vt, and allowing replication by default). The sql can be inlined or read from a file.
`

var noWaitForAction = flag.Bool("no-wait", false,
	"don't wait for action completion, detach")
var waitTime = flag.Duration("wait-time", 24*time.Hour, "time to wait on an action")
var force = flag.Bool("force", false, "force action")
var leaveMasterReadOnly = flag.Bool("leave-master-read-only", false, "only applies to ReparentShard")
var pingTablets = flag.Bool("ping-tablets", false, "ping all tablets during validate")
var dbNameOverride = flag.String("db-name-override", "", "override the name of the db used by vttablet")
var logLevel = flag.String("log.level", "INFO", "set log level")
var logfile = flag.String("logfile", "/vt/logs/vtctl.log", "log file")
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
	// FIXME(msolomon) just read the shard node data instead - that is tearing resistant.
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

func initTablet(zconn zk.Conn, params map[string]string, update bool) error {
	zkPath := params["zk_tablet_path"]
	keyspace := params["keyspace"]
	shardId := params["shard_id"]
	tabletType := params["tablet_type"]
	parentAlias := params["zk_parent_alias"]

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

	hostname := params["hostname"]
	tablet := tm.NewTablet(cell, uint(uid), parent, fmt.Sprintf("%v:%v", hostname, params["port"]), fmt.Sprintf("%v:%v", hostname, params["mysql_port"]), keyspace, shardId, tm.TabletType(tabletType))
	tablet.DbNameOverride = *dbNameOverride

	keyStart, ok := params["key_start"]
	if ok {
		tablet.KeyRange.Start = key.HexKeyspaceId(keyStart).Unhex()
	}
	keyEnd, ok := params["key_end"]
	if ok {
		tablet.KeyRange.End = key.HexKeyspaceId(keyEnd).Unhex()
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
		if *force {
			zk.DeleteRecursive(zconn, zkPath, -1)
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

func listTabletsByShard(zconn zk.Conn, zkShardPath string) error {
	tabletAliases, err := tm.FindAllTabletAliasesInShard(zconn, zkShardPath)
	if err != nil {
		return err
	}
	tabletPaths := make([]string, len(tabletAliases))
	for i, alias := range tabletAliases {
		tabletPaths[i] = tm.TabletPathForAlias(alias)
	}
	return dumpTablets(zconn, tabletPaths)
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
		}
		fmt.Println(fmtTabletAwkable(ti))
	}
	return nil
}

func listScrap(zconn zk.Conn, zkVtPath string) error {
	return listTabletsByType(zconn, zkVtPath, tm.TYPE_SCRAP)
}

func listIdle(zconn zk.Conn, zkVtPath string) error {
	return listTabletsByType(zconn, zkVtPath, tm.TYPE_IDLE)
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

// returns true if they are the right number of parameters,
// and none of them contains an '=' sign.
func oldStyleParameters(args []string, minNumber, maxNumber int) bool {
	if len(args) < minNumber || len(args) > maxNumber {
		return false
	}
	for _, arg := range args {
		if strings.Contains(arg, "=") {
			return false
		}
	}
	return true
}

func parseParams(args []string) map[string]string {
	params := make(map[string]string)
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) == 1 {
			params[parts[0]] = ""
		} else {
			params[parts[0]] = parts[1]
		}
	}
	return params
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

	logPrefix := "vtctl "
	logFlag := log.Ldate | log.Lmicroseconds | log.Lshortfile
	logLevel := relog.LogNameToLogLevel(*logLevel)
	logger := relog.New(os.Stderr, logPrefix, logFlag, logLevel)
	// Set default logger to stderr.
	relog.SetLogger(logger)

	startMsg := fmt.Sprintf("USER=%v SUDO_USER=%v %v", os.Getenv("USER"), os.Getenv("SUDO_USER"), strings.Join(os.Args, " "))

	if log, err := os.OpenFile(*logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		// Use a temp logger to keep a consistent trail of events in the log.
		fileLogger := relog.New(log, logPrefix, logFlag, logLevel)
		fileLogger.Info(startMsg)
		// Redefine the default logger to keep events in both places.
		logger = relog.New(io.MultiWriter(log, os.Stderr), logPrefix, logFlag, logLevel)
		relog.SetLogger(logger)
	} else {
		logger.Warning("cannot write to provided logfile: %v", err)
	}

	if syslogger, err := syslog.New(syslog.LOG_INFO, logPrefix); err == nil {
		syslogger.Info(startMsg)
	} else {
		relog.Warning("cannot connect to syslog: %v", err)
	}

	zconn := zk.NewMetaConn(5e9, false)
	defer zconn.Close()

	ai := tm.NewActionInitiator(zconn)
	wrangler := wr.NewWrangler(zconn, *waitTime)
	var actionPath string
	var err error

	switch args[0] {
	case "CreateKeyspace":
		if len(args) != 2 {
			relog.Fatal("action %v requires 1 arg", args[0])
		}
		err = createKeyspace(zconn, args[1])
	case "Query":
		if len(args) != 3 && len(args) != 5 {
			relog.Fatal("action %v requires 2 or 4 args", args[0])
		}
		if len(args) == 3 {
			err = kquery(zconn, args[1], "", "", args[2])
		} else {
			err = kquery(zconn, args[1], args[2], args[3], args[4])
		}
	case "InitTablet":
		var params map[string]string
		if oldStyleParameters(args, 8, 9) {
			params = make(map[string]string)
			params["zk_tablet_path"] = args[1]
			params["hostname"] = args[2]
			params["mysql_port"] = args[3]
			params["port"] = args[4]
			params["keyspace"] = args[5]
			params["shard_id"] = args[6]
			params["tablet_type"] = args[7]
			if len(args) == 9 {
				params["zk_parent_alias"] = args[8]
			}
		} else {
			params = parseParams(args)
		}
		err = initTablet(zconn, params, false)
	case "UpdateTablet":
		var params map[string]string
		if oldStyleParameters(args, 9, 9) {
			params = make(map[string]string)
			params["zk_tablet_path"] = args[1]
			params["hostname"] = args[2]
			params["mysql_port"] = args[3]
			params["port"] = args[4]
			params["keyspace"] = args[5]
			params["shard_id"] = args[6]
			params["tablet_type"] = args[7]
			params["zk_parent_alias"] = args[8]
		} else {
			params = parseParams(args)
		}
		err = initTablet(zconn, params, true)
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
		fallthrough
	case "ChangeSlaveType":
		if len(args) != 3 {
			relog.Fatal("action %v requires <zk tablet path> <db type>", args[0])
		}
		err = wrangler.ChangeType(args[1], tm.TabletType(args[2]), *force)
	case "DemoteMaster":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk tablet path>", args[0])
		}
		actionPath, err = ai.DemoteMaster(args[1])
	case "Clone":
		if len(args) != 3 {
			relog.Fatal("action %v requires <zk src tablet path> <zk dst tablet path>", args[0])
		}
		err = wrangler.Clone(args[1], args[2], *force)
	case "Restore":
		if len(args) != 3 {
			relog.Fatal("action %v requires <zk src tablet path> <zk dst tablet path>", args[0])
		}
		err = wrangler.Restore(args[1], args[2])
	case "Snapshot":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk src tablet path>", args[0])
		}
		err = wrangler.Snapshot(args[1], *force)
	case "PartialClone":
		if len(args) != 6 {
			relog.Fatal("action %v requires <zk src tablet path> <zk dst tablet path> <key name> <start key> <end key>", args[0])
		}
		err = wrangler.PartialClone(args[1], args[2], args[3], key.HexKeyspaceId(args[4]), key.HexKeyspaceId(args[5]), *force)
	case "PartialRestore":
		if len(args) != 3 {
			relog.Fatal("action %v requires <zk src tablet path> <zk dst tablet path>", args[0])
		}
		err = wrangler.PartialRestore(args[1], args[2])
	case "PartialSnapshot":
		if len(args) != 5 {
			relog.Fatal("action %v requires <zk src tablet path> <key name> <start key> <end key>", args[0])
		}
		err = wrangler.PartialSnapshot(args[1], args[2], key.HexKeyspaceId(args[3]), key.HexKeyspaceId(args[4]), *force)
	case "PurgeActions":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		err = tm.PurgeActions(zconn, args[1])
	case "RebuildShardGraph":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		actionPath, err = wrangler.RebuildShardGraph(args[1])
	case "RebuildKeyspaceGraph":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk keyspace path>", args[0])
		}
		actionPath, err = wrangler.RebuildKeyspaceGraph(args[1])
	case "RebuildReplicationGraph":
		// This is sort of a nuclear option.
		if len(args) < 2 {
			relog.Fatal("action %v requires zk-vt-paths=<zk vt path>,... keyspaces=<keyspace>,...", args[0])
		}

		params := parseParams(args)
		var keyspaces, zkVtPaths []string
		if _, ok := params["zk-vt-paths"]; ok {
			zkVtPaths = strings.Split(params["zk-vt-paths"], ",")
		}
		if _, ok := params["keyspaces"]; ok {
			keyspaces = strings.Split(params["keyspaces"], ",")
		}
		// RebuildReplicationGraph zk-vt-paths=/zk/test_nj/vt,/zk/test_ny/vt keyspaces=test_keyspace
		err = wrangler.RebuildReplicationGraph(zkVtPaths, keyspaces)
	case "ReparentShard":
		if len(args) != 3 {
			relog.Fatal("action %v requires <zk shard path> <zk tablet path>", args[0])
		}
		err = wrangler.ReparentShard(args[1], args[2], *leaveMasterReadOnly, *force)
	case "ReparentTablet":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk tablet path>", args[0])
		}
		err = wrangler.ReparentTablet(args[1])
	case "ExportZkns":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt root path>", args[0])
		}
		err = wrangler.ExportZkns(args[1])
	case "ExportZknsForKeyspace":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt root path>", args[0])
		}
		err = wrangler.ExportZknsForKeyspace(args[1])
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
		err = wrangler.Validate(args[1], *pingTablets)
	case "ValidateKeyspace":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk keyspace path>", args[0])
		}
		err = wrangler.ValidateKeyspace(args[1], *pingTablets)
	case "ValidateShard":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		err = wrangler.ValidateShard(args[1], *pingTablets)
	case "ShardReplicationPositions":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		tabletMap, posMap, wrErr := wrangler.ShardReplicationPositions(args[1])
		err = wrErr
		if tabletMap == nil {
			break
		}
		lines := make([]string, 0, 24)
		for uid, ti := range tabletMap {
			pos := posMap[uid]
			if pos == nil {
				lines = append(lines, fmtTabletAwkable(ti)+" <err> <err>")
			} else {
				lines = append(lines, fmtTabletAwkable(ti)+fmt.Sprintf(" %v:%010d %v:%010d", pos.MasterLogFile, pos.MasterLogPosition, pos.MasterLogFileIo, pos.MasterLogPositionIo))
			}
		}
		sort.Strings(lines)
		for _, l := range lines {
			fmt.Println(l)
		}
	case "ListIdle":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt path>", args[0])
		}
		err = listIdle(zconn, args[1])
	case "ListScrap":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt path>", args[0])
		}
		err = listScrap(zconn, args[1])
	case "ListShardTablets":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		err = listTabletsByShard(zconn, args[1])
	case "ListAllTablets":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk vt path>", args[0])
		}
		err = dumpAllTablets(zconn, args[1])
	case "ListTablets":
		if len(args) < 2 {
			relog.Fatal("action %v requires <zk tablet path> ...", args[0])
		}
		err = dumpTablets(zconn, args[1:])
	case "GetSchema":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk tablet path>", args[0])
		}
		var sd *mysqlctl.SchemaDefinition
		sd, err = wrangler.GetSchema(args[1])
		if err == nil {
			relog.Info(sd.String())
		}
	case "ApplySchema":
		params := parseParams(args)
		sc := &mysqlctl.SchemaChange{}
		zkTabletPath, ok := params["zk_tablet_path"]
		if !ok {
			relog.Fatal("action %v requires zk_tablet_path=<zk tablet path>", args[0])
		}
		sc.Sql, ok = params["sql"]
		if ok {
			if _, ok = params["sql_file"]; ok {
				relog.Fatal("action %v requires only one of sql or sql_file")
			}
		} else {
			sqlFile, ok := params["sql_file"]
			if !ok {
				relog.Fatal("action %v requires one of sql or sql_file")
			}
			data, err := ioutil.ReadFile(sqlFile)
			if err != nil {
				relog.Fatal("Cannot read file %v: %v", sqlFile, err)
			}
			sc.Sql = string(data)

		}
		sc.UseDb = params["use_db"]
		sc.AllowReplication = params["allow_replication"] != "false"
		scr, err := wrangler.ApplySchema(zkTabletPath, sc)
		if err == nil {
			relog.Info(scr.String())
			if scr.Error != "" {
				relog.Fatal(scr.Error)
			}
		}
	case "ValidateSchemaShard":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk shard path>", args[0])
		}
		err = wrangler.ValidateSchemaShard(args[1])
	case "ValidateSchemaKeyspace":
		if len(args) != 2 {
			relog.Fatal("action %v requires <zk keyspace path>", args[0])
		}
		err = wrangler.ValidateSchemaKeyspace(args[1])
	case "ExecuteHook":
		if len(args) < 3 {
			relog.Fatal("action %v requires <zk tablet path> <hook name>", args[0])
		}

		hook := &tm.Hook{Name: args[2], Parameters: parseParams(args[2:])}
		var hr *tm.HookResult
		hr, err = wrangler.ExecuteHook(args[1], hook)
		if err == nil {
			relog.Info(hr.String())
		}
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
