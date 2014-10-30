// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctl

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"sort"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/client2"
	hk "github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var (
	// Error returned for an unknown command
	ErrUnknownCommand = errors.New("unknown command")
)

type command struct {
	name   string
	method func(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error
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
				"[-force] [-parent] [-update] [-db-name-override=<db name>] [-hostname=<hostname>] [-mysql_port=<port>] [-port=<port>] [-vts_port=<port>] [-keyspace=<keyspace>] [-shard=<shard>] [-parent_alias=<parent alias>] <tablet alias> <tablet type>]",
				"Initializes a tablet in the topology.\n" +
					"Valid <tablet type>:\n" +
					"  " + strings.Join(topo.MakeStringTypeList(topo.AllTabletTypes), " ")},
			command{"GetTablet", commandGetTablet,
				"<tablet alias|zk tablet path>",
				"Outputs the json version of Tablet to stdout."},
			command{"UpdateTabletAddrs", commandUpdateTabletAddrs,
				"[-hostname <hostname>] [-ip-addr <ip addr>] [-mysql-port <mysql port>] [-vt-port <vt port>] [-vts-port <vts port>] <tablet alias|zk tablet path> ",
				"Updates the addresses of a tablet."},
			command{"ScrapTablet", commandScrapTablet,
				"[-force] [-skip-rebuild] <tablet alias|zk tablet path>",
				"Scraps a tablet."},
			command{"DeleteTablet", commandDeleteTablet,
				"<tablet alias|zk tablet path> ...",
				"Deletes scrapped tablet(s) from the topology."},
			command{"SetReadOnly", commandSetReadOnly,
				"[<tablet alias|zk tablet path>]",
				"Sets the tablet as ReadOnly."},
			command{"SetReadWrite", commandSetReadWrite,
				"[<tablet alias|zk tablet path>]",
				"Sets the tablet as ReadWrite."},
			command{"ChangeSlaveType", commandChangeSlaveType,
				"[-force] [-dry-run] <tablet alias|zk tablet path> <tablet type>",
				"Change the db type for this tablet if possible. This is mostly for arranging replicas - it will not convert a master.\n" +
					"NOTE: This will automatically update the serving graph.\n" +
					"Valid <tablet type>:\n" +
					"  " + strings.Join(topo.MakeStringTypeList(topo.SlaveTabletTypes), " ")},
			command{"Ping", commandPing,
				"<tablet alias|zk tablet path>",
				"Check that the agent is awake and responding to RPCs. Can be blocked by other in-flight operations."},
			command{"RefreshState", commandRefreshState,
				"<tablet alias|zk tablet path>",
				"Asks a remote tablet to reload its tablet record."},
			command{"RunHealthCheck", commandRunHealthCheck,
				"<tablet alias> <target tablet type>",
				"Asks a remote tablet to run a health check with the providd target type."},
			command{"Query", commandQuery,
				"<cell> <keyspace> <query>",
				"Send a SQL query to a tablet."},
			command{"Sleep", commandSleep,
				"<tablet alias|zk tablet path> <duration>",
				"Block the action queue for the specified duration (mostly for testing)."},
			command{"Snapshot", commandSnapshot,
				"[-force] [-server-mode] [-concurrency=4] <tablet alias|zk tablet path>",
				"Stop mysqld and copy compressed data aside."},
			command{"SnapshotSourceEnd", commandSnapshotSourceEnd,
				"[-slave-start] [-read-write] <tablet alias|zk tablet path> <original tablet type>",
				"Restart Mysql and restore original server type." +
					"Valid <tablet type>:\n" +
					"  " + strings.Join(topo.MakeStringTypeList(topo.AllTabletTypes), " ")},
			command{"Restore", commandRestore,
				"[-fetch-concurrency=3] [-fetch-retry-count=3] [-dont-wait-for-slave-start] <src tablet alias|zk src tablet path> <src manifest file> <dst tablet alias|zk dst tablet path> [<zk new master path>]",
				"Copy the given snaphot from the source tablet and restart replication to the new master path (or uses the <src tablet path> if not specified). If <src manifest file> is 'default', uses the default value.\n" +
					"NOTE: This does not wait for replication to catch up. The destination tablet must be 'idle' to begin with. It will transition to 'spare' once the restore is complete."},
			command{"Clone", commandClone,
				"[-force] [-concurrency=4] [-fetch-concurrency=3] [-fetch-retry-count=3] [-server-mode] <src tablet alias|zk src tablet path> <dst tablet alias|zk dst tablet path> ...",
				"This performs Snapshot and then Restore on all the targets in parallel. The advantage of having separate actions is that one snapshot can be used for many restores, and it's then easier to spread them over time."},
			command{"MultiSnapshot", commandMultiSnapshot,
				"[-force] [-concurrency=8] [-skip-slave-restart] [-maximum-file-size=134217728] -spec='-' [-tables=''] [-exclude_tables=''] <tablet alias|zk tablet path>",
				"Locks mysqld and copy compressed data aside."},
			command{"MultiRestore", commandMultiRestore,
				"[-force] [-concurrency=4] [-fetch-concurrency=4] [-insert-table-concurrency=4] [-fetch-retry-count=3] [-strategy=] <dst tablet alias|destination zk path> <source zk path>...",
				"Restores a snapshot from multiple hosts."},
			command{"ExecuteHook", commandExecuteHook,
				"<tablet alias|zk tablet path> <hook name> [<param1=value1> <param2=value2> ...]",
				"This runs the specified hook on the given tablet."},
			command{"ExecuteFetch", commandExecuteFetch,
				"[--max_rows=10000] [--want_fields] [--disable_binlogs] <tablet alias|zk tablet path> <sql command>",
				"Runs the given sql command as a DBA on the remote tablet"},
		},
	},
	commandGroup{
		"Shards", []command{
			command{"CreateShard", commandCreateShard,
				"[-force] [-parent] <keyspace/shard|zk shard path>",
				"Creates the given shard"},
			command{"GetShard", commandGetShard,
				"<keyspace/shard|zk shard path>",
				"Outputs the json version of Shard to stdout."},
			command{"RebuildShardGraph", commandRebuildShardGraph,
				"[-cells=a,b] <zk shard path> ... (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>)",
				"Rebuild the replication graph and shard serving data in zk. This may trigger an update to all connected clients."},
			command{"ShardExternallyReparented", commandShardExternallyReparented,
				"[-use_rpc] <keyspace/shard|zk shard path> <tablet alias|zk tablet path>",
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
			command{"SetShardServedTypes", commandSetShardServedTypes,
				"<keyspace/shard|zk shard path> [<served type1>,<served type2>,...]",
				"Sets a given shard's served types. Does not rebuild any serving graph."},
			command{"SetShardTabletControl", commandSetShardTabletControl,
				"[--cells=c1,c2,...] [--blacklisted_tables=t1,t2,...] [--remove] [--disable_query_service] <keyspace/shard|zk shard path> <tabletType>",
				"Sets the TabletControl record for a shard and type. Only use this for an emergency fix, or after a finished vertical split. MigrateServedFrom and MigrateServedType will set this field appropriately already. Always specify blacklisted_tables for vertical splits, never for horizontal splits."},
			command{"ShardMultiRestore", commandShardMultiRestore,
				"[-force] [-concurrency=4] [-fetch-concurrency=4] [-insert-table-concurrency=4] [-fetch-retry-count=3] [-strategy=] [-tables=<table1>,<table2>,...] <keyspace/shard|zk shard path> <source zk path>...",
				"Restore multi-snapshots on all the tablets of a shard."},
			command{"ShardReplicationAdd", commandShardReplicationAdd,
				"<keyspace/shard|zk shard path> <tablet alias|zk tablet path> <parent tablet alias|zk parent tablet path>",
				"HIDDEN Adds an entry to the replication graph in the given cell"},
			command{"ShardReplicationRemove", commandShardReplicationRemove,
				"<keyspace/shard|zk shard path> <tablet alias|zk tablet path>",
				"HIDDEN Removes an entry to the replication graph in the given cell"},
			command{"ShardReplicationFix", commandShardReplicationFix,
				"<cell> <keyspace/shard|zk shard path>",
				"Walks through a ShardReplication object and fixes the first error it encrounters"},
			command{"RemoveShardCell", commandRemoveShardCell,
				"[-force] <keyspace/shard|zk shard path> <cell>",
				"Removes the cell in the shard's Cells list."},
			command{"DeleteShard", commandDeleteShard,
				"<keyspace/shard|zk shard path> ...",
				"Deletes the given shard(s)"},
		},
	},
	commandGroup{
		"Keyspaces", []command{
			command{"CreateKeyspace", commandCreateKeyspace,
				"[-sharding_column_name=name] [-sharding_column_type=type] [-served_from=tablettype1:ks1,tablettype2,ks2,...] [-split_shard_count=N] [-force] <keyspace name|zk keyspace path>",
				"Creates the given keyspace"},
			command{"GetKeyspace", commandGetKeyspace,
				"<keyspace|zk keyspace path>",
				"Outputs the json version of Keyspace to stdout."},
			command{"SetKeyspaceShardingInfo", commandSetKeyspaceShardingInfo,
				"[-force] [-split_shard_count=N] <keyspace name|zk keyspace path> [<column name>] [<column type>]",
				"Updates the sharding info for a keyspace"},
			command{"SetKeyspaceServedFrom", commandSetKeyspaceServedFrom,
				"[-source=<source keyspace name>] [-remove] [-cells=c1,c2,...] <keyspace name> <tablet type>",
				"Manually change the ServedFromMap. Only use this for an emergency fix. MigrateServedFrom will set this field appropriately already. Does not rebuild the serving graph."},
			command{"RebuildKeyspaceGraph", commandRebuildKeyspaceGraph,
				"[-cells=a,b] <zk keyspace path> ... (/zk/global/vt/keyspaces/<keyspace>)",
				"Rebuild the serving data for all shards in this keyspace. This may trigger an update to all connected clients."},
			command{"ValidateKeyspace", commandValidateKeyspace,
				"[-ping-tablets] <keyspace name|zk keyspace path>",
				"Validate all nodes reachable from this keyspace are consistent."},
			command{"MigrateServedTypes", commandMigrateServedTypes,
				"[-reverse] [-skip-refresh-state] <keyspace/shard|zk shard path> <served type>",
				"Migrates a serving type from the source shard to the shards it replicates to. Will also rebuild the serving graph. keyspace/shard can be any of the involved shards in the migration."},
			command{"MigrateServedFrom", commandMigrateServedFrom,
				"[-cells=c1,c2,...] [-reverse] <destination keyspace/shard|zk destination shard path> <served type>",
				"Makes the destination keyspace/shard serve the given type. Will also rebuild the serving graph."},
		},
	},
	commandGroup{
		"Generic", []command{
			command{"Resolve", commandResolve,
				"<keyspace>.<shard>.<db type>:<port name>",
				"Read a list of addresses that can answer this query. The port name is usually _mysql or _vtocc."},
			command{"Validate", commandValidate,
				"[-ping-tablets]",
				"Validate all nodes reachable from global replication graph and all tablets in all discoverable cells are consistent."},
			command{"RebuildReplicationGraph", commandRebuildReplicationGraph,
				"<cell1|zk local vt path1>,<cell2|zk local vt path2>... <keyspace1>,<keyspace2>,...",
				"HIDDEN This takes the Thor's hammer approach of recovery and should only be used in emergencies.  cell1,cell2,... are the canonical source of data for the system. This function uses that canonical data to recover the replication graph, at which point further auditing with Validate can reveal any remaining issues."},
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
				"[-tables=<table1>,<table2>,...] [-exclude_tables=<table1>,<table2>,...] [-include-views] <tablet alias|zk tablet path>",
				"Display the full schema for a tablet, or just the schema for the provided tables."},
			command{"ReloadSchema", commandReloadSchema,
				"<tablet alias|zk tablet path>",
				"Asks a remote tablet to reload its schema."},
			command{"ValidateSchemaShard", commandValidateSchemaShard,
				"[-exclude_tables=''] [-include-views] <keyspace/shard|zk shard path>",
				"Validate the master schema matches all the slaves."},
			command{"ValidateSchemaKeyspace", commandValidateSchemaKeyspace,
				"[-exclude_tables=''] [-include-views] <keyspace name|zk keyspace path>",
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
	commandGroup{
		"Serving Graph", []command{
			command{"GetSrvKeyspace", commandGetSrvKeyspace,
				"<cell> <keyspace>",
				"Outputs the json version of SrvKeyspace to stdout."},
			command{"GetSrvKeyspaceNames", commandGetSrvKeyspaceNames,
				"<cell>",
				"Outputs a list of keyspace names."},
			command{"GetSrvShard", commandGetSrvShard,
				"<cell> <keyspace/shard|zk shard path>",
				"Outputs the json version of SrvShard to stdout."},
			command{"GetEndPoints", commandGetEndPoints,
				"<cell> <keyspace/shard|zk shard path> <tablet type>",
				"Outputs the json version of EndPoints to stdout."},
		},
	},
	commandGroup{
		"Replication Graph", []command{
			command{"GetShardReplication", commandGetShardReplication,
				"<cell> <keyspace/shard|zk shard path>",
				"Outputs the json version of ShardReplication to stdout."},
		},
	},
}

func addCommand(groupName string, c command) {
	for i, group := range commands {
		if group.name == groupName {
			commands[i].commands = append(commands[i].commands, c)
			return
		}
	}
	panic(fmt.Errorf("Trying to add to missing group %v", groupName))
}

var resolveWildcards = func(wr *wrangler.Wrangler, args []string) ([]string, error) {
	return args, nil
}

func fmtMapAwkable(m map[string]string) string {
	pairs := make([]string, len(m))
	i := 0
	for k, v := range m {
		pairs[i] = fmt.Sprintf("%v: %q", k, v)
		i++
	}
	sort.Strings(pairs)
	return "[" + strings.Join(pairs, " ") + "]"
}

func fmtTabletAwkable(ti *topo.TabletInfo) string {
	keyspace := ti.Keyspace
	shard := ti.Shard
	if keyspace == "" {
		keyspace = "<null>"
	}
	if shard == "" {
		shard = "<null>"
	}
	return fmt.Sprintf("%v %v %v %v %v %v %v", ti.Alias, keyspace, shard, ti.Type, ti.Addr(), ti.MysqlAddr(), fmtMapAwkable(ti.Tags))
}

func fmtAction(action *actionnode.ActionNode) string {
	state := string(action.State)
	// FIXME(msolomon) The default state should really just have the value "queued".
	if action.State == actionnode.ACTION_STATE_QUEUED {
		state = "queued"
	}
	return fmt.Sprintf("%v %v %v %v %v", action.Path, action.Action, state, action.ActionGuid, action.Error)
}

func listTabletsByShard(wr *wrangler.Wrangler, keyspace, shard string) error {
	tabletAliases, err := topo.FindAllTabletAliasesInShard(wr.TopoServer(), keyspace, shard)
	if err != nil {
		return err
	}
	return dumpTablets(wr, tabletAliases)
}

func dumpAllTablets(wr *wrangler.Wrangler, zkVtPath string) error {
	tablets, err := topotools.GetAllTablets(wr.TopoServer(), zkVtPath)
	if err != nil {
		return err
	}
	for _, ti := range tablets {
		wr.Logger().Printf("%v\n", fmtTabletAwkable(ti))
	}
	return nil
}

func dumpTablets(wr *wrangler.Wrangler, tabletAliases []topo.TabletAlias) error {
	tabletMap, err := topo.GetTabletMap(wr.TopoServer(), tabletAliases)
	if err != nil {
		return err
	}
	for _, tabletAlias := range tabletAliases {
		ti, ok := tabletMap[tabletAlias]
		if !ok {
			log.Warningf("failed to load tablet %v", tabletAlias)
		} else {
			wr.Logger().Printf("%v\n", fmtTabletAwkable(ti))
		}
	}
	return nil
}

func kquery(wr *wrangler.Wrangler, cell, keyspace, query string) error {
	sconn, err := client2.Dial(wr.TopoServer(), cell, keyspace, "master", false, 5*time.Second)
	if err != nil {
		return err
	}
	rows, err := sconn.Exec(query, nil)
	if err != nil {
		return err
	}
	cols := rows.Columns()
	wr.Logger().Printf("%v\n", strings.Join(cols, "\t"))

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

		wr.Logger().Printf("%v\n", strings.Join(rowStrs, "\t"))
	}
	return nil
}

// getFileParam returns a string containing either flag is not "",
// or the content of the file named flagFile
func getFileParam(flag, flagFile, name string) (string, error) {
	if flag != "" {
		if flagFile != "" {
			return "", fmt.Errorf("action requires only one of %v or %v-file", name, name)
		}
		return flag, nil
	}

	if flagFile == "" {
		return "", fmt.Errorf("action requires one of %v or %v-file", name, name)
	}
	data, err := ioutil.ReadFile(flagFile)
	if err != nil {
		return "", fmt.Errorf("Cannot read file %v: %v", flagFile, err)
	}
	return string(data), nil
}

func keyspaceParamToKeyspace(param string) (string, error) {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style string keyspace
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 6 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[2] != "global" || zkPathParts[3] != "vt" || zkPathParts[4] != "keyspaces" {
			return "", fmt.Errorf("Invalid keyspace path: %v", param)
		}
		return zkPathParts[5], nil
	}
	return param, nil
}

// keyspaceParamsToKeyspaces builds a list of keyspaces.
// It supports topology-based wildcards, and plain wildcards.
// For instance:
// /zk/global/vt/keyspaces/one     // using plugin_zktopo
// /zk/global/vt/keyspaces/*       // using plugin_zktopo
// us*                             // using plain matching
// *                               // using plain matching
func keyspaceParamsToKeyspaces(wr *wrangler.Wrangler, params []string) ([]string, error) {
	result := make([]string, 0, len(params))
	for _, param := range params {
		if param[0] == '/' {
			// this is a topology-specific path
			zkPaths, err := resolveWildcards(wr, params)
			if err != nil {
				return nil, fmt.Errorf("Failed to resolve wildcard: %v", err)
			}
			for _, zkPath := range zkPaths {
				subResult, err := keyspaceParamToKeyspace(zkPath)
				if err != nil {
					return nil, err
				}
				result = append(result, subResult)
			}
		} else {
			// this is not a path, so assume a keyspace name,
			// possibly with wildcards
			keyspaces, err := topo.ResolveKeyspaceWildcard(wr.TopoServer(), param)
			if err != nil {
				return nil, fmt.Errorf("Failed to resolve keyspace wildcard %v: %v", param, err)
			}
			result = append(result, keyspaces...)
		}
	}
	return result, nil
}

func shardParamToKeyspaceShard(param string) (string, string, error) {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 8 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[2] != "global" || zkPathParts[3] != "vt" || zkPathParts[4] != "keyspaces" || zkPathParts[6] != "shards" {
			return "", "", fmt.Errorf("Invalid shard path: %v", param)
		}
		return zkPathParts[5], zkPathParts[7], nil
	}
	zkPathParts := strings.Split(param, "/")
	if len(zkPathParts) != 2 {
		return "", "", fmt.Errorf("Invalid shard path: %v", param)
	}
	return zkPathParts[0], zkPathParts[1], nil
}

// shardParamsToKeyspaceShards builds a list of keyspace/shard pairs.
// It supports topology-based wildcards, and plain wildcards.
// For instance:
// /zk/global/vt/keyspaces/*/shards/* // using plugin_zktopo
// user/*                             // using plain matching
// */0                                // using plain matching
func shardParamsToKeyspaceShards(wr *wrangler.Wrangler, params []string) ([]topo.KeyspaceShard, error) {
	result := make([]topo.KeyspaceShard, 0, len(params))
	for _, param := range params {
		if param[0] == '/' {
			// this is a topology-specific path
			zkPaths, err := resolveWildcards(wr, params)
			if err != nil {
				return nil, fmt.Errorf("Failed to resolve wildcard: %v", err)
			}
			for _, zkPath := range zkPaths {
				keyspace, shard, err := shardParamToKeyspaceShard(zkPath)
				if err != nil {
					return nil, err
				}
				result = append(result, topo.KeyspaceShard{Keyspace: keyspace, Shard: shard})
			}
		} else {
			// this is not a path, so assume a keyspace
			// name / shard name, each possibly with wildcards
			keyspaceShards, err := topo.ResolveShardWildcard(wr.TopoServer(), param)
			if err != nil {
				return nil, fmt.Errorf("Failed to resolve keyspace/shard wildcard %v: %v", param, err)
			}
			result = append(result, keyspaceShards...)
		}
	}
	return result, nil
}

// tabletParamToTabletAlias takes either an old style ZK tablet path or a
// new style tablet alias as a string, and returns a TabletAlias.
func tabletParamToTabletAlias(param string) (topo.TabletAlias, error) {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style string tablet alias
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 6 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[3] != "vt" || zkPathParts[4] != "tablets" {
			return topo.TabletAlias{}, fmt.Errorf("Invalid tablet path: %v", param)
		}
		param = zkPathParts[2] + "-" + zkPathParts[5]
	}
	result, err := topo.ParseTabletAliasString(param)
	if err != nil {
		return topo.TabletAlias{}, fmt.Errorf("Invalid tablet alias %v: %v", param, err)
	}
	return result, nil
}

// tabletParamsToTabletAliases takes multiple params and converts them
// to tablet aliases.
func tabletParamsToTabletAliases(params []string) ([]topo.TabletAlias, error) {
	result := make([]topo.TabletAlias, len(params))
	var err error
	for i, param := range params {
		result[i], err = tabletParamToTabletAlias(param)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// tabletRepParamToTabletAlias takes either an old style ZK tablet replication
// path or a new style tablet alias as a string, and returns a
// TabletAlias.
func tabletRepParamToTabletAlias(param string) (topo.TabletAlias, error) {
	if param[0] == '/' {
		// old zookeeper replication path, e.g.
		// /zk/global/vt/keyspaces/ruser/shards/10-20/nyc-0000200278
		// convert to new-style string tablet alias
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 9 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[2] != "global" || zkPathParts[3] != "vt" || zkPathParts[4] != "keyspaces" || zkPathParts[6] != "shards" {
			return topo.TabletAlias{}, fmt.Errorf("Invalid tablet replication path: %v", param)
		}
		param = zkPathParts[8]
	}
	result, err := topo.ParseTabletAliasString(param)
	if err != nil {
		return topo.TabletAlias{}, fmt.Errorf("Invalid tablet alias %v: %v", param, err)
	}
	return result, nil
}

// vtPathToCell takes either an old style ZK vt path /zk/<cell>/vt or
// a new style cell and returns the cell name
func vtPathToCell(param string) (string, error) {
	if param[0] == '/' {
		// old zookeeper replication path like /zk/<cell>/vt
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 4 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[3] != "vt" {
			return "", fmt.Errorf("Invalid vt path: %v", param)
		}
		return zkPathParts[2], nil
	}
	return param, nil
}

// parseTabletType parses the string tablet type and verifies
// it is an accepted one
func parseTabletType(param string, types []topo.TabletType) (topo.TabletType, error) {
	tabletType := topo.TabletType(param)
	if !topo.IsTypeInList(tabletType, types) {
		return "", fmt.Errorf("Type %v is not one of: %v", tabletType, strings.Join(topo.MakeStringTypeList(types), " "))
	}
	return tabletType, nil
}

func commandInitTablet(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	var (
		dbNameOverride = subFlags.String("db-name-override", "", "override the name of the db used by vttablet")
		force          = subFlags.Bool("force", false, "will overwrite the node if it already exists")
		parent         = subFlags.Bool("parent", false, "will create the parent shard and keyspace if they don't exist yet")
		update         = subFlags.Bool("update", false, "perform update if a tablet with provided alias exists")
		hostname       = subFlags.String("hostname", "", "server the tablet is running on")
		mysqlPort      = subFlags.Int("mysql_port", 0, "mysql port for the mysql daemon")
		port           = subFlags.Int("port", 0, "main port for the vttablet process")
		vtsPort        = subFlags.Int("vts_port", 0, "encrypted port for the vttablet process")
		keyspace       = subFlags.String("keyspace", "", "keyspace this tablet belongs to")
		shard          = subFlags.String("shard", "", "shard this tablet belongs to")
		parentAlias    = subFlags.String("parent_alias", "", "alias of the mysql parent tablet for this tablet")
		tags           flagutil.StringMapValue
	)
	subFlags.Var(&tags, "tags", "comma separated list of key:value pairs used to tag the tablet")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 2 {
		return fmt.Errorf("action InitTablet requires <tablet alias> <tablet type>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletType, err := parseTabletType(subFlags.Arg(1), topo.AllTabletTypes)
	if err != nil {
		return err
	}

	// create tablet record
	tablet := &topo.Tablet{
		Alias:          tabletAlias,
		Hostname:       *hostname,
		Portmap:        make(map[string]int),
		Keyspace:       *keyspace,
		Shard:          *shard,
		Type:           tabletType,
		DbNameOverride: *dbNameOverride,
		Tags:           tags,
	}
	if *port != 0 {
		tablet.Portmap["vt"] = *port
	}
	if *mysqlPort != 0 {
		tablet.Portmap["mysql"] = *mysqlPort
	}
	if *vtsPort != 0 {
		tablet.Portmap["vts"] = *vtsPort
	}
	if *parentAlias != "" {
		tablet.Parent, err = tabletRepParamToTabletAlias(*parentAlias)
		if err != nil {
			return err
		}
	}

	return wr.InitTablet(tablet, *force, *parent, *update)
}

func commandGetTablet(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action GetTablet requires <tablet alias|zk tablet path>")
	}

	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(tabletAlias)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJson(tabletInfo))
	}
	return err
}

func commandUpdateTabletAddrs(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	hostname := subFlags.String("hostname", "", "fully qualified host name")
	ipAddr := subFlags.String("ip-addr", "", "IP address")
	mysqlPort := subFlags.Int("mysql-port", 0, "mysql port")
	vtPort := subFlags.Int("vt-port", 0, "vt port")
	vtsPort := subFlags.Int("vts-port", 0, "vts port")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 1 {
		return fmt.Errorf("action UpdateTabletAddrs requires <tablet alias|zk tablet path>")
	}
	if *ipAddr != "" && net.ParseIP(*ipAddr) == nil {
		return fmt.Errorf("malformed address: %v", *ipAddr)
	}

	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.TopoServer().UpdateTabletFields(tabletAlias, func(tablet *topo.Tablet) error {
		if *hostname != "" {
			tablet.Hostname = *hostname
		}
		if *ipAddr != "" {
			tablet.IPAddr = *ipAddr
		}
		if *vtPort != 0 || *vtsPort != 0 || *mysqlPort != 0 {
			if tablet.Portmap == nil {
				tablet.Portmap = make(map[string]int)
			}
			if *vtPort != 0 {
				tablet.Portmap["vt"] = *vtPort
			}
			if *vtsPort != 0 {
				tablet.Portmap["vts"] = *vtsPort
			}
			if *mysqlPort != 0 {
				tablet.Portmap["mysql"] = *mysqlPort
			}
		}
		return nil
	})
}

func commandScrapTablet(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "writes the scrap state in to zk, no questions asked, if a tablet is offline")
	skipRebuild := subFlags.Bool("skip-rebuild", false, "do not rebuild the shard and keyspace graph after scrapping")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ScrapTablet requires <tablet alias|zk tablet path>")
	}

	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.Scrap(tabletAlias, *force, *skipRebuild)
}

func commandDeleteTablet(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("action DeleteTablet requires at least one <tablet alias|zk tablet path> ...")
	}

	tabletAliases, err := tabletParamsToTabletAliases(subFlags.Args())
	if err != nil {
		return err
	}
	for _, tabletAlias := range tabletAliases {
		if err := wr.DeleteTablet(tabletAlias); err != nil {
			return err
		}
	}
	return nil
}

func commandSetReadOnly(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action SetReadOnly requires <tablet alias|zk tablet path>")
	}

	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().SetReadOnly(ti, wr.ActionTimeout())
}

func commandSetReadWrite(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action SetReadWrite requires <tablet alias|zk tablet path>")
	}

	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().SetReadWrite(ti, wr.ActionTimeout())
}

func commandChangeSlaveType(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will change the type in zookeeper, and not run hooks")
	dryRun := subFlags.Bool("dry-run", false, "just list the proposed change")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action ChangeSlaveType requires <zk tablet path> <db type>")
	}

	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	newType, err := parseTabletType(subFlags.Arg(1), topo.AllTabletTypes)
	if err != nil {
		return err
	}
	if *dryRun {
		ti, err := wr.TopoServer().GetTablet(tabletAlias)
		if err != nil {
			return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
		}
		if !topo.IsTrivialTypeChange(ti.Type, newType) || !topo.IsValidTypeChange(ti.Type, newType) {
			return fmt.Errorf("invalid type transition %v: %v -> %v", tabletAlias, ti.Type, newType)
		}
		wr.Logger().Printf("- %v\n", fmtTabletAwkable(ti))
		ti.Type = newType
		wr.Logger().Printf("+ %v\n", fmtTabletAwkable(ti))
		return nil
	}
	return wr.ChangeType(tabletAlias, newType, *force)
}

func commandPing(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action Ping requires <tablet alias|zk tablet path>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().Ping(tabletInfo, wr.ActionTimeout())
}

func commandRefreshState(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action RefreshState requires <tablet alias|zk tablet path>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().RefreshState(tabletInfo, wr.ActionTimeout())
}

func commandRunHealthCheck(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action RunHealthCheck requires <tablet alias> <target tablet type>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseTabletType(subFlags.Arg(1), []topo.TabletType{topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().RunHealthCheck(tabletInfo, servedType, wr.ActionTimeout())
}

func commandQuery(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 {
		return fmt.Errorf("action Query requires 3")
	}
	return kquery(wr, subFlags.Arg(0), subFlags.Arg(1), subFlags.Arg(2))
}

func commandSleep(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action Sleep requires <tablet alias|zk tablet path> <duration>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	duration, err := time.ParseDuration(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().Sleep(ti, duration, wr.ActionTimeout())
}

func commandSnapshotSourceEnd(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	slaveStartRequired := subFlags.Bool("slave-start", false, "will restart replication")
	readWrite := subFlags.Bool("read-write", false, "will make the server read-write")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action SnapshotSourceEnd requires <tablet alias|zk tablet path> <original server type>")
	}

	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletType, err := parseTabletType(subFlags.Arg(1), topo.AllTabletTypes)
	if err != nil {
		return err
	}
	return wr.SnapshotSourceEnd(tabletAlias, *slaveStartRequired, !(*readWrite), tabletType)
}

func commandSnapshot(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	serverMode := subFlags.Bool("server-mode", false, "will symlink the data files and leave mysqld stopped")
	concurrency := subFlags.Int("concurrency", 4, "how many compression/checksum jobs to run simultaneously")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action Snapshot requires <tablet alias|zk src tablet path>")
	}

	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	sr, originalType, err := wr.Snapshot(tabletAlias, *force, *concurrency, *serverMode)
	if err == nil {
		log.Infof("Manifest: %v", sr.ManifestPath)
		log.Infof("ParentAlias: %v", sr.ParentAlias)
		if *serverMode {
			log.Infof("SlaveStartRequired: %v", sr.SlaveStartRequired)
			log.Infof("ReadOnly: %v", sr.ReadOnly)
			log.Infof("OriginalType: %v", originalType)
		}
	}
	return err
}

func commandRestore(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	dontWaitForSlaveStart := subFlags.Bool("dont-wait-for-slave-start", false, "won't wait for replication to start (useful when restoring from snapshot source that is the replication master)")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 && subFlags.NArg() != 4 {
		return fmt.Errorf("action Restore requires <src tablet alias|zk src tablet path> <src manifest path> <dst tablet alias|zk dst tablet path> [<zk new master path>]")
	}
	srcTabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	dstTabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(2))
	if err != nil {
		return err
	}
	parentAlias := srcTabletAlias
	if subFlags.NArg() == 4 {
		parentAlias, err = tabletParamToTabletAlias(subFlags.Arg(3))
		if err != nil {
			return err
		}
	}
	return wr.Restore(srcTabletAlias, subFlags.Arg(1), dstTabletAlias, parentAlias, *fetchConcurrency, *fetchRetryCount, false, *dontWaitForSlaveStart)
}

func commandClone(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 4, "how many compression/checksum jobs to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	serverMode := subFlags.Bool("server-mode", false, "will keep the snapshot server offline to serve DB files directly")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() < 2 {
		return fmt.Errorf("action Clone requires <src tablet alias|zk src tablet path> <dst tablet alias|zk dst tablet path> ...")
	}

	srcTabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	dstTabletAliases := make([]topo.TabletAlias, subFlags.NArg()-1)
	for i := 1; i < subFlags.NArg(); i++ {
		dstTabletAliases[i-1], err = tabletParamToTabletAlias(subFlags.Arg(i))
		if err != nil {
			return err
		}
	}
	return wr.Clone(srcTabletAlias, dstTabletAliases, *force, *concurrency, *fetchConcurrency, *fetchRetryCount, *serverMode)
}

func commandMultiRestore(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	concurrency := subFlags.Int("concurrency", 8, "how many concurrent jobs to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 4, "how many files to fetch simultaneously")
	insertTableConcurrency := subFlags.Int("insert-table-concurrency", 4, "how many tables to load into a single destination table simultaneously")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, use 'mysqlctl multirestore -help' for more info")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() < 2 {
		return fmt.Errorf("MultiRestore requires <dst tablet alias|destination zk path> <source tablet alias|source zk path>... %v", args)
	}
	destination, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	sources := make([]topo.TabletAlias, subFlags.NArg()-1)
	for i := 1; i < subFlags.NArg(); i++ {
		sources[i-1], err = tabletParamToTabletAlias(subFlags.Arg(i))
		if err != nil {
			return err
		}
	}
	return wr.MultiRestore(destination, sources, *concurrency, *fetchConcurrency, *insertTableConcurrency, *fetchRetryCount, *strategy)
}

func commandMultiSnapshot(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will force the snapshot for a master, and turn it into a backup")
	concurrency := subFlags.Int("concurrency", 8, "how many compression jobs to run simultaneously")
	spec := subFlags.String("spec", "-", "shard specification")
	tablesString := subFlags.String("tables", "", "dump only this comma separated list of table regexp")
	excludeTablesString := subFlags.String("exclude_tables", "", "comma separated list of regexps for tables to exclude")
	skipSlaveRestart := subFlags.Bool("skip-slave-restart", false, "after the snapshot is done, do not restart slave replication")
	maximumFilesize := subFlags.Uint64("maximum-file-size", 128*1024*1024, "the maximum size for an uncompressed data file")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action MultiSnapshot requires <src tablet alias|zk src tablet path>")
	}

	shards, err := key.ParseShardingSpec(*spec)
	if err != nil {
		return fmt.Errorf("multisnapshot failed: %v", err)
	}
	var tables []string
	if *tablesString != "" {
		tables = strings.Split(*tablesString, ",")
	}
	var excludeTables []string
	if *excludeTablesString != "" {
		excludeTables = strings.Split(*excludeTablesString, ",")
	}

	source, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	filenames, parentAlias, err := wr.MultiSnapshot(shards, source, *concurrency, tables, excludeTables, *force, *skipSlaveRestart, *maximumFilesize)

	if err == nil {
		log.Infof("manifest locations: %v", filenames)
		log.Infof("ParentAlias: %v", parentAlias)
	}
	return err
}

func commandExecuteFetch(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	maxRows := subFlags.Int("max_rows", 10000, "maximum number of rows to allow in reset")
	wantFields := subFlags.Bool("want_fields", false, "also get the field names")
	disableBinlogs := subFlags.Bool("disable_binlogs", false, "disable writing to binlogs during the query")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action ExecuteFetch requires <tablet alias|zk tablet path> <sql command>")
	}

	alias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	query := subFlags.Arg(1)
	qr, err := wr.ExecuteFetch(alias, query, *maxRows, *wantFields, *disableBinlogs)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJson(qr))
	}
	return err
}

func commandExecuteHook(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() < 2 {
		return fmt.Errorf("action ExecuteHook requires <tablet alias|zk tablet path> <hook name>")
	}

	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	hook := &hk.Hook{Name: subFlags.Arg(1), Parameters: subFlags.Args()[2:]}
	hr, err := wr.ExecuteHook(tabletAlias, hook)
	if err == nil {
		log.Infof(hr.String())
	}
	return err
}

func commandCreateShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will keep going even if the keyspace already exists")
	parent := subFlags.Bool("parent", false, "creates the parent keyspace if it doesn't exist")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action CreateShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	if *parent {
		if err := wr.TopoServer().CreateKeyspace(keyspace, &topo.Keyspace{}); err != nil && err != topo.ErrNodeExists {
			return err
		}
	}

	err = topo.CreateShard(wr.TopoServer(), keyspace, shard)
	if *force && err == topo.ErrNodeExists {
		log.Infof("shard %v/%v already exists (ignoring error with -force)", keyspace, shard)
		err = nil
	}
	return err
}

func commandGetShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action GetShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	shardInfo, err := wr.TopoServer().GetShard(keyspace, shard)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJson(shardInfo))
	}
	return err
}

func commandRebuildShardGraph(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cells := subFlags.String("cells", "", "comma separated list of cells to update")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("action RebuildShardGraph requires at least one <zk shard path>")
	}

	var cellArray []string
	if *cells != "" {
		cellArray = strings.Split(*cells, ",")
	}

	keyspaceShards, err := shardParamsToKeyspaceShards(wr, subFlags.Args())
	if err != nil {
		return err
	}
	for _, ks := range keyspaceShards {
		if _, err := wr.RebuildShardGraph(ks.Keyspace, ks.Shard, cellArray); err != nil {
			return err
		}
	}
	return nil
}

func commandShardExternallyReparented(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	useRpc := subFlags.Bool("use_rpc", false, "send an RPC call to the new master instead of doing the operation internally")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action ShardExternallyReparented requires <keyspace/shard|zk shard path> <tablet alias|zk tablet path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(1))
	if err != nil {
		return err
	}
	if *useRpc {
		ti, err := wr.TopoServer().GetTablet(tabletAlias)
		if err != nil {
			return err
		}
		return wr.TabletManagerClient().TabletExternallyReparented(ti, wr.ActionTimeout())
	}
	return wr.ShardExternallyReparented(keyspace, shard, tabletAlias)
}

func commandValidateShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	pingTablets := subFlags.Bool("ping-tablets", true, "ping all tablets during validate")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ValidateShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ValidateShard(keyspace, shard, *pingTablets)
}

func commandShardReplicationPositions(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ShardReplicationPositions requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tablets, stats, err := wr.ShardReplicationStatuses(keyspace, shard)
	if tablets == nil {
		return err
	}

	lines := make([]string, 0, 24)
	for _, rt := range sortReplicatingTablets(tablets, stats) {
		status := rt.ReplicationStatus
		ti := rt.TabletInfo
		if status == nil {
			lines = append(lines, fmtTabletAwkable(ti)+" <err> <err> <err>")
		} else {
			lines = append(lines, fmtTabletAwkable(ti)+fmt.Sprintf(" %v %v", status.Position, status.SecondsBehindMaster))
		}
	}
	for _, l := range lines {
		wr.Logger().Printf("%v\n", l)
	}
	return nil
}

func commandListShardTablets(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ListShardTablets requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return listTabletsByShard(wr, keyspace, shard)
}

func commandSetShardServedTypes(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "comma separated list of cells to update")
	remove := subFlags.Bool("remove", false, "will remove the served type")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action SetShardServedTypes requires <keyspace/shard|zk shard path> <served type>")
	}
	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseTabletType(subFlags.Arg(1), []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	return wr.SetShardServedTypes(keyspace, shard, cells, servedType, *remove)
}

func commandSetShardTabletControl(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "comma separated list of cells to update")
	tablesStr := subFlags.String("tables", "", "comma separated list of tables to replicate (used for vertical split)")
	remove := subFlags.Bool("remove", false, "will remove cells for vertical splits (requires tables)")
	disableQueryService := subFlags.Bool("disableQueryService", false, "will disable query service on the provided nodes")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action SetShardTabletControl requires <keyspace/shard|zk shard path> <tabletType>")
	}
	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletType, err := parseTabletType(subFlags.Arg(1), []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}
	var tables []string
	if *tablesStr != "" {
		tables = strings.Split(*tablesStr, ",")
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	return wr.SetShardTabletControl(keyspace, shard, tabletType, cells, *remove, *disableQueryService, tables)
}

func commandShardMultiRestore(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	concurrency := subFlags.Int("concurrency", 8, "how many concurrent jobs to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 4, "how many files to fetch simultaneously")
	insertTableConcurrency := subFlags.Int("insert-table-concurrency", 4, "how many tables to load into a single destination table simultaneously")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, use 'mysqlctl multirestore -help' for more info")
	tables := subFlags.String("tables", "", "comma separated list of tables to replicate (used for vertical split)")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() < 2 {
		return fmt.Errorf("ShardMultiRestore requires <keyspace/shard|zk shard path> <source tablet alias|source zk path>... %v", args)
	}
	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	sources := make([]topo.TabletAlias, subFlags.NArg()-1)
	for i := 1; i < subFlags.NArg(); i++ {
		sources[i-1], err = tabletParamToTabletAlias(subFlags.Arg(i))
		if err != nil {
			return err
		}
	}
	var tableArray []string
	if *tables != "" {
		tableArray = strings.Split(*tables, ",")
	}
	return wr.ShardMultiRestore(keyspace, shard, sources, tableArray, *concurrency, *fetchConcurrency, *insertTableConcurrency, *fetchRetryCount, *strategy)
}

func commandShardReplicationAdd(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 {
		return fmt.Errorf("action ShardReplicationAdd requires <keyspace/shard|zk shard path> <tablet alias|zk tablet path> <parent tablet alias|zk parent tablet path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(1))
	if err != nil {
		return err
	}
	parentAlias, err := tabletParamToTabletAlias(subFlags.Arg(2))
	if err != nil {
		return err
	}
	return topo.UpdateShardReplicationRecord(wr.TopoServer(), keyspace, shard, tabletAlias, parentAlias)
}

func commandShardReplicationRemove(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action ShardReplicationRemove requires <keyspace/shard|zk shard path> <tablet alias|zk tablet path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return topo.RemoveShardReplicationRecord(wr.TopoServer(), tabletAlias.Cell, keyspace, shard, tabletAlias)
}

func commandShardReplicationFix(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action ShardReplicationRemove requires <cell> <keyspace/shard|zk shard path>")
	}

	cell := subFlags.Arg(0)
	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return topo.FixShardReplication(wr.TopoServer(), wr.Logger(), cell, keyspace, shard)
}

func commandRemoveShardCell(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will keep going even we can't reach the cell's topology server to check for tablets")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action RemoveShardCell requires <keyspace/shard|zk shard path> <cell>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.RemoveShardCell(keyspace, shard, subFlags.Arg(1), *force)
}

func commandDeleteShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("action DeleteShard requires <keyspace/shard|zk shard path> ...")
	}

	keyspaceShards, err := shardParamsToKeyspaceShards(wr, subFlags.Args())
	if err != nil {
		return err
	}
	for _, ks := range keyspaceShards {
		err := wr.DeleteShard(ks.Keyspace, ks.Shard)
		switch err {
		case nil:
			// keep going
		case topo.ErrNoNode:
			log.Infof("Shard %v/%v doesn't exist, skipping it", ks.Keyspace, ks.Shard)
		default:
			return err
		}
	}
	return nil
}

func commandCreateKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	shardingColumnName := subFlags.String("sharding_column_name", "", "column to use for sharding operations")
	shardingColumnType := subFlags.String("sharding_column_type", "", "type of the column to use for sharding operations")
	splitShardCount := subFlags.Int("split_shard_count", 0, "number of shards to use for data splits")
	force := subFlags.Bool("force", false, "will keep going even if the keyspace already exists")
	var servedFrom flagutil.StringMapValue
	subFlags.Var(&servedFrom, "served_from", "comma separated list of dbtype:keyspace pairs used to serve traffic")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action CreateKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	kit := key.KeyspaceIdType(*shardingColumnType)
	if !key.IsKeyspaceIdTypeInList(kit, key.AllKeyspaceIdTypes) {
		return fmt.Errorf("invalid sharding_column_type")
	}
	ki := &topo.Keyspace{
		ShardingColumnName: *shardingColumnName,
		ShardingColumnType: kit,
		SplitShardCount:    int32(*splitShardCount),
	}
	if len(servedFrom) > 0 {
		ki.ServedFromMap = make(map[topo.TabletType]*topo.KeyspaceServedFrom, len(servedFrom))
		for name, value := range servedFrom {
			tt := topo.TabletType(name)
			if !topo.IsInServingGraph(tt) {
				return fmt.Errorf("Cannot use tablet type that is not in serving graph: %v", tt)
			}
			ki.ServedFromMap[tt] = &topo.KeyspaceServedFrom{
				Keyspace: value,
			}
		}
	}
	err = wr.TopoServer().CreateKeyspace(keyspace, ki)
	if *force && err == topo.ErrNodeExists {
		log.Infof("keyspace %v already exists (ignoring error with -force)", keyspace)
		err = nil
	}
	return err
}

func commandGetKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action GetKeyspace requires <keyspace|zk keyspace path>")
	}

	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	keyspaceInfo, err := wr.TopoServer().GetKeyspace(keyspace)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJson(keyspaceInfo))
	}
	return err
}

func commandSetKeyspaceShardingInfo(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will update the fields even if they're already set, use with care")
	splitShardCount := subFlags.Int("split_shard_count", 0, "number of shards to use for data splits")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() > 3 || subFlags.NArg() < 1 {
		return fmt.Errorf("action SetKeyspaceShardingInfo requires <keyspace name|zk keyspace path> [<column name>] [<column type>]")
	}

	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	columnName := ""
	if subFlags.NArg() >= 2 {
		columnName = subFlags.Arg(1)
	}
	kit := key.KIT_UNSET
	if subFlags.NArg() >= 3 {
		kit = key.KeyspaceIdType(subFlags.Arg(2))
		if !key.IsKeyspaceIdTypeInList(kit, key.AllKeyspaceIdTypes) {
			return fmt.Errorf("invalid sharding_column_type")
		}
	}

	return wr.SetKeyspaceShardingInfo(keyspace, columnName, kit, int32(*splitShardCount), *force)
}

func commandSetKeyspaceServedFrom(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	source := subFlags.String("source", "", "source keyspace name")
	remove := subFlags.Bool("remove", false, "remove the served from record instead of adding it")
	cellsStr := subFlags.String("cells", "", "comma separated list of cells to affect")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action SetKeyspaceServedFrom requires <keyspace name> <tablet type>")
	}
	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseTabletType(subFlags.Arg(1), []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	return wr.SetKeyspaceServedFrom(keyspace, servedType, cells, *source, *remove)
}

func commandRebuildKeyspaceGraph(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cells := subFlags.String("cells", "", "comma separated list of cells to update")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("action RebuildKeyspaceGraph requires at least one <zk keyspace path>")
	}

	var cellArray []string
	if *cells != "" {
		cellArray = strings.Split(*cells, ",")
	}

	keyspaces, err := keyspaceParamsToKeyspaces(wr, subFlags.Args())
	if err != nil {
		return err
	}
	for _, keyspace := range keyspaces {
		if err := wr.RebuildKeyspaceGraph(keyspace, cellArray); err != nil {
			return err
		}
	}
	return nil
}

func commandValidateKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	pingTablets := subFlags.Bool("ping-tablets", false, "ping all tablets during validate")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ValidateKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ValidateKeyspace(keyspace, *pingTablets)
}

func commandMigrateServedTypes(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "comma separated list of cells to update")
	reverse := subFlags.Bool("reverse", false, "move the served type back instead of forward, use in case of trouble")
	skipReFreshState := subFlags.Bool("skip-refresh-state", false, "do not refresh the state of the source tablets after the migration (will need to be done manually, replica and rdonly only)")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action MigrateServedTypes requires <source keyspace/shard|zk source shard path> <served type>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseTabletType(subFlags.Arg(1), []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}
	if servedType == topo.TYPE_MASTER && *skipReFreshState {
		return fmt.Errorf("can only specify skip-refresh-state for non-master migrations")
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}
	return wr.MigrateServedTypes(keyspace, shard, cells, servedType, *reverse, *skipReFreshState)
}

func commandMigrateServedFrom(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	reverse := subFlags.Bool("reverse", false, "move the served from back instead of forward, use in case of trouble")
	cellsStr := subFlags.String("cells", "", "comma separated list of cells to update")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action MigrateServedFrom requires <destination keyspace/shard|zk source shard path> <served type>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseTabletType(subFlags.Arg(1), []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}
	return wr.MigrateServedFrom(keyspace, shard, servedType, cells, *reverse)
}

func commandResolve(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action Resolve requires <keyspace>.<shard>.<db type>:<port name>")
	}
	parts := strings.Split(subFlags.Arg(0), ":")
	if len(parts) != 2 {
		return fmt.Errorf("action Resolve requires <keyspace>.<shard>.<db type>:<port name>")
	}
	namedPort := parts[1]

	parts = strings.Split(parts[0], ".")
	if len(parts) != 3 {
		return fmt.Errorf("action Resolve requires <keyspace>.<shard>.<db type>:<port name>")
	}

	tabletType, err := parseTabletType(parts[2], topo.AllTabletTypes)
	if err != nil {
		return err
	}
	addrs, err := topo.LookupVtName(wr.TopoServer(), "local", parts[0], parts[1], tabletType, namedPort)
	if err != nil {
		return err
	}
	for _, addr := range addrs {
		wr.Logger().Printf("%v:%v\n", addr.Target, addr.Port)
	}
	return nil
}

func commandValidate(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	pingTablets := subFlags.Bool("ping-tablets", false, "ping all tablets during validate")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 0 {
		log.Warningf("action Validate doesn't take any parameter any more")
	}
	return wr.Validate(*pingTablets)
}

func commandRebuildReplicationGraph(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	// This is sort of a nuclear option.
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() < 2 {
		return fmt.Errorf("action RebuildReplicationGraph requires <cell1>,<cell2>,... <keyspace1>,<keyspace2>...")
	}

	cellParams := strings.Split(subFlags.Arg(0), ",")
	resolvedCells, err := resolveWildcards(wr, cellParams)
	if err != nil {
		return err
	}
	cells := make([]string, 0, len(cellParams))
	for _, cell := range resolvedCells {
		c, err := vtPathToCell(cell)
		if err != nil {
			return err
		}
		cells = append(cells, c)
	}

	keyspaceParams := strings.Split(subFlags.Arg(1), ",")
	keyspaces, err := keyspaceParamsToKeyspaces(wr, keyspaceParams)
	if err != nil {
		return err
	}
	return wr.RebuildReplicationGraph(cells, keyspaces)
}

func commandListAllTablets(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ListAllTablets requires <cell name|zk vt path>")
	}

	cell, err := vtPathToCell(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return dumpAllTablets(wr, cell)
}

func commandListTablets(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("action ListTablets requires <tablet alias|zk tablet path> ...")
	}

	zkPaths, err := resolveWildcards(wr, subFlags.Args())
	if err != nil {
		return err
	}
	aliases := make([]topo.TabletAlias, len(zkPaths))
	for i, zkPath := range zkPaths {
		aliases[i], err = tabletParamToTabletAlias(zkPath)
		if err != nil {
			return err
		}
	}
	return dumpTablets(wr, aliases)
}

func commandGetSchema(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	tables := subFlags.String("tables", "", "comma separated list of regexps for tables to gather schema information for")
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of regexps for tables to exclude")
	includeViews := subFlags.Bool("include-views", false, "include views in the output")
	tableNamesOnly := subFlags.Bool("table_names_only", false, "only display the table names that match")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action GetSchema requires <tablet alias|zk tablet path>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	var tableArray []string
	if *tables != "" {
		tableArray = strings.Split(*tables, ",")
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}

	sd, err := wr.GetSchema(tabletAlias, tableArray, excludeTableArray, *includeViews)
	if err == nil {
		if *tableNamesOnly {
			for _, td := range sd.TableDefinitions {
				wr.Logger().Printf("%v\n", td.Name)
			}
		} else {
			wr.Logger().Printf("%v\n", jscfg.ToJson(sd))
		}
	}
	return err
}

func commandReloadSchema(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ReloadSchema requires <tablet alias|zk tablet path>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ReloadSchema(tabletAlias)
}

func commandValidateSchemaShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of regexps for tables to exclude")
	includeViews := subFlags.Bool("include-views", false, "include views in the validation")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ValidateSchemaShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return wr.ValidateSchemaShard(keyspace, shard, excludeTableArray, *includeViews)
}

func commandValidateSchemaKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of regexps for tables to exclude")
	includeViews := subFlags.Bool("include-views", false, "include views in the validation")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ValidateSchemaKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return wr.ValidateSchemaKeyspace(keyspace, excludeTableArray, *includeViews)
}

func commandPreflightSchema(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	sql := subFlags.String("sql", "", "sql command")
	sqlFile := subFlags.String("sql-file", "", "file containing the sql commands")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 1 {
		return fmt.Errorf("action PreflightSchema requires <tablet alias|zk tablet path>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	change, err := getFileParam(*sql, *sqlFile, "sql")
	if err != nil {
		return err
	}
	scr, err := wr.PreflightSchema(tabletAlias, change)
	if err == nil {
		log.Infof(scr.String())
	}
	return err
}

func commandApplySchema(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will apply the schema even if preflight schema doesn't match")
	sql := subFlags.String("sql", "", "sql command")
	sqlFile := subFlags.String("sql-file", "", "file containing the sql commands")
	skipPreflight := subFlags.Bool("skip-preflight", false, "do not preflight the schema (use with care)")
	stopReplication := subFlags.Bool("stop-replication", false, "stop replication before applying schema")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ApplySchema requires <tablet alias|zk tablet path>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	change, err := getFileParam(*sql, *sqlFile, "sql")
	if err != nil {
		return err
	}

	sc := &myproto.SchemaChange{}
	sc.Sql = change
	sc.AllowReplication = !(*stopReplication)

	// do the preflight to get before and after schema
	if !(*skipPreflight) {
		scr, err := wr.PreflightSchema(tabletAlias, sc.Sql)
		if err != nil {
			return fmt.Errorf("preflight failed: %v", err)
		}
		log.Infof("Preflight: " + scr.String())
		sc.BeforeSchema = scr.BeforeSchema
		sc.AfterSchema = scr.AfterSchema
		sc.Force = *force
	}

	scr, err := wr.ApplySchema(tabletAlias, sc)
	if err == nil {
		log.Infof(scr.String())
	}
	return err
}

func commandApplySchemaShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will apply the schema even if preflight schema doesn't match")
	sql := subFlags.String("sql", "", "sql command")
	sqlFile := subFlags.String("sql-file", "", "file containing the sql commands")
	simple := subFlags.Bool("simple", false, "just apply change on master and let replication do the rest")
	newParent := subFlags.String("new-parent", "", "will reparent to this tablet after the change")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ApplySchemaShard requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	change, err := getFileParam(*sql, *sqlFile, "sql")
	if err != nil {
		return err
	}
	var newParentAlias topo.TabletAlias
	if *newParent != "" {
		newParentAlias, err = tabletParamToTabletAlias(*newParent)
		if err != nil {
			return err
		}
	}

	if (*simple) && (*newParent != "") {
		return fmt.Errorf("new_parent for action ApplySchemaShard can only be specified for complex schema upgrades")
	}

	scr, err := wr.ApplySchemaShard(keyspace, shard, change, newParentAlias, *simple, *force)
	if err == nil {
		log.Infof(scr.String())
	}
	return err
}

func commandApplySchemaKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will apply the schema even if preflight schema doesn't match")
	sql := subFlags.String("sql", "", "sql command")
	sqlFile := subFlags.String("sql-file", "", "file containing the sql commands")
	simple := subFlags.Bool("simple", false, "just apply change on master and let replication do the rest")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ApplySchemaKeyspace requires <keyspace|zk keyspace path>")
	}

	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	change, err := getFileParam(*sql, *sqlFile, "sql")
	if err != nil {
		return err
	}
	scr, err := wr.ApplySchemaKeyspace(keyspace, change, *simple, *force)
	if err == nil {
		log.Infof(scr.String())
	}
	return err
}

func commandValidateVersionShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ValidateVersionShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ValidateVersionShard(keyspace, shard)
}

func commandValidateVersionKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ValidateVersionKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ValidateVersionKeyspace(keyspace)
}

func commandGetPermissions(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action GetPermissions requires <tablet alias|zk tablet path>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	p, err := wr.GetPermissions(tabletAlias)
	if err == nil {
		log.Infof("%v", p.String()) // they can contain '%'
	}
	return err
}

func commandValidatePermissionsShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ValidatePermissionsShard requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ValidatePermissionsShard(keyspace, shard)
}

func commandValidatePermissionsKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ValidatePermissionsKeyspace requires <keyspace name|zk keyspace path>")
	}

	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ValidatePermissionsKeyspace(keyspace)
}

func commandGetSrvKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action GetSrvKeyspace requires <cell> <keyspace>")
	}

	srvKeyspace, err := wr.TopoServer().GetSrvKeyspace(subFlags.Arg(0), subFlags.Arg(1))
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJson(srvKeyspace))
	}
	return err
}

func commandGetSrvKeyspaceNames(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action GetSrvKeyspaceNames requires <cell>")
	}

	srvKeyspaceNames, err := wr.TopoServer().GetSrvKeyspaceNames(subFlags.Arg(0))
	if err != nil {
		return err
	}
	for _, ks := range srvKeyspaceNames {
		wr.Logger().Printf("%v\n", ks)
	}
	return nil
}

func commandGetSrvShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action GetSrvShard requires <cell> <keyspace/shard|zk shard path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(1))
	if err != nil {
		return err
	}
	srvShard, err := wr.TopoServer().GetSrvShard(subFlags.Arg(0), keyspace, shard)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJson(srvShard))
	}
	return err
}

func commandGetEndPoints(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 {
		return fmt.Errorf("action GetEndPoints requires <cell> <keyspace/shard|zk shard path> <tablet type>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(1))
	if err != nil {
		return err
	}
	tabletType := topo.TabletType(subFlags.Arg(2))
	endPoints, err := wr.TopoServer().GetEndPoints(subFlags.Arg(0), keyspace, shard, tabletType)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJson(endPoints))
	}
	return err
}

func commandGetShardReplication(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action GetShardReplication requires <cell> <keyspace/shard|zk shard path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(1))
	if err != nil {
		return err
	}
	shardReplication, err := wr.TopoServer().GetShardReplication(subFlags.Arg(0), keyspace, shard)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJson(shardReplication))
	}
	return err
}

type rTablet struct {
	*topo.TabletInfo
	*myproto.ReplicationStatus
}

type rTablets []*rTablet

func (rts rTablets) Len() int { return len(rts) }

func (rts rTablets) Swap(i, j int) { rts[i], rts[j] = rts[j], rts[i] }

// Sort for tablet replication.
// master first, then i/o position, then sql position
func (rts rTablets) Less(i, j int) bool {
	// NOTE: Swap order of unpack to reverse sort
	l, r := rts[j], rts[i]
	// l or r ReplicationStatus would be nil if we failed to get
	// the position (put them at the beginning of the list)
	if l.ReplicationStatus == nil {
		return r.ReplicationStatus != nil
	}
	if r.ReplicationStatus == nil {
		return false
	}
	var lTypeMaster, rTypeMaster int
	if l.Type == topo.TYPE_MASTER {
		lTypeMaster = 1
	}
	if r.Type == topo.TYPE_MASTER {
		rTypeMaster = 1
	}
	if lTypeMaster < rTypeMaster {
		return true
	}
	if lTypeMaster == rTypeMaster {
		return !l.Position.AtLeast(r.Position)
	}
	return false
}

func sortReplicatingTablets(tablets []*topo.TabletInfo, stats []*myproto.ReplicationStatus) []*rTablet {
	rtablets := make([]*rTablet, len(tablets))
	for i, status := range stats {
		rtablets[i] = &rTablet{TabletInfo: tablets[i], ReplicationStatus: status}
	}
	sort.Sort(rTablets(rtablets))
	return rtablets
}

// RunCommand will execute the command using the provided wrangler.
// It will return the actionPath to wait on for long remote actions if
// applicable.
func RunCommand(wr *wrangler.Wrangler, args []string) error {
	if len(args) == 0 {
		wr.Logger().Printf("No command specified. Please see the list below:\n\n")
		PrintAllCommands(wr.Logger())
		return fmt.Errorf("No command specified")
	}

	action := args[0]
	actionLowerCase := strings.ToLower(action)
	for _, group := range commands {
		for _, cmd := range group.commands {
			if strings.ToLower(cmd.name) == actionLowerCase {
				subFlags := flag.NewFlagSet(action, flag.ContinueOnError)
				subFlags.SetOutput(logutil.NewLoggerWriter(wr.Logger()))
				subFlags.Usage = func() {
					wr.Logger().Printf("Usage: %s %s\n\n", action, cmd.params)
					wr.Logger().Printf("%s\n\n", cmd.help)
					subFlags.PrintDefaults()
				}
				return cmd.method(wr, subFlags, args[1:])
			}
		}
	}

	wr.Logger().Printf("Unknown command: %v\n", action)
	return ErrUnknownCommand
}

// PrintAllCommands will print the list of commands to the logger
func PrintAllCommands(logger logutil.Logger) {
	for _, group := range commands {
		logger.Printf("%s:\n", group.name)
		for _, cmd := range group.commands {
			if strings.HasPrefix(cmd.help, "HIDDEN") {
				continue
			}
			logger.Printf("  %s %s\n", cmd.name, cmd.params)
		}
		logger.Printf("\n")
	}
}
