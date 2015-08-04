// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The following comment section contains definitions for command arguments.
/*
COMMAND ARGUMENT DEFINITIONS

- cell, cell name: A cell is a location for a service. Generally, a cell
        resides in only one cluster. In Vitess, the terms "cell" and
        "data center" are interchangeable. The argument value is a
        string that does not contain whitespace.

- tablet alias: A Tablet Alias uniquely identifies a vttablet. The argument
                value is in the format
                <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

- keyspace, keyspace name: The name of a sharded database that contains one
            or more tables. Vitess distributes keyspace shards into multiple
            machines and provides an SQL interface to query the data. The
            argument value must be a string that does not contain whitespace.

- port name: A port number. The argument value should be an integer between
             <code>0</code> and <code>65535</code>, inclusive.

- shard, shard name: The name of a shard. The argument value is typically in
         the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

- keyspace/shard: The name of a sharded database that contains one or more
                  tables as well as the shard associated with the command.
                  The keyspace must be identified by a string that does not
                  contain whitepace, while the shard is typically identified
                  by a string in the format
                  <code>&lt;range start&gt;-&lt;range end&gt;</code>.

- duration: The amount of time that the action queue should be blocked.
            The value is a string that contains a possibly signed sequence
            of decimal numbers, each with optional fraction and a unit
            suffix, such as "300ms" or "1h45m". See the definition of the
            Go language's <a
            href="http://golang.org/pkg/time/#ParseDuration">ParseDuration</a>
            function for more details. Note that, in practice, the value
            should be a positively signed value.

- db type, tablet type: The vttablet's role. Valid values are:
  -- backup: A slaved copy of data that is offline to queries other than
             for backup purposes
  -- batch: A slaved copy of data for OLAP load patterns (typically for
            MapReduce jobs)
  -- worker: A tablet that is in use by a vtworker process. The tablet is likely
             lagging in replication.
  -- experimental: A slaved copy of data that is ready but not serving query
                   traffic. The value indicates a special characteristic of
                   the tablet that indicates the tablet should not be
                   considered a potential master. Vitess also does not
                   worry about lag for experimental tablets when reparenting.
  -- idle: An idle vttablet that does not have a keyspace, shard
           or type assigned
  -- lag: A slaved copy of data intentionally lagged for pseudo-backup.
  -- lag_orphan: A tablet in the midst of a reparenting process. During that
                 process, the tablet goes into a <code>lag_orphan</code> state
                 until it is reparented properly.
  -- master: A primary copy of data
  -- rdonly: A slaved copy of data for OLAP load patterns
  -- replica: A slaved copy of data ready to be promoted to master
  -- restore: A tablet that has not been in the replication graph and is
              restoring from a snapshot. Typically, a tablet progresses from
              the <code>idle</code> state to the <code>restore</code> state
              and then to the <code>spare</code> state.
  -- schema_apply: A slaved copy of data that had been serving query traffic
                   but that is not applying a schema change. Following the
                   change, the tablet will revert to its serving type.
  -- scrap: A tablet that contains data that needs to be wiped.
  -- snapshot_source: A slaved copy of data where mysqld is <b>not</b>
                      running and where Vitess is serving data files to
                      clone slaves. Use this command to enter this mode:
                      <pre>vtctl Snapshot -server-mode ...</pre>
                      Use this command to exit this mode:
                      <pre>vtctl SnapshotSourceEnd ...</pre>
  -- spare: A slaved copy of data that is ready but not serving query traffic.
            The data could be a potential master tablet.
*/

package vtctl

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/netutil"
	hk "github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	// ErrUnknownCommand is returned for an unknown command
	ErrUnknownCommand = errors.New("unknown command")
)

type command struct {
	name   string
	method func(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error
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
				"[-force] [-parent] [-update] [-db-name-override=<db name>] [-hostname=<hostname>] [-mysql_port=<port>] [-port=<port>] [-grpc_port=<port>] [-keyspace=<keyspace>] [-shard=<shard>] [-parent_alias=<parent alias>] <tablet alias> <tablet type>",
				"Initializes a tablet in the topology.\n" +
					"Valid <tablet type> values are:\n" +
					"  " + strings.Join(topo.MakeStringTypeList(topo.AllTabletTypes), " ")},
			command{"GetTablet", commandGetTablet,
				"<tablet alias>",
				"Outputs a JSON structure that contains information about the Tablet."},
			command{"UpdateTabletAddrs", commandUpdateTabletAddrs,
				"[-hostname <hostname>] [-ip-addr <ip addr>] [-mysql-port <mysql port>] [-vt-port <vt port>] [-grpc-port <grpc port>] <tablet alias> ",
				"Updates the IP address and port numbers of a tablet."},
			command{"ScrapTablet", commandScrapTablet,
				"[-force] [-skip-rebuild] <tablet alias>",
				"Scraps a tablet."},
			command{"DeleteTablet", commandDeleteTablet,
				"<tablet alias> ...",
				"Deletes scrapped tablet(s) from the topology."},
			command{"SetReadOnly", commandSetReadOnly,
				"<tablet alias>",
				"Sets the tablet as read-only."},
			command{"SetReadWrite", commandSetReadWrite,
				"<tablet alias>",
				"Sets the tablet as read-write."},
			command{"StartSlave", commandStartSlave,
				"<tablet alias>",
				"Starts replication on the specified slave."},
			command{"StopSlave", commandStopSlave,
				"<tablet alias>",
				"Stops replication on the specified slave."},
			command{"ChangeSlaveType", commandChangeSlaveType,
				"[-force] [-dry-run] <tablet alias> <tablet type>",
				"Changes the db type for the specified tablet, if possible. This command is used primarily to arrange replicas, and it will not convert a master.\n" +
					"NOTE: This command automatically updates the serving graph.\n" +
					"Valid <tablet type> values are:\n" +
					"  " + strings.Join(topo.MakeStringTypeList(topo.SlaveTabletTypes), " ")},
			command{"Ping", commandPing,
				"<tablet alias>",
				"Checks that the specified tablet is awake and responding to RPCs. This command can be blocked by other in-flight operations."},
			command{"RefreshState", commandRefreshState,
				"<tablet alias>",
				"Reloads the tablet record on the specified tablet."},
			command{"RunHealthCheck", commandRunHealthCheck,
				"<tablet alias> <target tablet type>",
				"Runs a health check on a remote tablet with the specified target type."},
			command{"Sleep", commandSleep,
				"<tablet alias> <duration>",
				"Blocks the action queue on the specified tablet for the specified amount of time. This is typically used for testing."},
			command{"Backup", commandBackup,
				"[-concurrency=4] <tablet alias>",
				"Stops mysqld and uses the BackupStorage service to store a new backup. This function also remembers if the tablet was replicating so that it can restore the same state after the backup completes."},
			command{"ExecuteHook", commandExecuteHook,
				"<tablet alias> <hook name> [<param1=value1> <param2=value2> ...]",
				"Runs the specified hook on the given tablet. A hook is a script that resides in the $VTROOT/vthook directory. You can put any script into that directory and use this command to run that script.\n" +
					"For this command, the param=value arguments are parameters that the command passes to the specified hook."},
			command{"ExecuteFetchAsDba", commandExecuteFetchAsDba,
				"[--max_rows=10000] [--want_fields] [--disable_binlogs] <tablet alias> <sql command>",
				"Runs the given SQL command as a DBA on the remote tablet."},
		},
	},
	commandGroup{
		"Shards", []command{
			command{"CreateShard", commandCreateShard,
				"[-force] [-parent] <keyspace/shard>",
				"Creates the specified shard."},
			command{"GetShard", commandGetShard,
				"<keyspace/shard>",
				"Outputs a JSON structure that contains information about the Shard."},
			command{"RebuildShardGraph", commandRebuildShardGraph,
				"[-cells=a,b] <keyspace/shard> ... ",
				"Rebuilds the replication graph and shard serving data in ZooKeeper or etcd. This may trigger an update to all connected clients."},
			command{"TabletExternallyReparented", commandTabletExternallyReparented,
				"<tablet alias>",
				"Changes metadata in the topology server to acknowledge a shard master change performed by an external tool. See the Reparenting guide for more information:" +
					"https://github.com/youtube/vitess/blob/master/doc/Reparenting.md#external-reparents."},
			command{"ValidateShard", commandValidateShard,
				"[-ping-tablets] <keyspace/shard>",
				"Validates that all nodes that are reachable from this shard are consistent."},
			command{"ShardReplicationPositions", commandShardReplicationPositions,
				"<keyspace/shard>",
				"Shows the replication status of each slave machine in the shard graph. In this case, the status refers to the replication lag between the master vttablet and the slave vttablet. In Vitess, data is always written to the master vttablet first and then replicated to all slave vttablets."},
			command{"ListShardTablets", commandListShardTablets,
				"<keyspace/shard>",
				"Lists all tablets in the specified shard."},
			command{"SetShardServedTypes", commandSetShardServedTypes,
				"<keyspace/shard> [<served tablet type1>,<served tablet type2>,...]",
				"Sets a given shard's served tablet types. Does not rebuild any serving graph."},
			command{"SetShardTabletControl", commandSetShardTabletControl,
				"[--cells=c1,c2,...] [--blacklisted_tables=t1,t2,...] [--remove] [--disable_query_service] <keyspace/shard> <tablet type>",
				"Sets the TabletControl record for a shard and type. Only use this for an emergency fix or after a finished vertical split. The *MigrateServedFrom* and *MigrateServedType* commands set this field appropriately already. Always specify the blacklisted_tables flag for vertical splits, but never for horizontal splits."},
			command{"SourceShardDelete", commandSourceShardDelete,
				"<keyspace/shard> <uid>",
				"Deletes the SourceShard record with the provided index. This is meant as an emergency cleanup function. It does not call RefreshState for the shard master."},
			command{"SourceShardAdd", commandSourceShardAdd,
				"[--key_range=<keyrange>] [--tables=<table1,table2,...>] <keyspace/shard> <uid> <source keyspace/shard>",
				"Adds the SourceShard record with the provided index. This is meant as an emergency function. It does not call RefreshState for the shard master."},
			command{"ShardReplicationAdd", commandShardReplicationAdd,
				"<keyspace/shard> <tablet alias> <parent tablet alias>",
				"HIDDEN Adds an entry to the replication graph in the given cell."},
			command{"ShardReplicationRemove", commandShardReplicationRemove,
				"<keyspace/shard> <tablet alias>",
				"HIDDEN Removes an entry from the replication graph in the given cell."},
			command{"ShardReplicationFix", commandShardReplicationFix,
				"<cell> <keyspace/shard>",
				"Walks through a ShardReplication object and fixes the first error that it encounters."},
			command{"RemoveShardCell", commandRemoveShardCell,
				"[-force] [-recursive] <keyspace/shard> <cell>",
				"Removes the cell from the shard's Cells list."},
			command{"DeleteShard", commandDeleteShard,
				"[-recursive] <keyspace/shard> ...",
				"Deletes the specified shard(s). In recursive mode, it also deletes all tablets belonging to the shard. Otherwise, there must be no tablets left in the shard."},
		},
	},
	commandGroup{
		"Keyspaces", []command{
			command{"CreateKeyspace", commandCreateKeyspace,
				"[-sharding_column_name=name] [-sharding_column_type=type] [-served_from=tablettype1:ks1,tablettype2,ks2,...] [-split_shard_count=N] [-force] <keyspace name>",
				"Creates the specified keyspace."},
			command{"DeleteKeyspace", commandDeleteKeyspace,
				"[-recursive] <keyspace>",
				"Deletes the specified keyspace. In recursive mode, it also recursively deletes all shards in the keyspace. Otherwise, there must be no shards left in the keyspace."},
			command{"RemoveKeyspaceCell", commandRemoveKeyspaceCell,
				"[-force] [-recursive] <keyspace> <cell>",
				"Removes the cell from the Cells list for all shards in the keyspace."},
			command{"GetKeyspace", commandGetKeyspace,
				"<keyspace>",
				"Outputs a JSON structure that contains information about the Keyspace."},
			command{"SetKeyspaceShardingInfo", commandSetKeyspaceShardingInfo,
				"[-force] [-split_shard_count=N] <keyspace name> [<column name>] [<column type>]",
				"Updates the sharding information for a keyspace."},
			command{"SetKeyspaceServedFrom", commandSetKeyspaceServedFrom,
				"[-source=<source keyspace name>] [-remove] [-cells=c1,c2,...] <keyspace name> <tablet type>",
				"Changes the ServedFromMap manually. This command is intended for emergency fixes. This field is automatically set when you call the *MigrateServedFrom* command. This command does not rebuild the serving graph."},
			command{"RebuildKeyspaceGraph", commandRebuildKeyspaceGraph,
				"[-cells=a,b] [-rebuild_srv_shards] <keyspace> ...",
				"Rebuilds the serving data for the keyspace and, optionally, all shards in the specified keyspace. This command may trigger an update to all connected clients."},
			command{"ValidateKeyspace", commandValidateKeyspace,
				"[-ping-tablets] <keyspace name>",
				"Validates that all nodes reachable from the specified keyspace are consistent."},
			command{"MigrateServedTypes", commandMigrateServedTypes,
				"[-cells=c1,c2,...] [-reverse] [-skip-refresh-state] <keyspace/shard> <served tablet type>",
				"Migrates a serving type from the source shard to the shards that it replicates to. This command also rebuilds the serving graph. The <keyspace/shard> argument can specify any of the shards involved in the migration."},
			command{"MigrateServedFrom", commandMigrateServedFrom,
				"[-cells=c1,c2,...] [-reverse] <destination keyspace/shard> <served tablet type>",
				"Makes the <destination keyspace/shard> serve the given type. This command also rebuilds the serving graph."},
			command{"FindAllShardsInKeyspace", commandFindAllShardsInKeyspace,
				"<keyspace>",
				"Displays all of the shards in the specified keyspace."},
		},
	},
	commandGroup{
		"Generic", []command{
			command{"Resolve", commandResolve,
				"<keyspace>.<shard>.<db type>:<port name>",
				"Reads a list of addresses that can answer this query. The port name can be mysql, vt, or grpc. Vitess uses this name to retrieve the actual port number from the topology server (ZooKeeper or etcd)."},
			command{"RebuildReplicationGraph", commandRebuildReplicationGraph,
				"<cell1>,<cell2>... <keyspace1>,<keyspace2>,...",
				"HIDDEN This takes the Thor's hammer approach of recovery and should only be used in emergencies.  cell1,cell2,... are the canonical source of data for the system. This function uses that canonical data to recover the replication graph, at which point further auditing with Validate can reveal any remaining issues."},
			command{"Validate", commandValidate,
				"[-ping-tablets]",
				"Validates that all nodes reachable from the global replication graph and that all tablets in all discoverable cells are consistent."},
			command{"ListAllTablets", commandListAllTablets,
				"<cell name>",
				"Lists all tablets in an awk-friendly way."},
			command{"ListTablets", commandListTablets,
				"<tablet alias> ...",
				"Lists specified tablets in an awk-friendly way."},
			command{"Panic", commandPanic,
				"",
				"HIDDEN Triggers a panic on the server side, to test the handling."},
		},
	},
	commandGroup{
		"Schema, Version, Permissions", []command{
			command{"GetSchema", commandGetSchema,
				"[-tables=<table1>,<table2>,...] [-exclude_tables=<table1>,<table2>,...] [-include-views] <tablet alias>",
				"Displays the full schema for a tablet, or just the schema for the specified tables in that tablet."},
			command{"ReloadSchema", commandReloadSchema,
				"<tablet alias>",
				"Reloads the schema on a remote tablet."},
			command{"ValidateSchemaShard", commandValidateSchemaShard,
				"[-exclude_tables=''] [-include-views] <keyspace/shard>",
				"Validates that the master schema matches all of the slaves."},
			command{"ValidateSchemaKeyspace", commandValidateSchemaKeyspace,
				"[-exclude_tables=''] [-include-views] <keyspace name>",
				"Validates that the master schema from shard 0 matches the schema on all of the other tablets in the keyspace."},
			command{"ApplySchema", commandApplySchema,
				"[-force] {-sql=<sql> || -sql-file=<filename>} <keyspace>",
				"Applies the schema change to the specified keyspace on every master, running in parallel on all shards. The changes are then propagated to slaves via replication. If the force flag is set, then numerous checks will be ignored, so that option should be used very cautiously."},
			command{"CopySchemaShard", commandCopySchemaShard,
				"[-tables=<table1>,<table2>,...] [-exclude_tables=<table1>,<table2>,...] [-include-views] {<source keyspace/shard> || <source tablet alias>} <destination keyspace/shard>",
				"Copies the schema from a source shard's master (or a specific tablet) to a destination shard. The schema is applied directly on the master of the destination shard, and it is propagated to the replicas through binlogs."},

			command{"ValidateVersionShard", commandValidateVersionShard,
				"<keyspace/shard>",
				"Validates that the master version matches all of the slaves."},
			command{"ValidateVersionKeyspace", commandValidateVersionKeyspace,
				"<keyspace name>",
				"Validates that the master version from shard 0 matches all of the other tablets in the keyspace."},

			command{"GetPermissions", commandGetPermissions,
				"<tablet alias>",
				"Displays the permissions for a tablet."},
			command{"ValidatePermissionsShard", commandValidatePermissionsShard,
				"<keyspace/shard>",
				"Validates that the master permissions match all the slaves."},
			command{"ValidatePermissionsKeyspace", commandValidatePermissionsKeyspace,
				"<keyspace name>",
				"Validates that the master permissions from shard 0 match those of all of the other tablets in the keyspace."},

			command{"GetVSchema", commandGetVSchema,
				"",
				"Displays the VTGate routing schema."},
			command{"ApplyVSchema", commandApplyVSchema,
				"{-vschema=<vschema> || -vschema_file=<vschema file>}",
				"Applies the VTGate routing schema."},
		},
	},
	commandGroup{
		"Serving Graph", []command{
			command{"GetSrvKeyspace", commandGetSrvKeyspace,
				"<cell> <keyspace>",
				"Outputs a JSON structure that contains information about the SrvKeyspace."},
			command{"GetSrvKeyspaceNames", commandGetSrvKeyspaceNames,
				"<cell>",
				"Outputs a list of keyspace names."},
			command{"GetSrvShard", commandGetSrvShard,
				"<cell> <keyspace/shard>",
				"Outputs a JSON structure that contains information about the SrvShard."},
			command{"GetEndPoints", commandGetEndPoints,
				"<cell> <keyspace/shard> <tablet type>",
				"Outputs a JSON structure that contains information about the EndPoints."},
		},
	},
	commandGroup{
		"Replication Graph", []command{
			command{"GetShardReplication", commandGetShardReplication,
				"<cell> <keyspace/shard>",
				"Outputs a JSON structure that contains information about the ShardReplication."},
		},
	},
}

func init() {
	// This cannot be in the static 'commands ' array, as commands
	// would reference commandHelp that references commands
	// (circular reference)
	addCommand("Generic", command{"Help", commandHelp,
		"[command name]",
		"Prints the list of available commands, or help on a specific command."})
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

func addCommandGroup(groupName string) {
	commands = append(commands, commandGroup{
		name: groupName,
	})
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
	if action.State == actionnode.ActionStateQueued {
		state = "queued"
	}
	return fmt.Sprintf("%v %v %v %v %v", action.Path, action.Action, state, action.ActionGuid, action.Error)
}

func listTabletsByShard(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string) error {
	tabletAliases, err := topo.FindAllTabletAliasesInShard(ctx, wr.TopoServer(), keyspace, shard)
	if err != nil {
		return err
	}
	return dumpTablets(ctx, wr, tabletAliases)
}

func dumpAllTablets(ctx context.Context, wr *wrangler.Wrangler, zkVtPath string) error {
	tablets, err := topotools.GetAllTablets(ctx, wr.TopoServer(), zkVtPath)
	if err != nil {
		return err
	}
	for _, ti := range tablets {
		wr.Logger().Printf("%v\n", fmtTabletAwkable(ti))
	}
	return nil
}

func dumpTablets(ctx context.Context, wr *wrangler.Wrangler, tabletAliases []topo.TabletAlias) error {
	tabletMap, err := topo.GetTabletMap(ctx, wr.TopoServer(), tabletAliases)
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

// keyspaceParamsToKeyspaces builds a list of keyspaces.
// It supports topology-based wildcards, and plain wildcards.
// For instance:
// us*                             // using plain matching
// *                               // using plain matching
func keyspaceParamsToKeyspaces(ctx context.Context, wr *wrangler.Wrangler, params []string) ([]string, error) {
	result := make([]string, 0, len(params))
	for _, param := range params {
		if param[0] == '/' {
			// this is a topology-specific path
			for _, path := range params {
				result = append(result, path)
			}
		} else {
			// this is not a path, so assume a keyspace name,
			// possibly with wildcards
			keyspaces, err := topo.ResolveKeyspaceWildcard(ctx, wr.TopoServer(), param)
			if err != nil {
				return nil, fmt.Errorf("Failed to resolve keyspace wildcard %v: %v", param, err)
			}
			result = append(result, keyspaces...)
		}
	}
	return result, nil
}

// shardParamsToKeyspaceShards builds a list of keyspace/shard pairs.
// It supports topology-based wildcards, and plain wildcards.
// For instance:
// user/*                             // using plain matching
// */0                                // using plain matching
func shardParamsToKeyspaceShards(ctx context.Context, wr *wrangler.Wrangler, params []string) ([]topo.KeyspaceShard, error) {
	result := make([]topo.KeyspaceShard, 0, len(params))
	for _, param := range params {
		if param[0] == '/' {
			// this is a topology-specific path
			for _, path := range params {
				keyspace, shard, err := topo.ParseKeyspaceShardString(path)
				if err != nil {
					return nil, err
				}
				result = append(result, topo.KeyspaceShard{Keyspace: keyspace, Shard: shard})
			}
		} else {
			// this is not a path, so assume a keyspace
			// name / shard name, each possibly with wildcards
			keyspaceShards, err := topo.ResolveShardWildcard(ctx, wr.TopoServer(), param)
			if err != nil {
				return nil, fmt.Errorf("Failed to resolve keyspace/shard wildcard %v: %v", param, err)
			}
			result = append(result, keyspaceShards...)
		}
	}
	return result, nil
}

// tabletParamsToTabletAliases takes multiple params and converts them
// to tablet aliases.
func tabletParamsToTabletAliases(params []string) ([]topo.TabletAlias, error) {
	result := make([]topo.TabletAlias, len(params))
	var err error
	for i, param := range params {
		result[i], err = topo.ParseTabletAliasString(param)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
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

// parseKeyspaceIdType parses the keyspace id type into the enum
func parseKeyspaceIdType(param string) (pb.KeyspaceIdType, error) {
	if param == "" {
		return pb.KeyspaceIdType_UNSET, nil
	}
	value, ok := pb.KeyspaceIdType_value[strings.ToUpper(param)]
	if !ok {
		return pb.KeyspaceIdType_UNSET, fmt.Errorf("unknown KeyspaceIdType %v", param)
	}
	return pb.KeyspaceIdType(value), nil
}

// parseTabletType3 parses the tablet type into the enum
func parseTabletType3(param string) (pb.TabletType, error) {
	value, ok := pb.TabletType_value[strings.ToUpper(param)]
	if !ok {
		return pb.TabletType_UNKNOWN, fmt.Errorf("unknown TabletType %v", param)
	}
	return pb.TabletType(value), nil
}

// parseServingTabletType3 parses the tablet type into the enum,
// and makes sure the enum is of serving type (MASTER, REPLICA, RDONLY/BATCH)
func parseServingTabletType3(param string) (pb.TabletType, error) {
	servedType, err := parseTabletType3(param)
	if err != nil {
		return pb.TabletType_UNKNOWN, err
	}
	if !topo.IsInServingGraph(topo.ProtoToTabletType(servedType)) {
		return pb.TabletType_UNKNOWN, fmt.Errorf("served_type has to be in the serving graph, not %v", param)
	}
	return servedType, nil
}

func commandInitTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	var (
		dbNameOverride = subFlags.String("db-name-override", "", "Overrides the name of the database that the vttablet uses")
		force          = subFlags.Bool("force", false, "Overwrites the node if the node already exists")
		parent         = subFlags.Bool("parent", false, "Creates the parent shard and keyspace if they don't yet exist")
		update         = subFlags.Bool("update", false, "Performs update if a tablet with the provided alias already exists")
		hostname       = subFlags.String("hostname", "", "The server on which the tablet is running")
		mysqlPort      = subFlags.Int("mysql_port", 0, "The mysql port for the mysql daemon")
		port           = subFlags.Int("port", 0, "The main port for the vttablet process")
		grpcPort       = subFlags.Int("grpc_port", 0, "The gRPC port for the vttablet process")
		keyspace       = subFlags.String("keyspace", "", "The keyspace to which this tablet belongs")
		shard          = subFlags.String("shard", "", "The shard to which this tablet belongs")
		tags           flagutil.StringMapValue
	)
	subFlags.Var(&tags, "tags", "A comma-separated list of key:value pairs that are used to tag the tablet")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <tablet alias> and <tablet type> arguments are both required for the InitTablet command.")
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
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
	if *grpcPort != 0 {
		tablet.Portmap["grpc"] = *grpcPort
	}

	return wr.InitTablet(ctx, tablet, *force, *parent, *update)
}

func commandGetTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the GetTablet command.")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJSON(tabletInfo))
	}
	return err
}

func commandUpdateTabletAddrs(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	hostname := subFlags.String("hostname", "", "The fully qualified host name of the server on which the tablet is running.")
	ipAddr := subFlags.String("ip-addr", "", "IP address")
	mysqlPort := subFlags.Int("mysql-port", 0, "The mysql port for the mysql daemon")
	vtPort := subFlags.Int("vt-port", 0, "The main port for the vttablet process")
	grpcPort := subFlags.Int("grpc-port", 0, "The gRPC port for the vttablet process")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the UpdateTabletAddrs command.")
	}
	if *ipAddr != "" && net.ParseIP(*ipAddr) == nil {
		return fmt.Errorf("malformed address: %v", *ipAddr)
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.TopoServer().UpdateTabletFields(ctx, tabletAlias, func(tablet *topo.Tablet) error {
		if *hostname != "" {
			tablet.Hostname = *hostname
		}
		if *ipAddr != "" {
			tablet.IPAddr = *ipAddr
		}
		if *vtPort != 0 || *grpcPort != 0 || *mysqlPort != 0 {
			if tablet.Portmap == nil {
				tablet.Portmap = make(map[string]int)
			}
			if *vtPort != 0 {
				tablet.Portmap["vt"] = *vtPort
			}
			if *grpcPort != 0 {
				tablet.Portmap["grpc"] = *grpcPort
			}
			if *mysqlPort != 0 {
				tablet.Portmap["mysql"] = *mysqlPort
			}
		}
		return nil
	})
}

func commandScrapTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Changes the tablet type to <code>scrap</code> in ZooKeeper or etcd if a tablet is offline")
	skipRebuild := subFlags.Bool("skip-rebuild", false, "Skips rebuilding the shard graph after scrapping the tablet")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the ScrapTablet command.")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.Scrap(ctx, tabletAlias, *force, *skipRebuild)
}

func commandDeleteTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("The <tablet alias> argument must be used to specify at least one tablet when calling the DeleteTablet command.")
	}

	tabletAliases, err := tabletParamsToTabletAliases(subFlags.Args())
	if err != nil {
		return err
	}
	for _, tabletAlias := range tabletAliases {
		if err := wr.DeleteTablet(ctx, tabletAlias); err != nil {
			return err
		}
	}
	return nil
}

func commandSetReadOnly(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the SetReadOnly command.")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().SetReadOnly(ctx, ti)
}

func commandSetReadWrite(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the SetReadWrite command.")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().SetReadWrite(ctx, ti)
}

func commandStartSlave(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action StartSlave requires <tablet alias>")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().StartSlave(ctx, ti)
}

func commandStopSlave(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action StopSlave requires <tablet alias>")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().StopSlave(ctx, ti)
}

func commandChangeSlaveType(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Changes the slave type in ZooKeeper or etcd without running hooks")
	dryRun := subFlags.Bool("dry-run", false, "Lists the proposed change without actually executing it")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <tablet alias> and <db type> arguments are required for the ChangeSlaveType command.")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	newType, err := parseTabletType(subFlags.Arg(1), topo.AllTabletTypes)
	if err != nil {
		return err
	}
	if *dryRun {
		ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
		if err != nil {
			return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
		}
		if !topo.IsTrivialTypeChange(ti.Type, newType) {
			return fmt.Errorf("invalid type transition %v: %v -> %v", tabletAlias, ti.Type, newType)
		}
		wr.Logger().Printf("- %v\n", fmtTabletAwkable(ti))
		ti.Type = newType
		wr.Logger().Printf("+ %v\n", fmtTabletAwkable(ti))
		return nil
	}
	return wr.ChangeType(ctx, tabletAlias, newType, *force)
}

func commandPing(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the Ping command.")
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().Ping(ctx, tabletInfo)
}

func commandRefreshState(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the RefreshState command.")
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().RefreshState(ctx, tabletInfo)
}

func commandRunHealthCheck(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <tablet alias> and <target tablet type> arguments are required for the RunHealthCheck command.")
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseTabletType(subFlags.Arg(1), []topo.TabletType{topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().RunHealthCheck(ctx, tabletInfo, servedType)
}

func commandSleep(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <tablet alias> and <duration> arguments are required for the Sleep command.")
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	duration, err := time.ParseDuration(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().Sleep(ctx, ti, duration)
}

func commandBackup(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The Backup command requires the <tablet alias> argument.")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	logStream, errFunc, err := wr.TabletManagerClient().Backup(ctx, tabletInfo, *concurrency)
	if err != nil {
		return err
	}
	for e := range logStream {
		wr.Logger().Infof("%v", e)
	}
	return errFunc()
}

func commandExecuteFetchAsDba(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	maxRows := subFlags.Int("max_rows", 10000, "Specifies the maximum number of rows to allow in reset")
	wantFields := subFlags.Bool("want_fields", false, "Indicates whether the request should also get field names")
	disableBinlogs := subFlags.Bool("disable_binlogs", false, "Disables writing to binlogs during the query")
	reloadSchema := subFlags.Bool("reload_schema", false, "Indicates whether the tablet schema will be reloaded after executing the SQL command. The default value is <code>false</code>, which indicates that the tablet schema will not be reloaded.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <tablet alias> and <sql command> arguments are required for the ExecuteFetchAsDba command.")
	}

	alias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	query := subFlags.Arg(1)
	qr, err := wr.ExecuteFetchAsDba(ctx, alias, query, *maxRows, *wantFields, *disableBinlogs, *reloadSchema)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJSON(qr))
	}
	return err
}

func commandExecuteHook(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() < 2 {
		return fmt.Errorf("The <tablet alias> and <hook name> arguments are required for the ExecuteHook command.")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	hook := &hk.Hook{Name: subFlags.Arg(1), Parameters: subFlags.Args()[2:]}
	hr, err := wr.ExecuteHook(ctx, tabletAlias, hook)
	if err == nil {
		log.Infof(hr.String())
	}
	return err
}

func commandCreateShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Proceeds with the command even if the keyspace already exists")
	parent := subFlags.Bool("parent", false, "Creates the parent keyspace if it doesn't already exist")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace/shard> argument is required for the CreateShard command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	if *parent {
		if err := wr.TopoServer().CreateKeyspace(ctx, keyspace, &pb.Keyspace{}); err != nil && err != topo.ErrNodeExists {
			return err
		}
	}

	err = topotools.CreateShard(ctx, wr.TopoServer(), keyspace, shard)
	if *force && err == topo.ErrNodeExists {
		log.Infof("shard %v/%v already exists (ignoring error with -force)", keyspace, shard)
		err = nil
	}
	return err
}

func commandGetShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace/shard> argument is required for the GetShard command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	shardInfo, err := wr.TopoServer().GetShard(ctx, keyspace, shard)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJSON(shardInfo))
	}
	return err
}

func commandRebuildShardGraph(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cells := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("The <keyspace/shard> argument must be used to identify at least one keyspace and shard when calling the RebuildShardGraph command.")
	}

	var cellArray []string
	if *cells != "" {
		cellArray = strings.Split(*cells, ",")
	}

	keyspaceShards, err := shardParamsToKeyspaceShards(ctx, wr, subFlags.Args())
	if err != nil {
		return err
	}
	for _, ks := range keyspaceShards {
		if _, err := wr.RebuildShardGraph(ctx, ks.Keyspace, ks.Shard, cellArray); err != nil {
			return err
		}
	}
	return nil
}

func commandTabletExternallyReparented(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the TabletExternallyReparented command.")
	}

	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().TabletExternallyReparented(ctx, ti, "")
}

func commandValidateShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	pingTablets := subFlags.Bool("ping-tablets", true, "Indicates whether all tablets should be pinged during the validation process")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace/shard> argument is required for the ValidateShard command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ValidateShard(ctx, keyspace, shard, *pingTablets)
}

func commandShardReplicationPositions(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace/shard> argument is required for the ShardReplicationPositions command.")
	}
	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tablets, stats, err := wr.ShardReplicationStatuses(ctx, keyspace, shard)
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

func commandListShardTablets(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace/shard> argument is required for the ListShardTablets command.")
	}
	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return listTabletsByShard(ctx, wr, keyspace, shard)
}

func commandSetShardServedTypes(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	remove := subFlags.Bool("remove", false, "Removes the served tablet type")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <keyspace/shard> and <served tablet type> arguments are both required for the SetShardServedTypes command.")
	}
	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}

	servedType, err := parseServingTabletType3(subFlags.Arg(1))
	if err != nil {
		return err
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	return wr.SetShardServedTypes(ctx, keyspace, shard, cells, servedType, *remove)
}

func commandSetShardTabletControl(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	tablesStr := subFlags.String("tables", "", "Specifies a comma-separated list of tables to replicate (used for vertical split)")
	remove := subFlags.Bool("remove", false, "Removes cells for vertical splits. This flag requires the *tables* flag to also be set.")
	disableQueryService := subFlags.Bool("disable_query_service", false, "Disables query service on the provided nodes")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <keyspace/shard> and <tablet type> arguments are both required for the SetShardTabletControl command.")
	}
	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletType, err := parseServingTabletType3(subFlags.Arg(1))
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

	return wr.SetShardTabletControl(ctx, keyspace, shard, tabletType, cells, *remove, *disableQueryService, tables)
}

func commandSourceShardDelete(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() < 2 {
		return fmt.Errorf("The <keyspace/shard> and <uid> arguments are both required for the SourceShardDelete command.")
	}
	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	uid, err := strconv.Atoi(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return wr.SourceShardDelete(ctx, keyspace, shard, uint32(uid))
}

func commandSourceShardAdd(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	keyRange := subFlags.String("key_range", "", "Identifies the key range to use for the SourceShard")
	tablesStr := subFlags.String("tables", "", "Specifies a comma-separated list of tables to replicate (used for vertical split)")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 {
		return fmt.Errorf("The <keyspace/shard>, <uid>, and <source keyspace/shard> arguments are all required for the SourceShardAdd command.")
	}
	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	uid, err := strconv.Atoi(subFlags.Arg(1))
	if err != nil {
		return err
	}
	skeyspace, sshard, err := topo.ParseKeyspaceShardString(subFlags.Arg(2))
	if err != nil {
		return err
	}
	var tables []string
	if *tablesStr != "" {
		tables = strings.Split(*tablesStr, ",")
	}
	var kr *pb.KeyRange
	if *keyRange != "" {
		if _, kr, err = topo.ValidateShardName(*keyRange); err != nil {
			return err
		}
	}
	return wr.SourceShardAdd(ctx, keyspace, shard, uint32(uid), skeyspace, sshard, kr, tables)
}

func commandShardReplicationAdd(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <keyspace/shard> and <tablet alias> arguments are required for the ShardReplicationAdd command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return topo.UpdateShardReplicationRecord(ctx, wr.TopoServer(), keyspace, shard, tabletAlias)
}

func commandShardReplicationRemove(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <keyspace/shard> and <tablet alias> arguments are required for the ShardReplicationRemove command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return topo.RemoveShardReplicationRecord(ctx, wr.TopoServer(), tabletAlias.Cell, keyspace, shard, tabletAlias)
}

func commandShardReplicationFix(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <cell> and <keyspace/shard> arguments are required for the ShardReplicationRemove command.")
	}

	cell := subFlags.Arg(0)
	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return topo.FixShardReplication(ctx, wr.TopoServer(), wr.Logger(), cell, keyspace, shard)
}

func commandRemoveShardCell(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Proceeds even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data.")
	recursive := subFlags.Bool("recursive", false, "Also delete all tablets in that cell belonging to the specified shard.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <keyspace/shard> and <cell> arguments are required for the RemoveShardCell command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.RemoveShardCell(ctx, keyspace, shard, subFlags.Arg(1), *force, *recursive)
}

func commandDeleteShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	recursive := subFlags.Bool("recursive", false, "Also delete all tablets belonging to the shard.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("The <keyspace/shard> argument must be used to identify at least one keyspace and shard when calling the DeleteShard command.")
	}

	keyspaceShards, err := shardParamsToKeyspaceShards(ctx, wr, subFlags.Args())
	if err != nil {
		return err
	}
	for _, ks := range keyspaceShards {
		err := wr.DeleteShard(ctx, ks.Keyspace, ks.Shard, *recursive)
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

func commandCreateKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	shardingColumnName := subFlags.String("sharding_column_name", "", "Specifies the column to use for sharding operations")
	shardingColumnType := subFlags.String("sharding_column_type", "", "Specifies the type of the column to use for sharding operations")
	splitShardCount := subFlags.Int("split_shard_count", 0, "Specifies the number of shards to use for data splits")
	force := subFlags.Bool("force", false, "Proceeds even if the keyspace already exists")
	var servedFrom flagutil.StringMapValue
	subFlags.Var(&servedFrom, "served_from", "Specifies a comma-separated list of dbtype:keyspace pairs used to serve traffic")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace name> argument is required for the CreateKeyspace command.")
	}

	keyspace := subFlags.Arg(0)
	kit, err := parseKeyspaceIdType(*shardingColumnType)
	if err != nil {
		return err
	}
	ki := &pb.Keyspace{
		ShardingColumnName: *shardingColumnName,
		ShardingColumnType: kit,
		SplitShardCount:    int32(*splitShardCount),
	}
	if len(servedFrom) > 0 {
		for name, value := range servedFrom {
			tt, err := parseServingTabletType3(name)
			if err != nil {
				return err
			}
			ki.ServedFroms = append(ki.ServedFroms, &pb.Keyspace_ServedFrom{
				TabletType: tt,
				Keyspace:   value,
			})
		}
	}
	err = wr.TopoServer().CreateKeyspace(ctx, keyspace, ki)
	if *force && err == topo.ErrNodeExists {
		log.Infof("keyspace %v already exists (ignoring error with -force)", keyspace)
		err = nil
	}
	return err
}

func commandDeleteKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	recursive := subFlags.Bool("recursive", false, "Also recursively delete all shards in the keyspace.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("Must specify the <keyspace> argument for DeleteKeyspace.")
	}

	return wr.DeleteKeyspace(ctx, subFlags.Arg(0), *recursive)
}

func commandRemoveKeyspaceCell(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Proceeds even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data.")
	recursive := subFlags.Bool("recursive", false, "Also delete all tablets in that cell belonging to the specified keyspace.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <keyspace> and <cell> arguments are required for the RemoveKeyspaceCell command.")
	}

	return wr.RemoveKeyspaceCell(ctx, subFlags.Arg(0), subFlags.Arg(1), *force, *recursive)
}

func commandGetKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace> argument is required for the GetKeyspace command.")
	}

	keyspace := subFlags.Arg(0)
	keyspaceInfo, err := wr.TopoServer().GetKeyspace(ctx, keyspace)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJSON(keyspaceInfo))
	}
	return err
}

func commandSetKeyspaceShardingInfo(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Updates fields even if they are already set. Use caution before calling this command.")
	splitShardCount := subFlags.Int("split_shard_count", 0, "Specifies the number of shards to use for data splits")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() > 3 || subFlags.NArg() < 1 {
		return fmt.Errorf("The <keyspace name> argument is required for the SetKeyspaceShardingInfo command. The <column name> and <column type> arguments are both optional.")
	}

	keyspace := subFlags.Arg(0)
	columnName := ""
	if subFlags.NArg() >= 2 {
		columnName = subFlags.Arg(1)
	}
	kit := pb.KeyspaceIdType_UNSET
	if subFlags.NArg() >= 3 {
		var err error
		kit, err = parseKeyspaceIdType(subFlags.Arg(2))
		if err != nil {
			return err
		}
	}

	return wr.SetKeyspaceShardingInfo(ctx, keyspace, columnName, kit, int32(*splitShardCount), *force)
}

func commandSetKeyspaceServedFrom(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	source := subFlags.String("source", "", "Specifies the source keyspace name")
	remove := subFlags.Bool("remove", false, "Indicates whether to add (default) or remove the served from record")
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to affect")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <keyspace name> and <tablet type> arguments are required for the SetKeyspaceServedFrom command.")
	}
	keyspace := subFlags.Arg(0)
	servedType, err := parseTabletType(subFlags.Arg(1), []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	return wr.SetKeyspaceServedFrom(ctx, keyspace, topo.TabletTypeToProto(servedType), cells, *source, *remove)
}

func commandRebuildKeyspaceGraph(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cells := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	rebuildSrvShards := subFlags.Bool("rebuild_srv_shards", false, "Indicates whether all SrvShard objects should also be rebuilt. The default value is <code>false</code>.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("The <keyspace> argument must be used to specify at least one keyspace when calling the RebuildKeyspaceGraph command.")
	}

	var cellArray []string
	if *cells != "" {
		cellArray = strings.Split(*cells, ",")
	}

	keyspaces, err := keyspaceParamsToKeyspaces(ctx, wr, subFlags.Args())
	if err != nil {
		return err
	}
	for _, keyspace := range keyspaces {
		if err := wr.RebuildKeyspaceGraph(ctx, keyspace, cellArray, *rebuildSrvShards); err != nil {
			return err
		}
	}
	return nil
}

func commandValidateKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	pingTablets := subFlags.Bool("ping-tablets", false, "Specifies whether all tablets will be pinged during the validation process")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace name> argument is required for the ValidateKeyspace command.")
	}

	keyspace := subFlags.Arg(0)
	return wr.ValidateKeyspace(ctx, keyspace, *pingTablets)
}

func commandMigrateServedTypes(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	reverse := subFlags.Bool("reverse", false, "Moves the served tablet type backward instead of forward. Use in case of trouble")
	skipReFreshState := subFlags.Bool("skip-refresh-state", false, "Skips refreshing the state of the source tablets after the migration, meaning that the refresh will need to be done manually, replica and rdonly only)")
	filteredReplicationWaitTime := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <source keyspace/shard> and <served tablet type> arguments are both required for the MigrateServedTypes command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseServingTabletType3(subFlags.Arg(1))
	if err != nil {
		return err
	}
	if servedType == pb.TabletType_MASTER && *skipReFreshState {
		return fmt.Errorf("The skip-refresh-state flag can only be specified for non-master migrations.")
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}
	return wr.MigrateServedTypes(ctx, keyspace, shard, cells, servedType, *reverse, *skipReFreshState, *filteredReplicationWaitTime)
}

func commandMigrateServedFrom(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	reverse := subFlags.Bool("reverse", false, "Moves the served tablet type backward instead of forward. Use in case of trouble")
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	filteredReplicationWaitTime := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <destination keyspace/shard> and <served tablet type> arguments are both required for the MigrateServedFrom command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
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
	return wr.MigrateServedFrom(ctx, keyspace, shard, topo.TabletTypeToProto(servedType), cells, *reverse, *filteredReplicationWaitTime)
}

func commandFindAllShardsInKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace> argument is required for the FindAllShardsInKeyspace command.")
	}

	keyspace := subFlags.Arg(0)
	result, err := topo.FindAllShardsInKeyspace(ctx, wr.TopoServer(), keyspace)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJSON(result))
	}
	return err

}

func commandResolve(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The Resolve command requires a single argument, the value of which must be in the format <keyspace>.<shard>.<db type>:<port name>.")
	}
	parts := strings.Split(subFlags.Arg(0), ":")
	if len(parts) != 2 {
		return fmt.Errorf("The Resolve command requires a single argument, the value of which must be in the format <keyspace>.<shard>.<db type>:<port name>.")
	}
	namedPort := parts[1]

	parts = strings.Split(parts[0], ".")
	if len(parts) != 3 {
		return fmt.Errorf("The Resolve command requires a single argument, the value of which must be in the format <keyspace>.<shard>.<db type>:<port name>.")
	}

	tabletType, err := parseTabletType(parts[2], topo.AllTabletTypes)
	if err != nil {
		return err
	}
	addrs, err := topo.LookupVtName(ctx, wr.TopoServer(), "local", parts[0], parts[1], tabletType, namedPort)
	if err != nil {
		return err
	}
	for _, addr := range addrs {
		wr.Logger().Printf("%v\n", netutil.JoinHostPort(addr.Target, int32(addr.Port)))
	}
	return nil
}

func commandValidate(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	pingTablets := subFlags.Bool("ping-tablets", false, "Indicates whether all tablets should be pinged during the validation process")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 0 {
		log.Warningf("action Validate doesn't take any parameter any more")
	}
	return wr.Validate(ctx, *pingTablets)
}

func commandRebuildReplicationGraph(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	// This is sort of a nuclear option.
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() < 2 {
		return fmt.Errorf("The <cell> and <keyspace> arguments are both required for the RebuildReplicationGraph command. To specify multiple cells, separate the cell names with commas. Similarly, to specify multiple keyspaces, separate the keyspace names with commas.")
	}

	cells := strings.Split(subFlags.Arg(0), ",")
	keyspaceParams := strings.Split(subFlags.Arg(1), ",")
	keyspaces, err := keyspaceParamsToKeyspaces(ctx, wr, keyspaceParams)
	if err != nil {
		return err
	}
	return wr.RebuildReplicationGraph(ctx, cells, keyspaces)
}

func commandListAllTablets(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <cell name> argument is required for the ListAllTablets command.")
	}

	cell := subFlags.Arg(0)
	return dumpAllTablets(ctx, wr, cell)
}

func commandListTablets(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("The <tablet alias> argument is required for the ListTablets command.")
	}

	paths := subFlags.Args()
	aliases := make([]topo.TabletAlias, len(paths))
	var err error
	for i, path := range paths {
		aliases[i], err = topo.ParseTabletAliasString(path)
		if err != nil {
			return err
		}
	}
	return dumpTablets(ctx, wr, aliases)
}

func commandGetSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	tables := subFlags.String("tables", "", "Specifies a comma-separated list of regular expressions for which tables should gather information")
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of regular expressions for tables to exclude")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the output")
	tableNamesOnly := subFlags.Bool("table_names_only", false, "Only displays table names that match")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the GetSchema command.")
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
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

	sd, err := wr.GetSchema(ctx, tabletAlias, tableArray, excludeTableArray, *includeViews)
	if err == nil {
		if *tableNamesOnly {
			for _, td := range sd.TableDefinitions {
				wr.Logger().Printf("%v\n", td.Name)
			}
		} else {
			wr.Logger().Printf("%v\n", jscfg.ToJSON(sd))
		}
	}
	return err
}

func commandReloadSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the ReloadSchema command.")
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ReloadSchema(ctx, tabletAlias)
}

func commandValidateSchemaShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of regular expressions for tables to exclude")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the validation")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace/shard> argument is required for the ValidateSchemaShard command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return wr.ValidateSchemaShard(ctx, keyspace, shard, excludeTableArray, *includeViews)
}

func commandValidateSchemaKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of regular expressions for tables to exclude")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the validation")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace name> argument is required for the ValidateSchemaKeyspace command.")
	}

	keyspace := subFlags.Arg(0)
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return wr.ValidateSchemaKeyspace(ctx, keyspace, excludeTableArray, *includeViews)
}

func commandApplySchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Applies the schema even if the preflight schema doesn't match")
	sql := subFlags.String("sql", "", "A list of semicolon-delimited SQL commands")
	sqlFile := subFlags.String("sql-file", "", "Identifies the file that contains the SQL commands")
	waitSlaveTimeout := subFlags.Duration("wait_slave_timeout", 30*time.Second, "The amount of time to wait for slaves to catch up during reparenting. The default value is 30 seconds.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace> argument is required for the commandApplySchema command.")
	}

	keyspace := subFlags.Arg(0)
	change, err := getFileParam(*sql, *sqlFile, "sql")
	if err != nil {
		return err
	}
	scr, err := wr.ApplySchemaKeyspace(ctx, keyspace, change, true, *force, *waitSlaveTimeout)
	if err == nil {
		log.Infof(scr.String())
	}
	return err
}

func commandCopySchemaShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	tables := subFlags.String("tables", "", "Specifies a comma-separated list of regular expressions for which tables  gather schema information for")
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of regular expressions for which tables to exclude")
	includeViews := subFlags.Bool("include-views", true, "Includes views in the output")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <source keyspace/shard> and <destination keyspace/shard> arguments are both required for the CopySchemaShard command. Instead of the <source keyspace/shard> argument, you can also specify <tablet alias> which refers to a specific tablet of the shard in the source keyspace.")
	}
	var tableArray []string
	if *tables != "" {
		tableArray = strings.Split(*tables, ",")
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	destKeyspace, destShard, err := topo.ParseKeyspaceShardString(subFlags.Arg(1))
	if err != nil {
		return err
	}

	sourceKeyspace, sourceShard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err == nil {
		return wr.CopySchemaShardFromShard(ctx, tableArray, excludeTableArray, *includeViews, sourceKeyspace, sourceShard, destKeyspace, destShard)
	}
	sourceTabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err == nil {
		return wr.CopySchemaShard(ctx, sourceTabletAlias, tableArray, excludeTableArray, *includeViews, destKeyspace, destShard)
	}
	return err
}

func commandValidateVersionShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace/shard> argument is requird for the ValidateVersionShard command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ValidateVersionShard(ctx, keyspace, shard)
}

func commandValidateVersionKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace name> argument is required for the ValidateVersionKeyspace command.")
	}

	keyspace := subFlags.Arg(0)
	return wr.ValidateVersionKeyspace(ctx, keyspace)
}

func commandGetPermissions(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <tablet alias> argument is required for the GetPermissions command.")
	}
	tabletAlias, err := topo.ParseTabletAliasString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	p, err := wr.GetPermissions(ctx, tabletAlias)
	if err == nil {
		log.Infof("%v", p.String()) // they can contain '%'
	}
	return err
}

func commandValidatePermissionsShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace/shard> argument is required for the ValidatePermissionsShard command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ValidatePermissionsShard(ctx, keyspace, shard)
}

func commandValidatePermissionsKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <keyspace name> argument is required for the ValidatePermissionsKeyspace command.")
	}

	keyspace := subFlags.Arg(0)
	return wr.ValidatePermissionsKeyspace(ctx, keyspace)
}

func commandGetVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("The GetVSchema command does not support any arguments.")
	}
	ts := wr.TopoServer()
	schemafier, ok := ts.(topo.Schemafier)
	if !ok {
		return fmt.Errorf("%T does not support the vschema operations", ts)
	}
	schema, err := schemafier.GetVSchema(ctx)
	if err != nil {
		return err
	}
	wr.Logger().Printf("%s\n", schema)
	return nil
}

func commandApplyVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	vschema := subFlags.String("vschema", "", "Identifies the VTGate routing schema")
	vschemaFile := subFlags.String("vschema_file", "", "Identifies the VTGate routing schema file")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if (*vschema == "") == (*vschemaFile == "") {
		return fmt.Errorf("Either the vschema or vschemaFile flag must be specified when calling the ApplyVSchema command.")
	}
	ts := wr.TopoServer()
	schemafier, ok := ts.(topo.Schemafier)
	if !ok {
		return fmt.Errorf("%T does not support vschema operations", ts)
	}
	s := *vschema
	if *vschemaFile != "" {
		schema, err := ioutil.ReadFile(*vschemaFile)
		if err != nil {
			return err
		}
		s = string(schema)
	}
	return schemafier.SaveVSchema(ctx, s)
}

func commandGetSrvKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <cell> and <keyspace> arguments are required for the GetSrvKeyspace command.")
	}

	srvKeyspace, err := wr.TopoServer().GetSrvKeyspace(ctx, subFlags.Arg(0), subFlags.Arg(1))
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJSON(srvKeyspace))
	}
	return err
}

func commandGetSrvKeyspaceNames(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("The <cell> argument is required for the GetSrvKeyspaceNames command.")
	}

	srvKeyspaceNames, err := wr.TopoServer().GetSrvKeyspaceNames(ctx, subFlags.Arg(0))
	if err != nil {
		return err
	}
	for _, ks := range srvKeyspaceNames {
		wr.Logger().Printf("%v\n", ks)
	}
	return nil
}

func commandGetSrvShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <cell> and <keyspace/shard> arguments are required for the GetSrvShard command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(1))
	if err != nil {
		return err
	}
	srvShard, err := wr.TopoServer().GetSrvShard(ctx, subFlags.Arg(0), keyspace, shard)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJSON(srvShard))
	}
	return err
}

func commandGetEndPoints(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 {
		return fmt.Errorf("The <cell>, <keyspace/shard>, and <tablet type> arguments are required for the GetEndPoints command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(1))
	if err != nil {
		return err
	}
	tabletType := topo.TabletType(subFlags.Arg(2))
	endPoints, _, err := wr.TopoServer().GetEndPoints(ctx, subFlags.Arg(0), keyspace, shard, tabletType)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJSON(endPoints))
	}
	return err
}

func commandGetShardReplication(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("The <cell> and <keyspace/shard> arguments are required for the GetShardReplication command.")
	}

	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(1))
	if err != nil {
		return err
	}
	shardReplication, err := wr.TopoServer().GetShardReplication(ctx, subFlags.Arg(0), keyspace, shard)
	if err == nil {
		wr.Logger().Printf("%v\n", jscfg.ToJSON(shardReplication))
	}
	return err
}

func commandHelp(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	switch subFlags.NArg() {
	case 0:
		wr.Logger().Printf("Available commands:\n\n")
		PrintAllCommands(wr.Logger())
	case 1:
		RunCommand(ctx, wr, []string{subFlags.Arg(0), "--help"})
	default:
		return fmt.Errorf("When calling the Help command, either specify a single argument that identifies the name of the command to get help with or do not specify any additional arguments.")
	}

	return nil
}

func commandPanic(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	panic(fmt.Errorf("this command panics on purpose"))
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
func RunCommand(ctx context.Context, wr *wrangler.Wrangler, args []string) error {
	if len(args) == 0 {
		wr.Logger().Printf("No command specified. Please see the list below:\n\n")
		PrintAllCommands(wr.Logger())
		return fmt.Errorf("No command was specified.")
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
				return cmd.method(ctx, wr, subFlags, args[1:])
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
