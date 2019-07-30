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

// Package vtctl contains the implementation of all the Vitess management
// commands.
package vtctl

// The following comment section contains definitions for command arguments.
// It is parsed to generate the vtctl documentation automatically.
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
  -- drained: A tablet that is reserved for a background process. For example,
              a tablet used by a vtworker process, where the tablet is likely
              lagging in replication.
  -- experimental: A slaved copy of data that is ready but not serving query
                   traffic. The value indicates a special characteristic of
                   the tablet that indicates the tablet should not be
                   considered a potential master. Vitess also does not
                   worry about lag for experimental tablets when reparenting.
  -- master: A primary copy of data
  -- rdonly: A slaved copy of data for OLAP load patterns
  -- replica: A slaved copy of data ready to be promoted to master
  -- restore: A tablet that is restoring from a snapshot. Typically, this
              happens at tablet startup, then it goes to its right state.
  -- schema_apply: A slaved copy of data that had been serving query traffic
                   but that is now applying a schema change. Following the
                   change, the tablet will revert to its serving type.
  -- snapshot_source: A slaved copy of data where mysqld is <b>not</b>
                      running and where Vitess is serving data files to
                      clone slaves. Use this command to enter this mode:
                      <pre>vtctl Snapshot -server-mode ...</pre>
                      Use this command to exit this mode:
                      <pre>vtctl SnapshotSourceEnd ...</pre>
  -- spare: A slaved copy of data that is ready but not serving query traffic.
            The data could be a potential master tablet.
*/

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	hk "vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schemamanager"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/wrangler"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

var (
	// ErrUnknownCommand is returned for an unknown command
	ErrUnknownCommand = errors.New("unknown command")
)

// Flags are exported for use in go/vt/vtctld.
var (
	HealthCheckTopologyRefresh = flag.Duration("vtctl_healthcheck_topology_refresh", 30*time.Second, "refresh interval for re-reading the topology")
	HealthcheckRetryDelay      = flag.Duration("vtctl_healthcheck_retry_delay", 5*time.Second, "delay before retrying a failed healthcheck")
	HealthCheckTimeout         = flag.Duration("vtctl_healthcheck_timeout", time.Minute, "the health check timeout period")
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

// commandsMutex protects commands at init time. We use servenv, which calls
// all Run hooks in parallel.
var commandsMutex sync.Mutex

// TODO: Convert these commands to be automatically generated from flag parser.
var commands = []commandGroup{
	{
		"Tablets", []command{
			{"InitTablet", commandInitTablet,
				"[-allow_update] [-allow_different_shard] [-allow_master_override] [-parent] [-db_name_override=<db name>] [-hostname=<hostname>] [-mysql_port=<port>] [-port=<port>] [-grpc_port=<port>] [-tags=tag1:value1,tag2:value2] -keyspace=<keyspace> -shard=<shard> <tablet alias> <tablet type>",
				"Initializes a tablet in the topology.\n"},
			{"GetTablet", commandGetTablet,
				"<tablet alias>",
				"Outputs a JSON structure that contains information about the Tablet."},
			{"UpdateTabletAddrs", commandUpdateTabletAddrs,
				"[-hostname <hostname>] [-ip-addr <ip addr>] [-mysql-port <mysql port>] [-vt-port <vt port>] [-grpc-port <grpc port>] <tablet alias> ",
				"Updates the IP address and port numbers of a tablet."},
			{"DeleteTablet", commandDeleteTablet,
				"[-allow_master] <tablet alias> ...",
				"Deletes tablet(s) from the topology."},
			{"SetReadOnly", commandSetReadOnly,
				"<tablet alias>",
				"Sets the tablet as read-only."},
			{"SetReadWrite", commandSetReadWrite,
				"<tablet alias>",
				"Sets the tablet as read-write."},
			{"StartSlave", commandStartSlave,
				"<tablet alias>",
				"Starts replication on the specified slave."},
			{"StopSlave", commandStopSlave,
				"<tablet alias>",
				"Stops replication on the specified slave."},
			{"ChangeSlaveType", commandChangeSlaveType,
				"[-dry-run] <tablet alias> <tablet type>",
				"Changes the db type for the specified tablet, if possible. This command is used primarily to arrange replicas, and it will not convert a master.\n" +
					"NOTE: This command automatically updates the serving graph.\n"},
			{"Ping", commandPing,
				"<tablet alias>",
				"Checks that the specified tablet is awake and responding to RPCs. This command can be blocked by other in-flight operations."},
			{"RefreshState", commandRefreshState,
				"<tablet alias>",
				"Reloads the tablet record on the specified tablet."},
			{"RefreshStateByShard", commandRefreshStateByShard,
				"[-cells=c1,c2,...] <keyspace/shard>",
				"Runs 'RefreshState' on all tablets in the given shard."},
			{"RunHealthCheck", commandRunHealthCheck,
				"<tablet alias>",
				"Runs a health check on a remote tablet."},
			{"IgnoreHealthError", commandIgnoreHealthError,
				"<tablet alias> <ignore regexp>",
				"Sets the regexp for health check errors to ignore on the specified tablet. The pattern has implicit ^$ anchors. Set to empty string or restart vttablet to stop ignoring anything."},
			{"Sleep", commandSleep,
				"<tablet alias> <duration>",
				"Blocks the action queue on the specified tablet for the specified amount of time. This is typically used for testing."},
			{"ExecuteHook", commandExecuteHook,
				"<tablet alias> <hook name> [<param1=value1> <param2=value2> ...]",
				"Runs the specified hook on the given tablet. A hook is a script that resides in the $VTROOT/vthook directory. You can put any script into that directory and use this command to run that script.\n" +
					"For this command, the param=value arguments are parameters that the command passes to the specified hook."},
			{"ExecuteFetchAsApp", commandExecuteFetchAsApp,
				"[-max_rows=10000] [-json] [-use_pool] <tablet alias> <sql command>",
				"Runs the given SQL command as a App on the remote tablet."},
			{"ExecuteFetchAsDba", commandExecuteFetchAsDba,
				"[-max_rows=10000] [-disable_binlogs] [-json] <tablet alias> <sql command>",
				"Runs the given SQL command as a DBA on the remote tablet."},
			{"VReplicationExec", commandVReplicationExec,
				"[-json] <tablet alias> <sql command>",
				"Runs the given VReplication command on the remote tablet."},
		},
	},
	{
		"Shards", []command{
			{"CreateShard", commandCreateShard,
				"[-force] [-parent] <keyspace/shard>",
				"Creates the specified shard."},
			{"GetShard", commandGetShard,
				"<keyspace/shard>",
				"Outputs a JSON structure that contains information about the Shard."},
			{"TabletExternallyReparented", commandTabletExternallyReparented,
				"<tablet alias>",
				"Changes metadata in the topology server to acknowledge a shard master change performed by an external tool. See the Reparenting guide for more information:" +
					"https://github.com/vitessio/vitess/blob/master/doc/Reparenting.md#external-reparents."},
			{"ValidateShard", commandValidateShard,
				"[-ping-tablets] <keyspace/shard>",
				"Validates that all nodes that are reachable from this shard are consistent."},
			{"ShardReplicationPositions", commandShardReplicationPositions,
				"<keyspace/shard>",
				"Shows the replication status of each slave machine in the shard graph. In this case, the status refers to the replication lag between the master vttablet and the slave vttablet. In Vitess, data is always written to the master vttablet first and then replicated to all slave vttablets. Output is sorted by tablet type, then replication position. Use ctrl-C to interrupt command and see partial result if needed."},
			{"ListShardTablets", commandListShardTablets,
				"<keyspace/shard>",
				"Lists all tablets in the specified shard."},
			{"SetShardIsMasterServing", commandSetShardIsMasterServing,
				"<keyspace/shard> <is_master_serving>",
				"Add or remove a shard from serving. This is meant as an emergency function. It does not rebuild any serving graph i.e. does not run 'RebuildKeyspaceGraph'."},
			{"SetShardTabletControl", commandSetShardTabletControl,
				"[--cells=c1,c2,...] [--blacklisted_tables=t1,t2,...] [--remove] [--disable_query_service] <keyspace/shard> <tablet type>",
				"Sets the TabletControl record for a shard and type. Only use this for an emergency fix or after a finished vertical split. The *MigrateServedFrom* and *MigrateServedType* commands set this field appropriately already. Always specify the blacklisted_tables flag for vertical splits, but never for horizontal splits.\n" +
					"To set the DisableQueryServiceFlag, keep 'blacklisted_tables' empty, and set 'disable_query_service' to true or false. Useful to fix horizontal splits gone wrong.\n" +
					"To change the blacklisted tables list, specify the 'blacklisted_tables' parameter with the new list. Useful to fix tables that are being blocked after a vertical split.\n" +
					"To just remove the ShardTabletControl entirely, use the 'remove' flag, useful after a vertical split is finished to remove serving restrictions."},
			{"UpdateSrvKeyspacePartition", commandUpdateSrvKeyspacePartition,
				"[--cells=c1,c2,...] [--remove] <keyspace/shard> <tablet type>",
				"Updates KeyspaceGraph partition for a shard and type. Only use this for an emergency fix during an horizontal shard split. The *MigrateServedType* commands set this field appropriately already. Specify the remove flag, if you want the shard to be removed from the desired partition."},
			{"SourceShardDelete", commandSourceShardDelete,
				"<keyspace/shard> <uid>",
				"Deletes the SourceShard record with the provided index. This is meant as an emergency cleanup function. It does not call RefreshState for the shard master."},
			{"SourceShardAdd", commandSourceShardAdd,
				"[--key_range=<keyrange>] [--tables=<table1,table2,...>] <keyspace/shard> <uid> <source keyspace/shard>",
				"Adds the SourceShard record with the provided index. This is meant as an emergency function. It does not call RefreshState for the shard master."},
			{"ShardReplicationAdd", commandShardReplicationAdd,
				"<keyspace/shard> <tablet alias> <parent tablet alias>",
				"HIDDEN Adds an entry to the replication graph in the given cell."},
			{"ShardReplicationRemove", commandShardReplicationRemove,
				"<keyspace/shard> <tablet alias>",
				"HIDDEN Removes an entry from the replication graph in the given cell."},
			{"ShardReplicationFix", commandShardReplicationFix,
				"<cell> <keyspace/shard>",
				"Walks through a ShardReplication object and fixes the first error that it encounters."},
			{"WaitForFilteredReplication", commandWaitForFilteredReplication,
				"[-max_delay <max_delay, default 30s>] <keyspace/shard>",
				"Blocks until the specified shard has caught up with the filtered replication of its source shard."},
			{"RemoveShardCell", commandRemoveShardCell,
				"[-force] [-recursive] <keyspace/shard> <cell>",
				"Removes the cell from the shard's Cells list."},
			{"DeleteShard", commandDeleteShard,
				"[-recursive] [-even_if_serving] <keyspace/shard> ...",
				"Deletes the specified shard(s). In recursive mode, it also deletes all tablets belonging to the shard. Otherwise, there must be no tablets left in the shard."},
		},
	},
	{
		"Keyspaces", []command{
			{"CreateKeyspace", commandCreateKeyspace,
				"[-sharding_column_name=name] [-sharding_column_type=type] [-served_from=tablettype1:ks1,tablettype2:ks2,...] [-force] <keyspace name>",
				"Creates the specified keyspace."},
			{"DeleteKeyspace", commandDeleteKeyspace,
				"[-recursive] <keyspace>",
				"Deletes the specified keyspace. In recursive mode, it also recursively deletes all shards in the keyspace. Otherwise, there must be no shards left in the keyspace."},
			{"RemoveKeyspaceCell", commandRemoveKeyspaceCell,
				"[-force] [-recursive] <keyspace> <cell>",
				"Removes the cell from the Cells list for all shards in the keyspace, and the SrvKeyspace for that keyspace in that cell."},
			{"GetKeyspace", commandGetKeyspace,
				"<keyspace>",
				"Outputs a JSON structure that contains information about the Keyspace."},
			{"GetKeyspaces", commandGetKeyspaces,
				"",
				"Outputs a sorted list of all keyspaces."},
			{"SetKeyspaceShardingInfo", commandSetKeyspaceShardingInfo,
				"[-force] <keyspace name> [<column name>] [<column type>]",
				"Updates the sharding information for a keyspace."},
			{"SetKeyspaceServedFrom", commandSetKeyspaceServedFrom,
				"[-source=<source keyspace name>] [-remove] [-cells=c1,c2,...] <keyspace name> <tablet type>",
				"Changes the ServedFromMap manually. This command is intended for emergency fixes. This field is automatically set when you call the *MigrateServedFrom* command. This command does not rebuild the serving graph."},
			{"RebuildKeyspaceGraph", commandRebuildKeyspaceGraph,
				"[-cells=c1,c2,...] <keyspace> ...",
				"Rebuilds the serving data for the keyspace. This command may trigger an update to all connected clients."},
			{"ValidateKeyspace", commandValidateKeyspace,
				"[-ping-tablets] <keyspace name>",
				"Validates that all nodes reachable from the specified keyspace are consistent."},
			{"SplitClone", commandSplitClone,
				"<keyspace> <from_shards> <to_shards>",
				"Start the SplitClone process to perform horizontal resharding. Example: SplitClone ks '0' '-80,80-'"},
			{"VerticalSplitClone", commandVerticalSplitClone,
				"<from_keyspace> <to_keyspace> <tables>",
				"Start the VerticalSplitClone process to perform vertical resharding. Example: SplitClone from_ks to_ks 'a,/b.*/'"},
			{"MigrateServedTypes", commandMigrateServedTypes,
				"[-cells=c1,c2,...] [-reverse] [-skip-refresh-state] <keyspace/shard> <served tablet type>",
				"Migrates a serving type from the source shard to the shards that it replicates to. This command also rebuilds the serving graph. The <keyspace/shard> argument can specify any of the shards involved in the migration."},
			{"MigrateServedFrom", commandMigrateServedFrom,
				"[-cells=c1,c2,...] [-reverse] <destination keyspace/shard> <served tablet type>",
				"Makes the <destination keyspace/shard> serve the given type. This command also rebuilds the serving graph."},
			{"CancelResharding", commandCancelResharding,
				"<keyspace/shard>",
				"Permanently cancels a resharding in progress. All resharding related metadata will be deleted."},
			{"ShowResharding", commandShowResharding,
				"<keyspace/shard>",
				"Displays all metadata about a resharding in progress."},
			{"FindAllShardsInKeyspace", commandFindAllShardsInKeyspace,
				"<keyspace>",
				"Displays all of the shards in the specified keyspace."},
			{"WaitForDrain", commandWaitForDrain,
				"[-timeout <duration>] [-retry_delay <duration>] [-initial_wait <duration>] <keyspace/shard> <served tablet type>",
				"Blocks until no new queries were observed on all tablets with the given tablet type in the specifed keyspace. " +
					" This can be used as sanity check to ensure that the tablets were drained after running vtctl MigrateServedTypes " +
					" and vtgate is no longer using them. If -timeout is set, it fails when the timeout is reached."},
		},
	},
	{
		"Generic", []command{
			{"Validate", commandValidate,
				"[-ping-tablets]",
				"Validates that all nodes reachable from the global replication graph and that all tablets in all discoverable cells are consistent."},
			{"ListAllTablets", commandListAllTablets,
				"<cell name1>, <cell name2>, ...",
				"Lists all tablets in an awk-friendly way."},
			{"ListTablets", commandListTablets,
				"<tablet alias> ...",
				"Lists specified tablets in an awk-friendly way."},
			{"Panic", commandPanic,
				"",
				"HIDDEN Triggers a panic on the server side, to test the handling."},
		},
	},
	{
		"Schema, Version, Permissions", []command{
			{"GetSchema", commandGetSchema,
				"[-tables=<table1>,<table2>,...] [-exclude_tables=<table1>,<table2>,...] [-include-views] <tablet alias>",
				"Displays the full schema for a tablet, or just the schema for the specified tables in that tablet."},
			{"ReloadSchema", commandReloadSchema,
				"<tablet alias>",
				"Reloads the schema on a remote tablet."},
			{"ReloadSchemaShard", commandReloadSchemaShard,
				"[-concurrency=10] [-include_master=false] <keyspace/shard>",
				"Reloads the schema on all the tablets in a shard."},
			{"ReloadSchemaKeyspace", commandReloadSchemaKeyspace,
				"[-concurrency=10] [-include_master=false] <keyspace>",
				"Reloads the schema on all the tablets in a keyspace."},
			{"ValidateSchemaShard", commandValidateSchemaShard,
				"[-exclude_tables=''] [-include-views] <keyspace/shard>",
				"Validates that the master schema matches all of the slaves."},
			{"ValidateSchemaKeyspace", commandValidateSchemaKeyspace,
				"[-exclude_tables=''] [-include-views] <keyspace name>",
				"Validates that the master schema from shard 0 matches the schema on all of the other tablets in the keyspace."},
			{"ApplySchema", commandApplySchema,
				"[-allow_long_unavailability] [-wait_slave_timeout=10s] {-sql=<sql> || -sql-file=<filename>} <keyspace>",
				"Applies the schema change to the specified keyspace on every master, running in parallel on all shards. The changes are then propagated to slaves via replication. If -allow_long_unavailability is set, schema changes affecting a large number of rows (and possibly incurring a longer period of unavailability) will not be rejected."},
			{"CopySchemaShard", commandCopySchemaShard,
				"[-tables=<table1>,<table2>,...] [-exclude_tables=<table1>,<table2>,...] [-include-views] [-wait_slave_timeout=10s] {<source keyspace/shard> || <source tablet alias>} <destination keyspace/shard>",
				"Copies the schema from a source shard's master (or a specific tablet) to a destination shard. The schema is applied directly on the master of the destination shard, and it is propagated to the replicas through binlogs."},

			{"ValidateVersionShard", commandValidateVersionShard,
				"<keyspace/shard>",
				"Validates that the master version matches all of the slaves."},
			{"ValidateVersionKeyspace", commandValidateVersionKeyspace,
				"<keyspace name>",
				"Validates that the master version from shard 0 matches all of the other tablets in the keyspace."},

			{"GetPermissions", commandGetPermissions,
				"<tablet alias>",
				"Displays the permissions for a tablet."},
			{"ValidatePermissionsShard", commandValidatePermissionsShard,
				"<keyspace/shard>",
				"Validates that the master permissions match all the slaves."},
			{"ValidatePermissionsKeyspace", commandValidatePermissionsKeyspace,
				"<keyspace name>",
				"Validates that the master permissions from shard 0 match those of all of the other tablets in the keyspace."},

			{"GetVSchema", commandGetVSchema,
				"<keyspace>",
				"Displays the VTGate routing schema."},
			{"ApplyVSchema", commandApplyVSchema,
				"{-vschema=<vschema> || -vschema_file=<vschema file> || -sql=<sql> || -sql_file=<sql file>} [-cells=c1,c2,...] [-skip_rebuild] [-dry-run] <keyspace>",
				"Applies the VTGate routing schema to the provided keyspace. Shows the result after application."},
			{"GetRoutingRules", commandGetRoutingRules,
				"",
				"Displays the VSchema routing rules."},
			{"ApplyRoutingRules", commandApplyRoutingRules,
				"{-rules=<rules> || -rules_file=<rules_file=<sql file>} [-cells=c1,c2,...] [-skip_rebuild] [-dry-run]",
				"Applies the VSchema routing rules."},
			{"RebuildVSchemaGraph", commandRebuildVSchemaGraph,
				"[-cells=c1,c2,...]",
				"Rebuilds the cell-specific SrvVSchema from the global VSchema objects in the provided cells (or all cells if none provided)."},
		},
	},
	{
		"Serving Graph", []command{
			{"GetSrvKeyspaceNames", commandGetSrvKeyspaceNames,
				"<cell>",
				"Outputs a list of keyspace names."},
			{"GetSrvKeyspace", commandGetSrvKeyspace,
				"<cell> <keyspace>",
				"Outputs a JSON structure that contains information about the SrvKeyspace."},
			{"GetSrvVSchema", commandGetSrvVSchema,
				"<cell>",
				"Outputs a JSON structure that contains information about the SrvVSchema."},
			{"DeleteSrvVSchema", commandDeleteSrvVSchema,
				"<cell>",
				"Deletes the SrvVSchema object in the given cell."},
		},
	},
	{
		"Replication Graph", []command{
			{"GetShardReplication", commandGetShardReplication,
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
	commandsMutex.Lock()
	defer commandsMutex.Unlock()
	for i, group := range commands {
		if group.name == groupName {
			commands[i].commands = append(commands[i].commands, c)
			return
		}
	}
	panic(fmt.Errorf("trying to add to missing group %v", groupName))
}

func addCommandGroup(groupName string) {
	commandsMutex.Lock()
	defer commandsMutex.Unlock()
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
	return fmt.Sprintf("%v %v %v %v %v %v %v", topoproto.TabletAliasString(ti.Alias), keyspace, shard, topoproto.TabletTypeLString(ti.Type), ti.Addr(), ti.MysqlAddr(), fmtMapAwkable(ti.Tags))
}

func listTabletsByShard(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string) error {
	tabletAliases, err := wr.TopoServer().FindAllTabletAliasesInShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	return dumpTablets(ctx, wr, tabletAliases)
}

func dumpAllTablets(ctx context.Context, wr *wrangler.Wrangler, cell string) error {
	tablets, err := topotools.GetAllTablets(ctx, wr.TopoServer(), cell)
	if err != nil {
		return err
	}
	for _, ti := range tablets {
		wr.Logger().Printf("%v\n", fmtTabletAwkable(ti))
	}
	return nil
}

func dumpTablets(ctx context.Context, wr *wrangler.Wrangler, tabletAliases []*topodatapb.TabletAlias) error {
	tabletMap, err := wr.TopoServer().GetTabletMap(ctx, tabletAliases)
	if err != nil {
		return err
	}
	for _, tabletAlias := range tabletAliases {
		ti, ok := tabletMap[topoproto.TabletAliasString(tabletAlias)]
		if !ok {
			wr.Logger().Warningf("failed to load tablet %v", tabletAlias)
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
		return "", fmt.Errorf("cannot read file %v: %v", flagFile, err)
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
			result = append(result, params...)
		} else {
			// this is not a path, so assume a keyspace name,
			// possibly with wildcards
			keyspaces, err := wr.TopoServer().ResolveKeyspaceWildcard(ctx, param)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve keyspace wildcard %v: %v", param, err)
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
				keyspace, shard, err := topoproto.ParseKeyspaceShard(path)
				if err != nil {
					return nil, err
				}
				result = append(result, topo.KeyspaceShard{Keyspace: keyspace, Shard: shard})
			}
		} else {
			// this is not a path, so assume a keyspace
			// name / shard name, each possibly with wildcards
			keyspaceShards, err := wr.TopoServer().ResolveShardWildcard(ctx, param)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve keyspace/shard wildcard %v: %v", param, err)
			}
			result = append(result, keyspaceShards...)
		}
	}
	return result, nil
}

// tabletParamsToTabletAliases takes multiple params and converts them
// to tablet aliases.
func tabletParamsToTabletAliases(params []string) ([]*topodatapb.TabletAlias, error) {
	result := make([]*topodatapb.TabletAlias, len(params))
	var err error
	for i, param := range params {
		result[i], err = topoproto.ParseTabletAlias(param)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// parseTabletType parses the string tablet type and verifies
// it is an accepted one
func parseTabletType(param string, types []topodatapb.TabletType) (topodatapb.TabletType, error) {
	tabletType, err := topoproto.ParseTabletType(param)
	if err != nil {
		return topodatapb.TabletType_UNKNOWN, fmt.Errorf("invalid tablet type %v: %v", param, err)
	}
	if !topoproto.IsTypeInList(topodatapb.TabletType(tabletType), types) {
		return topodatapb.TabletType_UNKNOWN, fmt.Errorf("type %v is not one of: %v", tabletType, strings.Join(topoproto.MakeStringTypeList(types), " "))
	}
	return tabletType, nil
}

// parseServingTabletType3 parses the tablet type into the enum,
// and makes sure the enum is of serving type (MASTER, REPLICA, RDONLY/BATCH)
func parseServingTabletType3(param string) (topodatapb.TabletType, error) {
	servedType, err := topoproto.ParseTabletType(param)
	if err != nil {
		return topodatapb.TabletType_UNKNOWN, err
	}
	if !topo.IsInServingGraph(servedType) {
		return topodatapb.TabletType_UNKNOWN, fmt.Errorf("served_type has to be in the serving graph, not %v", param)
	}
	return servedType, nil
}

func commandInitTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	dbNameOverride := subFlags.String("db_name_override", "", "Overrides the name of the database that the vttablet uses")
	allowUpdate := subFlags.Bool("allow_update", false, "Use this flag to force initialization if a tablet with the same name already exists. Use with caution.")
	allowMasterOverride := subFlags.Bool("allow_master_override", false, "Use this flag to force initialization if a tablet is created as master, and a master for the keyspace/shard already exists. Use with caution.")
	createShardAndKeyspace := subFlags.Bool("parent", false, "Creates the parent shard and keyspace if they don't yet exist")
	hostname := subFlags.String("hostname", "", "The server on which the tablet is running")
	mysqlHost := subFlags.String("mysql_host", "", "The mysql host for the mysql server")
	mysqlPort := subFlags.Int("mysql_port", 0, "The mysql port for the mysql server")
	port := subFlags.Int("port", 0, "The main port for the vttablet process")
	grpcPort := subFlags.Int("grpc_port", 0, "The gRPC port for the vttablet process")
	keyspace := subFlags.String("keyspace", "", "The keyspace to which this tablet belongs")
	shard := subFlags.String("shard", "", "The shard to which this tablet belongs")

	var tags flagutil.StringMapValue
	subFlags.Var(&tags, "tags", "A comma-separated list of key:value pairs that are used to tag the tablet")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet alias> and <tablet type> arguments are both required for the InitTablet command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletType, err := parseTabletType(subFlags.Arg(1), topoproto.AllTabletTypes)
	if err != nil {
		return err
	}

	// create tablet record
	tablet := &topodatapb.Tablet{
		Alias:          tabletAlias,
		Hostname:       *hostname,
		MysqlHostname:  *mysqlHost,
		PortMap:        make(map[string]int32),
		Keyspace:       *keyspace,
		Shard:          *shard,
		Type:           tabletType,
		DbNameOverride: *dbNameOverride,
		Tags:           tags,
	}
	if *port != 0 {
		tablet.PortMap["vt"] = int32(*port)
	}
	if *mysqlPort != 0 {
		topoproto.SetMysqlPort(tablet, int32(*mysqlPort))
	}
	if *grpcPort != 0 {
		tablet.PortMap["grpc"] = int32(*grpcPort)
	}

	return wr.InitTablet(ctx, tablet, *allowMasterOverride, *createShardAndKeyspace, *allowUpdate)
}

func commandGetTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the GetTablet command")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	// Pass the embedded proto directly or jsonpb will panic.
	return printJSON(wr.Logger(), tabletInfo.Tablet)
}

func commandUpdateTabletAddrs(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	hostname := subFlags.String("hostname", "", "The fully qualified host name of the server on which the tablet is running.")
	mysqlHost := subFlags.String("mysql_host", "", "The mysql host for the mysql server")
	mysqlPort := subFlags.Int("mysql-port", 0, "The mysql port for the mysql daemon")
	vtPort := subFlags.Int("vt-port", 0, "The main port for the vttablet process")
	grpcPort := subFlags.Int("grpc-port", 0, "The gRPC port for the vttablet process")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the UpdateTabletAddrs command")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}

	_, err = wr.TopoServer().UpdateTabletFields(ctx, tabletAlias, func(tablet *topodatapb.Tablet) error {
		if *hostname != "" {
			tablet.Hostname = *hostname
		}
		if *mysqlHost != "" {
			tablet.MysqlHostname = *mysqlHost
		}
		if *vtPort != 0 || *grpcPort != 0 || *mysqlPort != 0 {
			if tablet.PortMap == nil {
				tablet.PortMap = make(map[string]int32)
			}
			if *vtPort != 0 {
				tablet.PortMap["vt"] = int32(*vtPort)
			}
			if *grpcPort != 0 {
				tablet.PortMap["grpc"] = int32(*grpcPort)
			}
			if *mysqlPort != 0 {
				topoproto.SetMysqlPort(tablet, int32(*mysqlPort))
			}
		}
		return nil
	})
	return err
}

func commandDeleteTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	allowMaster := subFlags.Bool("allow_master", false, "Allows for the master tablet of a shard to be deleted. Use with caution.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("the <tablet alias> argument must be used to specify at least one tablet when calling the DeleteTablet command")
	}

	tabletAliases, err := tabletParamsToTabletAliases(subFlags.Args())
	if err != nil {
		return err
	}
	for _, tabletAlias := range tabletAliases {
		if err := wr.DeleteTablet(ctx, tabletAlias, *allowMaster); err != nil {
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
		return fmt.Errorf("the <tablet alias> argument is required for the SetReadOnly command")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().SetReadOnly(ctx, ti.Tablet)
}

func commandSetReadWrite(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the SetReadWrite command")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().SetReadWrite(ctx, ti.Tablet)
}

func commandStartSlave(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action StartSlave requires <tablet alias>")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().StartSlave(ctx, ti.Tablet)
}

func commandStopSlave(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action StopSlave requires <tablet alias>")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().StopSlave(ctx, ti.Tablet)
}

func commandChangeSlaveType(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	dryRun := subFlags.Bool("dry-run", false, "Lists the proposed change without actually executing it")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet alias> and <db type> arguments are required for the ChangeSlaveType command")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	newType, err := parseTabletType(subFlags.Arg(1), topoproto.AllTabletTypes)
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
	return wr.ChangeSlaveType(ctx, tabletAlias, newType)
}

func commandPing(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the Ping command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().Ping(ctx, tabletInfo.Tablet)
}

func commandRefreshState(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the RefreshState command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().RefreshState(ctx, tabletInfo.Tablet)
}

func commandRefreshStateByShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells whose tablets are included. If empty, all cells are considered.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace/shard> argument is required for the RefreshStateByShard command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	si, err := wr.TopoServer().GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}
	return wr.RefreshTabletsByShard(ctx, si, nil /* tabletTypes */, cells)
}

func commandRunHealthCheck(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the RunHealthCheck command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().RunHealthCheck(ctx, tabletInfo.Tablet)
}

func commandIgnoreHealthError(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet alias> and <ignore regexp> arguments are required for the IgnoreHealthError command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	pattern := subFlags.Arg(1)
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().IgnoreHealthError(ctx, tabletInfo.Tablet, pattern)
}

func commandWaitForDrain(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	var cells flagutil.StringListValue
	subFlags.Var(&cells, "cells", "Specifies a comma-separated list of cells to look for tablets")
	timeout := subFlags.Duration("timeout", 0*time.Second, "Timeout after which the command fails")
	retryDelay := subFlags.Duration("retry_delay", 1*time.Second, "Time to wait between two checks")
	initialWait := subFlags.Duration("initial_wait", 1*time.Minute, "Time to wait for all tablets to check in")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <keyspace/shard> and <tablet type> arguments are both required for the WaitForDrain command")
	}
	if *timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseServingTabletType3(subFlags.Arg(1))
	if err != nil {
		return err
	}

	return wr.WaitForDrain(ctx, cells, keyspace, shard, servedType,
		*retryDelay, *HealthCheckTopologyRefresh, *HealthcheckRetryDelay, *HealthCheckTimeout, *initialWait)
}

func commandSleep(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet alias> and <duration> arguments are required for the Sleep command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
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
	return wr.TabletManagerClient().Sleep(ctx, ti.Tablet, duration)
}

func commandExecuteFetchAsApp(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	maxRows := subFlags.Int("max_rows", 10000, "Specifies the maximum number of rows to allow in fetch")
	usePool := subFlags.Bool("use_pool", false, "Use connection from pool")
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet alias> and <sql command> arguments are required for the ExecuteFetchAsApp command")
	}

	alias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	query := subFlags.Arg(1)
	qrproto, err := wr.ExecuteFetchAsApp(ctx, alias, *usePool, query, *maxRows)
	if err != nil {
		return err
	}
	qr := sqltypes.Proto3ToResult(qrproto)
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandExecuteFetchAsDba(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	maxRows := subFlags.Int("max_rows", 10000, "Specifies the maximum number of rows to allow in fetch")
	disableBinlogs := subFlags.Bool("disable_binlogs", false, "Disables writing to binlogs during the query")
	reloadSchema := subFlags.Bool("reload_schema", false, "Indicates whether the tablet schema will be reloaded after executing the SQL command. The default value is <code>false</code>, which indicates that the tablet schema will not be reloaded.")
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet alias> and <sql command> arguments are required for the ExecuteFetchAsDba command")
	}

	alias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	query := subFlags.Arg(1)
	qrproto, err := wr.ExecuteFetchAsDba(ctx, alias, query, *maxRows, *disableBinlogs, *reloadSchema)
	if err != nil {
		return err
	}
	qr := sqltypes.Proto3ToResult(qrproto)
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandVReplicationExec(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet alias> and <sql command> arguments are required for the VReplicationExec command")
	}

	alias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	query := subFlags.Arg(1)
	qrproto, err := wr.VReplicationExec(ctx, alias, query)
	if err != nil {
		return err
	}
	qr := sqltypes.Proto3ToResult(qrproto)
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandExecuteHook(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() < 2 {
		return fmt.Errorf("the <tablet alias> and <hook name> arguments are required for the ExecuteHook command")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	hook := &hk.Hook{Name: subFlags.Arg(1), Parameters: subFlags.Args()[2:]}
	hr, err := wr.ExecuteHook(ctx, tabletAlias, hook)
	if err != nil {
		return err
	}
	return printJSON(wr.Logger(), hr)
}

func commandCreateShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Proceeds with the command even if the keyspace already exists")
	parent := subFlags.Bool("parent", false, "Creates the parent keyspace if it doesn't already exist")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace/shard> argument is required for the CreateShard command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	if *parent {
		if err := wr.TopoServer().CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil && !topo.IsErrType(err, topo.NodeExists) {
			return err
		}
	}

	err = wr.TopoServer().CreateShard(ctx, keyspace, shard)
	if *force && topo.IsErrType(err, topo.NodeExists) {
		wr.Logger().Infof("shard %v/%v already exists (ignoring error with -force)", keyspace, shard)
		err = nil
	}
	return err
}

func commandGetShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace/shard> argument is required for the GetShard command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	shardInfo, err := wr.TopoServer().GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	// Pass the embedded proto directly or jsonpb will panic.
	return printJSON(wr.Logger(), shardInfo.Shard)
}

func commandTabletExternallyReparented(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the TabletExternallyReparented command")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().TabletExternallyReparented(ctx, ti.Tablet, "")
}

func commandValidateShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	pingTablets := subFlags.Bool("ping-tablets", true, "Indicates whether all tablets should be pinged during the validation process")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace/shard> argument is required for the ValidateShard command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
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
		return fmt.Errorf("the <keyspace/shard> argument is required for the ShardReplicationPositions command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tablets, stats, err := wr.ShardReplicationStatuses(ctx, keyspace, shard)
	if tablets == nil {
		return err
	}

	lines := make([]string, 0, 24)
	for _, rt := range sortReplicatingTablets(tablets, stats) {
		status := rt.Status
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
		return fmt.Errorf("the <keyspace/shard> argument is required for the ListShardTablets command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return listTabletsByShard(ctx, wr, keyspace, shard)
}

func commandSetShardIsMasterServing(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <keyspace/shard> <is_master_serving> arguments are both required for the SetShardIsMasterServing command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}

	isMasterServing, err := strconv.ParseBool(subFlags.Arg(1))
	if err != nil {
		return err
	}

	return wr.SetShardIsMasterServing(ctx, keyspace, shard, isMasterServing)
}

func commandUpdateSrvKeyspacePartition(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	remove := subFlags.Bool("remove", false, "Removes shard from serving keyspace partition")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <keyspace/shard> and <tablet type> arguments are both required for the UpdateSrvKeyspacePartition command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletType, err := parseServingTabletType3(subFlags.Arg(1))
	if err != nil {
		return err
	}

	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	err = wr.UpdateSrvKeyspacePartitions(ctx, keyspace, shard, tabletType, cells, *remove)
	if err != nil {
		return err
	}
	return nil
}

func commandSetShardTabletControl(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	blacklistedTablesStr := subFlags.String("blacklisted_tables", "", "Specifies a comma-separated list of tables to blacklist (used for vertical split). Each is either an exact match, or a regular expression of the form '/regexp/'.")
	remove := subFlags.Bool("remove", false, "Removes cells for vertical splits.")
	disableQueryService := subFlags.Bool("disable_query_service", false, "Disables query service on the provided nodes. This flag requires 'blacklisted_tables' and 'remove' to be unset, otherwise it's ignored.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <keyspace/shard> and <tablet type> arguments are both required for the SetShardTabletControl command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletType, err := parseServingTabletType3(subFlags.Arg(1))
	if err != nil {
		return err
	}
	var blacklistedTables []string
	if *blacklistedTablesStr != "" {
		blacklistedTables = strings.Split(*blacklistedTablesStr, ",")
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	err = wr.SetShardTabletControl(ctx, keyspace, shard, tabletType, cells, *remove, blacklistedTables)
	if err != nil {
		return err
	}
	if !*remove && len(blacklistedTables) == 0 {
		return wr.UpdateDisableQueryService(ctx, keyspace, shard, tabletType, cells, *disableQueryService)
	}
	return nil
}

func commandSourceShardDelete(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() < 2 {
		return fmt.Errorf("the <keyspace/shard> and <uid> arguments are both required for the SourceShardDelete command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
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
	tablesStr := subFlags.String("tables", "", "Specifies a comma-separated list of tables to replicate (used for vertical split). Each is either an exact match, or a regular expression of the form /regexp/")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 {
		return fmt.Errorf("the <keyspace/shard>, <uid>, and <source keyspace/shard> arguments are all required for the SourceShardAdd command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	uid, err := strconv.Atoi(subFlags.Arg(1))
	if err != nil {
		return err
	}
	skeyspace, sshard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(2))
	if err != nil {
		return err
	}
	var tables []string
	if *tablesStr != "" {
		tables = strings.Split(*tablesStr, ",")
	}
	var kr *topodatapb.KeyRange
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
		return fmt.Errorf("the <keyspace/shard> and <tablet alias> arguments are required for the ShardReplicationAdd command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(1))
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
		return fmt.Errorf("the <keyspace/shard> and <tablet alias> arguments are required for the ShardReplicationRemove command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(1))
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
		return fmt.Errorf("the <cell> and <keyspace/shard> arguments are required for the ShardReplicationFix command")
	}

	cell := subFlags.Arg(0)
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return topo.FixShardReplication(ctx, wr.TopoServer(), wr.Logger(), cell, keyspace, shard)
}

func commandWaitForFilteredReplication(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	maxDelay := subFlags.Duration("max_delay", wrangler.DefaultWaitForFilteredReplicationMaxDelay,
		"Specifies the maximum delay, in seconds, the filtered replication of the"+
			" given destination shard should lag behind the source shard. When"+
			" higher, the command will block and wait for the delay to decrease.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace/shard> argument is required for the WaitForFilteredReplication command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.WaitForFilteredReplication(ctx, keyspace, shard, *maxDelay)
}

func commandRemoveShardCell(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Proceeds even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data.")
	recursive := subFlags.Bool("recursive", false, "Also delete all tablets in that cell belonging to the specified shard.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <keyspace/shard> and <cell> arguments are required for the RemoveShardCell command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.RemoveShardCell(ctx, keyspace, shard, subFlags.Arg(1), *force, *recursive)
}

func commandDeleteShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	recursive := subFlags.Bool("recursive", false, "Also delete all tablets belonging to the shard.")
	evenIfServing := subFlags.Bool("even_if_serving", false, "Remove the shard even if it is serving. Use with caution.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("the <keyspace/shard> argument must be used to identify at least one keyspace and shard when calling the DeleteShard command")
	}

	keyspaceShards, err := shardParamsToKeyspaceShards(ctx, wr, subFlags.Args())
	if err != nil {
		return err
	}
	for _, ks := range keyspaceShards {
		err := wr.DeleteShard(ctx, ks.Keyspace, ks.Shard, *recursive, *evenIfServing)
		switch {
		case err == nil:
			// keep going
		case topo.IsErrType(err, topo.NoNode):
			wr.Logger().Infof("Shard %v/%v doesn't exist, skipping it", ks.Keyspace, ks.Shard)
		default:
			return err
		}
	}
	return nil
}

func commandCreateKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	shardingColumnName := subFlags.String("sharding_column_name", "", "Specifies the column to use for sharding operations")
	shardingColumnType := subFlags.String("sharding_column_type", "", "Specifies the type of the column to use for sharding operations")
	force := subFlags.Bool("force", false, "Proceeds even if the keyspace already exists")
	allowEmptyVSchema := subFlags.Bool("allow_empty_vschema", false, "If set this will allow a new keyspace to have no vschema")

	var servedFrom flagutil.StringMapValue
	subFlags.Var(&servedFrom, "served_from", "Specifies a comma-separated list of dbtype:keyspace pairs used to serve traffic")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace name> argument is required for the CreateKeyspace command")
	}

	keyspace := subFlags.Arg(0)
	kit, err := key.ParseKeyspaceIDType(*shardingColumnType)
	if err != nil {
		return err
	}
	ki := &topodatapb.Keyspace{
		ShardingColumnName: *shardingColumnName,
		ShardingColumnType: kit,
	}
	if len(servedFrom) > 0 {
		for name, value := range servedFrom {
			tt, err := parseServingTabletType3(name)
			if err != nil {
				return err
			}
			ki.ServedFroms = append(ki.ServedFroms, &topodatapb.Keyspace_ServedFrom{
				TabletType: tt,
				Keyspace:   value,
			})
		}
	}
	err = wr.TopoServer().CreateKeyspace(ctx, keyspace, ki)
	if *force && topo.IsErrType(err, topo.NodeExists) {
		wr.Logger().Infof("keyspace %v already exists (ignoring error with -force)", keyspace)
		err = nil
	}

	if !*allowEmptyVSchema {
		err = wr.TopoServer().EnsureVSchema(ctx, keyspace)
	}

	return err
}

func commandDeleteKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	recursive := subFlags.Bool("recursive", false, "Also recursively delete all shards in the keyspace.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("must specify the <keyspace> argument for DeleteKeyspace")
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
		return fmt.Errorf("the <keyspace> and <cell> arguments are required for the RemoveKeyspaceCell command")
	}

	return wr.RemoveKeyspaceCell(ctx, subFlags.Arg(0), subFlags.Arg(1), *force, *recursive)
}

func commandGetKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace> argument is required for the GetKeyspace command")
	}

	keyspace := subFlags.Arg(0)
	keyspaceInfo, err := wr.TopoServer().GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	// Pass the embedded proto directly or jsonpb will panic.
	return printJSON(wr.Logger(), keyspaceInfo.Keyspace)
}

func commandGetKeyspaces(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	keyspaces, err := wr.TopoServer().GetKeyspaces(ctx)
	if err != nil {
		return err
	}
	wr.Logger().Printf("%v\n", strings.Join(keyspaces, "\n"))
	return nil
}

func commandSetKeyspaceShardingInfo(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Updates fields even if they are already set. Use caution before calling this command.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() > 3 || subFlags.NArg() < 1 {
		return fmt.Errorf("the <keyspace name> argument is required for the SetKeyspaceShardingInfo command. The <column name> and <column type> arguments are both optional")
	}

	keyspace := subFlags.Arg(0)
	columnName := ""
	if subFlags.NArg() >= 2 {
		columnName = subFlags.Arg(1)
	}
	kit := topodatapb.KeyspaceIdType_UNSET
	if subFlags.NArg() >= 3 {
		var err error
		kit, err = key.ParseKeyspaceIDType(subFlags.Arg(2))
		if err != nil {
			return err
		}
	}

	keyspaceIDTypeSet := (kit != topodatapb.KeyspaceIdType_UNSET)
	columnNameSet := (columnName != "")
	if (keyspaceIDTypeSet && !columnNameSet) || (!keyspaceIDTypeSet && columnNameSet) {
		return fmt.Errorf("both <column name> and <column type> must be set, or both must be unset")
	}

	return wr.SetKeyspaceShardingInfo(ctx, keyspace, columnName, kit, *force)
}

func commandSetKeyspaceServedFrom(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	source := subFlags.String("source", "", "Specifies the source keyspace name")
	remove := subFlags.Bool("remove", false, "Indicates whether to add (default) or remove the served from record")
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to affect")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <keyspace name> and <tablet type> arguments are required for the SetKeyspaceServedFrom command")
	}
	keyspace := subFlags.Arg(0)
	servedType, err := parseTabletType(subFlags.Arg(1), []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY})
	if err != nil {
		return err
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	return wr.SetKeyspaceServedFrom(ctx, keyspace, servedType, cells, *source, *remove)
}

func commandRebuildKeyspaceGraph(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cells := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("the <keyspace> argument must be used to specify at least one keyspace when calling the RebuildKeyspaceGraph command")
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
		if err := wr.RebuildKeyspaceGraph(ctx, keyspace, cellArray); err != nil {
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
		return fmt.Errorf("the <keyspace name> argument is required for the ValidateKeyspace command")
	}

	keyspace := subFlags.Arg(0)
	return wr.ValidateKeyspace(ctx, keyspace, *pingTablets)
}

func commandSplitClone(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 {
		return fmt.Errorf("three arguments are required: keyspace, from_shards, to_shards")
	}
	keyspace := subFlags.Arg(0)
	from := strings.Split(subFlags.Arg(1), ",")
	to := strings.Split(subFlags.Arg(2), ",")
	return wr.SplitClone(ctx, keyspace, from, to)
}

func commandVerticalSplitClone(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 {
		return fmt.Errorf("three arguments are required: from_keyspace, to_keyspace, tables")
	}
	fromKeyspace := subFlags.Arg(0)
	toKeyspace := subFlags.Arg(1)
	tables := strings.Split(subFlags.Arg(2), ",")
	return wr.VerticalSplitClone(ctx, fromKeyspace, toKeyspace, tables)
}

func commandMigrateServedTypes(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	reverse := subFlags.Bool("reverse", false, "Moves the served tablet type backward instead of forward. Use in case of trouble")
	skipReFreshState := subFlags.Bool("skip-refresh-state", false, "Skips refreshing the state of the source tablets after the migration, meaning that the refresh will need to be done manually, replica and rdonly only)")
	filteredReplicationWaitTime := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations")
	reverseReplication := subFlags.Bool("reverse_replication", false, "For master migration, enabling this flag reverses replication which allows you to rollback")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <source keyspace/shard> and <served tablet type> arguments are both required for the MigrateServedTypes command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseServingTabletType3(subFlags.Arg(1))
	if err != nil {
		return err
	}
	if servedType == topodatapb.TabletType_MASTER && *skipReFreshState {
		return fmt.Errorf("the skip-refresh-state flag can only be specified for non-master migrations")
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}
	return wr.MigrateServedTypes(ctx, keyspace, shard, cells, servedType, *reverse, *skipReFreshState, *filteredReplicationWaitTime, *reverseReplication)
}

func commandMigrateServedFrom(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	reverse := subFlags.Bool("reverse", false, "Moves the served tablet type backward instead of forward. Use in case of trouble")
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	filteredReplicationWaitTime := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <destination keyspace/shard> and <served tablet type> arguments are both required for the MigrateServedFrom command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	servedType, err := parseTabletType(subFlags.Arg(1), []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY})
	if err != nil {
		return err
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}
	return wr.MigrateServedFrom(ctx, keyspace, shard, servedType, cells, *reverse, *filteredReplicationWaitTime)
}

func commandCancelResharding(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("<keyspace/shard> required for CancelResharding command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.CancelResharding(ctx, keyspace, shard)
}

func commandShowResharding(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("<keyspace/shard> required for ShowResharding command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ShowResharding(ctx, keyspace, shard)
}

func commandFindAllShardsInKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace> argument is required for the FindAllShardsInKeyspace command")
	}

	keyspace := subFlags.Arg(0)
	result, err := wr.TopoServer().FindAllShardsInKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	return printJSON(wr.Logger(), result)
}

func commandValidate(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	pingTablets := subFlags.Bool("ping-tablets", false, "Indicates whether all tablets should be pinged during the validation process")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 0 {
		wr.Logger().Warningf("action Validate doesn't take any parameter any more")
	}
	return wr.Validate(ctx, *pingTablets)
}

func commandListAllTablets(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	var cells []string
	var err error
	if subFlags.NArg() == 1 {
		cells = strings.Split(subFlags.Arg(0), ",")
	} else {
		cells, err = wr.TopoServer().GetKnownCells(ctx)
		if err != nil {
			return err
		}
	}

	for _, cell := range cells {
		err := dumpAllTablets(ctx, wr, cell)
		if err != nil {
			return err
		}
	}
	return nil
}

func commandListTablets(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 0 {
		return fmt.Errorf("the <tablet alias> argument is required for the ListTablets command")
	}

	paths := subFlags.Args()
	aliases := make([]*topodatapb.TabletAlias, len(paths))
	var err error
	for i, path := range paths {
		aliases[i], err = topoproto.ParseTabletAlias(path)
		if err != nil {
			return err
		}
	}
	return dumpTablets(ctx, wr, aliases)
}

func commandGetSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	tables := subFlags.String("tables", "", "Specifies a comma-separated list of tables for which we should gather information. Each is either an exact match, or a regular expression of the form /regexp/")
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the output")
	tableNamesOnly := subFlags.Bool("table_names_only", false, "Only displays table names that match")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the GetSchema command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
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
	if err != nil {
		return err
	}
	if *tableNamesOnly {
		for _, td := range sd.TableDefinitions {
			wr.Logger().Printf("%v\n", td.Name)
		}
		return nil
	}
	return printJSON(wr.Logger(), sd)
}

func commandReloadSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the ReloadSchema command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ReloadSchema(ctx, tabletAlias)
}

func commandReloadSchemaShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 10, "How many tablets to reload in parallel")
	includeMaster := subFlags.Bool("include_master", true, "Include the master tablet")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace/shard> argument is required for the ReloadSchemaShard command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	sema := sync2.NewSemaphore(*concurrency, 0)
	wr.ReloadSchemaShard(ctx, keyspace, shard, "" /* waitPosition */, sema, *includeMaster)
	return nil
}

func commandReloadSchemaKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 10, "How many tablets to reload in parallel")
	includeMaster := subFlags.Bool("include_master", true, "Include the master tablet(s)")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace> argument is required for the ReloadSchemaKeyspace command")
	}
	sema := sync2.NewSemaphore(*concurrency, 0)
	return wr.ReloadSchemaKeyspace(ctx, subFlags.Arg(0), sema, *includeMaster)
}

func commandValidateSchemaShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the validation")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace/shard> argument is required for the ValidateSchemaShard command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
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
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the validation")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace name> argument is required for the ValidateSchemaKeyspace command")
	}

	keyspace := subFlags.Arg(0)
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return wr.ValidateSchemaKeyspace(ctx, keyspace, excludeTableArray, *includeViews)
}

func commandApplySchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	allowLongUnavailability := subFlags.Bool("allow_long_unavailability", false, "Allow large schema changes which incur a longer unavailability of the database.")
	sql := subFlags.String("sql", "", "A list of semicolon-delimited SQL commands")
	sqlFile := subFlags.String("sql-file", "", "Identifies the file that contains the SQL commands")
	waitSlaveTimeout := subFlags.Duration("wait_slave_timeout", wrangler.DefaultWaitSlaveTimeout, "The amount of time to wait for slaves to receive the schema change via replication.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace> argument is required for the commandApplySchema command")
	}

	keyspace := subFlags.Arg(0)
	change, err := getFileParam(*sql, *sqlFile, "sql")
	if err != nil {
		return err
	}

	executor := schemamanager.NewTabletExecutor(wr, *waitSlaveTimeout)
	if *allowLongUnavailability {
		executor.AllowBigSchemaChange()
	}
	return schemamanager.Run(
		ctx,
		schemamanager.NewPlainController(change, keyspace),
		executor,
	)
}

func commandCopySchemaShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	tables := subFlags.String("tables", "", "Specifies a comma-separated list of tables to copy. Each is either an exact match, or a regular expression of the form /regexp/")
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	includeViews := subFlags.Bool("include-views", true, "Includes views in the output")
	waitSlaveTimeout := subFlags.Duration("wait_slave_timeout", 10*time.Second, "The amount of time to wait for slaves to receive the schema change via replication.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <source keyspace/shard> and <destination keyspace/shard> arguments are both required for the CopySchemaShard command. Instead of the <source keyspace/shard> argument, you can also specify <tablet alias> which refers to a specific tablet of the shard in the source keyspace")
	}
	var tableArray []string
	if *tables != "" {
		tableArray = strings.Split(*tables, ",")
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	destKeyspace, destShard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(1))
	if err != nil {
		return err
	}

	sourceKeyspace, sourceShard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err == nil {
		return wr.CopySchemaShardFromShard(ctx, tableArray, excludeTableArray, *includeViews, sourceKeyspace, sourceShard, destKeyspace, destShard, *waitSlaveTimeout)
	}
	sourceTabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err == nil {
		return wr.CopySchemaShard(ctx, sourceTabletAlias, tableArray, excludeTableArray, *includeViews, destKeyspace, destShard, *waitSlaveTimeout)
	}
	return err
}

func commandValidateVersionShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace/shard> argument is required for the ValidateVersionShard command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
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
		return fmt.Errorf("the <keyspace name> argument is required for the ValidateVersionKeyspace command")
	}

	keyspace := subFlags.Arg(0)
	return wr.ValidateVersionKeyspace(ctx, keyspace)
}

func commandGetPermissions(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the GetPermissions command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	p, err := wr.GetPermissions(ctx, tabletAlias)
	if err == nil {
		printJSON(wr.Logger(), p)
	}
	return err
}

func commandValidatePermissionsShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace/shard> argument is required for the ValidatePermissionsShard command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
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
		return fmt.Errorf("the <keyspace name> argument is required for the ValidatePermissionsKeyspace command")
	}

	keyspace := subFlags.Arg(0)
	return wr.ValidatePermissionsKeyspace(ctx, keyspace)
}

func commandGetVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace> argument is required for the GetVSchema command")
	}
	keyspace := subFlags.Arg(0)
	schema, err := wr.TopoServer().GetVSchema(ctx, keyspace)
	if err != nil {
		return err
	}
	b, err := json2.MarshalIndentPB(schema, "  ")
	if err != nil {
		wr.Logger().Printf("%v\n", err)
		return err
	}
	wr.Logger().Printf("%s\n", b)
	return nil
}

func commandGetRoutingRules(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	rr, err := wr.TopoServer().GetRoutingRules(ctx)
	if err != nil {
		return err
	}
	b, err := json2.MarshalIndentPB(rr, "  ")
	if err != nil {
		wr.Logger().Printf("%v\n", err)
		return err
	}
	wr.Logger().Printf("%s\n", b)
	return nil
}

func commandRebuildVSchemaGraph(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	var cells flagutil.StringListValue
	subFlags.Var(&cells, "cells", "Specifies a comma-separated list of cells to look for tablets")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("RebuildVSchemaGraph doesn't take any arguments")
	}

	return wr.TopoServer().RebuildSrvVSchema(ctx, cells)
}

func commandApplyVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	vschema := subFlags.String("vschema", "", "Identifies the VTGate routing schema")
	vschemaFile := subFlags.String("vschema_file", "", "Identifies the VTGate routing schema file")
	sql := subFlags.String("sql", "", "A vschema ddl SQL statement (e.g. `add vindex`, `alter table t add vindex hash(id)`, etc)")
	sqlFile := subFlags.String("sql_file", "", "A vschema ddl SQL statement (e.g. `add vindex`, `alter table t add vindex hash(id)`, etc)")
	dryRun := subFlags.Bool("dry-run", false, "If set, do not save the altered vschema, simply echo to console.")
	skipRebuild := subFlags.Bool("skip_rebuild", false, "If set, do no rebuild the SrvSchema objects.")
	var cells flagutil.StringListValue
	subFlags.Var(&cells, "cells", "If specified, limits the rebuild to the cells, after upload. Ignored if skipRebuild is set.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace> argument is required for the ApplyVSchema command")
	}
	keyspace := subFlags.Arg(0)

	var vs *vschemapb.Keyspace
	var err error

	sqlMode := (*sql != "") != (*sqlFile != "")
	jsonMode := (*vschema != "") != (*vschemaFile != "")

	if sqlMode && jsonMode {
		return fmt.Errorf("only one of the sql, sql_file, vschema, or vschema_file flags may be specified when calling the ApplyVSchema command")
	}

	if !sqlMode && !jsonMode {
		return fmt.Errorf("one of the sql, sql_file, vschema, or vschema_file flags must be specified when calling the ApplyVSchema command")
	}

	if sqlMode {
		if *sqlFile != "" {
			sqlBytes, err := ioutil.ReadFile(*sqlFile)
			if err != nil {
				return err
			}
			*sql = string(sqlBytes)
		}

		stmt, err := sqlparser.Parse(*sql)
		if err != nil {
			return fmt.Errorf("error parsing vschema statement `%s`: %v", *sql, err)
		}
		ddl, ok := stmt.(*sqlparser.DDL)
		if !ok {
			return fmt.Errorf("error parsing vschema statement `%s`: not a ddl statement", *sql)
		}

		vs, err = wr.TopoServer().GetVSchema(ctx, keyspace)
		if err != nil {
			if topo.IsErrType(err, topo.NoNode) {
				vs = &vschemapb.Keyspace{}
			} else {
				return err
			}
		}

		vs, err = topotools.ApplyVSchemaDDL(keyspace, vs, ddl)
		if err != nil {
			return err
		}

	} else {
		// json mode
		var schema []byte
		if *vschemaFile != "" {
			var err error
			schema, err = ioutil.ReadFile(*vschemaFile)
			if err != nil {
				return err
			}
		} else {
			schema = []byte(*vschema)
		}

		vs = &vschemapb.Keyspace{}
		err := json2.Unmarshal(schema, vs)
		if err != nil {
			return err
		}
	}

	b, err := json2.MarshalIndentPB(vs, "  ")
	if err != nil {
		wr.Logger().Errorf2(err, "Failed to marshal VSchema for display")
	} else {
		wr.Logger().Printf("New VSchema object:\n%s\nIf this is not what you expected, check the input data (as JSON parsing will skip unexpected fields).\n", b)
	}

	if *dryRun {
		wr.Logger().Printf("Dry run: Skipping update of VSchema\n")
		return nil
	}

	if err := wr.TopoServer().SaveVSchema(ctx, keyspace, vs); err != nil {
		return err
	}

	if *skipRebuild {
		wr.Logger().Warningf("Skipping rebuild of SrvVSchema, will need to run RebuildVSchemaGraph for changes to take effect")
		return nil
	}
	return wr.TopoServer().RebuildSrvVSchema(ctx, cells)
}

func commandApplyRoutingRules(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	routingRules := subFlags.String("rules", "", "Specify rules as a string")
	routingRulesFile := subFlags.String("vschema_file", "", "Specify rules in a file")
	skipRebuild := subFlags.Bool("skip_rebuild", false, "If set, do no rebuild the SrvSchema objects.")
	var cells flagutil.StringListValue
	subFlags.Var(&cells, "cells", "If specified, limits the rebuild to the cells, after upload. Ignored if skipRebuild is set.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("ApplyRoutingRules doesn't take any arguments")
	}

	var rulesBytes []byte
	if *routingRulesFile != "" {
		var err error
		rulesBytes, err = ioutil.ReadFile(*routingRulesFile)
		if err != nil {
			return err
		}
	} else {
		rulesBytes = []byte(*routingRules)
	}

	rr := &vschemapb.RoutingRules{}
	if err := json2.Unmarshal(rulesBytes, rr); err != nil {
		return err
	}

	b, err := json2.MarshalIndentPB(rr, "  ")
	if err != nil {
		wr.Logger().Errorf2(err, "Failed to marshal RoutingRules for display")
	} else {
		wr.Logger().Printf("New RoutingRules object:\n%s\nIf this is not what you expected, check the input data (as JSON parsing will skip unexpected fields).\n", b)
	}

	if err := wr.TopoServer().SaveRoutingRules(ctx, rr); err != nil {
		return err
	}

	if *skipRebuild {
		wr.Logger().Warningf("Skipping rebuild of SrvVSchema, will need to run RebuildVSchemaGraph for changes to take effect")
		return nil
	}
	return wr.TopoServer().RebuildSrvVSchema(ctx, cells)
}

func commandGetSrvKeyspaceNames(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <cell> argument is required for the GetSrvKeyspaceNames command")
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

func commandGetSrvKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <cell> and <keyspace> arguments are required for the GetSrvKeyspace command")
	}

	srvKeyspace, err := wr.TopoServer().GetSrvKeyspace(ctx, subFlags.Arg(0), subFlags.Arg(1))
	if err != nil {
		return err
	}
	return printJSON(wr.Logger(), srvKeyspace)
}

func commandGetSrvVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <cell> argument is required for the GetSrvVSchema command")
	}

	srvVSchema, err := wr.TopoServer().GetSrvVSchema(ctx, subFlags.Arg(0))
	if err != nil {
		return err
	}
	return printJSON(wr.Logger(), srvVSchema)
}

func commandDeleteSrvVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <cell> argument is required for the DeleteSrvVSchema command")
	}

	return wr.TopoServer().DeleteSrvVSchema(ctx, subFlags.Arg(0))
}

func commandGetShardReplication(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <cell> and <keyspace/shard> arguments are required for the GetShardReplication command")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(1))
	if err != nil {
		return err
	}
	shardReplication, err := wr.TopoServer().GetShardReplication(ctx, subFlags.Arg(0), keyspace, shard)
	if err != nil {
		return err
	}
	// Pass the embedded proto directly or jsonpb will panic.
	return printJSON(wr.Logger(), shardReplication.ShardReplication)
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
		return fmt.Errorf("when calling the Help command, either specify a single argument that identifies the name of the command to get help with or do not specify any additional arguments")
	}

	return nil
}

func commandPanic(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	panic(fmt.Errorf("this command panics on purpose"))
}

type rTablet struct {
	*topo.TabletInfo
	*replicationdatapb.Status
}

type rTablets []*rTablet

func (rts rTablets) Len() int { return len(rts) }

func (rts rTablets) Swap(i, j int) { rts[i], rts[j] = rts[j], rts[i] }

// Sort for tablet replication.
// Tablet type first (with master first), then replication positions.
func (rts rTablets) Less(i, j int) bool {
	l, r := rts[i], rts[j]
	// l or r ReplicationStatus would be nil if we failed to get
	// the position (put them at the beginning of the list)
	if l.Status == nil {
		return r.Status != nil
	}
	if r.Status == nil {
		return false
	}
	// the type proto has MASTER first, so sort by that. Will show
	// the MASTER first, then each slave type sorted by
	// replication position.
	if l.Type < r.Type {
		return true
	}
	if l.Type > r.Type {
		return false
	}
	// then compare replication positions
	lpos, err := mysql.DecodePosition(l.Position)
	if err != nil {
		return true
	}
	rpos, err := mysql.DecodePosition(r.Position)
	if err != nil {
		return false
	}
	return !lpos.AtLeast(rpos)
}

func sortReplicatingTablets(tablets []*topo.TabletInfo, stats []*replicationdatapb.Status) []*rTablet {
	rtablets := make([]*rTablet, len(tablets))
	for i, status := range stats {
		rtablets[i] = &rTablet{
			TabletInfo: tablets[i],
			Status:     status,
		}
	}
	sort.Sort(rTablets(rtablets))
	return rtablets
}

// printJSON will print the JSON version of the structure to the logger.
func printJSON(logger logutil.Logger, val interface{}) error {
	data, err := MarshalJSON(val)
	if err != nil {
		return fmt.Errorf("cannot marshal data: %v", err)
	}
	logger.Printf("%v\n", string(data))
	return nil
}

// MarshalJSON marshals "obj" to a JSON string. It uses the "jsonpb" marshaler
// or Go's standard one.
//
// We use jsonpb for protobuf messages because it is the only supported
// way to marshal protobuf messages to JSON.
// In addition to that, it's the only way to emit zero values in the JSON
// output.
// Unfortunately, jsonpb works only for protobuf messages. Therefore, we use
// the default marshaler for the remaining structs (which are possibly
// mixed protobuf and non-protobuf).
//
// TODO(mberlin): Switch "EnumAsInts" to "false" once the frontend is
//                updated and mixed types will use jsonpb as well.
func MarshalJSON(obj interface{}) (data []byte, err error) {
	switch obj := obj.(type) {
	case proto.Message:
		// Note: We also end up in this case if "obj" is NOT a proto.Message but
		// has an anonymous (embedded) field of the type "proto.Message".
		// In that case jsonpb may panic if the "obj" has non-exported fields.

		// Marshal the protobuf message.
		var b bytes.Buffer
		m := jsonpb.Marshaler{EnumsAsInts: true, EmitDefaults: true, Indent: "  ", OrigName: true}
		if err := m.Marshal(&b, obj); err != nil {
			return nil, fmt.Errorf("jsonpb error: %v", err)
		}
		data = b.Bytes()
	case []string:
		if len(obj) == 0 {
			return []byte{'[', ']'}, nil
		}
		data, err = json.MarshalIndent(obj, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("json error: %v", err)
		}
	default:
		data, err = json.MarshalIndent(obj, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("json error: %v", err)
		}
	}

	return data, nil
}

// RunCommand will execute the command using the provided wrangler.
// It will return the actionPath to wait on for long remote actions if
// applicable.
func RunCommand(ctx context.Context, wr *wrangler.Wrangler, args []string) error {
	if len(args) == 0 {
		wr.Logger().Printf("No command specified. Please see the list below:\n\n")
		PrintAllCommands(wr.Logger())
		return fmt.Errorf("no command was specified")
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
