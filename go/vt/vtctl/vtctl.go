/*
Copyright 2019 The Vitess Authors.

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
            href="https://golang.org/pkg/time/#ParseDuration">ParseDuration</a>
            function for more details. Note that, in practice, the value
            should be a positively signed value.

- db type, tablet type: The vttablet's role. Valid values are:
  -- backup: A replica copy of data that is offline to queries other than
             for backup purposes
  -- batch: A replicated copy of data for OLAP load patterns (typically for
            MapReduce jobs)
  -- drained: A tablet that is reserved for a background process. For example,
              a tablet used by a vtworker process, where the tablet is likely
              lagging in replication.
  -- experimental: A replica copy of data that is ready but not serving query
                   traffic. The value indicates a special characteristic of
                   the tablet that indicates the tablet should not be
                   considered a potential master. Vitess also does not
                   worry about lag for experimental tablets when reparenting.
  -- master: A primary copy of data
  -- rdonly: A replica copy of data for OLAP load patterns
  -- replica: A replica copy of data ready to be promoted to master
  -- restore: A tablet that is restoring from a snapshot. Typically, this
              happens at tablet startup, then it goes to its right state.
  -- spare: A replica copy of data that is ready but not serving query traffic.
            The data could be a potential master tablet.
*/

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"context"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	hk "vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/schemamanager"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/proto/vttime"
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
				"DEPRECATED [-allow_update] [-allow_different_shard] [-allow_master_override] [-parent] [-db_name_override=<db name>] [-hostname=<hostname>] [-mysql_port=<port>] [-port=<port>] [-grpc_port=<port>] [-tags=tag1:value1,tag2:value2] -keyspace=<keyspace> -shard=<shard> <tablet alias> <tablet type>",
				"Initializes a tablet in the topology.\n"},
			{"GetTablet", commandGetTablet,
				"<tablet alias>",
				"Outputs a JSON structure that contains information about the Tablet."},
			{"DEPRECATED UpdateTabletAddrs", commandUpdateTabletAddrs,
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
			{"StartReplication", commandStartReplication,
				"<table alias>",
				"Starts replication on the specified tablet."},
			{"StopReplication", commandStopReplication,
				"<tablet alias>",
				"Stops replication on the specified tablet."},
			{"ChangeTabletType", commandChangeTabletType,
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
			{"ValidateShard", commandValidateShard,
				"[-ping-tablets] <keyspace/shard>",
				"Validates that all nodes that are reachable from this shard are consistent."},
			{"ShardReplicationPositions", commandShardReplicationPositions,
				"<keyspace/shard>",
				"Shows the replication status of each replica machine in the shard graph. In this case, the status refers to the replication lag between the master vttablet and the replica vttablet. In Vitess, data is always written to the master vttablet first and then replicated to all replica vttablets. Output is sorted by tablet type, then replication position. Use ctrl-C to interrupt command and see partial result if needed."},
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
				"[-sharding_column_name=name] [-sharding_column_type=type] [-served_from=tablettype1:ks1,tablettype2:ks2,...] [-force] [-keyspace_type=type] [-base_keyspace=base_keyspace] [-snapshot_time=time] <keyspace name>",
				"Creates the specified keyspace. keyspace_type can be NORMAL or SNAPSHOT. For a SNAPSHOT keyspace you must specify the name of a base_keyspace, and a snapshot_time in UTC, in RFC3339 time format, e.g. 2006-01-02T15:04:05+00:00"},
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
				"[-cells=c1,c2,...] [-allow_partial] <keyspace> ...",
				"Rebuilds the serving data for the keyspace. This command may trigger an update to all connected clients."},
			{"ValidateKeyspace", commandValidateKeyspace,
				"[-ping-tablets] <keyspace name>",
				"Validates that all nodes reachable from the specified keyspace are consistent."},
			{"Reshard", commandReshard,
				"[-cells=<cells>] [-tablet_types=<source_tablet_types>] [-skip_schema_copy] <keyspace.workflow> <source_shards> <target_shards>",
				"Start a Resharding process. Example: Reshard -cells='zone1,alias1' -tablet_types='master,replica,rdonly'  ks.workflow001 '0' '-80,80-'"},
			{"MoveTables", commandMoveTables,
				"[-cells=<cells>] [-tablet_types=<source_tablet_types>] -workflow=<workflow> <source_keyspace> <target_keyspace> <table_specs>",
				`Move table(s) to another keyspace, table_specs is a list of tables or the tables section of the vschema for the target keyspace. Example: '{"t1":{"column_vindexes": [{"column": "id1", "name": "hash"}]}, "t2":{"column_vindexes": [{"column": "id2", "name": "hash"}]}}'.  In the case of an unsharded target keyspace the vschema for each table may be empty. Example: '{"t1":{}, "t2":{}}'.`},
			{"Migrate", commandMigrate,
				"[-cells=<cells>] [-tablet_types=<source_tablet_types>] -workflow=<workflow> <source_keyspace> <target_keyspace> <table_specs>",
				`Move table(s) to another keyspace, table_specs is a list of tables or the tables section of the vschema for the target keyspace. Example: '{"t1":{"column_vindexes": [{"column": "id1", "name": "hash"}]}, "t2":{"column_vindexes": [{"column": "id2", "name": "hash"}]}}'.  In the case of an unsharded target keyspace the vschema for each table may be empty. Example: '{"t1":{}, "t2":{}}'.`},
			{"DropSources", commandDropSources,
				"[-dry_run] [-rename_tables] <keyspace.workflow>",
				"After a MoveTables or Resharding workflow cleanup unused artifacts like source tables, source shards and blacklists"},
			{"CreateLookupVindex", commandCreateLookupVindex,
				"[-cell=<source_cells> DEPRECATED] [-cells=<source_cells>] [-tablet_types=<source_tablet_types>] <keyspace> <json_spec>",
				`Create and backfill a lookup vindex. the json_spec must contain the vindex and colvindex specs for the new lookup.`},
			{"ExternalizeVindex", commandExternalizeVindex,
				"<keyspace>.<vindex>",
				`Externalize a backfilled vindex.`},
			{"Materialize", commandMaterialize,
				`[-cells=<cells>] [-tablet_types=<source_tablet_types>] <json_spec>, example : '{"workflow": "aaa", "source_keyspace": "source", "target_keyspace": "target", "table_settings": [{"target_table": "customer", "source_expression": "select * from customer", "create_ddl": "copy"}]}'`,
				"Performs materialization based on the json spec. Is used directly to form VReplication rules, with an optional step to copy table structure/DDL."},
			{"SplitClone", commandSplitClone,
				"<keyspace> <from_shards> <to_shards>",
				"Start the SplitClone process to perform horizontal resharding. Example: SplitClone ks '0' '-80,80-'"},
			{"VerticalSplitClone", commandVerticalSplitClone,
				"<from_keyspace> <to_keyspace> <tables>",
				"Start the VerticalSplitClone process to perform vertical resharding. Example: SplitClone from_ks to_ks 'a,/b.*/'"},
			{"VDiff", commandVDiff,
				"[-source_cell=<cell>] [-target_cell=<cell>] [-tablet_types=replica] [-filtered_replication_wait_time=30s] <keyspace.workflow>",
				"Perform a diff of all tables in the workflow"},
			{"MigrateServedTypes", commandMigrateServedTypes,
				"[-cells=c1,c2,...] [-reverse] [-skip-refresh-state] [-filtered_replication_wait_time=30s] [-reverse_replication=false] <keyspace/shard> <served tablet type>",
				"Migrates a serving type from the source shard to the shards that it replicates to. This command also rebuilds the serving graph. The <keyspace/shard> argument can specify any of the shards involved in the migration."},
			{"MigrateServedFrom", commandMigrateServedFrom,
				"[-cells=c1,c2,...] [-reverse] [-filtered_replication_wait_time=30s] <destination keyspace/shard> <served tablet type>",
				"Makes the <destination keyspace/shard> serve the given type. This command also rebuilds the serving graph."},
			{"SwitchReads", commandSwitchReads,
				"[-cells=c1,c2,...] [-reverse] -tablet_type={replica|rdonly} [-dry-run] <keyspace.workflow>",
				"Switch read traffic for the specified workflow."},
			{"SwitchWrites", commandSwitchWrites,
				"[-timeout=30s] [-reverse] [-reverse_replication=true] [-dry-run] <keyspace.workflow>",
				"Switch write traffic for the specified workflow."},
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
				"Blocks until no new queries were observed on all tablets with the given tablet type in the specified keyspace. " +
					" This can be used as sanity check to ensure that the tablets were drained after running vtctl MigrateServedTypes " +
					" and vtgate is no longer using them. If -timeout is set, it fails when the timeout is reached."},
			{"Mount", commandMount,
				"[-topo_type=etcd2|consul|zookeeper] [-topo_server=topo_url] [-topo_root=root_topo_node> [-unmount] [-list] [-show]  [<cluster_name>]",
				"Add/Remove/Display/List external cluster(s) to this vitess cluster"},
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
			{"GenerateShardRanges", commandGenerateShardRanges,
				"<num shards>",
				"Generates shard ranges assuming a keyspace with N shards."},
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
				"Validates that the master schema matches all of the replicas."},
			{"ValidateSchemaKeyspace", commandValidateSchemaKeyspace,
				"[-exclude_tables=''] [-include-views] [-skip-no-master] <keyspace name>",
				"Validates that the master schema from shard 0 matches the schema on all of the other tablets in the keyspace."},
			{"ApplySchema", commandApplySchema,
				"[-allow_long_unavailability] [-wait_replicas_timeout=10s] [-ddl_strategy=<ddl_strategy>] [-skip_preflight] {-sql=<sql> || -sql-file=<filename>} <keyspace>",
				"Applies the schema change to the specified keyspace on every master, running in parallel on all shards. The changes are then propagated to replicas via replication. If -allow_long_unavailability is set, schema changes affecting a large number of rows (and possibly incurring a longer period of unavailability) will not be rejected. ddl_strategy is used to intruct migrations via gh-ost or pt-osc with optional parameters. If -skip_preflight, SQL goes directly to shards without going through sanity checks"},
			{"CopySchemaShard", commandCopySchemaShard,
				"[-tables=<table1>,<table2>,...] [-exclude_tables=<table1>,<table2>,...] [-include-views] [-skip-verify] [-wait_replicas_timeout=10s] {<source keyspace/shard> || <source tablet alias>} <destination keyspace/shard>",
				"Copies the schema from a source shard's master (or a specific tablet) to a destination shard. The schema is applied directly on the master of the destination shard, and it is propagated to the replicas through binlogs."},
			{"OnlineDDL", commandOnlineDDL,
				"<keyspace> <command> [<migration_uuid>]",
				"Operates on online DDL (migrations). Examples:" +
					" \nvtctl OnlineDDL test_keyspace show 82fa54ac_e83e_11ea_96b7_f875a4d24e90" +
					" \nvtctl OnlineDDL test_keyspace show all" +
					" \nvtctl OnlineDDL test_keyspace show running" +
					" \nvtctl OnlineDDL test_keyspace show complete" +
					" \nvtctl OnlineDDL test_keyspace show failed" +
					" \nvtctl OnlineDDL test_keyspace retry 82fa54ac_e83e_11ea_96b7_f875a4d24e90" +
					" \nvtctl OnlineDDL test_keyspace cancel 82fa54ac_e83e_11ea_96b7_f875a4d24e90",
			},

			{"ValidateVersionShard", commandValidateVersionShard,
				"<keyspace/shard>",
				"Validates that the master version matches all of the replicas."},
			{"ValidateVersionKeyspace", commandValidateVersionKeyspace,
				"<keyspace name>",
				"Validates that the master version from shard 0 matches all of the other tablets in the keyspace."},

			{"GetPermissions", commandGetPermissions,
				"<tablet alias>",
				"Displays the permissions for a tablet."},
			{"ValidatePermissionsShard", commandValidatePermissionsShard,
				"<keyspace/shard>",
				"Validates that the master permissions match all the replicas."},
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
				"{-rules=<rules> || -rules_file=<rules_file>} [-cells=c1,c2,...] [-skip_rebuild] [-dry-run]",
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
	{
		"Workflow", []command{
			{"VExec", commandVExec,
				"<ks.workflow> <query> --dry-run",
				"Runs query on all tablets in workflow. Example: VExec merchant.morders \"update _vt.vreplication set Status='Running'\"",
			},
		},
	},
	{
		"Workflow", []command{
			{"Workflow", commandWorkflow,
				"<ks.workflow> <action> --dry-run",
				"Start/Stop/Delete/Show/ListAll Workflow on all target tablets in workflow. Example: Workflow merchant.morders Start",
			},
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
	mtst := "<null>"
	// special case for old master that hasn't updated topo yet
	if ti.MasterTermStartTime != nil && ti.MasterTermStartTime.Seconds > 0 {
		mtst = logutil.ProtoToTime(ti.MasterTermStartTime).Format(time.RFC3339)
	}
	return fmt.Sprintf("%v %v %v %v %v %v %v %v", topoproto.TabletAliasString(ti.Alias), keyspace, shard, topoproto.TabletTypeLString(ti.Type), ti.Addr(), ti.MysqlAddr(), fmtMapAwkable(ti.Tags), mtst)
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
		tablet.MysqlPort = int32(*mysqlPort)
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
				tablet.MysqlPort = int32(*mysqlPort)
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

func commandStartReplication(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action StartReplication requires <tablet alias>")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().StartReplication(ctx, ti.Tablet)
}

func commandStopReplication(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action StopReplication requires <tablet alias>")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	ti, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("failed reading tablet %v: %v", tabletAlias, err)
	}
	return wr.TabletManagerClient().StopReplication(ctx, ti.Tablet)
}

func commandChangeTabletType(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	dryRun := subFlags.Bool("dry-run", false, "Lists the proposed change without actually executing it")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet alias> and <db type> arguments are required for the ChangeTabletType command")
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
	return wr.ChangeTabletType(ctx, tabletAlias, newType)
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
	return wr.RefreshTabletsByShard(ctx, si, cells)
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
	servedType, err := topo.ParseServingTabletType(subFlags.Arg(1))
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
	force := subFlags.Bool("force", false, "Proceeds with the command even if the shard already exists")
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

	resp, err := wr.VtctldServer().ShardReplicationPositions(ctx, &vtctldatapb.ShardReplicationPositionsRequest{
		Keyspace: keyspace,
		Shard:    shard,
	})
	if err != nil {
		return err
	}

	lines := make([]string, 0, 24)
	for _, rt := range cli.SortedReplicatingTablets(resp.TabletMap, resp.ReplicationStatuses) {
		status := rt.Status
		tablet := rt.Tablet
		if status == nil {
			lines = append(lines, cli.MarshalTabletAWK(tablet)+" <err> <err> <err>")
		} else {
			lines = append(lines, cli.MarshalTabletAWK(tablet)+fmt.Sprintf(" %v %v", status.Position, status.SecondsBehindMaster))
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

	resp, err := wr.VtctldServer().GetTablets(ctx, &vtctldatapb.GetTabletsRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Strict:   false,
	})
	if err != nil {
		return err
	}

	for _, tablet := range resp.Tablets {
		wr.Logger().Printf("%v\n", cli.MarshalTabletAWK(tablet))
	}

	return nil
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
	tabletType, err := topo.ParseServingTabletType(subFlags.Arg(1))
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
	tabletType, err := topo.ParseServingTabletType(subFlags.Arg(1))
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
	keyspaceType := subFlags.String("keyspace_type", "", "Specifies the type of the keyspace")
	baseKeyspace := subFlags.String("base_keyspace", "", "Specifies the base keyspace for a snapshot keyspace")
	timestampStr := subFlags.String("snapshot_time", "", "Specifies the snapshot time for this keyspace")
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
	ktype := topodatapb.KeyspaceType_NORMAL
	if *keyspaceType != "" {
		kt, err := topoproto.ParseKeyspaceType(*keyspaceType)
		if err != nil {
			wr.Logger().Infof("error parsing keyspace type %v, defaulting to NORMAL", *keyspaceType)
		} else {
			ktype = kt
		}
	}

	var snapshotTime *vttime.Time
	if ktype == topodatapb.KeyspaceType_SNAPSHOT {
		if *baseKeyspace == "" {
			return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "base_keyspace must be specified while creating a snapshot keyspace")
		}
		if _, err := wr.TopoServer().GetKeyspace(ctx, *baseKeyspace); err != nil {
			return vterrors.Wrapf(err, "Cannot find base_keyspace: %v", *baseKeyspace)
		}
		// process snapshot_time
		if *timestampStr == "" {
			return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "snapshot_time must be specified when creating a snapshot keyspace")
		}
		timeTime, err := time.Parse(time.RFC3339, *timestampStr)
		if err != nil {
			return err
		}
		if timeTime.After(time.Now()) {
			return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "snapshot_time can not be more than current time")
		}
		snapshotTime = logutil.TimeToProto(timeTime)
	}
	ki := &topodatapb.Keyspace{
		ShardingColumnName: *shardingColumnName,
		ShardingColumnType: kit,
		KeyspaceType:       ktype,
		BaseKeyspace:       *baseKeyspace,
		SnapshotTime:       snapshotTime,
	}
	if len(servedFrom) > 0 {
		for name, value := range servedFrom {
			tt, err := topo.ParseServingTabletType(name)
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
	if err != nil {
		return err
	}

	if !*allowEmptyVSchema {
		if err := wr.TopoServer().EnsureVSchema(ctx, keyspace); err != nil {
			return err
		}
	}

	if ktype == topodatapb.KeyspaceType_SNAPSHOT {
		// copy vschema from base keyspace
		vs, err := wr.TopoServer().GetVSchema(ctx, *baseKeyspace)
		if err != nil {
			wr.Logger().Infof("error from GetVSchema for base_keyspace: %v, %v", *baseKeyspace, err)
			if topo.IsErrType(err, topo.NoNode) {
				vs = &vschemapb.Keyspace{
					Sharded:                false,
					Tables:                 make(map[string]*vschemapb.Table),
					Vindexes:               make(map[string]*vschemapb.Vindex),
					RequireExplicitRouting: true,
				}
			} else {
				return err
			}
		} else {
			// SNAPSHOT keyspaces are excluded from global routing.
			vs.RequireExplicitRouting = true
		}
		if err := wr.TopoServer().SaveVSchema(ctx, keyspace, vs); err != nil {
			wr.Logger().Infof("error from SaveVSchema %v:%v", vs, err)
			return err
		}
	}

	return wr.TopoServer().RebuildSrvVSchema(ctx, []string{} /* cells */)
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

	keyspaceInfo, err := wr.VtctldServer().GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{
		Keyspace: keyspace,
	})
	if err != nil {
		return err
	}
	// Pass the embedded proto directly or jsonpb will panic.
	return printJSON(wr.Logger(), keyspaceInfo.Keyspace.Keyspace)
}

func commandGetKeyspaces(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	resp, err := wr.VtctldServer().GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		return err
	}

	names := make([]string, len(resp.Keyspaces))
	for i, ks := range resp.Keyspaces {
		names[i] = ks.Name
	}

	wr.Logger().Printf("%v\n", strings.Join(names, "\n"))
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
	allowPartial := subFlags.Bool("allow_partial", false, "Specifies whether a SNAPSHOT keyspace is allowed to serve with an incomplete set of shards. Ignored for all other types of keyspaces")
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
		if err := wr.RebuildKeyspaceGraph(ctx, keyspace, cellArray, *allowPartial); err != nil {
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

func commandReshard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	for _, arg := range args {
		if arg == "-v2" {
			fmt.Println("*** Using Reshard v2 flow ***")
			return commandVRWorkflow(ctx, wr, subFlags, args, wrangler.ReshardWorkflow)
		}
	}
	cells := subFlags.String("cells", "", "Cell(s) or CellAlias(es) (comma-separated) to replicate from.")
	tabletTypes := subFlags.String("tablet_types", "", "Source tablet types to replicate from.")
	skipSchemaCopy := subFlags.Bool("skip_schema_copy", false, "Skip copying of schema to targets")

	autoStart := subFlags.Bool("auto_start", true, "If false, streams will start in the Stopped state and will need to be explicitly started")
	stopAfterCopy := subFlags.Bool("stop_after_copy", false, "Streams will be stopped once the copy phase is completed")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 3 {
		return fmt.Errorf("three arguments are required: <keyspace.workflow>, source_shards, target_shards")
	}
	keyspace, workflow, err := splitKeyspaceWorkflow(subFlags.Arg(0))
	if err != nil {
		return err
	}
	source := strings.Split(subFlags.Arg(1), ",")
	target := strings.Split(subFlags.Arg(2), ",")
	return wr.Reshard(ctx, keyspace, workflow, source, target, *skipSchemaCopy, *cells,
		*tabletTypes, *autoStart, *stopAfterCopy)
}

func commandMoveTables(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	for _, arg := range args {
		if arg == "-v2" {
			fmt.Println("*** Using MoveTables v2 flow ***")
			return commandVRWorkflow(ctx, wr, subFlags, args, wrangler.MoveTablesWorkflow)
		}
	}
	workflow := subFlags.String("workflow", "", "Workflow name. Can be any descriptive string. Will be used to later migrate traffic via SwitchReads/SwitchWrites.")
	cells := subFlags.String("cells", "", "Cell(s) or CellAlias(es) (comma-separated) to replicate from.")
	tabletTypes := subFlags.String("tablet_types", "", "Source tablet types to replicate from (e.g. master, replica, rdonly). Defaults to -vreplication_tablet_type parameter value for the tablet, which has the default value of replica.")
	allTables := subFlags.Bool("all", false, "Move all tables from the source keyspace")
	excludes := subFlags.String("exclude", "", "Tables to exclude (comma-separated) if -all is specified")

	autoStart := subFlags.Bool("auto_start", true, "If false, streams will start in the Stopped state and will need to be explicitly started")
	stopAfterCopy := subFlags.Bool("stop_after_copy", false, "Streams will be stopped once the copy phase is completed")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *workflow == "" {
		return fmt.Errorf("a workflow name must be specified")
	}
	if !*allTables && len(*excludes) > 0 {
		return fmt.Errorf("you can only specify tables to exclude if all tables are to be moved (with -all)")
	}
	if *allTables {
		if subFlags.NArg() != 2 {
			return fmt.Errorf("two arguments are required: source_keyspace, target_keyspace")
		}
	} else {
		if subFlags.NArg() != 3 {
			return fmt.Errorf("three arguments are required: source_keyspace, target_keyspace, tableSpecs")
		}
	}

	source := subFlags.Arg(0)
	target := subFlags.Arg(1)
	tableSpecs := subFlags.Arg(2)
	return wr.MoveTables(ctx, *workflow, source, target, tableSpecs, *cells, *tabletTypes, *allTables,
		*excludes, *autoStart, *stopAfterCopy, "")
}

// VReplicationWorkflowAction defines subcommands passed to vtctl for movetables or reshard
type VReplicationWorkflowAction string

const (
	vReplicationWorkflowActionCreate         = "create"
	vReplicationWorkflowActionSwitchTraffic  = "switchtraffic"
	vReplicationWorkflowActionReverseTraffic = "reversetraffic"
	vReplicationWorkflowActionComplete       = "complete"
	vReplicationWorkflowActionCancel         = "cancel"
	vReplicationWorkflowActionShow           = "show"
	vReplicationWorkflowActionProgress       = "progress"
	vReplicationWorkflowActionGetState       = "getstate"
)

func commandMigrate(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	return commandVRWorkflow(ctx, wr, subFlags, args, wrangler.MigrateWorkflow)
}

// getSourceKeyspace expects a keyspace of the form "externalClusterName.keyspaceName" and returns the components
func getSourceKeyspace(clusterKeyspace string) (clusterName string, sourceKeyspace string, err error) {
	splits := strings.Split(clusterKeyspace, ".")
	if len(splits) != 2 {
		return "", "", fmt.Errorf("invalid format for external source cluster: %s", clusterKeyspace)
	}
	return splits[0], splits[1], nil
}

// commandVRWorkflow is the common entry point for MoveTables/Reshard/Migrate workflows
// FIXME: this needs a refactor. Also validations for params need to be done per workflow type
func commandVRWorkflow(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string,
	workflowType wrangler.VReplicationWorkflowType) error {

	cells := subFlags.String("cells", "", "Cell(s) or CellAlias(es) (comma-separated) to replicate from.")
	tabletTypes := subFlags.String("tablet_types", "master,replica,rdonly", "Source tablet types to replicate from (e.g. master, replica, rdonly). Defaults to -vreplication_tablet_type parameter value for the tablet, which has the default value of replica.")
	dryRun := subFlags.Bool("dry_run", false, "Does a dry run of SwitchReads and only reports the actions to be taken. -dry_run is only supported for SwitchTraffic, ReverseTraffic and Complete.")
	timeout := subFlags.Duration("timeout", 30*time.Second, "Specifies the maximum time to wait, in seconds, for vreplication to catch up on master migrations. The migration will be cancelled on a timeout.")
	reverseReplication := subFlags.Bool("reverse_replication", true, "Also reverse the replication")
	keepData := subFlags.Bool("keep_data", false, "Do not drop tables or shards (if true, only vreplication artifacts are cleaned up)")

	autoStart := subFlags.Bool("auto_start", true, "If false, streams will start in the Stopped state and will need to be explicitly started")
	stopAfterCopy := subFlags.Bool("stop_after_copy", false, "Streams will be stopped once the copy phase is completed")

	// MoveTables and Migrate params
	tables := subFlags.String("tables", "", "A table spec or a list of tables")
	allTables := subFlags.Bool("all", false, "Move all tables from the source keyspace")
	excludes := subFlags.String("exclude", "", "Tables to exclude (comma-separated) if -all is specified")
	sourceKeyspace := subFlags.String("source", "", "Source keyspace")

	// MoveTables-only params
	renameTables := subFlags.Bool("rename_tables", false, "Rename tables instead of dropping them")

	// Reshard params
	sourceShards := subFlags.String("source_shards", "", "Source shards")
	targetShards := subFlags.String("target_shards", "", "Target shards")
	skipSchemaCopy := subFlags.Bool("skip_schema_copy", false, "Skip copying of schema to target shards")

	_ = subFlags.Bool("v2", true, "")

	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 2 {
		return fmt.Errorf("two arguments are needed: action, keyspace.workflow")
	}
	action := subFlags.Arg(0)
	ksWorkflow := subFlags.Arg(1)
	target, workflow, err := splitKeyspaceWorkflow(ksWorkflow)
	if err != nil {
		return err
	}
	_, err = wr.TopoServer().GetKeyspace(ctx, target)
	if err != nil {
		wr.Logger().Errorf("keyspace %s not found", target)
		return err
	}

	vrwp := &wrangler.VReplicationWorkflowParams{
		TargetKeyspace: target,
		Workflow:       workflow,
		DryRun:         *dryRun,
		AutoStart:      *autoStart,
		StopAfterCopy:  *stopAfterCopy,
	}

	printDetails := func() error {
		s := ""
		res, err := wr.ShowWorkflow(ctx, workflow, target)
		if err != nil {
			return err
		}
		s += fmt.Sprintf("Following vreplication streams are running for workflow %s.%s:\n\n", target, workflow)
		for ksShard := range res.ShardStatuses {
			statuses := res.ShardStatuses[ksShard].MasterReplicationStatuses
			for _, st := range statuses {
				now := time.Now().Nanosecond()
				msg := ""
				updateLag := int64(now) - st.TimeUpdated
				if updateLag > 0*1e9 {
					msg += " Vstream may not be running."
				}
				txLag := int64(now) - st.TransactionTimestamp
				msg += fmt.Sprintf(" VStream Lag: %ds.", txLag/1e9)
				if st.TransactionTimestamp > 0 { // if no events occur after copy phase, TransactionTimeStamp can be 0
					msg += fmt.Sprintf(" Tx time: %s.", time.Unix(st.TransactionTimestamp, 0).Format(time.ANSIC))
				}
				s += fmt.Sprintf("id=%d on %s: Status: %s.%s\n", st.ID, ksShard, st.State, msg)
			}
		}
		wr.Logger().Printf("\n%s\n", s)
		return nil
	}

	wrapError := func(wf *wrangler.VReplicationWorkflow, err error) error {
		wr.Logger().Errorf("\n%s\n", err.Error())
		log.Infof("In wrapError wf is %+v", wf)
		wr.Logger().Infof("Workflow Status: %s\n", wf.CurrentState())
		if wf.Exists() {
			printDetails()
		}
		return err
	}

	//TODO: check if invalid parameters were passed in that do not apply to this action
	originalAction := action
	action = strings.ToLower(action) // allow users to input action in a case-insensitive manner
	if workflowType == wrangler.MigrateWorkflow {
		switch action {
		case vReplicationWorkflowActionCreate, vReplicationWorkflowActionCancel, vReplicationWorkflowActionComplete:
		default:
			return fmt.Errorf("invalid action for Migrate: %s", action)
		}
	}

	switch action {
	case vReplicationWorkflowActionCreate:
		switch workflowType {
		case wrangler.MoveTablesWorkflow, wrangler.MigrateWorkflow:
			var sourceTopo *topo.Server
			var externalClusterName string

			sourceTopo = wr.TopoServer()
			if *sourceKeyspace == "" {
				return fmt.Errorf("source keyspace is not specified")
			}
			if workflowType == wrangler.MigrateWorkflow {
				externalClusterName, *sourceKeyspace, err = getSourceKeyspace(*sourceKeyspace)
				if err != nil {
					return err
				}
				sourceTopo, err = sourceTopo.OpenExternalVitessClusterServer(ctx, externalClusterName)
				if err != nil {
					return err
				}
			}

			_, err := sourceTopo.GetKeyspace(ctx, *sourceKeyspace)
			if err != nil {
				wr.Logger().Errorf("keyspace %s not found", *sourceKeyspace)
				return err
			}
			if !*allTables && *tables == "" {
				return fmt.Errorf("no tables specified to move")
			}
			vrwp.SourceKeyspace = *sourceKeyspace
			vrwp.Tables = *tables
			vrwp.AllTables = *allTables
			vrwp.ExcludeTables = *excludes
			vrwp.Timeout = *timeout
			vrwp.ExternalCluster = externalClusterName
		case wrangler.ReshardWorkflow:
			if *sourceShards == "" || *targetShards == "" {
				return fmt.Errorf("source and target shards are not specified")
			}
			vrwp.SourceShards = strings.Split(*sourceShards, ",")
			vrwp.TargetShards = strings.Split(*targetShards, ",")
			vrwp.SkipSchemaCopy = *skipSchemaCopy
			vrwp.SourceKeyspace = target
		default:
			return fmt.Errorf("unknown workflow type passed: %v", workflowType)
		}
		vrwp.Cells = *cells
		vrwp.TabletTypes = *tabletTypes
	case vReplicationWorkflowActionSwitchTraffic, vReplicationWorkflowActionReverseTraffic:
		vrwp.Cells = *cells
		vrwp.TabletTypes = *tabletTypes
		if vrwp.TabletTypes == "" {
			vrwp.TabletTypes = "master,replica,rdonly"
		}
		vrwp.Timeout = *timeout
		vrwp.EnableReverseReplication = *reverseReplication
	case vReplicationWorkflowActionCancel:
		vrwp.KeepData = *keepData
	case vReplicationWorkflowActionComplete:
		switch workflowType {
		case wrangler.MoveTablesWorkflow:
			vrwp.RenameTables = *renameTables
		case wrangler.ReshardWorkflow:
		case wrangler.MigrateWorkflow:
		default:
			return fmt.Errorf("unknown workflow type passed: %v", workflowType)
		}
		vrwp.KeepData = *keepData
	}
	vrwp.WorkflowType = workflowType
	wf, err := wr.NewVReplicationWorkflow(ctx, workflowType, vrwp)
	if err != nil {
		log.Warningf("NewVReplicationWorkflow returned error %+v", wf)
		return err
	}
	if !wf.Exists() && action != vReplicationWorkflowActionCreate {
		return fmt.Errorf("workflow %s does not exist", ksWorkflow)
	}

	printCopyProgress := func() error {
		copyProgress, err := wf.GetCopyProgress()
		if err != nil {
			return err
		}
		if copyProgress == nil {
			wr.Logger().Printf("\nCopy Completed.\n")
		} else {
			wr.Logger().Printf("\nCopy Progress (approx):\n")
			var tables []string
			for table := range *copyProgress {
				tables = append(tables, table)
			}
			sort.Strings(tables)
			s := ""
			var progress wrangler.TableCopyProgress
			for table := range *copyProgress {
				progress = *(*copyProgress)[table]
				rowCountPct := 100.0 * progress.TargetRowCount / progress.SourceRowCount
				tableSizePct := 100.0 * progress.TargetTableSize / progress.SourceTableSize
				s += fmt.Sprintf("%s: rows copied %d/%d (%d%%), size copied %d/%d (%d%%)\n",
					table, progress.TargetRowCount, progress.SourceRowCount, rowCountPct,
					progress.TargetTableSize, progress.SourceTableSize, tableSizePct)
			}
			wr.Logger().Printf("\n%s\n", s)
		}
		return printDetails()

	}

	if *dryRun {
		switch action {
		case vReplicationWorkflowActionSwitchTraffic, vReplicationWorkflowActionReverseTraffic, vReplicationWorkflowActionComplete:
		default:
			return fmt.Errorf("-dry_run is only supported for SwitchTraffic, ReverseTraffic and Complete, not for %s", originalAction)
		}
	}

	var dryRunResults *[]string
	startState := wf.CachedState()
	switch action {
	case vReplicationWorkflowActionShow:
		return printDetails()
	case vReplicationWorkflowActionProgress:
		return printCopyProgress()
	case vReplicationWorkflowActionCreate:
		err = wf.Create()
		if err != nil {
			return err
		}
		if !*autoStart {
			wr.Logger().Printf("Workflow has been created in Stopped state\n")
			break
		}
		wr.Logger().Printf("Waiting for workflow to start:\n")

		type streamCount struct {
			total, running int64
		}
		errCh := make(chan error)
		wfErrCh := make(chan []*wrangler.WorkflowError)
		progressCh := make(chan *streamCount)
		timedCtx, cancelTimedCtx := context.WithTimeout(ctx, *timeout)
		defer cancelTimedCtx()

		go func(ctx context.Context) {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					errCh <- fmt.Errorf("workflow did not start within %s", (*timeout).String())
					return
				case <-ticker.C:
					totalStreams, runningStreams, workflowErrors, err := wf.GetStreamCount()
					if err != nil {
						errCh <- err
						close(errCh)
						return
					}
					if len(workflowErrors) > 0 {
						wfErrCh <- workflowErrors
					}
					progressCh <- &streamCount{
						total:   totalStreams,
						running: runningStreams,
					}
				}
			}
		}(timedCtx)

		for {
			select {
			case progress := <-progressCh:
				if progress.running == progress.total {
					wr.Logger().Printf("\nWorkflow started successfully with %d stream(s)\n", progress.total)
					printDetails()
					return nil
				}
				wr.Logger().Printf("%d%% ... ", 100*progress.running/progress.total)
			case err := <-errCh:
				wr.Logger().Error(err)
				cancelTimedCtx()
				return err
			case wfErrs := <-wfErrCh:
				wr.Logger().Printf("Found problems with the streams created for this workflow:\n")
				for _, wfErr := range wfErrs {
					wr.Logger().Printf("\tTablet: %d, Id: %d :: %s\n", wfErr.Tablet, wfErr.ID, wfErr.Description)
				}
				return fmt.Errorf("errors starting workflow")
			}
		}
	case vReplicationWorkflowActionSwitchTraffic:
		dryRunResults, err = wf.SwitchTraffic(wrangler.DirectionForward)
	case vReplicationWorkflowActionReverseTraffic:
		dryRunResults, err = wf.ReverseTraffic()
	case vReplicationWorkflowActionComplete:
		dryRunResults, err = wf.Complete()
	case vReplicationWorkflowActionCancel:
		err = wf.Cancel()
	case vReplicationWorkflowActionGetState:
		wr.Logger().Printf(wf.CachedState() + "\n")
		return nil
	default:
		return fmt.Errorf("found unsupported action %s", originalAction)
	}
	if err != nil {
		log.Warningf(" %s error: %v", originalAction, wf)
		return wrapError(wf, err)
	}
	if *dryRun {
		if len(*dryRunResults) > 0 {
			wr.Logger().Printf("Dry Run results for %s run at %s\nParameters: %s\n\n", time.RFC822, originalAction, strings.Join(args, " "))
			wr.Logger().Printf("%s\n", strings.Join(*dryRunResults, "\n"))
			return nil
		}
	}
	wr.Logger().Printf("%s was successful\nStart State: %s\nCurrent State: %s\n\n",
		originalAction, startState, wf.CurrentState())
	return nil
}

func commandCreateLookupVindex(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cells := subFlags.String("cells", "", "Source cells to replicate from.")
	//TODO: keep -cell around for backward compatibility and remove it in a future version
	cell := subFlags.String("cell", "", "Cell to replicate from.")
	tabletTypes := subFlags.String("tablet_types", "", "Source tablet types to replicate from.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("two arguments are required: keyspace and json_spec")
	}
	if *cells == "" && *cell != "" {
		*cells = *cell
	}
	keyspace := subFlags.Arg(0)
	specs := &vschemapb.Keyspace{}
	if err := json2.Unmarshal([]byte(subFlags.Arg(1)), specs); err != nil {
		return err
	}
	return wr.CreateLookupVindex(ctx, keyspace, specs, *cell, *tabletTypes)
}

func commandExternalizeVindex(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("one argument is required: keyspace.vindex")
	}
	return wr.ExternalizeVindex(ctx, subFlags.Arg(0))
}

func commandMaterialize(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cells := subFlags.String("cells", "", "Source cells to replicate from.")
	tabletTypes := subFlags.String("tablet_types", "", "Source tablet types to replicate from.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("a single argument is required: <json_spec>")
	}
	ms := &vtctldatapb.MaterializeSettings{}
	if err := json2.Unmarshal([]byte(subFlags.Arg(0)), ms); err != nil {
		return err
	}
	ms.Cell = *cells
	ms.TabletTypes = *tabletTypes
	return wr.Materialize(ctx, ms)
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

func commandVDiff(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	sourceCell := subFlags.String("source_cell", "", "The source cell to compare from")
	targetCell := subFlags.String("target_cell", "", "The target cell to compare with")
	tabletTypes := subFlags.String("tablet_types", "master,replica,rdonly", "Tablet types for source and target")
	filteredReplicationWaitTime := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations. The migration will be cancelled on a timeout.")
	maxRows := subFlags.Int64("limit", math.MaxInt64, "Max rows to stop comparing after")
	format := subFlags.String("format", "", "Format of report") //"json" or ""
	tables := subFlags.String("tables", "", "Only run vdiff for these tables in the workflow")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 1 {
		return fmt.Errorf("<keyspace.workflow> is required")
	}
	keyspace, workflow, err := splitKeyspaceWorkflow(subFlags.Arg(0))
	if err != nil {
		return err
	}
	if *maxRows <= 0 {
		return fmt.Errorf("maximum number of rows to compare needs to be greater than 0")
	}
	_, err = wr.
		VDiff(ctx, keyspace, workflow, *sourceCell, *targetCell, *tabletTypes, *filteredReplicationWaitTime, *format, *maxRows, *tables)
	if err != nil {
		log.Errorf("vdiff returning with error: %v", err)
		if strings.Contains(err.Error(), "context deadline exceeded") {
			return fmt.Errorf("vdiff timed out: you may want to increase it with the flag -filtered_replication_wait_time=<timeoutSeconds>")
		}
	}
	return err
}

func splitKeyspaceWorkflow(in string) (keyspace, workflow string, err error) {
	splits := strings.Split(in, ".")
	if len(splits) != 2 {
		return "", "", fmt.Errorf("invalid format for <keyspace.workflow>: %s", in)
	}
	return splits[0], splits[1], nil
}

func commandMigrateServedTypes(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	reverse := subFlags.Bool("reverse", false, "Moves the served tablet type backward instead of forward.")
	skipReFreshState := subFlags.Bool("skip-refresh-state", false, "Skips refreshing the state of the source tablets after the migration, meaning that the refresh will need to be done manually, replica and rdonly only)")
	filteredReplicationWaitTime := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations. The migration will be cancelled on a timeout.")
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
	servedType, err := topo.ParseServingTabletType(subFlags.Arg(1))
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
	reverse := subFlags.Bool("reverse", false, "Moves the served tablet type backward instead of forward.")
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	filteredReplicationWaitTime := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations. The migration will be cancelled on a timeout.")
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

func commandDropSources(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	dryRun := subFlags.Bool("dry_run", false, "Does a dry run of commandDropSources and only reports the actions to be taken")
	renameTables := subFlags.Bool("rename_tables", false, "Rename tables instead of dropping them")
	keepData := subFlags.Bool("keep_data", false, "Do not drop tables or shards (if true, only vreplication artifacts are cleaned up)")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("<keyspace.workflow> is required")
	}
	keyspace, workflow, err := splitKeyspaceWorkflow(subFlags.Arg(0))
	if err != nil {
		return err
	}

	removalType := wrangler.DropTable
	if *renameTables {
		removalType = wrangler.RenameTable
	}

	_, _, _ = dryRun, keyspace, workflow
	dryRunResults, err := wr.DropSources(ctx, keyspace, workflow, removalType, *keepData, false, *dryRun)
	if err != nil {
		return err
	}
	if *dryRun {
		wr.Logger().Printf("Dry Run results for commandDropSources run at %s\nParameters: %s\n\n", time.RFC822, strings.Join(args, " "))
		wr.Logger().Printf("%s\n", strings.Join(*dryRunResults, "\n"))
	}
	return nil
}

func commandSwitchReads(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	reverse := subFlags.Bool("reverse", false, "Moves the served tablet type backward instead of forward.")
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	tabletTypes := subFlags.String("tablet_types", "rdonly,replica", "Tablet types to switch one or both or rdonly/replica")
	deprecatedTabletType := subFlags.String("tablet_type", "", "(DEPRECATED) one of rdonly/replica")
	dryRun := subFlags.Bool("dry_run", false, "Does a dry run of SwitchReads and only reports the actions to be taken")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if !(*deprecatedTabletType == "" || *deprecatedTabletType == "replica" || *deprecatedTabletType == "rdonly") {
		return fmt.Errorf("invalid value specified for -tablet_type: %s", *deprecatedTabletType)
	}

	if *deprecatedTabletType != "" {
		*tabletTypes = *deprecatedTabletType
	}

	tabletTypesArr := strings.Split(*tabletTypes, ",")
	var servedTypes []topodatapb.TabletType
	for _, tabletType := range tabletTypesArr {
		servedType, err := parseTabletType(tabletType, []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY})
		if err != nil {
			return err
		}
		servedTypes = append(servedTypes, servedType)
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}
	direction := wrangler.DirectionForward
	if *reverse {
		direction = wrangler.DirectionBackward
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("<keyspace.workflow> is required")
	}
	keyspace, workflow, err := splitKeyspaceWorkflow(subFlags.Arg(0))
	if err != nil {
		return err
	}
	dryRunResults, err := wr.SwitchReads(ctx, keyspace, workflow, servedTypes, cells, direction, *dryRun)
	if err != nil {
		return err
	}
	if *dryRun {
		wr.Logger().Printf("Dry Run results for SwitchReads run at %s\nParameters: %s\n\n", time.RFC822, strings.Join(args, " "))
		wr.Logger().Printf("%s\n", strings.Join(*dryRunResults, "\n"))
	}
	return nil
}

func commandSwitchWrites(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	timeout := subFlags.Duration("timeout", 30*time.Second, "Specifies the maximum time to wait, in seconds, for vreplication to catch up on master migrations. The migration will be cancelled on a timeout.")
	filteredReplicationWaitTime := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "DEPRECATED Specifies the maximum time to wait, in seconds, for vreplication to catch up on master migrations. The migration will be cancelled on a timeout.")
	reverseReplication := subFlags.Bool("reverse_replication", true, "Also reverse the replication")
	cancel := subFlags.Bool("cancel", false, "Cancel the failed migration and serve from source")
	reverse := subFlags.Bool("reverse", false, "Reverse a previous SwitchWrites serve from source")
	dryRun := subFlags.Bool("dry_run", false, "Does a dry run of SwitchWrites and only reports the actions to be taken")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 1 {
		return fmt.Errorf("<keyspace.workflow> is required")
	}
	keyspace, workflow, err := splitKeyspaceWorkflow(subFlags.Arg(0))
	if err != nil {
		return err
	}
	if filteredReplicationWaitTime != timeout {
		timeout = filteredReplicationWaitTime
	}

	journalID, dryRunResults, err := wr.SwitchWrites(ctx, keyspace, workflow, *timeout, *cancel, *reverse, *reverseReplication, *dryRun)
	if err != nil {
		return err
	}
	if *dryRun {
		wr.Logger().Printf("Dry Run results for SwitchWrites run at %s\nParameters: %s\n\n", time.RFC822, strings.Join(args, " "))
		wr.Logger().Printf("%s\n", strings.Join(*dryRunResults, "\n"))
	} else {
		wr.Logger().Infof("Migration Journal ID: %v", journalID)
	}

	return nil
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
	result, err := wr.VtctldServer().FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{
		Keyspace: keyspace,
	})
	if err != nil {
		return err
	}

	// reformat data into structure of old interface
	legacyShardMap := make(map[string]*topodatapb.Shard, len(result.Shards))

	for _, shard := range result.Shards {
		legacyShardMap[shard.Name] = shard.Shard
	}

	return printJSON(wr.Logger(), legacyShardMap)
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

	if subFlags.NArg() == 1 {
		cells = strings.Split(subFlags.Arg(0), ",")
	}

	resp, err := wr.VtctldServer().GetTablets(ctx, &vtctldatapb.GetTabletsRequest{
		Cells:  cells,
		Strict: false,
	})

	if err != nil {
		return err
	}

	for _, tablet := range resp.Tablets {
		wr.Logger().Printf("%v\n", cli.MarshalTabletAWK(tablet))
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

	resp, err := wr.VtctldServer().GetTablets(ctx, &vtctldatapb.GetTabletsRequest{
		TabletAliases: aliases,
		Strict:        false,
	})
	if err != nil {
		return err
	}

	for _, tablet := range resp.Tablets {
		wr.Logger().Printf("%v\n", cli.MarshalTabletAWK(tablet))
	}

	return nil
}

func commandGetSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	tables := subFlags.String("tables", "", "Specifies a comma-separated list of tables for which we should gather information. Each is either an exact match, or a regular expression of the form /regexp/")
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the output")
	tableNamesOnly := subFlags.Bool("table_names_only", false, "Only displays table names that match")
	tableSizesOnly := subFlags.Bool("table_sizes_only", false, "Only displays size information for tables. Ignored if -table_names_only is passed.")

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

	resp, err := wr.VtctldServer().GetSchema(ctx, &vtctldatapb.GetSchemaRequest{
		TabletAlias:    tabletAlias,
		Tables:         tableArray,
		ExcludeTables:  excludeTableArray,
		IncludeViews:   *includeViews,
		TableNamesOnly: *tableNamesOnly,
		TableSizesOnly: *tableSizesOnly,
	})
	if err != nil {
		return err
	}

	if *tableNamesOnly {
		for _, td := range resp.Schema.TableDefinitions {
			wr.Logger().Printf("%v\n", td.Name)
		}
		return nil
	}

	return printJSON(wr.Logger(), resp.Schema)
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
	skipNoMaster := subFlags.Bool("skip-no-master", false, "Skip shards that don't have master when performing validation")
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
	return wr.ValidateSchemaKeyspace(ctx, keyspace, excludeTableArray, *includeViews, *skipNoMaster)
}

func commandApplySchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	allowLongUnavailability := subFlags.Bool("allow_long_unavailability", false, "Allow large schema changes which incur a longer unavailability of the database.")
	sql := subFlags.String("sql", "", "A list of semicolon-delimited SQL commands")
	sqlFile := subFlags.String("sql-file", "", "Identifies the file that contains the SQL commands")
	ddlStrategy := subFlags.String("ddl_strategy", string(schema.DDLStrategyDirect), "Online DDL strategy, compatible with @@ddl_strategy session variable (examples: 'gh-ost', 'pt-osc', 'gh-ost --max-load=Threads_running=100'")
	waitReplicasTimeout := subFlags.Duration("wait_replicas_timeout", wrangler.DefaultWaitReplicasTimeout, "The amount of time to wait for replicas to receive the schema change via replication.")
	skipPreflight := subFlags.Bool("skip_preflight", false, "Skip pre-apply schema checks, and dircetly forward schema change query to shards")
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
	executionUUID, err := schema.CreateUUID()
	if err != nil {
		return err
	}
	requestContext := fmt.Sprintf("vtctl:%s", executionUUID)
	executor := schemamanager.NewTabletExecutor(requestContext, wr, *waitReplicasTimeout)
	if *allowLongUnavailability {
		executor.AllowBigSchemaChange()
	}
	if *skipPreflight {
		executor.SkipPreflight()
	}
	if err := executor.SetDDLStrategy(*ddlStrategy); err != nil {
		return nil
	}

	return schemamanager.Run(
		ctx,
		schemamanager.NewPlainController(change, keyspace),
		executor,
	)
}

func commandOnlineDDL(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() < 1 {
		return fmt.Errorf("the <keyspace> argument is required for the OnlineDDL command")
	}
	keyspace := subFlags.Args()[0]
	if subFlags.NArg() < 2 {
		return fmt.Errorf("the <command> argument is required for the OnlineDDL command")
	}
	command := subFlags.Args()[1]
	arg := ""
	if subFlags.NArg() >= 3 {
		arg = subFlags.Args()[2]
	}
	query := ""
	uuid := ""
	var bindErr error
	switch command {
	case "show":
		{
			condition := ""
			switch arg {
			case "", "all":
				condition = "migration_uuid like '%'"
			case "recent":
				condition = "requested_timestamp > now() - interval 1 week"
			case
				string(schema.OnlineDDLStatusCancelled),
				string(schema.OnlineDDLStatusQueued),
				string(schema.OnlineDDLStatusReady),
				string(schema.OnlineDDLStatusRunning),
				string(schema.OnlineDDLStatusComplete),
				string(schema.OnlineDDLStatusFailed):
				condition, bindErr = sqlparser.ParseAndBind("migration_status=%a", sqltypes.StringBindVariable(arg))
			default:
				if schema.IsOnlineDDLUUID(arg) {
					uuid = arg
					condition, bindErr = sqlparser.ParseAndBind("migration_uuid=%a", sqltypes.StringBindVariable(arg))
				} else {
					condition, bindErr = sqlparser.ParseAndBind("migration_context=%a", sqltypes.StringBindVariable(arg))
				}
			}
			query = fmt.Sprintf(`select
				shard, mysql_schema, mysql_table, ddl_action, migration_uuid, strategy, started_timestamp, completed_timestamp, migration_status
				from _vt.schema_migrations where %s`, condition)
		}
	case "retry":
		{
			if arg == "" {
				return fmt.Errorf("UUID required")
			}
			uuid = arg
			query, bindErr = sqlparser.ParseAndBind(`update _vt.schema_migrations set migration_status='retry' where migration_uuid=%a`, sqltypes.StringBindVariable(arg))
		}
	case "cancel":
		{
			if arg == "" {
				return fmt.Errorf("UUID required")
			}
			uuid = arg
			query, bindErr = sqlparser.ParseAndBind(`update _vt.schema_migrations set migration_status='cancel' where migration_uuid=%a`, sqltypes.StringBindVariable(arg))
		}
	case "cancel-all":
		{
			if arg != "" {
				return fmt.Errorf("UUID not allowed in %s", command)
			}
			query = `update _vt.schema_migrations set migration_status='cancel-all'`
		}
	case "revert":
		{
			if arg == "" {
				return fmt.Errorf("UUID required")
			}
			uuid = arg
			contextUUID, err := schema.CreateUUID()
			if err != nil {
				return err
			}
			requestContext := fmt.Sprintf("vtctl:%s", contextUUID)

			onlineDDL, err := schema.NewOnlineDDL(keyspace, "", fmt.Sprintf("revert %s", uuid), schema.DDLStrategyOnline, "", requestContext)
			if err != nil {
				return err
			}
			conn, err := wr.TopoServer().ConnForCell(ctx, topo.GlobalCell)
			if err != nil {
				return err
			}
			err = onlineDDL.WriteTopo(ctx, conn, schema.MigrationRequestsPath())
			if err != nil {
				return err
			}
			wr.Logger().Infof("UUID=%+v", onlineDDL.UUID)
			wr.Logger().Printf("%s\n", onlineDDL.UUID)
			return nil
		}
	default:
		return fmt.Errorf("Unknown OnlineDDL command: %s", command)
	}
	if bindErr != nil {
		return fmt.Errorf("Error generating OnlineDDL query: %+v", bindErr)
	}

	qr, err := wr.VExecResult(ctx, uuid, keyspace, query, false)
	if err != nil {
		return err
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandCopySchemaShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	tables := subFlags.String("tables", "", "Specifies a comma-separated list of tables to copy. Each is either an exact match, or a regular expression of the form /regexp/")
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	includeViews := subFlags.Bool("include-views", true, "Includes views in the output")
	skipVerify := subFlags.Bool("skip-verify", false, "Skip verification of source and target schema after copy")
	// for backwards compatibility
	waitReplicasTimeout := subFlags.Duration("wait_replicas_timeout", wrangler.DefaultWaitReplicasTimeout, "The amount of time to wait for replicas to receive the schema change via replication.")
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
		return wr.CopySchemaShardFromShard(ctx, tableArray, excludeTableArray, *includeViews, sourceKeyspace, sourceShard, destKeyspace, destShard, *waitReplicasTimeout, *skipVerify)
	}
	sourceTabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err == nil {
		return wr.CopySchemaShard(ctx, sourceTabletAlias, tableArray, excludeTableArray, *includeViews, destKeyspace, destShard, *waitReplicasTimeout, *skipVerify)
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
		ddl, ok := stmt.(*sqlparser.AlterVschema)
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

	if _, err := wr.TopoServer().GetKeyspace(ctx, keyspace); err != nil {
		if strings.Contains(err.Error(), "node doesn't exist") {
			return fmt.Errorf("keyspace(%s) doesn't exist, check if the keyspace is initialized", keyspace)
		}
		return err
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
	routingRulesFile := subFlags.String("rules_file", "", "Specify rules in a file")
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

	cell := subFlags.Arg(0)
	keyspace := subFlags.Arg(1)

	resp, err := wr.VtctldServer().GetSrvKeyspaces(ctx, &vtctldatapb.GetSrvKeyspacesRequest{
		Keyspace: keyspace,
		Cells:    []string{cell},
	})
	if err != nil {
		return err
	}

	return printJSON(wr.Logger(), resp.SrvKeyspaces[cell])
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

func commandVExec(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")
	dryRun := subFlags.Bool("dry_run", false, "Does a dry run of VExec and only reports the final query and list of masters on which it will be applied")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("usage: VExec --dry-run keyspace.workflow \"<query>\"")
	}
	keyspace, workflow, err := splitKeyspaceWorkflow(subFlags.Arg(0))
	if err != nil {
		return err
	}
	_, err = wr.TopoServer().GetKeyspace(ctx, keyspace)
	if err != nil {
		wr.Logger().Errorf("keyspace %s not found", keyspace)
	}
	query := subFlags.Arg(1)

	qr, err := wr.VExecResult(ctx, workflow, keyspace, query, *dryRun)
	if err != nil {
		return err
	}
	if *dryRun {
		return nil
	}
	if qr == nil {
		wr.Logger().Printf("no result returned\n")
	}
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandWorkflow(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	dryRun := subFlags.Bool("dry_run", false, "Does a dry run of Workflow and only reports the final query and list of masters on which the operation will be applied")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("usage: Workflow --dry-run keyspace[.workflow] start/stop/delete/list/listall")
	}
	keyspace := subFlags.Arg(0)
	action := strings.ToLower(subFlags.Arg(1))
	// Note: List is deprecated and replaced by show.
	if action == "list" {
		action = "show"
	}
	var workflow string
	var err error
	if action != "listall" {
		keyspace, workflow, err = splitKeyspaceWorkflow(subFlags.Arg(0))
		if err != nil {
			return err
		}
	}
	_, err = wr.TopoServer().GetKeyspace(ctx, keyspace)
	if err != nil {
		wr.Logger().Errorf("Keyspace %s not found", keyspace)
	}

	results, err := wr.WorkflowAction(ctx, workflow, keyspace, action, *dryRun)
	if err != nil {
		return err
	}
	if action == "show" || action == "listall" {
		return nil
	}
	if len(results) == 0 {
		wr.Logger().Printf("no result returned\n")
		return nil
	}
	qr := wr.QueryResultForRowsAffected(results)

	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandMount(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	clusterType := subFlags.String("type", "vitess", "Specify cluster type: mysql or vitess, only vitess clustered right now")
	unmount := subFlags.Bool("unmount", false, "Unmount cluster")
	show := subFlags.Bool("show", false, "Display contents of cluster")
	list := subFlags.Bool("list", false, "List all clusters")

	// vitess cluster params
	topoType := subFlags.String("topo_type", "", "Type of cluster's topology server")
	topoServer := subFlags.String("topo_server", "", "Server url of cluster's topology server")
	topoRoot := subFlags.String("topo_root", "", "Root node of cluster's topology")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *list {
		clusters, err := wr.TopoServer().GetExternalVitessClusters(ctx)
		if err != nil {
			return err
		}
		wr.Logger().Printf("%s\n", strings.Join(clusters, ","))
		return nil
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("cluster name needs to be provided")
	}

	clusterName := subFlags.Arg(0)
	switch *clusterType {
	case "vitess":
		switch {
		case *unmount:
			return wr.UnmountExternalVitessCluster(ctx, clusterName)
		case *show:
			vci, err := wr.TopoServer().GetExternalVitessCluster(ctx, clusterName)
			if err != nil {
				return err
			}
			if vci == nil {
				return fmt.Errorf("there is no vitess cluster named %s", clusterName)
			}
			data, err := json.Marshal(vci)
			if err != nil {
				return err
			}
			wr.Logger().Printf("%s\n", string(data))
			return nil
		default:
			return wr.MountExternalVitessCluster(ctx, clusterName, *topoType, *topoServer, *topoRoot)
		}
	case "mysql":
		return fmt.Errorf("mysql cluster type not yet supported")
	default:
		return fmt.Errorf("cluster type can be only one of vitess or mysql")
	}
}

func commandGenerateShardRanges(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	numShards := subFlags.Int("num_shards", 2, "Number of shards to generate shard ranges for.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}

	shardRanges, err := generateShardRanges(*numShards)
	if err != nil {
		return err
	}

	return printJSON(wr.Logger(), shardRanges)
}

func generateShardRanges(shards int) ([]string, error) {
	var format string
	var maxShards int

	switch {
	case shards <= 0:
		return nil, errors.New("shards must be greater than zero")
	case shards <= 256:
		format = "%02x"
		maxShards = 256
	case shards <= 65536:
		format = "%04x"
		maxShards = 65536
	default:
		return nil, errors.New("this tool does not support more than 65336 shards in a single keyspace")
	}

	rangeFormatter := func(start, end int) string {
		var (
			startKid string
			endKid   string
		)

		if start != 0 {
			startKid = fmt.Sprintf(format, start)
		}

		if end != maxShards {
			endKid = fmt.Sprintf(format, end)
		}

		return fmt.Sprintf("%s-%s", startKid, endKid)
	}

	start := 0
	end := 0

	// If shards does not divide evenly into maxShards, then there is some lossiness,
	// where each shard is smaller than it should technically be (if, for example, size == 25.6).
	// If we choose to keep everything in ints, then we have two choices:
	// 	- Have every shard in #numshards be a uniform size, tack on an additional shard
	//	  at the end of the range to account for the loss. This is bad because if you ask for
	//	  7 shards, you'll actually get 7 uniform shards with 1 small shard, for 8 total shards.
	//	  It's also bad because one shard will have much different data distribution than the rest.
	//	- Expand the final shard to include whatever is left in the keyrange. This will give the
	//	  correct number of shards, which is good, but depending on how lossy each individual shard is,
	//	  you could end with that final shard being significantly larger than the rest of the shards,
	//	  so this doesn't solve the data distribution problem.
	//
	// By tracking the "real" end (both in the real number sense, and in the truthfulness of the value sense),
	// we can re-truncate the integer end on each iteration, which spreads the lossiness more
	// evenly across the shards.
	//
	// This implementation has no impact on shard numbers that are powers of 2, even at large numbers,
	// which you can see in the tests.
	size := float64(maxShards) / float64(shards)
	realEnd := float64(0)
	shardRanges := make([]string, 0, shards)

	for i := 1; i <= shards; i++ {
		realEnd = float64(i) * size

		end = int(realEnd)
		shardRanges = append(shardRanges, rangeFormatter(start, end))
		start = end
	}

	return shardRanges, nil
}

func commandPanic(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	panic(fmt.Errorf("this command panics on purpose"))
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
