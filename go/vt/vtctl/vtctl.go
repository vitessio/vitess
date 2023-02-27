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
  -- drained: A tablet that is reserved for a background process.
  -- experimental: A replica copy of data that is ready but not serving query
                   traffic. The value indicates a special characteristic of
                   the tablet that indicates the tablet should not be
                   considered a potential primary. Vitess also does not
                   worry about lag for experimental tablets when reparenting.
  -- primary: A primary copy of data
  -- master: Deprecated, same as primary
  -- rdonly: A replica copy of data for OLAP load patterns
  -- replica: A replica copy of data ready to be promoted to primary
  -- restore: A tablet that is restoring from a snapshot. Typically, this
              happens at tablet startup, then it goes to its right state.
  -- spare: A replica copy of data that is ready but not serving query traffic.
            The data could be a potential primary tablet.
*/

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/discovery"
	hk "vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	// ErrUnknownCommand is returned for an unknown command
	ErrUnknownCommand = errors.New("unknown command")

	// Flag variables.
	healthCheckRetryDelay = 5 * time.Second
	healthCheckTimeout    = time.Minute
)

func init() {
	servenv.OnParseFor("vtctl", registerFlags)
	servenv.OnParseFor("vtctld", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	// TODO: https://github.com/vitessio/vitess/issues/11973
	// Then remove this function and associated code (NewHealthCheck, servenv
	// OnParseFor hooks, etc) entirely.
	fs.Duration("vtctl_healthcheck_topology_refresh", 30*time.Second, "refresh interval for re-reading the topology")
	fs.MarkDeprecated("vtctl_healthcheck_topology_refresh", "")

	fs.DurationVar(&healthCheckRetryDelay, "vtctl_healthcheck_retry_delay", healthCheckRetryDelay, "delay before retrying a failed healthcheck")
	fs.MarkDeprecated("vtctl_healthcheck_retry_delay", "This is used only by the legacy vtctld UI that is already deprecated and will be removed in the next release.")
	fs.DurationVar(&healthCheckTimeout, "vtctl_healthcheck_timeout", healthCheckTimeout, "the health check timeout period")
	fs.MarkDeprecated("vtctl_healthcheck_timeout", "This is used only by the legacy vtctld UI that is already deprecated and will be removed in the next release.")
}

// NewHealthCheck returns a healthcheck implementation based on the vtctl flags.
// It is exported for use in go/vt/vtctld.
func NewHealthCheck(ctx context.Context, ts *topo.Server, local string, cellsToWatch []string) discovery.HealthCheck {
	return discovery.NewHealthCheck(ctx, healthCheckRetryDelay, healthCheckTimeout, ts, local, strings.Join(cellsToWatch, ","))
}

type command struct {
	name   string
	method func(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error
	params string
	help   string // if help is empty, won't list the command

	// if set, PrintAllCommands will not show this command
	hidden bool

	// (ajm188) - before we transition to cobra, we need to know whether to
	// strip off a -- after the action (i.e. "command") name in RunCommand so
	// that parsing continues to work.
	disableFlagInterspersal bool

	// deprecation support
	deprecated   bool
	deprecatedBy string
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
			{
				name:       "InitTablet",
				method:     commandInitTablet,
				params:     "[--allow_update] [--allow_different_shard] [--allow_master_override] [--parent] [--db_name_override=<db name>] [--hostname=<hostname>] [--mysql_port=<port>] [--port=<port>] [--grpc_port=<port>] [--tags=tag1:value1,tag2:value2] --keyspace=<keyspace> --shard=<shard> <tablet alias> <tablet type>",
				help:       "Initializes a tablet in the topology.",
				deprecated: true,
			},
			{
				name:   "GetTablet",
				method: commandGetTablet,
				params: "<tablet alias>",
				help:   "Outputs a JSON structure that contains information about the Tablet.",
			},
			{
				name:       "UpdateTabletAddrs",
				method:     commandUpdateTabletAddrs,
				params:     "[--hostname <hostname>] [--ip-addr <ip addr>] [--mysql-port <mysql port>] [--vt-port <vt port>] [--grpc-port <grpc port>] <tablet alias> ",
				help:       "Updates the IP address and port numbers of a tablet.",
				deprecated: true,
			},
			{
				name:   "DeleteTablet",
				method: commandDeleteTablet,
				params: "[--allow_primary] <tablet alias> ...",
				help:   "Deletes tablet(s) from the topology.",
			},
			{
				name:   "SetReadOnly",
				method: commandSetReadOnly,
				params: "<tablet alias>",
				help:   "Sets the tablet as read-only.",
			},
			{
				name:   "SetReadWrite",
				method: commandSetReadWrite,
				params: "<tablet alias>",
				help:   "Sets the tablet as read-write.",
			},
			{
				name:   "StartReplication",
				method: commandStartReplication,
				params: "<table alias>",
				help:   "Starts replication on the specified tablet.",
			},
			{
				name:   "StopReplication",
				method: commandStopReplication,
				params: "<tablet alias>",
				help:   "Stops replication on the specified tablet.",
			},
			{
				name:   "ChangeTabletType",
				method: commandChangeTabletType,
				params: "[--dry-run] <tablet alias> <tablet type>",
				help: "Changes the db type for the specified tablet, if possible. This command is used primarily to arrange replicas, and it will not convert a primary.\n" +
					"NOTE: This command automatically updates the serving graph.\n",
			},
			{
				name:   "Ping",
				method: commandPing,
				params: "<tablet alias>",
				help:   "Checks that the specified tablet is awake and responding to RPCs. This command can be blocked by other in-flight operations.",
			},
			{
				name:   "RefreshState",
				method: commandRefreshState,
				params: "<tablet alias>",
				help:   "Reloads the tablet record on the specified tablet.",
			},
			{
				name:   "RefreshStateByShard",
				method: commandRefreshStateByShard,
				params: "[--cells=c1,c2,...] <keyspace/shard>",
				help:   "Runs 'RefreshState' on all tablets in the given shard.",
			},
			{
				name:   "RunHealthCheck",
				method: commandRunHealthCheck,
				params: "<tablet alias>",
				help:   "Runs a health check on a remote tablet.",
			},
			{
				name:   "Sleep",
				method: commandSleep,
				params: "<tablet alias> <duration>",
				help:   "Blocks the action queue on the specified tablet for the specified amount of time. This is typically used for testing.",
			},
			{
				name:   "ExecuteHook",
				method: commandExecuteHook,
				params: "<tablet alias> <hook name> [<param1=value1> <param2=value2> ...]",
				help: "Runs the specified hook on the given tablet. A hook is a script that resides in the $VTROOT/vthook directory. You can put any script into that directory and use this command to run that script.\n" +
					"For this command, the param=value arguments are parameters that the command passes to the specified hook.",
				disableFlagInterspersal: true,
			},
			{
				name:   "ExecuteFetchAsApp",
				method: commandExecuteFetchAsApp,
				params: "[--max_rows=10000] [--json] [--use_pool] <tablet alias> <sql command>",
				help:   "Runs the given SQL command as a App on the remote tablet.",
			},
			{
				name:   "ExecuteFetchAsDba",
				method: commandExecuteFetchAsDba,
				params: "[--max_rows=10000] [--disable_binlogs] [--json] <tablet alias> <sql command>",
				help:   "Runs the given SQL command as a DBA on the remote tablet.",
			},
			{
				name:         "VReplicationExec",
				method:       commandVReplicationExec,
				params:       "[--json] <tablet alias> <sql command>",
				help:         "Runs the given VReplication command on the remote tablet.",
				deprecated:   true,
				deprecatedBy: "Workflow -- <keyspace.workflow> <action>",
			},
		},
	},
	{
		"Shards", []command{
			{
				name:   "CreateShard",
				method: commandCreateShard,
				params: "[--force] [--parent] <keyspace/shard>",
				help:   "Creates the specified shard.",
			},
			{
				name:   "GetShard",
				method: commandGetShard,
				params: "<keyspace/shard>",
				help:   "Outputs a JSON structure that contains information about the Shard.",
			},
			{
				name:   "ValidateShard",
				method: commandValidateShard,
				params: "[--ping-tablets] <keyspace/shard>",
				help:   "Validates that all nodes that are reachable from this shard are consistent.",
			},
			{
				name:   "ShardReplicationPositions",
				method: commandShardReplicationPositions,
				params: "<keyspace/shard>",
				help:   "Shows the replication status of each replica in the shard graph. In this case, the status refers to the replication lag between the primary vttablet and the replica vttablet. In Vitess, data is always written to the primary vttablet first and then replicated to all replica vttablets. Output is sorted by tablet type, then replication position. Use ctrl-C to interrupt command and see partial result if needed.",
			},
			{
				name:   "ListShardTablets",
				method: commandListShardTablets,
				params: "<keyspace/shard>",
				help:   "Lists all tablets in the specified shard.",
			},
			{
				name:   "SetShardIsPrimaryServing",
				method: commandSetShardIsPrimaryServing,
				params: "<keyspace/shard> <is_serving>",
				help:   "Add or remove a shard from serving. This is meant as an emergency function. It does not rebuild any serving graph i.e. does not run 'RebuildKeyspaceGraph'.",
			},
			{
				name:   "SetShardTabletControl",
				method: commandSetShardTabletControl,
				params: "[--cells=c1,c2,...] [--denied_tables=t1,t2,...] [--remove] [--disable_query_service] <keyspace/shard> <tablet type>",
				help: "Sets the TabletControl record for a shard and tablet type. Only use this for an emergency fix.\n" +
					"To set the DisableQueryServiceFlag, keep 'denied_tables' empty, and set 'disable_query_service' to true or false.\n" +
					"To change the list of denied tables, specify the 'denied_tables' parameter with the new list.\n" +
					"To just remove the ShardTabletControl entirely, use the 'remove' flag.",
			},
			{
				name:   "UpdateSrvKeyspacePartition",
				method: commandUpdateSrvKeyspacePartition,
				params: "[--cells=c1,c2,...] [--remove] <keyspace/shard> <tablet type>",
				help:   "Updates KeyspaceGraph partition for a shard and tablet type. Only use this for emergency fixes. Specify the remove flag, if you want the shard to be removed from the desired partition.",
			},
			{
				name:   "SourceShardDelete",
				method: commandSourceShardDelete,
				params: "<keyspace/shard> <uid>",
				help:   "Deletes the SourceShard record with the provided index. This is meant as an emergency cleanup function. It does not call RefreshState for the shard primary.",
			},
			{
				name:   "SourceShardAdd",
				method: commandSourceShardAdd,
				params: "[--key_range=<keyrange>] [--tables=<table1,table2,...>] <keyspace/shard> <uid> <source keyspace/shard>",
				help:   "Adds the SourceShard record with the provided index. This is meant as an emergency function. It does not call RefreshState for the shard primary.",
			},
			{
				name:   "ShardReplicationAdd",
				method: commandShardReplicationAdd,
				params: "<keyspace/shard> <tablet alias> <parent tablet alias>",
				help:   "Adds an entry to the replication graph in the given cell.",
				hidden: true,
			},
			{
				name:   "ShardReplicationRemove",
				method: commandShardReplicationRemove,
				params: "<keyspace/shard> <tablet alias>",
				help:   "Removes an entry from the replication graph in the given cell.",
				hidden: true,
			},
			{
				name:   "ShardReplicationFix",
				method: commandShardReplicationFix,
				params: "<cell> <keyspace/shard>",
				help:   "Walks through a ShardReplication object and fixes the first error that it encounters.",
			},
			{
				name:   "WaitForFilteredReplication",
				method: commandWaitForFilteredReplication,
				params: "[--max_delay <max_delay, default 30s>] <keyspace/shard>",
				help:   "Blocks until the specified shard has caught up with the filtered replication of its source shard.",
			},
			{
				name:   "RemoveShardCell",
				method: commandRemoveShardCell,
				params: "[--force] [--recursive] <keyspace/shard> <cell>",
				help:   "Removes the cell from the shard's Cells list.",
			},
			{
				name:   "DeleteShard",
				method: commandDeleteShard,
				params: "[--recursive] [--even_if_serving] <keyspace/shard> ...",
				help:   "Deletes the specified shard(s). In recursive mode, it also deletes all tablets belonging to the shard. Otherwise, there must be no tablets left in the shard.",
			},
		},
	},
	{
		"Keyspaces", []command{
			{
				name:   "CreateKeyspace",
				method: commandCreateKeyspace,
				params: "[--sharding_column_name=name] [--sharding_column_type=type] [--served_from=tablettype1:ks1,tablettype2:ks2,...] [--force] [--keyspace_type=type] [--base_keyspace=base_keyspace] [--snapshot_time=time] [--durability-policy=policy_name] <keyspace name>",
				help:   "Creates the specified keyspace. keyspace_type can be NORMAL or SNAPSHOT. For a SNAPSHOT keyspace you must specify the name of a base_keyspace, and a snapshot_time in UTC, in RFC3339 time format, e.g. 2006-01-02T15:04:05+00:00",
			},
			{
				name:   "DeleteKeyspace",
				method: commandDeleteKeyspace,
				params: "[--recursive] <keyspace>",
				help:   "Deletes the specified keyspace. In recursive mode, it also recursively deletes all shards in the keyspace. Otherwise, there must be no shards left in the keyspace.",
			},
			{
				name:   "RemoveKeyspaceCell",
				method: commandRemoveKeyspaceCell,
				params: "[--force] [--recursive] <keyspace> <cell>",
				help:   "Removes the cell from the Cells list for all shards in the keyspace, and the SrvKeyspace for that keyspace in that cell.",
			},
			{
				name:   "GetKeyspace",
				method: commandGetKeyspace,
				params: "<keyspace>",
				help:   "Outputs a JSON structure that contains information about the Keyspace.",
			},
			{
				name:   "GetKeyspaces",
				method: commandGetKeyspaces,
				params: "",
				help:   "Outputs a sorted list of all keyspaces.",
			},
			{
				name:   "RebuildKeyspaceGraph",
				method: commandRebuildKeyspaceGraph,
				params: "[--cells=c1,c2,...] [--allow_partial] <keyspace> ...",
				help:   "Rebuilds the serving data for the keyspace. This command may trigger an update to all connected clients.",
			},
			{
				name:   "ValidateKeyspace",
				method: commandValidateKeyspace,
				params: "[--ping-tablets] <keyspace name>",
				help:   "Validates that all nodes reachable from the specified keyspace are consistent.",
			},
			{
				name:   "Reshard",
				method: commandReshard,
				params: "[--source_shards=<source_shards>] [--target_shards=<target_shards>] [--cells=<cells>] [--tablet_types=<source_tablet_types>] [--on-ddl=<ddl-action>] [--defer-secondary-keys] [--skip_schema_copy] <action> 'action must be one of the following: Create, Complete, Cancel, SwitchTraffic, ReverseTrafffic, Show, or Progress' <keyspace.workflow>",
				help:   "Start a Resharding process.",
			},
			{
				name:   "MoveTables",
				method: commandMoveTables,
				params: "[--source=<sourceKs>] [--tables=<tableSpecs>] [--cells=<cells>] [--tablet_types=<source_tablet_types>] [--all] [--exclude=<tables>] [--auto_start] [--stop_after_copy] [--defer-secondary-keys] [--on-ddl=<ddl-action>] [--source_shards=<source_shards>] <action> 'action must be one of the following: Create, Complete, Cancel, SwitchTraffic, ReverseTrafffic, Show, or Progress' <targetKs.workflow>",
				help:   `Move table(s) to another keyspace, table_specs is a list of tables or the tables section of the vschema for the target keyspace. Example: '{"t1":{"column_vindexes": [{"column": "id1", "name": "hash"}]}, "t2":{"column_vindexes": [{"column": "id2", "name": "hash"}]}}'.  In the case of an unsharded target keyspace the vschema for each table may be empty. Example: '{"t1":{}, "t2":{}}'.`,
			},
			{
				name:   "Migrate",
				method: commandMigrate,
				params: "[--cells=<cells>] [--tablet_types=<source_tablet_types>] [--defer-secondary-keys] --workflow=<workflow> <source_keyspace> <target_keyspace> <table_specs>",
				help:   `Move table(s) to another keyspace, table_specs is a list of tables or the tables section of the vschema for the target keyspace. Example: '{"t1":{"column_vindexes": [{"column": "id1", "name": "hash"}]}, "t2":{"column_vindexes": [{"column": "id2", "name": "hash"}]}}'.  In the case of an unsharded target keyspace the vschema for each table may be empty. Example: '{"t1":{}, "t2":{}}'.`,
			},
			{
				name:   "CreateLookupVindex",
				method: commandCreateLookupVindex,
				params: "[--cells=<source_cells>] [--tablet_types=<source_tablet_types>] <keyspace> <json_spec>",
				help:   `Create and backfill a lookup vindex. the json_spec must contain the vindex and colvindex specs for the new lookup.`,
			},
			{
				name:   "ExternalizeVindex",
				method: commandExternalizeVindex,
				params: "<keyspace>.<vindex>",
				help:   `Externalize a backfilled vindex.`,
			},
			{
				name:   "Materialize",
				method: commandMaterialize,
				params: `[--cells=<cells>] [--tablet_types=<source_tablet_types>] <json_spec>, example : '{"workflow": "aaa", "source_keyspace": "source", "target_keyspace": "target", "table_settings": [{"target_table": "customer", "source_expression": "select * from customer", "create_ddl": "copy"}]}'`,
				help:   "Performs materialization based on the json spec. Is used directly to form VReplication rules, with an optional step to copy table structure/DDL.",
			},
			{
				name:   "VDiff",
				method: commandVDiff,
				params: "[--source_cell=<cell>] [--target_cell=<cell>] [--tablet_types=in_order:RDONLY,REPLICA,PRIMARY] [--limit=<max rows to diff>] [--tables=<table list>] [--format=json] [--auto-retry] [--verbose] [--max_extra_rows_to_compare=1000] [--filtered_replication_wait_time=30s] [--debug_query] [--only_pks] [--wait] [--wait-update-interval=1m] <keyspace.workflow> [<action>] [<UUID>]",
				help:   "Perform a diff of all tables in the workflow",
			},
			{
				name:   "FindAllShardsInKeyspace",
				method: commandFindAllShardsInKeyspace,
				params: "<keyspace>",
				help:   "Displays all of the shards in the specified keyspace.",
			},
			{
				name:   "Mount",
				method: commandMount,
				params: "[--topo_type=etcd2|consul|zookeeper] [--topo_server=topo_url] [--topo_root=root_topo_node> [--unmount] [--list] [--show]  [<cluster_name>]",
				help:   "Add/Remove/Display/List external cluster(s) to this vitess cluster",
			},
		},
	},
	{
		"Generic", []command{
			{
				name:   "Validate",
				method: commandValidate,
				params: "[--ping-tablets]",
				help:   "Validates that all nodes reachable from the global replication graph and that all tablets in all discoverable cells are consistent.",
			},
			{
				name:   "ListAllTablets",
				method: commandListAllTablets,
				params: "[--keyspace=''] [--tablet_type=<PRIMARY,REPLICA,RDONLY,SPARE>] [<cell_name1>,<cell_name2>,...]",
				help:   "Lists all tablets in an awk-friendly way.",
			},
			{
				name:   "ListTablets",
				method: commandListTablets,
				params: "<tablet alias> ...",
				help:   "Lists specified tablets in an awk-friendly way.",
			},
			{
				name:   "GenerateShardRanges",
				method: commandGenerateShardRanges,
				params: "[--num_shards 2]",
				help:   "Generates shard ranges assuming a keyspace with N shards.",
			},
			{
				name:   "Panic",
				method: commandPanic,
				params: "",
				help:   "Triggers a panic on the server side, to test the handling.",
				hidden: true,
			},
			{
				name: "LegacyVtctlCommand",
				method: func(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
					subFlags.Usage = func() {
						wr.Logger().Printf("Runs the vtctl request through the legacy vtctlclient program syntax (default).\n")
					}

					return subFlags.Parse(args)
				},
				params: "<command> [args...]",
				help:   "Runs the vtctl request through the legacy vtctlclient program syntax (default).",
			},
			{
				name: "VtctldCommand",
				method: func(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
					subFlags.Usage = func() {
						wr.Logger().Printf("Runs the vtctl request through the new vtctldclient program syntax. This will become the default in a future version of Vitess.\n")
					}

					return subFlags.Parse(args)
				},
				params: "<command> [args ...]",
				help:   "Runs the vtctl request through the new vtctldclient program syntax. This will become the default in a future version of Vitess.",
			},
		},
	},
	{
		"Schema, Version, Permissions", []command{
			{
				name:   "GetSchema",
				method: commandGetSchema,
				params: "[--tables=<table1>,<table2>,...] [--exclude_tables=<table1>,<table2>,...] [--include-views] <tablet alias>",
				help:   "Displays the full schema for a tablet, or just the schema for the specified tables in that tablet.",
			},
			{
				name:   "ReloadSchema",
				method: commandReloadSchema,
				params: "<tablet alias>",
				help:   "Reloads the schema on a remote tablet.",
			},
			{
				name:   "ReloadSchemaShard",
				method: commandReloadSchemaShard,
				params: "[--concurrency=10] [--include_primary=false] <keyspace/shard>",
				help:   "Reloads the schema on all the tablets in a shard.",
			},
			{
				name:   "ReloadSchemaKeyspace",
				method: commandReloadSchemaKeyspace,
				params: "[--concurrency=10] [--include_primary=false] <keyspace>",
				help:   "Reloads the schema on all the tablets in a keyspace.",
			},
			{
				name:   "ValidateSchemaShard",
				method: commandValidateSchemaShard,
				params: "[--exclude_tables=''] [--include-views] [--include-vschema] <keyspace/shard>",
				help:   "Validates that the schema on primary tablet matches all of the replica tablets.",
			},
			{
				name:   "ValidateSchemaKeyspace",
				method: commandValidateSchemaKeyspace,
				params: "[--exclude_tables=''] [--include-views] [--skip-no-primary] [--include-vschema] <keyspace name>",
				help:   "Validates that the schema on the primary tablet for shard 0 matches the schema on all of the other tablets in the keyspace.",
			},
			{
				name:   "ApplySchema",
				method: commandApplySchema,
				params: "[--allow_long_unavailability] [--wait_replicas_timeout=10s] [--ddl_strategy=<ddl_strategy>] [--uuid_list=<comma_separated_uuids>] [--migration_context=<unique-request-context>] [--skip_preflight] {--sql=<sql> || --sql-file=<filename>} <keyspace>",
				help:   "Applies the schema change to the specified keyspace on every primary, running in parallel on all shards. The changes are then propagated to replicas via replication. If --allow_long_unavailability is set, schema changes affecting a large number of rows (and possibly incurring a longer period of unavailability) will not be rejected. -ddl_strategy is used to instruct migrations via vreplication, gh-ost or pt-osc with optional parameters. -migration_context allows the user to specify a custom request context for online DDL migrations. If -skip_preflight, SQL goes directly to shards without going through sanity checks.",
			},
			{
				name:   "CopySchemaShard",
				method: commandCopySchemaShard,
				params: "[--tables=<table1>,<table2>,...] [--exclude_tables=<table1>,<table2>,...] [--include-views] [--skip-verify] [--wait_replicas_timeout=10s] {<source keyspace/shard> || <source tablet alias>} <destination keyspace/shard>",
				help:   "Copies the schema from a source shard's primary (or a specific tablet) to a destination shard. The schema is applied directly on the primary of the destination shard, and it is propagated to the replicas through binlogs.",
			},
			{
				name:   "OnlineDDL",
				method: commandOnlineDDL,
				params: "[--json] <keyspace> <command> [<migration_uuid>]",
				help: "Operates on online DDL (migrations). Examples:" +
					" \nvtctl OnlineDDL test_keyspace show 82fa54ac_e83e_11ea_96b7_f875a4d24e90" +
					" \nvtctl OnlineDDL test_keyspace show all" +
					" \nvtctl OnlineDDL  --order descending test_keyspace show all" +
					" \nvtctl OnlineDDL  --limit 10 test_keyspace show all" +
					" \nvtctl OnlineDDL  --skip 5 --limit 10 test_keyspace show all" +
					" \nvtctl OnlineDDL test_keyspace show running" +
					" \nvtctl OnlineDDL test_keyspace show complete" +
					" \nvtctl OnlineDDL test_keyspace show failed" +
					" \nvtctl OnlineDDL test_keyspace retry 82fa54ac_e83e_11ea_96b7_f875a4d24e90" +
					" \nvtctl OnlineDDL test_keyspace cancel 82fa54ac_e83e_11ea_96b7_f875a4d24e90",
			},
			{
				name:   "ValidateVersionShard",
				method: commandValidateVersionShard,
				params: "<keyspace/shard>",
				help:   "Validates that the version on primary matches all of the replicas.",
			},
			{
				name:   "ValidateVersionKeyspace",
				method: commandValidateVersionKeyspace,
				params: "<keyspace name>",
				help:   "Validates that the version on primary of shard 0 matches all of the other tablets in the keyspace.",
			},
			{
				name:   "GetPermissions",
				method: commandGetPermissions,
				params: "<tablet alias>",
				help:   "Displays the permissions for a tablet.",
			},
			{
				name:   "ValidatePermissionsShard",
				method: commandValidatePermissionsShard,
				params: "<keyspace/shard>",
				help:   "Validates that the permissions on primary match all the replicas.",
			},
			{
				name:   "ValidatePermissionsKeyspace",
				method: commandValidatePermissionsKeyspace,
				params: "<keyspace name>",
				help:   "Validates that the permissions on primary of shard 0 match those of all of the other tablets in the keyspace.",
			},
			{
				name:   "GetVSchema",
				method: commandGetVSchema,
				params: "<keyspace>",
				help:   "Displays the VTGate routing schema.",
			},
			{
				name:   "ApplyVSchema",
				method: commandApplyVSchema,
				params: "{--vschema=<vschema> || --vschema_file=<vschema file> || --sql=<sql> || --sql_file=<sql file>} [--cells=c1,c2,...] [--skip_rebuild] [--dry-run] <keyspace>",
				help:   "Applies the VTGate routing schema to the provided keyspace. Shows the result after application.",
			},
			{
				name:   "GetRoutingRules",
				method: commandGetRoutingRules,
				params: "",
				help:   "Displays the VSchema routing rules.",
			},
			{
				name:   "ApplyRoutingRules",
				method: commandApplyRoutingRules,
				params: "{--rules=<rules> || --rules_file=<rules_file>} [--cells=c1,c2,...] [--skip_rebuild] [--dry-run]",
				help:   "Applies the VSchema routing rules.",
			},
			{
				name:   "RebuildVSchemaGraph",
				method: commandRebuildVSchemaGraph,
				params: "[--cells=c1,c2,...]",
				help:   "Rebuilds the cell-specific SrvVSchema from the global VSchema objects in the provided cells (or all cells if none provided).",
			},
		},
	},
	{
		"Serving Graph", []command{
			{
				name:   "GetSrvKeyspaceNames",
				method: commandGetSrvKeyspaceNames,
				params: "<cell>",
				help:   "Outputs a list of keyspace names.",
			},
			{
				name:   "GetSrvKeyspace",
				method: commandGetSrvKeyspace,
				params: "<cell> <keyspace>",
				help:   "Outputs a JSON structure that contains information about the SrvKeyspace.",
			},
			{
				name:   "UpdateThrottlerConfig",
				method: commandUpdateThrottlerConfig,
				params: "[--enable|--disable] [--threshold=<float64>] [--custom-query=<query>] [--check-as-check-self|--check-as-check-shard] <keyspace>",
				help:   "Update the table throttler configuration for all cells and tablets of a given keyspace",
			},
			{
				name:   "GetSrvVSchema",
				method: commandGetSrvVSchema,
				params: "<cell>",
				help:   "Outputs a JSON structure that contains information about the SrvVSchema.",
			},
			{
				name:   "DeleteSrvVSchema",
				method: commandDeleteSrvVSchema,
				params: "<cell>",
				help:   "Deletes the SrvVSchema object in the given cell.",
			},
		},
	},
	{
		"Replication Graph", []command{
			{
				name:   "GetShardReplication",
				method: commandGetShardReplication,
				params: "<cell> <keyspace/shard>",
				help:   "Outputs a JSON structure that contains information about the ShardReplication.",
			},
		},
	},
	{
		"Workflow", []command{
			{
				name:   "Workflow",
				method: commandWorkflow,
				params: "<ks.workflow> <action> --dry-run",
				help:   "Start/Stop/Delete/Show/ListAll/Tags Workflow on all target tablets in workflow. Example: Workflow merchant.morders Start",
			},
		},
	},
}

func init() {
	// This cannot be in the static `commands` slice, as it causes an init cycle.
	// Specifically, we would see:
	// `commands` => refers to `commandHelp` => refers to `PrintAllCommands` => refers to `commands`
	addCommand("Generic", command{
		name:   "Help",
		method: commandHelp,
		params: "[command name]",
		help:   "Prints the list of available commands, or help on a specific command.",
	})
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
	// special case for old primary that hasn't updated topo yet
	if ti.PrimaryTermStartTime != nil && ti.PrimaryTermStartTime.Seconds > 0 {
		mtst = logutil.ProtoToTime(ti.PrimaryTermStartTime).Format(time.RFC3339)
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
	data, err := os.ReadFile(flagFile)
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
		if len(param) == 0 {
			return nil, fmt.Errorf("empty keyspace param in list")
		}
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

func commandInitTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	dbNameOverride := subFlags.String("db_name_override", "", "Overrides the name of the database that the vttablet uses")
	allowUpdate := subFlags.Bool("allow_update", false, "Use this flag to force initialization if a tablet with the same name already exists. Use with caution.")
	allowPrimaryOverride := subFlags.Bool("allow_master_override", false, "Use this flag to force initialization if a tablet is created as primary, and a primary for the keyspace/shard already exists. Use with caution.")
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

	return wr.TopoServer().InitTablet(ctx, tablet, *allowPrimaryOverride, *createShardAndKeyspace, *allowUpdate)
}

func commandGetTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandUpdateTabletAddrs(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandDeleteTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	allowPrimary := subFlags.Bool("allow_primary", false, "Allows for the primary tablet of a shard to be deleted. Use with caution.")

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
		if err := wr.DeleteTablet(ctx, tabletAlias, *allowPrimary); err != nil {
			return err
		}
	}
	return nil
}

func commandSetReadOnly(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
	_, err = wr.VtctldServer().SetWritable(ctx, &vtctldatapb.SetWritableRequest{
		TabletAlias: tabletAlias,
		Writable:    false,
	})
	return err
}

func commandSetReadWrite(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
	_, err = wr.VtctldServer().SetWritable(ctx, &vtctldatapb.SetWritableRequest{
		TabletAlias: tabletAlias,
		Writable:    true,
	})
	return err
}

func commandStartReplication(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	_, err = wr.VtctldServer().StartReplication(ctx, &vtctldatapb.StartReplicationRequest{
		TabletAlias: tabletAlias,
	})
	return err
}

func commandStopReplication(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	_, err = wr.VtctldServer().StopReplication(ctx, &vtctldatapb.StopReplicationRequest{
		TabletAlias: tabletAlias,
	})
	return err
}

func commandChangeTabletType(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

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

func commandPing(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
	_, err = wr.VtctldServer().PingTablet(ctx, &vtctldatapb.PingTabletRequest{
		TabletAlias: tabletAlias,
	})
	return err
}

func commandRefreshState(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	_, err = wr.VtctldServer().RefreshState(ctx, &vtctldatapb.RefreshStateRequest{
		TabletAlias: tabletAlias,
	})
	return err
}

func commandRefreshStateByShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	_, err = wr.VtctldServer().RefreshStateByShard(ctx, &vtctldatapb.RefreshStateByShardRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Cells:    cells,
	})
	return err
}

func commandRunHealthCheck(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	_, err = wr.VtctldServer().RunHealthCheck(ctx, &vtctldatapb.RunHealthCheckRequest{
		TabletAlias: tabletAlias,
	})
	return err
}

func commandSleep(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
	duration, err := time.ParseDuration(subFlags.Arg(1))
	if err != nil {
		return err
	}

	_, err = wr.VtctldServer().SleepTablet(ctx, &vtctldatapb.SleepTabletRequest{
		TabletAlias: tabletAlias,
		Duration:    protoutil.DurationToProto(duration),
	})
	return err
}

func commandExecuteFetchAsApp(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandExecuteFetchAsDba(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandVReplicationExec(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	wr.Logger().Printf("\nWARNING: VReplicationExec is deprecated and will be removed in a future release. Please use 'Workflow -- <keyspace.workflow> <action>' instead.\n\n")

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

func commandExecuteHook(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	subFlags.SetInterspersed(false) // all flags should be treated as posargs to pass them to the actual hook

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

	resp, err := wr.VtctldServer().ExecuteHook(ctx, &vtctldatapb.ExecuteHookRequest{
		TabletAlias: tabletAlias,
		TabletHookRequest: &tabletmanagerdatapb.ExecuteHookRequest{
			Name:       subFlags.Arg(1),
			Parameters: subFlags.Args()[2:],
		},
	})
	if err != nil {
		return err
	}

	hr := hk.HookResult{
		ExitStatus: int(resp.HookResult.ExitStatus),
		Stdout:     resp.HookResult.Stdout,
		Stderr:     resp.HookResult.Stderr,
	}
	return printJSON(wr.Logger(), hr)
}

func commandCreateShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
		wr.Logger().Infof("shard %v/%v already exists (ignoring error with --force)", keyspace, shard)
		err = nil
	}
	return err
}

func commandGetShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandValidateShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandShardReplicationPositions(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
			lines = append(lines, cli.MarshalTabletAWK(tablet)+fmt.Sprintf(" %v %v", status.Position, status.ReplicationLagSeconds))
		}
	}
	for _, l := range lines {
		wr.Logger().Printf("%v\n", l)
	}
	return nil
}

func commandListShardTablets(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandSetShardIsPrimaryServing(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <keyspace/shard> <is_serving> arguments are both required for the SetShardIsPrimaryServing command")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}

	isServing, err := strconv.ParseBool(subFlags.Arg(1))
	if err != nil {
		return err
	}

	_, err = wr.VtctldServer().SetShardIsPrimaryServing(ctx, &vtctldatapb.SetShardIsPrimaryServingRequest{
		Keyspace:  keyspace,
		Shard:     shard,
		IsServing: isServing,
	})
	return err
}

func commandUpdateSrvKeyspacePartition(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandSetShardTabletControl(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	cellsStr := subFlags.String("cells", "", "Specifies a comma-separated list of cells to update")
	deniedTablesStr := subFlags.String("denied_tables", "", "Specifies a comma-separated list of tables to add to the denylist (used for VReplication). Each is either an exact match, or a regular expression of the form '/regexp/'.")

	remove := subFlags.Bool("remove", false, "Removes cells.")
	disableQueryService := subFlags.Bool("disable_query_service", false, "Disables query service on the provided nodes. This flag requires 'denied_tables' and 'remove' to be unset, otherwise it's ignored.")
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
	var deniedTables []string
	if *deniedTablesStr != "" {
		deniedTables = strings.Split(*deniedTablesStr, ",")
	}
	var cells []string
	if *cellsStr != "" {
		cells = strings.Split(*cellsStr, ",")
	}

	_, err = wr.VtctldServer().SetShardTabletControl(ctx, &vtctldatapb.SetShardTabletControlRequest{
		Keyspace:            keyspace,
		Shard:               shard,
		TabletType:          tabletType,
		Cells:               cells,
		Remove:              *remove,
		DeniedTables:        deniedTables,
		DisableQueryService: *disableQueryService,
	})
	return err
}

func commandSourceShardDelete(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandSourceShardAdd(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	keyRange := subFlags.String("key_range", "", "Identifies the key range to use for the SourceShard")
	tablesStr := subFlags.String("tables", "", "Specifies a comma-separated list of tables to replicate. Each is either an exact match, or a regular expression of the form /regexp/")
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

func commandShardReplicationAdd(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	_, err = wr.VtctldServer().ShardReplicationAdd(ctx, &vtctldatapb.ShardReplicationAddRequest{
		TabletAlias: tabletAlias,
		Keyspace:    keyspace,
		Shard:       shard,
	})
	return err
}

func commandShardReplicationRemove(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	_, err = wr.VtctldServer().ShardReplicationRemove(ctx, &vtctldatapb.ShardReplicationRemoveRequest{
		TabletAlias: tabletAlias,
		Keyspace:    keyspace,
		Shard:       shard,
	})
	return err
}

func commandShardReplicationFix(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
	_, err = topo.FixShardReplication(ctx, wr.TopoServer(), wr.Logger(), cell, keyspace, shard)
	return err
}

func commandWaitForFilteredReplication(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandRemoveShardCell(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	cell := subFlags.Arg(1)

	_, err = wr.VtctldServer().RemoveShardCell(ctx, &vtctldatapb.RemoveShardCellRequest{
		Keyspace:  keyspace,
		ShardName: shard,
		Cell:      cell,
		Force:     *force,
		Recursive: *recursive,
	})
	return err
}

func commandDeleteShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandCreateKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Proceeds even if the keyspace already exists")
	allowEmptyVSchema := subFlags.Bool("allow_empty_vschema", false, "If set this will allow a new keyspace to have no vschema")

	var servedFrom flagutil.StringMapValue
	subFlags.Var(&servedFrom, "served_from", "Specifies a comma-separated list of tablet_type:keyspace pairs used to serve traffic")
	keyspaceType := subFlags.String("keyspace_type", "", "Specifies the type of the keyspace")
	baseKeyspace := subFlags.String("base_keyspace", "", "Specifies the base keyspace for a snapshot keyspace")
	timestampStr := subFlags.String("snapshot_time", "", "Specifies the snapshot time for this keyspace")
	durabilityPolicy := subFlags.String("durability-policy", "none", "Type of durability to enforce for this keyspace. Default is none. Possible values include 'semi_sync' and others as dictated by registered plugins.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace name> argument is required for the CreateKeyspace command")
	}

	keyspace := subFlags.Arg(0)
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
		if *durabilityPolicy != "none" {
			return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "durability-policy cannot be specified while creating a snapshot keyspace")
		}
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
		KeyspaceType:     ktype,
		BaseKeyspace:     *baseKeyspace,
		SnapshotTime:     snapshotTime,
		DurabilityPolicy: *durabilityPolicy,
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
	err := wr.TopoServer().CreateKeyspace(ctx, keyspace, ki)
	if *force && topo.IsErrType(err, topo.NodeExists) {
		wr.Logger().Infof("keyspace %v already exists (ignoring error with --force)", keyspace)
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

func commandDeleteKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	recursive := subFlags.Bool("recursive", false, "Also recursively delete all shards in the keyspace.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("must specify the <keyspace> argument for DeleteKeyspace")
	}

	_, err := wr.VtctldServer().DeleteKeyspace(ctx, &vtctldatapb.DeleteKeyspaceRequest{
		Keyspace:  subFlags.Arg(0),
		Recursive: *recursive,
	})
	return err
}

func commandRemoveKeyspaceCell(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Proceeds even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data.")
	recursive := subFlags.Bool("recursive", false, "Also delete all tablets in that cell belonging to the specified keyspace.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <keyspace> and <cell> arguments are required for the RemoveKeyspaceCell command")
	}

	keyspace := subFlags.Arg(0)
	cell := subFlags.Arg(1)

	_, err := wr.VtctldServer().RemoveKeyspaceCell(ctx, &vtctldatapb.RemoveKeyspaceCellRequest{
		Keyspace:  keyspace,
		Cell:      cell,
		Force:     *force,
		Recursive: *recursive,
	})
	return err
}

func commandGetKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandGetKeyspaces(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandRebuildKeyspaceGraph(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
		_, err := wr.VtctldServer().RebuildKeyspaceGraph(ctx, &vtctldatapb.RebuildKeyspaceGraphRequest{
			Keyspace:     keyspace,
			Cells:        cellArray,
			AllowPartial: *allowPartial,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func commandValidateKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandReshard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	return commandVRWorkflow(ctx, wr, subFlags, args, wrangler.ReshardWorkflow)
}

func commandMoveTables(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	return commandVRWorkflow(ctx, wr, subFlags, args, wrangler.MoveTablesWorkflow)
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

func commandMigrate(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
// FIXME: this function needs a refactor. Also validations for params should to be done per workflow type
func commandVRWorkflow(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string,
	workflowType wrangler.VReplicationWorkflowType) error {

	const defaultWaitTime = time.Duration(30 * time.Second)
	// for backward compatibility we default the lag to match the timeout for switching primary traffic
	// this should probably be much smaller so that target and source are almost in sync before switching traffic
	const defaultMaxReplicationLagAllowed = defaultWaitTime

	cells := subFlags.String("cells", "", "Cell(s) or CellAlias(es) (comma-separated) to replicate from.")
	tabletTypes := subFlags.String("tablet_types", "in_order:REPLICA,PRIMARY", "Source tablet types to replicate from (e.g. PRIMARY, REPLICA, RDONLY). Defaults to --vreplication_tablet_type parameter value for the tablet, which has the default value of in_order:REPLICA,PRIMARY. Note: SwitchTraffic overrides this default and uses in_order:RDONLY,REPLICA,PRIMARY to switch all traffic by default.")
	dryRun := subFlags.Bool("dry_run", false, "Does a dry run of SwitchTraffic and only reports the actions to be taken. --dry_run is only supported for SwitchTraffic, ReverseTraffic and Complete.")
	timeout := subFlags.Duration("timeout", defaultWaitTime, "Specifies the maximum time to wait, in seconds, for vreplication to catch up on primary migrations. The migration will be cancelled on a timeout. --timeout is only supported for SwitchTraffic and ReverseTraffic.")
	reverseReplication := subFlags.Bool("reverse_replication", true, "Also reverse the replication (default true). --reverse_replication is only supported for SwitchTraffic.")
	keepData := subFlags.Bool("keep_data", false, "Do not drop tables or shards (if true, only vreplication artifacts are cleaned up).  --keep_data is only supported for Complete and Cancel.")
	keepRoutingRules := subFlags.Bool("keep_routing_rules", false, "Do not remove the routing rules for the source keyspace.  --keep_routing_rules is only supported for Complete and Cancel.")
	autoStart := subFlags.Bool("auto_start", true, "If false, streams will start in the Stopped state and will need to be explicitly started")
	stopAfterCopy := subFlags.Bool("stop_after_copy", false, "Streams will be stopped once the copy phase is completed")
	dropForeignKeys := subFlags.Bool("drop_foreign_keys", false, "If true, tables in the target keyspace will be created without foreign keys.")
	maxReplicationLagAllowed := subFlags.Duration("max_replication_lag_allowed", defaultMaxReplicationLagAllowed, "Allow traffic to be switched only if vreplication lag is below this (in seconds)")

	onDDL := "IGNORE"
	subFlags.StringVar(&onDDL, "on-ddl", onDDL, "What to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE.")

	// MoveTables and Migrate params
	tables := subFlags.String("tables", "", "MoveTables only. A table spec or a list of tables. Either table_specs or --all needs to be specified.")
	allTables := subFlags.Bool("all", false, "MoveTables only. Move all tables from the source keyspace. Either table_specs or --all needs to be specified.")
	excludes := subFlags.String("exclude", "", "MoveTables only. Tables to exclude (comma-separated) if --all is specified")
	sourceKeyspace := subFlags.String("source", "", "MoveTables only. Source keyspace")

	// if sourceTimeZone is specified, the target needs to have time zones loaded
	// note we make an opinionated decision to not allow specifying a different target time zone than UTC.
	sourceTimeZone := subFlags.String("source_time_zone", "", "MoveTables only. Specifying this causes any DATETIME fields to be converted from given time zone into UTC")

	// MoveTables-only params
	renameTables := subFlags.Bool("rename_tables", false, "MoveTables only. Rename tables instead of dropping them. --rename_tables is only supported for Complete.")

	// MoveTables and Reshard params
	sourceShards := subFlags.String("source_shards", "", "Source shards")
	*sourceShards = strings.TrimSpace(*sourceShards)
	deferNonPKeys := subFlags.Bool("defer-secondary-keys", false, "Defer secondary index creation for a table until after it has been copied.")

	// Reshard params
	targetShards := subFlags.String("target_shards", "", "Reshard only. Target shards")
	*targetShards = strings.TrimSpace(*targetShards)
	skipSchemaCopy := subFlags.Bool("skip_schema_copy", false, "Reshard only. Skip copying of schema to target shards")

	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 2 {
		return fmt.Errorf("two arguments are needed: action, keyspace.workflow")
	}

	onDDL = strings.ToUpper(onDDL)
	if _, ok := binlogdatapb.OnDDLAction_value[onDDL]; !ok {
		return fmt.Errorf("invalid value for on-ddl: %v", onDDL)
	}

	action := subFlags.Arg(0)
	ksWorkflow := subFlags.Arg(1)
	target, workflowName, err := splitKeyspaceWorkflow(ksWorkflow)
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
		Workflow:       workflowName,
		DryRun:         *dryRun,
		AutoStart:      *autoStart,
		StopAfterCopy:  *stopAfterCopy,
	}

	printDetails := func() error {
		s := ""
		res, err := wr.ShowWorkflow(ctx, workflowName, target)
		if err != nil {
			return err
		}
		s += fmt.Sprintf("The following vreplication streams exist for workflow %s.%s:\n\n", target, workflowName)
		for ksShard := range res.ShardStatuses {
			statuses := res.ShardStatuses[ksShard].PrimaryReplicationStatuses
			for _, st := range statuses {
				msg := ""
				if st.State == "Error" {
					msg += fmt.Sprintf(": %s.", st.Message)
				} else if st.Pos == "" {
					msg += ". VStream has not started."
				} else {
					now := time.Now().Nanosecond()
					updateLag := int64(now) - st.TimeUpdated
					if updateLag > 0*1e9 {
						msg += ". VStream may not be running"
					}
					txLag := int64(now) - st.TransactionTimestamp
					msg += fmt.Sprintf(". VStream Lag: %ds.", txLag/1e9)
					if st.TransactionTimestamp > 0 { // if no events occur after copy phase, TransactionTimeStamp can be 0
						msg += fmt.Sprintf(" Tx time: %s.", time.Unix(st.TransactionTimestamp, 0).Format(time.ANSIC))
					}
				}
				s += fmt.Sprintf("id=%d on %s: Status: %s%s\n", st.ID, ksShard, st.State, msg)
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

	// TODO: check if invalid parameters were passed in that do not apply to this action
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
			vrwp.SourceTimeZone = *sourceTimeZone
			vrwp.DropForeignKeys = *dropForeignKeys
			if *sourceShards != "" {
				vrwp.SourceShards = strings.Split(*sourceShards, ",")
			}
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
		vrwp.OnDDL = onDDL
		vrwp.DeferSecondaryKeys = *deferNonPKeys
		vrwp.Cells = *cells
		vrwp.TabletTypes = *tabletTypes
	case vReplicationWorkflowActionSwitchTraffic, vReplicationWorkflowActionReverseTraffic:
		vrwp.Cells = *cells
		if subFlags.Changed("tablet_types") {
			vrwp.TabletTypes = *tabletTypes
		} else {
			// When no tablet types are specified we are supposed to switch all traffic so
			// we override the normal default for tablet_types.
			vrwp.TabletTypes = "in_order:RDONLY,REPLICA,PRIMARY"
		}
		vrwp.Timeout = *timeout
		vrwp.EnableReverseReplication = *reverseReplication
		vrwp.MaxAllowedTransactionLagSeconds = int64(math.Ceil(maxReplicationLagAllowed.Seconds()))
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
		vrwp.KeepRoutingRules = *keepRoutingRules
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
		if copyProgress != nil {
			wr.Logger().Printf("\nCopy Progress (approx):\n")
			var tables []string
			for table := range *copyProgress {
				tables = append(tables, table)
			}
			sort.Strings(tables)
			s := ""
			var progress wrangler.TableCopyProgress
			for table := range *copyProgress {
				var rowCountPct, tableSizePct int64
				progress = *(*copyProgress)[table]
				if progress.SourceRowCount > 0 {
					rowCountPct = 100.0 * progress.TargetRowCount / progress.SourceRowCount
				}
				if progress.SourceTableSize > 0 {
					tableSizePct = 100.0 * progress.TargetTableSize / progress.SourceTableSize
				}
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
			return fmt.Errorf("--dry_run is only supported for SwitchTraffic, ReverseTraffic and Complete, not for %s", originalAction)
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
		err = wf.Create(ctx)
		if err != nil {
			return err
		}
		if !*autoStart {
			wr.Logger().Printf("Workflow has been created in Stopped state\n")
			break
		}
		wr.Logger().Printf("Waiting for workflow to start:\n")

		type streamCount struct {
			total, started int64
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
					return
				case <-ticker.C:
					totalStreams, startedStreams, workflowErrors, err := wf.GetStreamCount()
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
						started: startedStreams,
					}
				}
			}
		}(timedCtx)

		for {
			select {
			case progress := <-progressCh:
				if progress.started == progress.total {
					wr.Logger().Printf("\nWorkflow started successfully with %d stream(s)\n", progress.total)
					printDetails()
					return nil
				}
				wr.Logger().Printf("%d%% ... ", 100*progress.started/progress.total)
			case <-timedCtx.Done():
				wr.Logger().Printf("\nThe workflow did not start within %s. The workflow may simply be slow to start or there may be an issue.\n",
					(*timeout).String())
				wr.Logger().Printf("Check the status using the 'Workflow %s show' client command for details.\n", ksWorkflow)
				return fmt.Errorf("timed out waiting for workflow to start")
			case err := <-errCh:
				wr.Logger().Error(err)
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
		dryRunResults, err = wf.SwitchTraffic(workflow.DirectionForward)
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
			wr.Logger().Printf("Dry Run results for %s run at %s\nParameters: %s\n\n", originalAction, time.Now().Format(time.RFC822), strings.Join(args, " "))
			wr.Logger().Printf("%s\n", strings.Join(*dryRunResults, "\n"))
			return nil
		}
	}
	wr.Logger().Printf("%s was successful for workflow %s.%s\nStart State: %s\nCurrent State: %s\n\n",
		originalAction, vrwp.TargetKeyspace, vrwp.Workflow, startState, wf.CurrentState())
	return nil
}

func commandCreateLookupVindex(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	cells := subFlags.String("cells", "", "Source cells to replicate from.")
	tabletTypes := subFlags.String("tablet_types", "", "Source tablet types to replicate from.")
	continueAfterCopyWithOwner := subFlags.Bool("continue_after_copy_with_owner", false, "Vindex will continue materialization after copy when an owner is provided")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("two arguments are required: keyspace and json_spec")
	}
	keyspace := subFlags.Arg(0)
	specs := &vschemapb.Keyspace{}
	if err := json2.Unmarshal([]byte(subFlags.Arg(1)), specs); err != nil {
		return err
	}
	return wr.CreateLookupVindex(ctx, keyspace, specs, *cells, *tabletTypes, *continueAfterCopyWithOwner)
}

func commandExternalizeVindex(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("one argument is required: keyspace.vindex")
	}
	return wr.ExternalizeVindex(ctx, subFlags.Arg(0))
}

func commandMaterialize(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func useVDiffV1(args []string) bool {
	for _, arg := range args {
		if arg == "-v1" || arg == "--v1" {
			return true
		}
	}
	return false
}

func commandVDiff(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if !useVDiffV1(args) {
		return commandVDiff2(ctx, wr, subFlags, args)
	}
	_ = subFlags.Bool("v1", false, "Use legacy VDiff v1")

	sourceCell := subFlags.String("source_cell", "", "The source cell to compare from; default is any available cell")
	targetCell := subFlags.String("target_cell", "", "The target cell to compare with; default is any available cell")
	tabletTypes := subFlags.String("tablet_types", "in_order:RDONLY,REPLICA,PRIMARY", "Tablet types for source and target")
	filteredReplicationWaitTime := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on primary migrations. The migration will be cancelled on a timeout.")
	maxRows := subFlags.Int64("limit", math.MaxInt64, "Max rows to stop comparing after")
	debugQuery := subFlags.Bool("debug_query", false, "Adds a mysql query to the report that can be used for further debugging")
	onlyPks := subFlags.Bool("only_pks", false, "When reporting missing rows, only show primary keys in the report.")
	format := subFlags.String("format", "", "Format of report") // "json" or ""
	tables := subFlags.String("tables", "", "Only run vdiff for these tables in the workflow")
	maxExtraRowsToCompare := subFlags.Int("max_extra_rows_to_compare", 1000, "If there are collation differences between the source and target, you can have rows that are identical but simply returned in a different order from MySQL. We will do a second pass to compare the rows for any actual differences in this case and this flag allows you to control the resources used for this operation.")
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

	now := time.Now()
	defer func() {
		if *format == "" {
			wr.Logger().Printf("\nVDiff took %d seconds\n", int64(time.Since(now).Seconds()))
		}
	}()

	_, err = wr.VDiff(ctx, keyspace, workflow, *sourceCell, *targetCell, *tabletTypes, *filteredReplicationWaitTime, *format,
		*maxRows, *tables, *debugQuery, *onlyPks, *maxExtraRowsToCompare)
	if err != nil {
		log.Errorf("vdiff returning with error: %v", err)
		if strings.Contains(err.Error(), "context deadline exceeded") {
			return fmt.Errorf("vdiff timed out: you may want to increase it with the flag --filtered_replication_wait_time=<timeoutSeconds>")
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

func commandFindAllShardsInKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandValidate(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	pingTablets := subFlags.Bool("ping-tablets", false, "Indicates whether all tablets should be pinged during the validation process")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() != 0 {
		wr.Logger().Warningf("action Validate doesn't take any parameter any more")
	}
	return wr.Validate(ctx, *pingTablets)
}

func commandListAllTablets(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	keyspaceFilter := subFlags.String("keyspace", "", "Keyspace to filter on")
	tabletTypeStr := subFlags.String("tablet_type", "", "Tablet type to filter on")
	var err error
	if err = subFlags.Parse(args); err != nil {
		return err
	}
	var tabletTypeFilter topodatapb.TabletType
	if *tabletTypeStr != "" {
		tabletTypeFilter, err = parseTabletType(*tabletTypeStr, topoproto.AllTabletTypes)
		if err != nil {
			return err
		}
	}
	var cells []string
	if subFlags.NArg() == 1 {
		cells = strings.Split(subFlags.Arg(0), ",")
	}

	resp, err := wr.VtctldServer().GetTablets(ctx, &vtctldatapb.GetTabletsRequest{
		Cells:      cells,
		Strict:     false,
		Keyspace:   *keyspaceFilter,
		TabletType: tabletTypeFilter,
	})

	if err != nil {
		return err
	}

	for _, tablet := range resp.Tablets {
		wr.Logger().Printf("%v\n", cli.MarshalTabletAWK(tablet))
	}

	return nil
}

func commandListTablets(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandGetSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	tables := subFlags.String("tables", "", "Specifies a comma-separated list of tables for which we should gather information. Each is either an exact match, or a regular expression of the form /regexp/")
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the output")
	// skipFields := subFlags.Bool("skip-fields", false, "Skip fields introspection")
	tableNamesOnly := subFlags.Bool("table_names_only", false, "Only displays table names that match")
	tableSizesOnly := subFlags.Bool("table_sizes_only", false, "Only displays size information for tables. Ignored if --table_names_only is passed.")
	tableSchemaOnly := subFlags.Bool("table_schema_only", false, "Only displays table schema. Skip columns and fields.")

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
		TabletAlias:     tabletAlias,
		Tables:          tableArray,
		ExcludeTables:   excludeTableArray,
		IncludeViews:    *includeViews,
		TableNamesOnly:  *tableNamesOnly,
		TableSizesOnly:  *tableSizesOnly,
		TableSchemaOnly: *tableSchemaOnly,
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

func commandReloadSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
	_, err = wr.VtctldServer().ReloadSchema(ctx, &vtctldatapb.ReloadSchemaRequest{
		TabletAlias: tabletAlias,
	})
	return err
}

func commandReloadSchemaShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 10, "How many tablets to reload in parallel")
	includePrimary := subFlags.Bool("include_primary", true, "Include the primary tablet")

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
	resp, err := wr.VtctldServer().ReloadSchemaShard(ctx, &vtctldatapb.ReloadSchemaShardRequest{
		Keyspace:       keyspace,
		Shard:          shard,
		WaitPosition:   "",
		IncludePrimary: *includePrimary,
		Concurrency:    uint32(*concurrency),
	})
	if resp != nil {
		for _, e := range resp.Events {
			logutil.LogEvent(wr.Logger(), e)
		}
	}
	return err
}

func commandReloadSchemaKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 10, "How many tablets to reload in parallel")
	includePrimary := subFlags.Bool("include_primary", true, "Include the primary tablet(s)")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace> argument is required for the ReloadSchemaKeyspace command")
	}
	resp, err := wr.VtctldServer().ReloadSchemaKeyspace(ctx, &vtctldatapb.ReloadSchemaKeyspaceRequest{
		Keyspace:       subFlags.Arg(0),
		WaitPosition:   "",
		IncludePrimary: *includePrimary,
		Concurrency:    uint32(*concurrency),
	})
	if resp != nil {
		for _, e := range resp.Events {
			logutil.LogEvent(wr.Logger(), e)
		}
	}
	return err
}

func commandValidateSchemaShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the validation")
	includeVSchema := subFlags.Bool("include-vschema", false, "Validate schemas against the vschema")
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
	return wr.ValidateSchemaShard(ctx, keyspace, shard, excludeTableArray, *includeViews, *includeVSchema)
}

func commandValidateSchemaKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	excludeTables := subFlags.String("exclude_tables", "", "Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	includeViews := subFlags.Bool("include-views", false, "Includes views in the validation")
	skipNoPrimary := subFlags.Bool("skip-no-primary", true, "Skip shards that don't have primary when performing validation")
	includeVSchema := subFlags.Bool("include-vschema", false, "Validate schemas against the vschema")

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
	resp, err := wr.VtctldServer().ValidateSchemaKeyspace(ctx, &vtctldatapb.ValidateSchemaKeyspaceRequest{
		Keyspace:       keyspace,
		ExcludeTables:  excludeTableArray,
		IncludeViews:   *includeViews,
		SkipNoPrimary:  *skipNoPrimary,
		IncludeVschema: *includeVSchema,
	})

	if err != nil {
		wr.Logger().Errorf("%s\n", err.Error())
		return err
	}

	for _, result := range resp.Results {
		wr.Logger().Printf("%s\n", result)
	}

	if len(resp.Results) > 0 {
		return fmt.Errorf("%s", resp.Results[0])
	}

	return nil
}

func commandApplySchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	allowLongUnavailability := subFlags.Bool("allow_long_unavailability", false, "Allow large schema changes which incur a longer unavailability of the database.")
	sql := subFlags.String("sql", "", "A list of semicolon-delimited SQL commands")
	sqlFile := subFlags.String("sql-file", "", "Identifies the file that contains the SQL commands")
	ddlStrategy := subFlags.String("ddl_strategy", string(schema.DDLStrategyDirect), "Online DDL strategy, compatible with @@ddl_strategy session variable (examples: 'gh-ost', 'pt-osc', 'gh-ost --max-load=Threads_running=100'")
	uuidList := subFlags.String("uuid_list", "", "Optional: comma delimited explicit UUIDs for migration. If given, must match number of DDL changes")
	migrationContext := subFlags.String("migration_context", "", "For Online DDL, optionally supply a custom unique string used as context for the migration(s) in this command. By default a unique context is auto-generated by Vitess")
	requestContext := subFlags.String("request_context", "", "synonym for --migration_context")
	waitReplicasTimeout := subFlags.Duration("wait_replicas_timeout", wrangler.DefaultWaitReplicasTimeout, "The amount of time to wait for replicas to receive the schema change via replication.")
	skipPreflight := subFlags.Bool("skip_preflight", false, "Skip pre-apply schema checks, and directly forward schema change query to shards")

	callerID := subFlags.String("caller_id", "", "This is the effective caller ID used for the operation and should map to an ACL name which grants this identity the necessary permissions to perform the operation (this is only necessary when strict table ACLs are used)")
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

	var cID *vtrpcpb.CallerID

	if *callerID != "" {
		cID = &vtrpcpb.CallerID{Principal: *callerID}
	}

	if *migrationContext == "" {
		// --request_context is a legacy flag name
		// we now prefer to use --migration_context, but we also keep backwards compatibility
		*migrationContext = *requestContext
	}

	parts, err := sqlparser.SplitStatementToPieces(change)
	if err != nil {
		return err
	}

	log.Info("Calling ApplySchema on VtctldServer")

	resp, err := wr.VtctldServer().ApplySchema(ctx, &vtctldatapb.ApplySchemaRequest{
		Keyspace:                keyspace,
		AllowLongUnavailability: *allowLongUnavailability,
		DdlStrategy:             *ddlStrategy,
		Sql:                     parts,
		SkipPreflight:           *skipPreflight,
		UuidList:                textutil.SplitDelimitedList(*uuidList),
		MigrationContext:        *migrationContext,
		WaitReplicasTimeout:     protoutil.DurationToProto(*waitReplicasTimeout),
		CallerId:                cID,
	})

	if err != nil {
		wr.Logger().Errorf("%s\n", err.Error())
		return err
	}

	for _, uuid := range resp.UuidList {
		wr.Logger().Printf("%s\n", uuid)
	}

	return nil
}

func commandOnlineDDL(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")
	orderBy := subFlags.String("order", "ascending", "Sort the results by `id` property of the Schema migration (default is ascending. Allowed values are `ascending` or `descending`.")
	limit := subFlags.Int64("limit", 0, "Limit number of rows returned in output")
	skip := subFlags.Int64("skip", 0, "Skip specified number of rows returned in output")
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

	applySchemaQuery := ""
	executeFetchQuery := ""
	var bindErr error
	switch command {
	case "show":
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
				condition, bindErr = sqlparser.ParseAndBind("migration_uuid=%a", sqltypes.StringBindVariable(arg))
			} else {
				condition, bindErr = sqlparser.ParseAndBind("migration_context=%a", sqltypes.StringBindVariable(arg))
			}
		}
		order := " order by `id` "
		switch *orderBy {
		case "desc", "descending":
			order = order + "DESC"
		default:
			order = order + "ASC"
		}

		skipLimit := ""
		if *limit > 0 {
			skipLimit = fmt.Sprintf("LIMIT %v,%v", *skip, *limit)
		}

		executeFetchQuery = fmt.Sprintf(`select
				*
				from _vt.schema_migrations where %s %s %s`, condition, order, skipLimit)
	case "retry":
		if arg == "" {
			return fmt.Errorf("UUID required")
		}
		applySchemaQuery, bindErr = sqlparser.ParseAndBind(`alter vitess_migration %a retry`, sqltypes.StringBindVariable(arg))
	case "complete":
		if arg == "" {
			return fmt.Errorf("UUID required")
		}
		applySchemaQuery, bindErr = sqlparser.ParseAndBind(`alter vitess_migration %a complete`, sqltypes.StringBindVariable(arg))
	case "cancel":
		if arg == "" {
			return fmt.Errorf("UUID required")
		}
		applySchemaQuery, bindErr = sqlparser.ParseAndBind(`alter vitess_migration %a cancel`, sqltypes.StringBindVariable(arg))
	case "cancel-all":
		if arg != "" {
			return fmt.Errorf("UUID not allowed in %s", command)
		}
		applySchemaQuery = `alter vitess_migration cancel all`
	default:
		return fmt.Errorf("Unknown OnlineDDL command: %s", command)
	}
	if bindErr != nil {
		return fmt.Errorf("Error generating OnlineDDL query: %+v", bindErr)
	}

	if applySchemaQuery != "" {
		log.Info("Calling ApplySchema on VtctldServer")

		resp, err := wr.VtctldServer().ApplySchema(ctx, &vtctldatapb.ApplySchemaRequest{
			Keyspace:            keyspace,
			Sql:                 []string{applySchemaQuery},
			SkipPreflight:       true,
			WaitReplicasTimeout: protoutil.DurationToProto(wrangler.DefaultWaitReplicasTimeout),
		})
		if err != nil {
			return err
		}
		loggerWriter{wr.Logger()}.Printf("resp: %v\n", resp)
	} else {
		// This is a SELECT. We run this on all PRIMARY tablets of this keyspace, and return the combined result
		resp, err := wr.VtctldServer().GetTablets(ctx, &vtctldatapb.GetTabletsRequest{
			Cells:      nil,
			Strict:     false,
			Keyspace:   keyspace,
			TabletType: topodatapb.TabletType_PRIMARY,
		})
		if err != nil {
			return err
		}

		tabletResults := map[string]*sqltypes.Result{}
		for _, tablet := range resp.Tablets {
			tabletAlias := topoproto.TabletAliasString(tablet.Alias)

			qrproto, err := wr.ExecuteFetchAsDba(ctx, tablet.Alias, executeFetchQuery, 10000, false, false)
			if err != nil {
				return err
			}
			tabletResults[tabletAlias] = sqltypes.Proto3ToResult(qrproto)
		}
		// combine results. This loses sorting if there's more then 1 tablet
		combinedResults := queryResultForTabletResults(tabletResults)
		if *json {
			printJSON(wr.Logger(), combinedResults)
		} else {
			printQueryResult(loggerWriter{wr.Logger()}, combinedResults)
		}
	}
	return nil
}

func commandCopySchemaShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandValidateVersionShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandValidateVersionKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace name> argument is required for the ValidateVersionKeyspace command")
	}

	keyspace := subFlags.Arg(0)
	res, err := wr.VtctldServer().ValidateVersionKeyspace(ctx, &vtctldatapb.ValidateVersionKeyspaceRequest{Keyspace: keyspace})

	if err != nil {
		return err
	}

	for _, result := range res.Results {
		wr.Logger().Printf("%s\n", result)
	}

	if len(res.Results) > 0 {
		return fmt.Errorf("%s", res.Results[0])
	}

	return nil
}

func commandGetPermissions(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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
	resp, err := wr.VtctldServer().GetPermissions(ctx, &vtctldatapb.GetPermissionsRequest{
		TabletAlias: tabletAlias,
	})
	if err != nil {
		return err
	}
	p, err := json2.MarshalIndentPB(resp.Permissions, "	")
	if err != nil {
		return err
	}
	wr.Logger().Printf("%s\n", p)
	return nil
}

func commandValidatePermissionsShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandValidatePermissionsKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace name> argument is required for the ValidatePermissionsKeyspace command")
	}

	keyspace := subFlags.Arg(0)
	return wr.ValidatePermissionsKeyspace(ctx, keyspace)
}

func commandGetVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandGetRoutingRules(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	resp, err := wr.VtctldServer().GetRoutingRules(ctx, &vtctldatapb.GetRoutingRulesRequest{})
	if err != nil {
		return err
	}

	b, err := json2.MarshalIndentPB(resp.RoutingRules, "  ")
	if err != nil {
		wr.Logger().Printf("%v\n", err)
		return err
	}
	wr.Logger().Printf("%s\n", b)
	return nil
}

func commandRebuildVSchemaGraph(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	var cells []string
	subFlags.StringSliceVar(&cells, "cells", cells, "Specifies a comma-separated list of cells to look for tablets")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("RebuildVSchemaGraph doesn't take any arguments")
	}

	_, err := wr.VtctldServer().RebuildVSchemaGraph(ctx, &vtctldatapb.RebuildVSchemaGraphRequest{
		Cells: cells,
	})
	return err
}

func commandApplyVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	vschema := subFlags.String("vschema", "", "Identifies the VTGate routing schema")
	vschemaFile := subFlags.String("vschema_file", "", "Identifies the VTGate routing schema file")
	sql := subFlags.String("sql", "", "A vschema ddl SQL statement (e.g. `add vindex`, `alter table t add vindex hash(id)`, etc)")
	sqlFile := subFlags.String("sql_file", "", "A vschema ddl SQL statement (e.g. `add vindex`, `alter table t add vindex hash(id)`, etc)")
	dryRun := subFlags.Bool("dry-run", false, "If set, do not save the altered vschema, simply echo to console.")
	skipRebuild := subFlags.Bool("skip_rebuild", false, "If set, do not rebuild the SrvSchema objects.")
	var cells []string
	subFlags.StringSliceVar(&cells, "cells", cells, "If specified, limits the rebuild to the cells, after upload. Ignored if --skip_rebuild is set.")

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
		return fmt.Errorf("only one of the --sql, --sql_file, --vschema, or --vschema_file flags may be specified when calling the ApplyVSchema command")
	}

	if !sqlMode && !jsonMode {
		return fmt.Errorf("one of the --sql, --sql_file, --vschema, or --vschema_file flags must be specified when calling the ApplyVSchema command")
	}

	if sqlMode {
		if *sqlFile != "" {
			sqlBytes, err := os.ReadFile(*sqlFile)
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
			schema, err = os.ReadFile(*vschemaFile)
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

func commandApplyRoutingRules(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	routingRules := subFlags.String("rules", "", "Specify rules as a string")
	routingRulesFile := subFlags.String("rules_file", "", "Specify rules in a file")
	skipRebuild := subFlags.Bool("skip_rebuild", false, "If set, do no rebuild the SrvSchema objects.")
	dryRun := subFlags.Bool("dry-run", false, "Do not upload the routing rules, but print what actions would be taken")
	var cells []string
	subFlags.StringSliceVar(&cells, "cells", cells, "If specified, limits the rebuild to the cells, after upload. Ignored if skipRebuild is set.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("ApplyRoutingRules doesn't take any arguments")
	}

	var rulesBytes []byte
	if *routingRulesFile != "" {
		var err error
		rulesBytes, err = os.ReadFile(*routingRulesFile)
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
		msg := &strings.Builder{}
		if *dryRun {
			msg.WriteString("DRY RUN: ")
		}
		msg.WriteString("Failed to marshal RoutingRules for display")

		wr.Logger().Errorf2(err, msg.String())
	} else {
		msg := &strings.Builder{}
		if *dryRun {
			msg.WriteString("=== DRY RUN ===\n")
		}
		msg.WriteString(fmt.Sprintf("New RoutingRules object:\n%s\nIf this is not what you expected, check the input data (as JSON parsing will skip unexpected fields).\n", b))
		if *dryRun {
			msg.WriteString("=== (END) DRY RUN ===\n")
		}

		wr.Logger().Printf(msg.String())
	}

	if !*dryRun {
		_, err = wr.VtctldServer().ApplyRoutingRules(ctx, &vtctldatapb.ApplyRoutingRulesRequest{
			RoutingRules: rr,
			SkipRebuild:  *skipRebuild,
			RebuildCells: cells,
		})
		if err != nil {
			return err
		}
	}

	if *skipRebuild {
		msg := &strings.Builder{}
		if *dryRun {
			msg.WriteString("DRY RUN: ")
		}
		msg.WriteString("Skipping rebuild of SrvVSchema, will need to run RebuildVSchemaGraph for changes to take effect")
		wr.Logger().Warningf(msg.String())
	}

	return nil
}

func commandGetSrvKeyspaceNames(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <cell> argument is required for the GetSrvKeyspaceNames command")
	}

	cell := subFlags.Arg(0)
	resp, err := wr.VtctldServer().GetSrvKeyspaceNames(ctx, &vtctldatapb.GetSrvKeyspaceNamesRequest{
		Cells: []string{cell},
	})
	if err != nil {
		return err
	}

	names, ok := resp.Names[cell]
	if !ok {
		// Technically this should be impossible, but we handle it explicitly.
		return nil
	}

	for _, ks := range names.Names {
		wr.Logger().Printf("%v\n", ks)
	}

	return nil
}

func commandGetSrvKeyspace(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

	cellKs := resp.SrvKeyspaces[cell]
	if cellKs == nil {
		return fmt.Errorf("missing keyspace %q in cell %q", keyspace, cell)
	}
	return printJSON(wr.Logger(), cellKs)
}

func commandUpdateThrottlerConfig(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) (err error) {
	enable := subFlags.Bool("enable", false, "Enable the throttler")
	disable := subFlags.Bool("disable", false, "Disable the throttler")
	threshold := subFlags.Float64("threshold", 0, "threshold for the either default check (replication lag seconds) or custom check")
	customQuery := subFlags.String("custom-query", "", "custom throttler check query")
	checkAsCheckSelf := subFlags.Bool("check-as-check-self", false, "/throttler/check requests behave as is /throttler/check-self was called")
	checkAsCheckShard := subFlags.Bool("check-as-check-shard", false, "use standard behavior for /throttler/check requests")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	customQuerySet := subFlags.Changed("custom-query")
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <keyspace> arguments are required for the SetThrottlerConfig command")
	}
	if *enable && *disable {
		return fmt.Errorf("--enable and --disable are mutually exclusive")
	}
	if *checkAsCheckSelf && *checkAsCheckShard {
		return fmt.Errorf("--check-as-check-self and --check-as-check-shard are mutually exclusive")
	}

	keyspace := subFlags.Arg(0)

	update := func(throttlerConfig *topodatapb.ThrottlerConfig) *topodatapb.ThrottlerConfig {
		if throttlerConfig == nil {
			throttlerConfig = &topodatapb.ThrottlerConfig{}
		}
		if customQuerySet {
			// custom query provided
			throttlerConfig.CustomQuery = *customQuery
			throttlerConfig.Threshold = *threshold // allowed to be zero/negative because who knows what kind of custom query this is
		} else {
			// no custom query, throttler works by querying replication lag. We only allow positive values
			if *threshold > 0 {
				throttlerConfig.Threshold = *threshold
			}
		}
		if *enable {
			throttlerConfig.Enabled = true
		}
		if *disable {
			throttlerConfig.Enabled = false
		}
		if *checkAsCheckSelf {
			throttlerConfig.CheckAsCheckSelf = true
		}
		if *checkAsCheckShard {
			throttlerConfig.CheckAsCheckSelf = false
		}
		return throttlerConfig
	}

	ctx, unlock, lockErr := wr.TopoServer().LockKeyspace(ctx, keyspace, "UpdateThrottlerConfig")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	_, err = wr.TopoServer().UpdateSrvKeyspaceThrottlerConfig(ctx, keyspace, []string{}, update)
	return err
}

func commandGetSrvVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandDeleteSrvVSchema(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <cell> argument is required for the DeleteSrvVSchema command")
	}

	_, err := wr.VtctldServer().DeleteSrvVSchema(ctx, &vtctldatapb.DeleteSrvVSchemaRequest{
		Cell: subFlags.Arg(0),
	})
	return err
}

func commandGetShardReplication(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandHelp(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandWorkflow(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	dryRun := subFlags.Bool("dry_run", false, "Does a dry run of Workflow and only reports the final query and list of tablets on which the operation will be applied")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() < 2 {
		return fmt.Errorf("usage: Workflow --dry-run keyspace[.workflow] start/stop/delete/show/listall/tags [<tags>]")
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
		if workflow == "" {
			return fmt.Errorf("workflow has to be defined for action %s", action)
		}
	}
	_, err = wr.TopoServer().GetKeyspace(ctx, keyspace)
	if err != nil {
		wr.Logger().Errorf("Keyspace %s not found", keyspace)
	}
	var results map[*topo.TabletInfo]*sqltypes.Result
	if action == "tags" {
		tags := ""
		if subFlags.NArg() != 3 {
			return fmt.Errorf("tags incorrectly specified, usage: Workflow keyspace.workflow tags <tags>")
		}
		tags = strings.ToLower(subFlags.Arg(2))
		results, err = wr.WorkflowTagAction(ctx, keyspace, workflow, tags)
		if err != nil {
			return err
		}
	} else {
		if subFlags.NArg() != 2 {
			return fmt.Errorf("usage: Workflow --dry-run keyspace[.workflow] start/stop/delete/show/listall")
		}
		results, err = wr.WorkflowAction(ctx, workflow, keyspace, action, *dryRun)
		if err != nil {
			return err
		}
		if action == "show" || action == "listall" {
			return nil
		}
	}

	if len(results) == 0 {
		wr.Logger().Printf("no result returned\n")
		return nil
	}
	qr := wr.QueryResultForRowsAffected(results)

	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandMount(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
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

func commandGenerateShardRanges(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	numShards := subFlags.Int("num_shards", 2, "Number of shards to generate shard ranges for.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}

	shardRanges, err := key.GenerateShardRanges(*numShards)
	if err != nil {
		return err
	}

	return printJSON(wr.Logger(), shardRanges)
}

func commandPanic(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	panic(fmt.Errorf("this command panics on purpose"))
}

// printJSON will print the JSON version of the structure to the logger.
func printJSON(logger logutil.Logger, val any) error {
	data, err := MarshalJSON(val)
	if err != nil {
		return fmt.Errorf("cannot marshal data: %v", err)
	}
	logger.Printf("%s\n", data)
	return nil
}

// loggerWriter turns a Logger into a Writer by decorating it with a Write()
// method that sends everything to Logger.Printf().
type loggerWriter struct {
	logutil.Logger
}

func (lw loggerWriter) Write(p []byte) (int, error) {
	lw.Logger.Printf("%s", p)
	return len(p), nil
}

// printQueryResult will pretty-print a QueryResult to the logger.
func printQueryResult(writer io.Writer, qr *sqltypes.Result) {
	cli.WriteQueryResultTable(writer, qr)
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
//
//	updated and mixed types will use jsonpb as well.
func MarshalJSON(obj any) (data []byte, err error) {
	switch obj := obj.(type) {
	case proto.Message:
		// Note: We also end up in this case if "obj" is NOT a proto.Message but
		// has an anonymous (embedded) field of the type "proto.Message".
		// In that case jsonpb may panic if the "obj" has non-exported fields.

		// Marshal the protobuf message.
		data, err = protojson.MarshalOptions{
			Multiline:       true,
			Indent:          "  ",
			UseProtoNames:   true,
			UseEnumNumbers:  true,
			EmitUnpopulated: true,
		}.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("protojson error: %v", err)
		}
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
				subFlags := pflag.NewFlagSet(action, pflag.ContinueOnError)
				subFlags.SetOutput(logutil.NewLoggerWriter(wr.Logger()))
				subFlags.Usage = func() {
					if cmd.deprecated {
						msg := &strings.Builder{}
						msg.WriteString("WARNING: ")
						msg.WriteString(action)
						msg.WriteString(" is deprecated and will be removed in a future release.")
						if cmd.deprecatedBy != "" {
							msg.WriteString(" Use ")
							msg.WriteString(cmd.deprecatedBy)
							msg.WriteString(" instead.")
						}

						wr.Logger().Printf("%s\n", msg.String())
					}

					wr.Logger().Printf("Usage: %s %s\n\n", action, cmd.params)
					wr.Logger().Printf("%s\n\n", cmd.help)

					wr.Logger().Printf("%s\n", subFlags.FlagUsages())
				}

				if len(args) > 1 && args[1] == "--" && !cmd.disableFlagInterspersal {
					PrintDoubleDashDeprecationNotice(wr)
					args = args[1:]
				}

				switch err := cmd.method(ctx, wr, subFlags, args[1:]); err {
				case pflag.ErrHelp:
					// Don't actually error if the user requested --help on a
					// subcommand.
					return nil
				default:
					return err
				}
			}
		}
	}

	wr.Logger().Printf("Unknown command: %v\n", action)
	return ErrUnknownCommand
}

func PrintDoubleDashDeprecationNotice(wr *wrangler.Wrangler) {
	msg := (`DEPRECATION NOTICE: in v14, users needed to add a double-dash ("--") separator to split up top-level and sub-command arguments/flags.
Beginning in v16, this will no longer work properly. Please remove any double-dashes that preceed sub-command **flags** only.
	
Note that this does not mean you do not need a separator to split up position arguments.
For example, to pass a flag to a hook via ExecuteHook, "vtctl ExecuteHook myhook.sh -- --hook-flag 5" is the correct formation.
For v1 Reshard commands, you will also need a separator if your shard names begin with a hyphen, i.e. "Reshard ks.workflow -- -80,80-" needs a double-dash, but "Reshard ks.workflow 40-80,80-c0" does not.`)

	wr.Logger().Warningf(msg)
}

// PrintAllCommands will print the list of commands to the logger
func PrintAllCommands(logger logutil.Logger) {
	msg := &strings.Builder{}

	for _, group := range commands {
		logger.Printf("%s:\n", group.name)
		for _, cmd := range group.commands {
			if cmd.hidden {
				continue
			}

			msg.WriteString("  ")

			if cmd.deprecated {
				msg.WriteString("(DEPRECATED) ")
			}

			msg.WriteString(cmd.name)
			msg.WriteString(" ")
			msg.WriteString(cmd.params)
			logger.Printf("%s\n", msg.String())

			msg.Reset()
		}
		logger.Printf("\n")
	}
}

// queryResultForTabletResults aggregates given results into a combined result set
func queryResultForTabletResults(results map[string]*sqltypes.Result) *sqltypes.Result {
	var qr = &sqltypes.Result{}
	defaultFields := []*querypb.Field{{
		Name: "Tablet",
		Type: sqltypes.VarBinary,
	}}
	var row2 []sqltypes.Value
	for tabletAlias, result := range results {
		if qr.Fields == nil {
			qr.Fields = append(qr.Fields, defaultFields...)
			qr.Fields = append(qr.Fields, result.Fields...)
		}
		for _, row := range result.Rows {
			row2 = nil
			row2 = append(row2, sqltypes.NewVarBinary(tabletAlias))
			row2 = append(row2, row...)
			qr.Rows = append(qr.Rows, row2)
		}
	}
	return qr
}
