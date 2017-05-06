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

package vtctl

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"github.com/youtube/vitess/go/vt/vttablet/tabletconn"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the query command group for vtctl.

const queriesGroupName = "Queries"

var (
	enableQueries = flag.Bool("enable_queries", false, "if set, allows vtgate and vttablet queries. May have security implications, as the queries will be run from this process.")
)

func init() {
	servenv.OnRun(func() {
		if !*enableQueries {
			return
		}

		addCommandGroup(queriesGroupName)

		// VtGate commands
		addCommand(queriesGroupName, command{
			"VtGateExecute",
			commandVtGateExecute,
			"-server <vtgate> [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] [-keyspace <default keyspace>] [-tablet_type <tablet type>] [-options <proto text options>] [-json] <sql>",
			"Executes the given SQL query with the provided bound variables against the vtgate server."})
		addCommand(queriesGroupName, command{
			"VtGateExecuteShards",
			commandVtGateExecuteShards,
			"-server <vtgate> -keyspace <keyspace> -shards <shard0>,<shard1>,... [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] [-tablet_type <tablet type>] [-options <proto text options>] [-json] <sql>",
			"Executes the given SQL query with the provided bound variables against the vtgate server. It is routed to the provided shards."})
		addCommand(queriesGroupName, command{
			"VtGateExecuteKeyspaceIds",
			commandVtGateExecuteKeyspaceIds,
			"-server <vtgate> -keyspace <keyspace> -keyspace_ids <ks1 in hex>,<k2 in hex>,... [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] [-tablet_type <tablet type>] [-options <proto text options>] [-json] <sql>",
			"Executes the given SQL query with the provided bound variables against the vtgate server. It is routed to the shards that contain the provided keyspace ids."})
		addCommand(queriesGroupName, command{
			"VtGateSplitQuery",
			commandVtGateSplitQuery,
			"-server <vtgate> -keyspace <keyspace> [-split_column <split_column>] -split_count <split_count> [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] <sql>",
			"Executes the SplitQuery computation for the given SQL query with the provided bound variables against the vtgate server (this is the base query for Map-Reduce workloads, and is provided here for debug / test purposes)."})

		// VtTablet commands
		addCommand(queriesGroupName, command{
			"VtTabletExecute",
			commandVtTabletExecute,
			"[-username <TableACL user>] [-connect_timeout <connect timeout>] [-transaction_id <transaction_id>] [-options <proto text options>] [-json] <tablet alias> <sql>",
			"Executes the given query on the given tablet. -transaction_id is optional. Use VtTabletBegin to start a transaction."})
		addCommand(queriesGroupName, command{
			"VtTabletBegin",
			commandVtTabletBegin,
			"[-username <TableACL user>] [-connect_timeout <connect timeout>] <tablet alias>",
			"Starts a transaction on the provided server."})
		addCommand(queriesGroupName, command{
			"VtTabletCommit",
			commandVtTabletCommit,
			"[-username <TableACL user>] [-connect_timeout <connect timeout>] <transaction_id>",
			"Commits the given transaction on the provided server."})
		addCommand(queriesGroupName, command{
			"VtTabletRollback",
			commandVtTabletRollback,
			"[-username <TableACL user>] [-connect_timeout <connect timeout>] <tablet alias> <transaction_id>",
			"Rollbacks the given transaction on the provided server."})
		addCommand(queriesGroupName, command{
			"VtTabletStreamHealth",
			commandVtTabletStreamHealth,
			"[-count <count, default 1>] [-connect_timeout <connect timeout>] <tablet alias>",
			"Executes the StreamHealth streaming query to a vttablet process. Will stop after getting <count> answers."})
		addCommand(queriesGroupName, command{
			"VtTabletUpdateStream",
			commandVtTabletUpdateStream,
			"[-count <count, default 1>] [-connect_timeout <connect timeout>] [-position <position>] [-timestamp <timestamp>] <tablet alias>",
			"Executes the UpdateStream streaming query to a vttablet process. Will stop after getting <count> answers."})
	})
}

type bindvars map[string]interface{}

func (bv *bindvars) String() string {
	b, err := json.Marshal(bv)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func (bv *bindvars) Set(s string) (err error) {
	err = json.Unmarshal([]byte(s), &bv)
	if err != nil {
		return fmt.Errorf("error json-unmarshaling '%v': %v", s, err)
	}
	// json reads all numbers as float64
	// So, we just ditch floats for bindvars
	for k, v := range *bv {
		if f, ok := v.(float64); ok {
			if f > 0 {
				(*bv)[k] = uint64(f)
			} else {
				(*bv)[k] = int64(f)
			}
		}
	}
	return nil
}

// For internal flag compatibility
func (bv *bindvars) Get() interface{} {
	return bv
}

func newBindvars(subFlags *flag.FlagSet) *bindvars {
	var bv bindvars
	subFlags.Var(&bv, "bind_variables", "bind variables as a json list")
	return &bv
}

func parseExecuteOptions(value string) (*querypb.ExecuteOptions, error) {
	if value == "" {
		return nil, nil
	}
	result := &querypb.ExecuteOptions{}
	if err := proto.UnmarshalText(value, result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal options: %v", err)
	}
	return result, nil
}

func commandVtGateExecute(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "VtGate server to connect to")
	bindVariables := newBindvars(subFlags)
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vtgate client")
	targetString := subFlags.String("target", "", "keyspace:shard@tablet_type")
	options := subFlags.String("options", "", "execute options values as a text encoded proto of the ExecuteOptions structure")
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <sql> argument is required for the VtGateExecute command")
	}
	executeOptions, err := parseExecuteOptions(*options)
	if err != nil {
		return err
	}

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	session := vtgateConn.Session(*targetString, executeOptions)
	defer vtgateConn.Close()
	qr, err := session.Execute(ctx, subFlags.Arg(0), *bindVariables)
	if err != nil {
		return fmt.Errorf("Execute failed: %v", err)
	}
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandVtGateExecuteShards(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "VtGate server to connect to")
	bindVariables := newBindvars(subFlags)
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vtgate client")
	tabletType := subFlags.String("tablet_type", "master", "tablet type to query")
	keyspace := subFlags.String("keyspace", "", "keyspace to send query to")
	shardsStr := subFlags.String("shards", "", "comma-separated list of shards to send query to")
	options := subFlags.String("options", "", "execute options values as a text encoded proto of the ExecuteOptions structure")
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <sql> argument is required for the VtGateExecuteShards command")
	}
	t, err := parseTabletType(*tabletType, []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY})
	if err != nil {
		return err
	}
	var shards []string
	if *shardsStr != "" {
		shards = strings.Split(*shardsStr, ",")
	}
	executeOptions, err := parseExecuteOptions(*options)
	if err != nil {
		return err
	}

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	qr, err := vtgateConn.ExecuteShards(ctx, subFlags.Arg(0), *keyspace, shards, *bindVariables, t, executeOptions)
	if err != nil {
		return fmt.Errorf("Execute failed: %v", err)
	}
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandVtGateExecuteKeyspaceIds(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "VtGate server to connect to")
	bindVariables := newBindvars(subFlags)
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vtgate client")
	tabletType := subFlags.String("tablet_type", "master", "tablet type to query")
	keyspace := subFlags.String("keyspace", "", "keyspace to send query to")
	keyspaceIDsStr := subFlags.String("keyspace_ids", "", "comma-separated list of keyspace ids (in hex) that will map into shards to send query to")
	options := subFlags.String("options", "", "execute options values as a text encoded proto of the ExecuteOptions structure")
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <sql> argument is required for the VtGateExecuteKeyspaceIds command")
	}
	t, err := parseTabletType(*tabletType, []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY})
	if err != nil {
		return err
	}
	var keyspaceIDs [][]byte
	if *keyspaceIDsStr != "" {
		keyspaceIDHexs := strings.Split(*keyspaceIDsStr, ",")
		keyspaceIDs = make([][]byte, len(keyspaceIDHexs))
		for i, keyspaceIDHex := range keyspaceIDHexs {
			keyspaceIDs[i], err = hex.DecodeString(keyspaceIDHex)
			if err != nil {
				return fmt.Errorf("cannot hex-decode value %v '%v': %v", i, keyspaceIDHex, err)
			}
		}
	}
	executeOptions, err := parseExecuteOptions(*options)
	if err != nil {
		return err
	}

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	qr, err := vtgateConn.ExecuteKeyspaceIds(ctx, subFlags.Arg(0), *keyspace, keyspaceIDs, *bindVariables, t, executeOptions)
	if err != nil {
		return fmt.Errorf("Execute failed: %v", err)
	}
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandVtGateSplitQuery(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "VtGate server to connect to")
	bindVariables := newBindvars(subFlags)
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vtgate client")
	splitColumnsStr := subFlags.String(
		"split_columns",
		"",
		"A comma-separated list of the split columns to use to split the query."+
			" If this is empty the table's primary key columns will be used.")
	splitCount := subFlags.Int64("split_count", 0, "number of splits to generate.")
	numRowsPerQueryPart := subFlags.Int64(
		"num_rows_per_query_part", 0, "The number of rows to return in each query part.")
	algorithmStr := subFlags.String("algorithm", "EQUAL_SPLITS", "The algorithm to"+
		" use for splitting the query. Either 'FULL_SCAN' or 'EQUAL_SPLITS'")
	keyspace := subFlags.String("keyspace", "", "keyspace to send query to")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if (*splitCount == 0 && *numRowsPerQueryPart == 0) ||
		(*splitCount != 0 && *numRowsPerQueryPart != 0) {
		return fmt.Errorf("Exactly one of split_count or num_rows_per_query_part"+
			"must be nonzero. Got: split_count:%v, num_rows_per_query_part:%v",
			*splitCount, *numRowsPerQueryPart)
	}
	splitColumns := []string{}
	if *splitColumnsStr != "" {
		splitColumns = strings.Split(*splitColumnsStr, ",")
	}
	var algorithm querypb.SplitQueryRequest_Algorithm
	switch *algorithmStr {
	case "FULL_SCAN":
		algorithm = querypb.SplitQueryRequest_FULL_SCAN
	case "EQUAL_SPLITS":
		algorithm = querypb.SplitQueryRequest_EQUAL_SPLITS
	default:
		return fmt.Errorf("Unknown split-query algorithm: %v", algorithmStr)
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <sql> argument is required for the VtGateSplitQuery command")
	}
	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	r, err := vtgateConn.SplitQuery(
		ctx,
		*keyspace,
		subFlags.Arg(0),
		*bindVariables,
		splitColumns,
		int64(*splitCount),
		int64(*numRowsPerQueryPart),
		algorithm,
	)
	if err != nil {
		return fmt.Errorf("SplitQuery failed: %v", err)
	}
	return printJSON(wr.Logger(), r)
}

func commandVtTabletExecute(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	username := subFlags.String("username", "", "If set, value is set as immediate caller id in the request and used by vttablet for TableACL check")
	transactionID := subFlags.Int("transaction_id", 0, "transaction id to use, if inside a transaction.")
	bindVariables := newBindvars(subFlags)
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vttablet client")
	options := subFlags.String("options", "", "execute options values as a text encoded proto of the ExecuteOptions structure")
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet_alias> and <sql> arguments are required for the VtTabletExecute command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	executeOptions, err := parseExecuteOptions(*options)
	if err != nil {
		return err
	}

	if *username != "" {
		ctx = callerid.NewContext(ctx,
			callerid.NewEffectiveCallerID("vtctl", "" /* component */, "" /* subComponent */),
			callerid.NewImmediateCallerID(*username))
	}

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close(ctx)

	qr, err := conn.Execute(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, subFlags.Arg(1), *bindVariables, int64(*transactionID), executeOptions)
	if err != nil {
		return fmt.Errorf("Execute failed: %v", err)
	}
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandVtTabletBegin(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	username := subFlags.String("username", "", "If set, value is set as immediate caller id in the request and used by vttablet for TableACL check")
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vttablet client")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet_alias> argument is required for the VtTabletBegin command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	if *username != "" {
		ctx = callerid.NewContext(ctx,
			callerid.NewEffectiveCallerID("vtctl", "" /* component */, "" /* subComponent */),
			callerid.NewImmediateCallerID(*username))
	}

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close(ctx)

	transactionID, err := conn.Begin(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	})
	if err != nil {
		return fmt.Errorf("Begin failed: %v", err)
	}
	result := map[string]int64{
		"transaction_id": transactionID,
	}
	return printJSON(wr.Logger(), result)
}

func commandVtTabletCommit(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	username := subFlags.String("username", "", "If set, value is set as immediate caller id in the request and used by vttablet for TableACL check")
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vttablet client")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet_alias> and <transaction_id> arguments are required for the VtTabletCommit command")
	}
	transactionID, err := strconv.ParseInt(subFlags.Arg(1), 10, 64)
	if err != nil {
		return err
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	if *username != "" {
		ctx = callerid.NewContext(ctx,
			callerid.NewEffectiveCallerID("vtctl", "" /* component */, "" /* subComponent */),
			callerid.NewImmediateCallerID(*username))
	}

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close(ctx)

	return conn.Commit(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, transactionID)
}

func commandVtTabletRollback(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	username := subFlags.String("username", "", "If set, value is set as immediate caller id in the request and used by vttablet for TableACL check")
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vttablet client")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("the <tablet_alias> and <transaction_id> arguments are required for the VtTabletRollback command")
	}
	transactionID, err := strconv.ParseInt(subFlags.Arg(1), 10, 64)
	if err != nil {
		return err
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	if *username != "" {
		ctx = callerid.NewContext(ctx,
			callerid.NewEffectiveCallerID("vtctl", "" /* component */, "" /* subComponent */),
			callerid.NewImmediateCallerID(*username))
	}

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close(ctx)

	return conn.Rollback(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, transactionID)
}

func commandVtTabletStreamHealth(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	count := subFlags.Int("count", 1, "number of responses to wait for")
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vttablet client")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the VtTabletStreamHealth command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}

	i := 0
	err = conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		data, err := json.Marshal(shr)
		if err != nil {
			wr.Logger().Errorf("cannot json-marshal structure: %v", err)
		} else {
			wr.Logger().Printf("%v\n", string(data))
		}
		i++
		if i >= *count {
			return io.EOF
		}
		return nil
	})
	if err != nil {
		return err
	}
	if i < *count {
		return errors.New("stream ended early")
	}
	return nil
}

func commandVtTabletUpdateStream(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	count := subFlags.Int("count", 1, "number of responses to wait for")
	timestamp := subFlags.Int("timestamp", 0, "timestamp to start the stream from")
	position := subFlags.String("position", "", "position to start the stream from")
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vttablet client")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <tablet alias> argument is required for the VtTabletUpdateStream command")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}

	i := 0
	err = conn.UpdateStream(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, *position, int64(*timestamp), func(se *querypb.StreamEvent) error {
		data, err := json.Marshal(se)
		if err != nil {
			wr.Logger().Errorf("cannot json-marshal structure: %v", err)
		} else {
			wr.Logger().Printf("%v\n", string(data))
		}
		i++
		if i >= *count {
			return io.EOF
		}
		return nil
	})
	if err != nil {
		return err
	}
	if i < *count {
		return errors.New("stream ended early")
	}
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
	table := tablewriter.NewWriter(writer)
	table.SetAutoFormatHeaders(false)

	// Make header.
	header := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		header = append(header, field.Name)
	}
	table.SetHeader(header)

	// Add rows.
	for _, row := range qr.Rows {
		vals := make([]string, 0, len(row))
		for _, val := range row {
			vals = append(vals, val.String())
		}
		table.Append(vals)
	}

	// Print table.
	table.Render()
}
