// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctl

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
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
			"-server <vtgate> [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] [-keyspace <default keyspace>] [-tablet_type <tablet type>] [-json] <sql>",
			"Executes the given SQL query with the provided bound variables against the vtgate server."})
		addCommand(queriesGroupName, command{
			"VtGateExecuteShards",
			commandVtGateExecuteShards,
			"-server <vtgate> -keyspace <keyspace> -shards <shard0>,<shard1>,... [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] [-tablet_type <tablet type>] [-json] <sql>",
			"Executes the given SQL query with the provided bound variables against the vtgate server. It is routed to the provided shards."})
		addCommand(queriesGroupName, command{
			"VtGateExecuteKeyspaceIds",
			commandVtGateExecuteKeyspaceIds,
			"-server <vtgate> -keyspace <keyspace> -keyspace_ids <ks1 in hex>,<k2 in hex>,... [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] [-tablet_type <tablet type>] [-json] <sql>",
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
			"[-bind_variables <JSON map>] [-connect_timeout <connect timeout>] [-transaction_id <transaction_id>] [-tablet_type <tablet_type>] [-json] -keyspace <keyspace> -shard <shard> <tablet alias> <sql>",
			"Executes the given query on the given tablet."})
		addCommand(queriesGroupName, command{
			"VtTabletBegin",
			commandVtTabletBegin,
			"[-connect_timeout <connect timeout>] [-tablet_type <tablet_type>] -keyspace <keyspace> -shard <shard> <tablet alias>",
			"Starts a transaction on the provided server."})
		addCommand(queriesGroupName, command{
			"VtTabletCommit",
			commandVtTabletCommit,
			"[-connect_timeout <connect timeout>] [-tablet_type <tablet_type>] -keyspace <keyspace> -shard <shard> <tablet alias> <transaction_id>",
			"Commits a transaction on the provided server."})
		addCommand(queriesGroupName, command{
			"VtTabletRollback",
			commandVtTabletRollback,
			"[-connect_timeout <connect timeout>] [-tablet_type <tablet_type>] -keyspace <keyspace> -shard <shard> <tablet alias> <transaction_id>",
			"Rollbacks a transaction on the provided server."})
		addCommand(queriesGroupName, command{
			"VtTabletStreamHealth",
			commandVtTabletStreamHealth,
			"[-count <count, default 1>] [-connect_timeout <connect timeout>] <tablet alias>",
			"Executes the StreamHealth streaming query to a vttablet process. Will stop after getting <count> answers."})
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

func commandVtGateExecute(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "VtGate server to connect to")
	bindVariables := newBindvars(subFlags)
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vtgate client")
	keyspace := subFlags.String("keyspace", "", "default keyspace to use")
	tabletType := subFlags.String("tablet_type", "master", "tablet type to query")
	json := subFlags.Bool("json", false, "Output JSON instead of human-readable table")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <sql> argument is required for the VtGateExecute command")
	}
	t, err := parseTabletType(*tabletType, []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY})
	if err != nil {
		return err
	}

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout, *keyspace)
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	qr, err := vtgateConn.Execute(ctx, subFlags.Arg(0), *bindVariables, t)
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

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout, "")
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	qr, err := vtgateConn.ExecuteShards(ctx, subFlags.Arg(0), *keyspace, shards, *bindVariables, t)
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

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout, "")
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	qr, err := vtgateConn.ExecuteKeyspaceIds(ctx, subFlags.Arg(0), *keyspace, keyspaceIDs, *bindVariables, t)
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
	splitColumn := subFlags.String("split_column", "", "force the use of this column to split the query")
	splitCount := subFlags.Int("split_count", 16, "number of splits to generate")
	keyspace := subFlags.String("keyspace", "", "keyspace to send query to")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <sql> argument is required for the VtGateSplitQuery command")
	}

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout, "")
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	r, err := vtgateConn.SplitQuery(ctx, *keyspace, subFlags.Arg(0), *bindVariables, *splitColumn, int64(*splitCount))
	if err != nil {
		return fmt.Errorf("SplitQuery failed: %v", err)
	}
	return printJSON(wr.Logger(), r)
}

func commandVtTabletExecute(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	transactionID := subFlags.Int("transaction_id", 0, "transaction id to use, if inside a transaction.")
	bindVariables := newBindvars(subFlags)
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vttablet client")
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

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close()

	qr, err := conn.Execute(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, subFlags.Arg(1), *bindVariables, int64(*transactionID))
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

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close()

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

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close()

	return conn.Commit(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, transactionID)
}

func commandVtTabletRollback(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
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

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, *connectTimeout)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close()

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
		return fmt.Errorf("The <tablet alias> argument is required for the VtTabletStreamHealth command.")
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

	stream, err := conn.StreamHealth(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < *count; i++ {
		shr, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("stream ended early: %v", err)
		}
		data, err := json.Marshal(shr)
		if err != nil {
			wr.Logger().Errorf("cannot json-marshal structure: %v", err)
		} else {
			wr.Logger().Printf("%v\n", string(data))
		}
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
