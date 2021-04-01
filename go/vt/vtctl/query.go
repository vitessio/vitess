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

package vtctl

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"

	"context"

	"github.com/golang/protobuf/proto"
	"github.com/olekukonko/tablewriter"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/wrangler"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// This file contains the query command group for vtctl.

const queriesGroupName = "Queries"

var (
	enableQueries = flag.Bool("enable_queries", false, "if set, allows vtgate and vttablet queries. May have security implications, as the queries will be run from this process.")
)

func init() {
	addCommandGroup(queriesGroupName)

	// VtGate commands
	addCommand(queriesGroupName, command{
		"VtGateExecute",
		commandVtGateExecute,
		"-server <vtgate> [-bind_variables <JSON map>] [-keyspace <default keyspace>] [-tablet_type <tablet type>] [-options <proto text options>] [-json] <sql>",
		"Executes the given SQL query with the provided bound variables against the vtgate server."})

	// VtTablet commands
	addCommand(queriesGroupName, command{
		"VtTabletExecute",
		commandVtTabletExecute,
		"[-username <TableACL user>] [-transaction_id <transaction_id>] [-options <proto text options>] [-json] <tablet alias> <sql>",
		"Executes the given query on the given tablet. -transaction_id is optional. Use VtTabletBegin to start a transaction."})
	addCommand(queriesGroupName, command{
		"VtTabletBegin",
		commandVtTabletBegin,
		"[-username <TableACL user>] <tablet alias>",
		"Starts a transaction on the provided server."})
	addCommand(queriesGroupName, command{
		"VtTabletCommit",
		commandVtTabletCommit,
		"[-username <TableACL user>] <transaction_id>",
		"Commits the given transaction on the provided server."})
	addCommand(queriesGroupName, command{
		"VtTabletRollback",
		commandVtTabletRollback,
		"[-username <TableACL user>] <tablet alias> <transaction_id>",
		"Rollbacks the given transaction on the provided server."})
	addCommand(queriesGroupName, command{
		"VtTabletStreamHealth",
		commandVtTabletStreamHealth,
		"[-count <count, default 1>] <tablet alias>",
		"Executes the StreamHealth streaming query to a vttablet process. Will stop after getting <count> answers."})
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
	if !*enableQueries {
		return fmt.Errorf("query commands are disabled (set the -enable_queries flag to enable)")
	}

	server := subFlags.String("server", "", "VtGate server to connect to")
	bindVariables := newBindvars(subFlags)
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

	vtgateConn, err := vtgateconn.Dial(ctx, *server)
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	session := vtgateConn.Session(*targetString, executeOptions)
	defer vtgateConn.Close()

	bindVars, err := sqltypes.BuildBindVariables(*bindVariables)
	if err != nil {
		return fmt.Errorf("BuildBindVariables failed: %v", err)
	}

	qr, err := session.Execute(ctx, subFlags.Arg(0), bindVars)
	if err != nil {
		return fmt.Errorf("execute failed: %v", err)
	}
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandVtTabletExecute(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if !*enableQueries {
		return fmt.Errorf("query commands are disabled (set the -enable_queries flag to enable)")
	}

	username := subFlags.String("username", "", "If set, value is set as immediate caller id in the request and used by vttablet for TableACL check")
	transactionID := subFlags.Int("transaction_id", 0, "transaction id to use, if inside a transaction.")
	bindVariables := newBindvars(subFlags)
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

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, grpcclient.FailFast(false))
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close(ctx)

	bindVars, err := sqltypes.BuildBindVariables(*bindVariables)
	if err != nil {
		return fmt.Errorf("BuildBindVariables failed: %v", err)
	}

	// We do not support reserve connection through vtctl commands, so reservedID is always 0.
	qr, err := conn.Execute(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, subFlags.Arg(1), bindVars, int64(*transactionID), 0, executeOptions)
	if err != nil {
		return fmt.Errorf("execute failed: %v", err)
	}
	if *json {
		return printJSON(wr.Logger(), qr)
	}
	printQueryResult(loggerWriter{wr.Logger()}, qr)
	return nil
}

func commandVtTabletBegin(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if !*enableQueries {
		return fmt.Errorf("query commands are disabled (set the -enable_queries flag to enable)")
	}

	username := subFlags.String("username", "", "If set, value is set as immediate caller id in the request and used by vttablet for TableACL check")
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

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, grpcclient.FailFast(false))
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close(ctx)

	transactionID, _, err := conn.Begin(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, nil)
	if err != nil {
		return fmt.Errorf("begin failed: %v", err)
	}
	result := map[string]int64{
		"transaction_id": transactionID,
	}
	return printJSON(wr.Logger(), result)
}

func commandVtTabletCommit(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if !*enableQueries {
		return fmt.Errorf("query commands are disabled (set the -enable_queries flag to enable)")
	}

	username := subFlags.String("username", "", "If set, value is set as immediate caller id in the request and used by vttablet for TableACL check")
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

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, grpcclient.FailFast(false))
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close(ctx)

	// we do not support reserving through vtctl commands
	_, err = conn.Commit(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, transactionID)
	return err
}

func commandVtTabletRollback(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if !*enableQueries {
		return fmt.Errorf("query commands are disabled (set the -enable_queries flag to enable)")
	}

	username := subFlags.String("username", "", "If set, value is set as immediate caller id in the request and used by vttablet for TableACL check")
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

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, grpcclient.FailFast(false))
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}
	defer conn.Close(ctx)

	// we do not support reserving through vtctl commands
	_, err = conn.Rollback(ctx, &querypb.Target{
		Keyspace:   tabletInfo.Tablet.Keyspace,
		Shard:      tabletInfo.Tablet.Shard,
		TabletType: tabletInfo.Tablet.Type,
	}, transactionID)
	return err
}

func commandVtTabletStreamHealth(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if !*enableQueries {
		return fmt.Errorf("query commands are disabled (set the -enable_queries flag to enable)")
	}

	count := subFlags.Int("count", 1, "number of responses to wait for")
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

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, grpcclient.FailFast(false))
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", tabletAlias, err)
	}

	i := 0
	err = conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		data, err := json.Marshal(shr)
		if err != nil {
			wr.Logger().Errorf2(err, "cannot json-marshal structure")
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
	if qr == nil {
		return
	}
	if len(qr.Rows) == 0 {
		return
	}

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
			vals = append(vals, val.ToString())
		}
		table.Append(vals)
	}

	// Print table.
	table.Render()
}
