// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctl

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/youtube/vitess/go/jscfg"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

// This file contains the query command group for vtctl.

const queriesGroupName = "Queries"

func init() {
	addCommandGroup(queriesGroupName)
	addCommand(queriesGroupName, command{
		"VtGateExecute",
		commandVtGateExecute,
		"-server <vtgate> [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] [-tablet_type <tablet type>] <sql>",
		"Executes the given SQL query with the provided bound variables against the vtgate server."})
	addCommand(queriesGroupName, command{
		"VtGateExecuteShard",
		commandVtGateExecuteShard,
		"-server <vtgate> -keyspace <keyspace> -shards <shard0>,<shard1>,... [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] [-tablet_type <tablet type>] <sql>",
		"Executes the given SQL query with the provided bound variables against the vtgate server."})
	addCommand(queriesGroupName, command{
		"VtGateSplitQuery",
		commandVtGateSplitQuery,
		"-server <vtgate> -keyspace <keyspace> -split_count <split_count> [-bind_variables <JSON map>] [-connect_timeout <connect timeout>] <sql>",
		"Executes the SplitQuery computation for the given SQL query with the provided bound variables against the vtgate server (this is the base query for Map-Reduce workloads, and is provided here for debug / test purposes)."})
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
	tabletType := subFlags.String("tablet_type", "master", "tablet type to query")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <sql> argument is required for the VtGateExecute command")
	}
	t, err := parseTabletType(*tabletType, []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	qr, err := vtgateConn.Execute(ctx, subFlags.Arg(0), *bindVariables, t)
	if err != nil {
		return fmt.Errorf("Execute failed: %v", err)
	}
	wr.Logger().Printf("%v\n", jscfg.ToJSON(qr))
	return nil
}

func commandVtGateExecuteShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "VtGate server to connect to")
	bindVariables := newBindvars(subFlags)
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vtgate client")
	tabletType := subFlags.String("tablet_type", "master", "tablet type to query")
	keyspace := subFlags.String("keyspace", "", "keyspace to send query to")
	shardsStr := subFlags.String("shards", "", "comma-separated list of shards to send query to")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <sql> argument is required for the VtGateExecuteShard command")
	}
	t, err := parseTabletType(*tabletType, []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY})
	if err != nil {
		return err
	}
	var shards []string
	if *shardsStr != "" {
		shards = strings.Split(*shardsStr, ",")
	}

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	qr, err := vtgateConn.ExecuteShard(ctx, subFlags.Arg(0), *keyspace, shards, *bindVariables, t)
	if err != nil {
		return fmt.Errorf("Execute failed: %v", err)
	}
	wr.Logger().Printf("%v\n", jscfg.ToJSON(qr))
	return nil
}

func commandVtGateSplitQuery(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	server := subFlags.String("server", "", "VtGate server to connect to")
	bindVariables := newBindvars(subFlags)
	connectTimeout := subFlags.Duration("connect_timeout", 30*time.Second, "Connection timeout for vtgate client")
	splitCount := subFlags.Int("split_count", 16, "number of splits to generate")
	keyspace := subFlags.String("keyspace", "", "keyspace to send query to")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <sql> argument is required for the VtGateSplitQuery command")
	}

	vtgateConn, err := vtgateconn.Dial(ctx, *server, *connectTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to vtgate '%v': %v", *server, err)
	}
	defer vtgateConn.Close()
	r, err := vtgateConn.SplitQuery(ctx, *keyspace, tproto.BoundQuery{
		Sql:           subFlags.Arg(0),
		BindVariables: *bindVariables,
	}, *splitCount)
	if err != nil {
		return fmt.Errorf("SplitQuery failed: %v", err)
	}
	wr.Logger().Printf("%v\n", jscfg.ToJSON(r))
	return nil
}
