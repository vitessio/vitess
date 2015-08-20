// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TestInitTablet will test the InitTablet code creates / updates the
// tablet node correctly. Note we modify global parameters (the flags)
// so this has to be in one test.
func TestInitTablet(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	tabletAlias := &pb.TabletAlias{
		Cell: "cell1",
		Uid:  1,
	}

	// start with idle, and a tablet record that doesn't exist
	port := int32(1234)
	gRPCPort := int32(3456)
	mysqlDaemon := mysqlctl.NewFakeMysqlDaemon()
	agent := &ActionAgent{
		TopoServer:         ts,
		TabletAlias:        tabletAlias,
		MysqlDaemon:        mysqlDaemon,
		DBConfigs:          nil,
		SchemaOverrides:    nil,
		BinlogPlayerMap:    nil,
		LockTimeout:        10 * time.Second,
		batchCtx:           ctx,
		History:            history.New(historyLength),
		lastHealthMapCount: new(stats.Int),
		_healthy:           fmt.Errorf("healthcheck not run yet"),
	}
	*initTabletType = "idle"
	*tabletHostname = "localhost"
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("NewTestActionAgent(idle) failed: %v", err)
	}
	ti, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != pb.TabletType_IDLE {
		t.Errorf("wrong type for tablet: %v", ti.Type)
	}
	if ti.Hostname != "localhost" {
		t.Errorf("wrong hostname for tablet: %v", ti.Hostname)
	}
	if ti.PortMap["vt"] != port {
		t.Errorf("wrong port for tablet: %v", ti.PortMap["vt"])
	}
	if ti.PortMap["grpc"] != gRPCPort {
		t.Errorf("wrong gRPC port for tablet: %v", ti.PortMap["grpc"])
	}

	// try again now that the node exists
	port = 3456
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("NewTestActionAgent(idle again) failed: %v", err)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.PortMap["vt"] != port {
		t.Errorf("wrong port for tablet: %v", ti.PortMap["vt"])
	}
	if ti.PortMap["grpc"] != gRPCPort {
		t.Errorf("wrong gRPC port for tablet: %v", ti.PortMap["grpc"])
	}

	// try with a keyspace and shard on the previously idle tablet,
	// should fail
	*initTabletType = "replica"
	*initKeyspace = "test_keyspace"
	*initShard = "-80"
	if err := agent.InitTablet(port, gRPCPort); err == nil || !strings.Contains(err.Error(), "InitTablet failed because existing tablet keyspace and shard / differ from the provided ones test_keyspace/-80") {
		t.Fatalf("InitTablet(type over idle) didn't fail correctly: %v", err)
	}

	// now let's use a different real tablet in a shard, that will create
	// the keyspace and shard.
	tabletAlias = &pb.TabletAlias{
		Cell: "cell1",
		Uid:  2,
	}
	agent.TabletAlias = tabletAlias
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type) failed: %v", err)
	}
	si, err := ts.GetShard(ctx, "test_keyspace", "-80")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if len(si.Cells) != 1 || si.Cells[0] != "cell1" {
		t.Errorf("shard.Cells not updated properly: %v", si)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != pb.TabletType_REPLICA {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}

	// try to init again, this time with health check on
	*initTabletType = ""
	*targetTabletType = "replica"
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type, healthcheck) failed: %v", err)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != pb.TabletType_SPARE {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}

	// update shard's master to our alias, then try to init again
	si, err = ts.GetShard(ctx, "test_keyspace", "-80")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.MasterAlias = tabletAlias
	if err := ts.UpdateShard(ctx, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
	}
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type, healthcheck) failed: %v", err)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != pb.TabletType_MASTER {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}

	// init again with the tablet_type set, no healthcheck
	// (also check db name override and tags here)
	*initTabletType = "replica"
	*targetTabletType = ""
	*initDbNameOverride = "DBNAME"
	initTags.Set("aaa:bbb")
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type, healthcheck) failed: %v", err)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != pb.TabletType_MASTER {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	if ti.DbNameOverride != "DBNAME" {
		t.Errorf("wrong tablet DbNameOverride: %v", ti.DbNameOverride)
	}
	if len(ti.Tags) != 1 || ti.Tags["aaa"] != "bbb" {
		t.Errorf("wrong tablet tags: %v", ti.Tags)
	}
}
