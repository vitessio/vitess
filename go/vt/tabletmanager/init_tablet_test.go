// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TestInitTablet will test the InitTablet code creates / updates the
// tablet node correctly. Note we modify global parameters (the flags)
// so this has to be in one test.
func TestInitTablet(t *testing.T) {
	ctx := context.Background()
	db := fakesqldb.Register()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	tabletAlias := &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  1,
	}

	// start with a tablet record that doesn't exist
	port := int32(1234)
	gRPCPort := int32(3456)
	mysqlDaemon := mysqlctl.NewFakeMysqlDaemon(db)
	agent := &ActionAgent{
		TopoServer:         ts,
		TabletAlias:        tabletAlias,
		MysqlDaemon:        mysqlDaemon,
		DBConfigs:          dbconfigs.DBConfigs{},
		SchemaOverrides:    nil,
		BinlogPlayerMap:    nil,
		batchCtx:           ctx,
		History:            history.New(historyLength),
		lastHealthMapCount: new(stats.Int),
		_healthy:           fmt.Errorf("healthcheck not run yet"),
	}

	// let's use a real tablet in a shard, that will create
	// the keyspace and shard.
	*tabletHostname = "localhost"
	*initTabletType = "replica"
	*initKeyspace = "test_keyspace"
	*initShard = "-80"
	tabletAlias = &topodatapb.TabletAlias{
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
	ti, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("wrong tablet type: %v", ti.Type)
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
	if ti.Type != topodatapb.TabletType_SPARE {
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
	if ti.Type != topodatapb.TabletType_MASTER {
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
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	if ti.DbNameOverride != "DBNAME" {
		t.Errorf("wrong tablet DbNameOverride: %v", ti.DbNameOverride)
	}
	if len(ti.Tags) != 1 || ti.Tags["aaa"] != "bbb" {
		t.Errorf("wrong tablet tags: %v", ti.Tags)
	}
}
