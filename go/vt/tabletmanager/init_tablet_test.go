// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TestInitTablet will test the InitTablet code creates / updates the
// tablet node correctly. Note we modify global parameters (the flags)
// so this has to be in one test.
func TestInitTablet(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	tabletAlias := &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  1,
	}

	// start with a tablet record that doesn't exist
	port := int32(1234)
	gRPCPort := int32(3456)
	mysqlDaemon := mysqlctl.NewFakeMysqlDaemon(nil)
	agent := &ActionAgent{
		TopoServer:      ts,
		TabletAlias:     tabletAlias,
		MysqlDaemon:     mysqlDaemon,
		DBConfigs:       dbconfigs.DBConfigs{},
		BinlogPlayerMap: nil,
		batchCtx:        ctx,
		History:         history.New(historyLength),
		_healthy:        fmt.Errorf("healthcheck not run yet"),
	}

	// let's use a real tablet in a shard, that will create
	// the keyspace and shard.
	*tabletHostname = "localhost"
	*initKeyspace = "test_keyspace"
	*initShard = "-80"
	*initTabletType = "replica"
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

	// update shard's master to our alias, then try to init again
	si, err = agent.TopoServer.UpdateShardFields(ctx, "test_keyspace", "-80", func(si *topo.ShardInfo) error {
		si.MasterAlias = tabletAlias
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type, healthcheck) failed: %v", err)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	// It should still be replica, because the tablet record doesn't agree.
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}

	// Fix the tablet record to agree that we're master.
	ti.Type = topodatapb.TabletType_MASTER
	if err := ts.UpdateTablet(ctx, ti); err != nil {
		t.Fatalf("UpdateTablet failed: %v", err)
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

	// init again with the tablet_type set, using init_tablet_type
	// (also check db name override and tags here)
	*initTabletType = "replica"
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
