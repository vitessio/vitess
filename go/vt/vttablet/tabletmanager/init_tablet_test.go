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

	// 1. Initialize the tablet as REPLICA.
	// This will create the respective topology records.
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
	if got := agent._tabletExternallyReparentedTime; !got.IsZero() {
		t.Fatalf("REPLICA tablet should not have an ExternallyReparentedTimestamp set: %v", got)
	}

	// 2. Update shard's master to our alias, then try to init again.
	// (This simulates the case where the MasterAlias in the shard record says
	// that we are the master but the tablet record says otherwise. In that case,
	// we assume we are not the MASTER.)
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
	if got := agent._tabletExternallyReparentedTime; !got.IsZero() {
		t.Fatalf("REPLICA tablet should not have an ExternallyReparentedTimestamp set: %v", got)
	}

	// 3. Delete the tablet record. The shard record still says that we are the
	// MASTER. Since it is the only source, we assume that its information is
	// correct and start as MASTER.
	if err := ts.DeleteTablet(ctx, tabletAlias); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
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
	ter1 := agent._tabletExternallyReparentedTime
	if ter1.IsZero() {
		t.Fatalf("MASTER tablet should have an ExternallyReparentedTimestamp set")
	}

	// 4. Fix the tablet record to agree that we're master.
	// Shard and tablet record are in sync now and we assume that we are actually
	// the MASTER.
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
	ter2 := agent._tabletExternallyReparentedTime
	if ter2.IsZero() || !ter2.After(ter1) {
		t.Fatalf("After a restart, ExternallyReparentedTimestamp must be set to the current time. Previous timestamp: %v current timestamp: %v", ter1, ter2)
	}

	// 5. Subsequent inits will still start the vttablet as MASTER.
	// (Also check db name override and tags here.)
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
	ter3 := agent._tabletExternallyReparentedTime
	if ter3.IsZero() || !ter3.After(ter2) {
		t.Fatalf("After a restart, ExternallyReparentedTimestamp must be set to the current time. Previous timestamp: %v current timestamp: %v", ter2, ter3)
	}
}
