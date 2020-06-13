/*
Copyright 2020 The Vitess Authors.

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

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

// Init tablet fixes replication data when safe
func TestInitTabletFixesReplicationData(t *testing.T) {
	ctx := context.Background()
	cell := "cell1"
	ts := memorytopo.NewServer(cell, "cell2")
	tabletAlias := &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  1,
	}

	// start with a tablet record that doesn't exist
	tm := &TabletManager{
		TopoServer:  ts,
		tabletAlias: tabletAlias,
		MysqlDaemon: fakemysqldaemon.NewFakeMysqlDaemon(nil),
		DBConfigs:   &dbconfigs.DBConfigs{},
		batchCtx:    ctx,
		History:     history.New(historyLength),
		_healthy:    fmt.Errorf("healthcheck not run yet"),
	}

	// 1. Initialize the tablet as REPLICA.
	*tabletHostname = "localhost"
	*initKeyspace = "test_keyspace"
	*initShard = "-C0"
	*initTabletType = "replica"
	tabletAlias = &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  2,
	}
	tm.tabletAlias = tabletAlias

	tablet, err := buildTabletFromInput(tabletAlias, int32(1234), int32(3456))
	require.NoError(t, err)
	tm.tablet = tablet
	err = tm.createKeyspaceShard(context.Background())
	require.NoError(t, err)
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

	sri, err := ts.GetShardReplication(ctx, cell, *initKeyspace, "-c0")
	if err != nil || len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, tabletAlias) {
		t.Fatalf("Created ShardReplication doesn't match: %v %v", sri, err)
	}

	// Remove the ShardReplication record, try to create the
	// tablets again, make sure it's fixed.
	err = topo.RemoveShardReplicationRecord(ctx, ts, cell, *initKeyspace, "-c0", tabletAlias)
	require.NoError(t, err)
	sri, err = ts.GetShardReplication(ctx, cell, *initKeyspace, "-c0")
	if err != nil || len(sri.Nodes) != 0 {
		t.Fatalf("Modifed ShardReplication doesn't match: %v %v", sri, err)
	}

	// An initTablet will recreate the shard replication data.
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

	sri, err = ts.GetShardReplication(ctx, cell, *initKeyspace, "-c0")
	if err != nil || len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, tabletAlias) {
		t.Fatalf("Created ShardReplication doesn't match: %v %v", sri, err)
	}
}

// This is a test to make sure a regression does not happen in the future.
// There is code in InitTablet that updates replication data if tablet fails
// to be created due to a NodeExists error. During this particular error we were not doing
// the sanity checks that the provided tablet was the same in the topo.
func TestInitTabletDoesNotUpdateReplicationDataForTabletInWrongShard(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	tabletAlias := &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  1,
	}

	// start with a tablet record that doesn't exist
	tm := &TabletManager{
		TopoServer:  ts,
		tabletAlias: tabletAlias,
		MysqlDaemon: fakemysqldaemon.NewFakeMysqlDaemon(nil),
		DBConfigs:   &dbconfigs.DBConfigs{},
		batchCtx:    ctx,
		History:     history.New(historyLength),
		_healthy:    fmt.Errorf("healthcheck not run yet"),
	}

	// 1. Initialize the tablet as REPLICA.
	*tabletHostname = "localhost"
	*initKeyspace = "test_keyspace"
	*initShard = "-C0"
	*initTabletType = "replica"
	tabletAlias = &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  2,
	}
	tm.tabletAlias = tabletAlias

	tablet, err := buildTabletFromInput(tabletAlias, int32(1234), int32(3456))
	require.NoError(t, err)
	tm.tablet = tablet
	err = tm.createKeyspaceShard(context.Background())
	require.NoError(t, err)
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

	tabletAliases, err := ts.FindAllTabletAliasesInShard(ctx, "test_keyspace", "-c0")
	if err != nil {
		t.Fatalf("Could not fetch tablet aliases for shard: %v", err)
	}

	if len(tabletAliases) != 1 {
		t.Fatalf("Expected to have only one tablet alias, got: %v", len(tabletAliases))
	}
	if tabletAliases[0].Uid != 2 {
		t.Fatalf("Expected table UID be equal to 2, got: %v", tabletAliases[0].Uid)
	}

	// Try to initialize a tablet with the same uid in a different shard.
	*initShard = "-D0"
	tablet, err = buildTabletFromInput(tabletAlias, int32(1234), int32(3456))
	require.NoError(t, err)
	tm.tablet = tablet
	err = tm.createKeyspaceShard(context.Background())
	require.NoError(t, err)
	err = tm.initTablet(context.Background())
	// This should fail.
	require.Error(t, err)

	if tablets, _ := ts.FindAllTabletAliasesInShard(ctx, "test_keyspace", "-d0"); len(tablets) != 0 {
		t.Fatalf("Tablet shouldn't be added to replication data")
	}
}

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
	db := fakesqldb.New(t)
	defer db.Close()

	// start with a tablet record that doesn't exist
	port := int32(1234)
	gRPCPort := int32(3456)
	mysqlDaemon := fakemysqldaemon.NewFakeMysqlDaemon(db)
	tm := &TabletManager{
		TopoServer:     ts,
		tabletAlias:    tabletAlias,
		MysqlDaemon:    mysqlDaemon,
		DBConfigs:      &dbconfigs.DBConfigs{},
		VREngine:       vreplication.NewTestEngine(nil, "", nil, nil, "", nil),
		batchCtx:       ctx,
		History:        history.New(historyLength),
		baseTabletType: topodatapb.TabletType_REPLICA,
		_healthy:       fmt.Errorf("healthcheck not run yet"),
	}

	// 1. Initialize the tablet as REPLICA.
	// This will create the respective topology records.
	// We use a capitalized shard name here, to make sure the
	// Keyrange computation works, fills in the KeyRange, and converts
	// it to lower case.
	*tabletHostname = "localhost"
	*initKeyspace = "test_keyspace"
	*initShard = "-C0"
	tabletAlias = &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  2,
	}

	_, err := tm.TopoServer.GetSrvKeyspace(ctx, "cell1", "test_keyspace")
	switch {
	case topo.IsErrType(err, topo.NoNode):
		// srvKeyspace should not be when tablets haven't been registered to this cell
	default:
		t.Fatalf("GetSrvKeyspace failed: %v", err)
	}

	tm.tabletAlias = tabletAlias

	tablet, err := buildTabletFromInput(tabletAlias, port, gRPCPort)
	require.NoError(t, err)
	tm.tablet = tablet
	err = tm.createKeyspaceShard(context.Background())
	require.NoError(t, err)
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

	si, err := ts.GetShard(ctx, "test_keyspace", "-c0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	_, err = tm.TopoServer.GetSrvKeyspace(ctx, "cell1", "test_keyspace")
	switch {
	case err != nil:
		// srvKeyspace should not be when tablets haven't been registered to this cell
	default:
		t.Errorf("Serving keyspace was not generated for cell: %v", si)
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
	if ti.Shard != "-c0" {
		t.Errorf("wrong shard for tablet: %v", ti.Shard)
	}
	if string(ti.KeyRange.Start) != "" || string(ti.KeyRange.End) != "\xc0" {
		t.Errorf("wrong KeyRange for tablet: %v", ti.KeyRange)
	}
	if got := tm.masterTermStartTime(); !got.IsZero() {
		t.Fatalf("REPLICA tablet should not have a MasterTermStartTime set: %v", got)
	}

	// 2. Update shard's master to our alias, then try to init again.
	// (This simulates the case where the MasterAlias in the shard record says
	// that we are the master but the tablet record says otherwise. In that case,
	// we assume we are not the MASTER.)
	_, err = tm.TopoServer.UpdateShardFields(ctx, "test_keyspace", "-c0", func(si *topo.ShardInfo) error {
		si.MasterAlias = tabletAlias
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	tablet, err = buildTabletFromInput(tabletAlias, port, gRPCPort)
	require.NoError(t, err)
	tm.tablet = tablet
	err = tm.createKeyspaceShard(context.Background())
	require.NoError(t, err)
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	// It should still be replica, because the tablet record doesn't agree.
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	if got := tm.masterTermStartTime(); !got.IsZero() {
		t.Fatalf("REPLICA tablet should not have a masterTermStartTime set: %v", got)
	}

	// 3. Delete the tablet record. The shard record still says that we are the
	// MASTER. Since it is the only source, we assume that its information is
	// correct and start as MASTER.
	if err := ts.DeleteTablet(ctx, tabletAlias); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}

	tablet, err = buildTabletFromInput(tabletAlias, port, gRPCPort)
	require.NoError(t, err)
	tm.tablet = tablet
	err = tm.createKeyspaceShard(context.Background())
	require.NoError(t, err)
	err = tm.checkMastership(ctx)
	require.NoError(t, err)
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	ter1 := ti.GetMasterTermStartTime()
	if ter1.IsZero() {
		t.Fatalf("MASTER tablet should have a masterTermStartTime set")
	}

	// 4. Fix the tablet record to agree that we're master.
	// Shard and tablet record are in sync now and we assume that we are actually
	// the MASTER.
	ti.Type = topodatapb.TabletType_MASTER
	if err := ts.UpdateTablet(ctx, ti); err != nil {
		t.Fatalf("UpdateTablet failed: %v", err)
	}

	tablet, err = buildTabletFromInput(tabletAlias, port, gRPCPort)
	require.NoError(t, err)
	tm.tablet = tablet
	err = tm.createKeyspaceShard(context.Background())
	require.NoError(t, err)
	err = tm.checkMastership(ctx)
	require.NoError(t, err)
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	ter2 := ti.GetMasterTermStartTime()
	if ter2.IsZero() || !ter2.Equal(ter1) {
		t.Fatalf("After a restart, masterTermStartTime must be equal to the previous time saved in the tablet record. Previous timestamp: %v current timestamp: %v", ter1, ter2)
	}

	// 5. Subsequent inits will still start the vttablet as MASTER.
	// (Also check db name override and tags here.)
	*initDbNameOverride = "DBNAME"
	initTags.Set("aaa:bbb")

	tablet, err = buildTabletFromInput(tabletAlias, port, gRPCPort)
	require.NoError(t, err)
	tm.tablet = tablet
	err = tm.createKeyspaceShard(context.Background())
	require.NoError(t, err)
	err = tm.checkMastership(ctx)
	require.NoError(t, err)
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

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
	ter3 := ti.GetMasterTermStartTime()
	if ter3.IsZero() || !ter3.Equal(ter2) {
		t.Fatalf("After a restart, masterTermStartTime must be set to the previous time saved in the tablet record. Previous timestamp: %v current timestamp: %v", ter2, ter3)
	}
}
