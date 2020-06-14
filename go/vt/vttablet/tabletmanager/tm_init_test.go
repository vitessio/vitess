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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

func TestStartBuildTabletFromInput(t *testing.T) {
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	port := int32(12)
	grpcport := int32(34)

	// Hostname should be used as is.
	*tabletHostname = "foo"
	*initKeyspace = "test_keyspace"
	*initShard = "0"
	*initTabletType = "replica"
	*initDbNameOverride = "aa"
	wantTablet := &topodatapb.Tablet{
		Alias:    alias,
		Hostname: "foo",
		PortMap: map[string]int32{
			"vt":   port,
			"grpc": grpcport,
		},
		Keyspace:       "test_keyspace",
		Shard:          "0",
		KeyRange:       nil,
		Type:           topodatapb.TabletType_REPLICA,
		DbNameOverride: "aa",
	}

	gotTablet, err := BuildTabletFromInput(alias, port, grpcport)
	require.NoError(t, err)

	// Hostname should be resolved.
	assert.Equal(t, wantTablet, gotTablet)
	*tabletHostname = ""
	gotTablet, err = BuildTabletFromInput(alias, port, grpcport)
	require.NoError(t, err)
	assert.NotEqual(t, "", gotTablet.Hostname)

	// Cannonicalize shard name and compute keyrange.
	*tabletHostname = "foo"
	*initShard = "-C0"
	wantTablet.Shard = "-c0"
	wantTablet.KeyRange = &topodatapb.KeyRange{
		Start: []byte(""),
		End:   []byte("\xc0"),
	}
	gotTablet, err = BuildTabletFromInput(alias, port, grpcport)
	require.NoError(t, err)
	// KeyRange check is explicit because the next comparison doesn't
	// show the diff well enough.
	assert.Equal(t, wantTablet.KeyRange, gotTablet.KeyRange)
	assert.Equal(t, wantTablet, gotTablet)

	// Invalid inputs.
	*initKeyspace = ""
	*initShard = "0"
	_, err = BuildTabletFromInput(alias, port, grpcport)
	assert.Contains(t, err.Error(), "init_keyspace and init_shard must be specified")

	*initKeyspace = "test_keyspace"
	*initShard = ""
	_, err = BuildTabletFromInput(alias, port, grpcport)
	assert.Contains(t, err.Error(), "init_keyspace and init_shard must be specified")

	*initShard = "x-y"
	_, err = BuildTabletFromInput(alias, port, grpcport)
	assert.Contains(t, err.Error(), "cannot validate shard name")

	*initShard = "0"
	*initTabletType = "bad"
	_, err = BuildTabletFromInput(alias, port, grpcport)
	assert.Contains(t, err.Error(), "unknown TabletType bad")

	*initTabletType = "master"
	_, err = BuildTabletFromInput(alias, port, grpcport)
	assert.Contains(t, err.Error(), "invalid init_tablet_type MASTER")
}

func TestStartCreateKeyspaceShard(t *testing.T) {
	defer func(saved time.Duration) { rebuildKeyspaceRetryInterval = saved }(rebuildKeyspaceRetryInterval)
	rebuildKeyspaceRetryInterval = 10 * time.Millisecond

	ctx := context.Background()
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	_ = newTestTM(t, ts, 1, "ks", "0")

	_, err := ts.GetShard(ctx, "ks", "0")
	require.NoError(t, err)

	ensureSrvKeyspace(t, ts, cell, "ks")

	srvVSchema, err := ts.GetSrvVSchema(context.Background(), cell)
	require.NoError(t, err)
	wantVSchema := &vschemapb.Keyspace{}
	assert.Equal(t, wantVSchema, srvVSchema.Keyspaces["ks"])

	// keyspace-shard already created.
	_, err = ts.GetOrCreateShard(ctx, "ks1", "0")
	require.NoError(t, err)
	_ = newTestTM(t, ts, 2, "ks1", "0")
	_, err = ts.GetShard(ctx, "ks1", "0")
	require.NoError(t, err)
	ensureSrvKeyspace(t, ts, cell, "ks1")
	srvVSchema, err = ts.GetSrvVSchema(context.Background(), cell)
	require.NoError(t, err)
	assert.Equal(t, wantVSchema, srvVSchema.Keyspaces["ks1"])

	// srvKeyspace already created
	_, err = ts.GetOrCreateShard(ctx, "ks2", "0")
	require.NoError(t, err)
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, "ks2", []string{cell})
	require.NoError(t, err)
	_ = newTestTM(t, ts, 3, "ks2", "0")
	_, err = ts.GetShard(ctx, "ks2", "0")
	require.NoError(t, err)
	_, err = ts.GetSrvKeyspace(context.Background(), cell, "ks2")
	require.NoError(t, err)
	srvVSchema, err = ts.GetSrvVSchema(context.Background(), cell)
	require.NoError(t, err)
	assert.Equal(t, wantVSchema, srvVSchema.Keyspaces["ks2"])

	// srvVSchema already created
	_, err = ts.GetOrCreateShard(ctx, "ks3", "0")
	require.NoError(t, err)
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, "ks3", []string{cell})
	require.NoError(t, err)
	err = ts.RebuildSrvVSchema(ctx, []string{cell})
	require.NoError(t, err)
	_ = newTestTM(t, ts, 4, "ks3", "0")
	_, err = ts.GetShard(ctx, "ks3", "0")
	require.NoError(t, err)
	_, err = ts.GetSrvKeyspace(context.Background(), cell, "ks3")
	require.NoError(t, err)
	srvVSchema, err = ts.GetSrvVSchema(context.Background(), cell)
	require.NoError(t, err)
	assert.Equal(t, wantVSchema, srvVSchema.Keyspaces["ks3"])
}

func ensureSrvKeyspace(t *testing.T, ts *topo.Server, cell, keyspace string) {
	t.Helper()
	found := false
	for i := 0; i < 10; i++ {
		_, err := ts.GetSrvKeyspace(context.Background(), cell, "ks")
		if err == nil {
			found = true
			break
		}
		require.True(t, topo.IsErrType(err, topo.NoNode), err)
		time.Sleep(rebuildKeyspaceRetryInterval)
	}
	assert.True(t, found)
}

// Init tablet fixes replication data when safe
func TestStartFixesReplicationData(t *testing.T) {
	ctx := context.Background()
	cell := "cell1"
	ts := memorytopo.NewServer(cell, "cell2")
	tm := newTestTM(t, ts, 1, "ks", "0")
	tabletAlias := tm.tabletAlias

	sri, err := ts.GetShardReplication(ctx, cell, "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, tabletAlias, sri.Nodes[0].TabletAlias)

	// Remove the ShardReplication record, try to create the
	// tablets again, make sure it's fixed.
	err = topo.RemoveShardReplicationRecord(ctx, ts, cell, "ks", "0", tabletAlias)
	require.NoError(t, err)
	sri, err = ts.GetShardReplication(ctx, cell, "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, 0, len(sri.Nodes))

	// An initTablet will recreate the shard replication data.
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

	sri, err = ts.GetShardReplication(ctx, cell, "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, tabletAlias, sri.Nodes[0].TabletAlias)
}

// This is a test to make sure a regression does not happen in the future.
// There is code in Start that updates replication data if tablet fails
// to be created due to a NodeExists error. During this particular error we were not doing
// the sanity checks that the provided tablet was the same in the topo.
func TestStartDoesNotUpdateReplicationDataForTabletInWrongShard(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	tm := newTestTM(t, ts, 1, "ks", "0")

	tabletAliases, err := ts.FindAllTabletAliasesInShard(ctx, "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, uint32(1), tabletAliases[0].Uid)

	tablet := newTestTablet(t, 1, "ks", "-d0")
	require.NoError(t, err)
	err = tm.Start(tablet)
	assert.Contains(t, err.Error(), "existing tablet keyspace and shard ks/0 differ")

	tablets, err := ts.FindAllTabletAliasesInShard(ctx, "ks", "-d0")
	require.NoError(t, err)
	assert.Equal(t, 0, len(tablets))
}

// TestStart will test the Start code creates / updates the
// tablet node correctly. Note we modify global parameters (the flags)
// so this has to be in one test.
func TestStart(t *testing.T) {
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
		BatchCtx:       ctx,
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
	*initShard = "0"
	*initTabletType = "replica"
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

	tablet, err := BuildTabletFromInput(tabletAlias, port, gRPCPort)
	require.NoError(t, err)
	tm.tablet = tablet
	err = tm.createKeyspaceShard(context.Background())
	require.NoError(t, err)
	err = tm.initTablet(context.Background())
	require.NoError(t, err)

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
	if ti.Shard != "0" {
		t.Errorf("wrong shard for tablet: %v", ti.Shard)
	}
	if ti.KeyRange != nil {
		t.Errorf("wrong KeyRange for tablet: %v", ti.KeyRange)
	}
	if got := tm.masterTermStartTime(); !got.IsZero() {
		t.Fatalf("REPLICA tablet should not have a MasterTermStartTime set: %v", got)
	}

	// 2. Update shard's master to our alias, then try to init again.
	// (This simulates the case where the MasterAlias in the shard record says
	// that we are the master but the tablet record says otherwise. In that case,
	// we assume we are not the MASTER.)
	_, err = tm.TopoServer.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.MasterAlias = tabletAlias
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	tablet, err = BuildTabletFromInput(tabletAlias, port, gRPCPort)
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

	tablet, err = BuildTabletFromInput(tabletAlias, port, gRPCPort)
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

	tablet, err = BuildTabletFromInput(tabletAlias, port, gRPCPort)
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

	tablet, err = BuildTabletFromInput(tabletAlias, port, gRPCPort)
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

func newTestTM(t *testing.T, ts *topo.Server, uid int, keyspace, shard string) *TabletManager {
	t.Helper()
	tablet := newTestTablet(t, uid, keyspace, shard)
	tm := &TabletManager{
		BatchCtx:            context.Background(),
		TopoServer:          ts,
		MysqlDaemon:         &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(-1)},
		DBConfigs:           &dbconfigs.DBConfigs{},
		QueryServiceControl: tabletservermock.NewController(),
	}
	err := tm.Start(tablet)
	require.NoError(t, err)
	return tm
}

func newTestTablet(t *testing.T, uid int, keyspace, shard string) *topodatapb.Tablet {
	shard, keyRange, err := topo.ValidateShardName(shard)
	require.NoError(t, err)
	return &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  uint32(uid),
		},
		Hostname: "localhost",
		PortMap: map[string]int32{
			"vt":   int32(1234),
			"grpc": int32(3456),
		},
		Keyspace: keyspace,
		Shard:    shard,
		KeyRange: keyRange,
		Type:     topodatapb.TabletType_REPLICA,
	}
}
