/*
Copyright 2026 The Vitess Authors.

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
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// hangingCellFactory is a memorytopo factory whose cell's reads do not answer while hang is set,
// like the topology server of a partitioned cell.
type hangingCellFactory struct {
	*memorytopo.Factory
	cell string
	hang atomic.Bool
}

// Create is part of the topo.Factory interface.
func (f *hangingCellFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil || cell != f.cell {
		return conn, err
	}
	return &hangingConn{Conn: conn, hang: &f.hang}, nil
}

type hangingConn struct {
	topo.Conn
	hang *atomic.Bool
}

// Get is part of the topo.Conn interface.
func (c *hangingConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if c.hang.Load() {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	return c.Conn.Get(ctx, filePath)
}

// TestGroupReplicationSyncPromotesWhileOldPrimaryCellTopoHangs reproduces the S9i chaos scenario:
// the primary's cell, including its topology server, is partitioned from the other cells, and the
// group elects a member in another cell. Reading the shard's tablet records waited for the
// partitioned cell until the step of the sync loop timed out, every step, and the reads of the
// other cells' tablet records failed with it, so the elected member's tablet was not promoted
// until the partition healed: 140s without a primary. Each cell is now read with its own timeout,
// and the tablet records of the cells that answer are used.
func TestGroupReplicationSyncPromotesWhileOldPrimaryCellTopoHangs(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	_, mf := memorytopo.NewServerAndFactory(ctx, "cell1", "cell2")
	f := &hangingCellFactory{Factory: mf, cell: "cell2"}
	ts, err := topo.NewWithFactory(f, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplicationCrossCell}))
	_, err = ts.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)
	addPeerTablets(t, ts, 2)
	oldPrimary := &topodatapb.TabletAlias{Cell: "cell2", Uid: 3}
	require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias: oldPrimary, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_PRIMARY,
		Hostname: "tablet3", MysqlHostname: "mysql3", MysqlPort: 3306,
	}))
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = oldPrimary
		si.GroupReplicationVoters = []*topodatapb.TabletAlias{{Cell: "cell1", Uid: 1}, {Cell: "cell1", Uid: 2}, oldPrimary}
		si.GroupReplicationIncarnation = "1780000001"
		return nil
	})
	require.NoError(t, err)

	peers := newGRPeersTMC()
	peers.set(2, &replicationdatapb.FullStatus{ServerUuid: testServerUUID(2)})
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, peers, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	t.Cleanup(func() { f.hang.Store(false) })
	// The tablet has not learned the server_uuids of its peers, for example because it restarted:
	// it asks the voters of the cells that answer for them, from their tablet records.
	tm.groupReplicationPeers.mu.Lock()
	tm.groupReplicationPeers.serverUUIDs = nil
	tm.groupReplicationPeers.mu.Unlock()

	// The old primary's cell is cut off, and the group elected this member.
	f.hang.Store(true)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:5"))

	// One step of the sync loop, with the loop's own deadline.
	stepCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	start := time.Now()
	newGroupReplicationSync(tm).reconcile(stepCtx)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type, "the elected member must be promoted")
	assert.Less(t, time.Since(start), topo.RemoteOperationTimeout/2, "reading the partitioned cell must not take the whole step")
}

// TestGroupReplicationSyncCountsVoterOfUnreachableCell checks that a voter whose tablet record
// cannot be read, because its cell's topology server is cut off, still counts toward the majority
// of the voters when its MySQL is ONLINE in the view: the tablet knows its server_uuid (S9b chaos
// scenario: the primary dies while another cell's topology server is down).
func TestGroupReplicationSyncCountsVoterOfUnreachableCell(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	_, mf := memorytopo.NewServerAndFactory(ctx, "cell1", "cell2")
	f := &hangingCellFactory{Factory: mf, cell: "cell2"}
	ts, err := topo.NewWithFactory(f, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplicationCrossCell}))
	_, err = ts.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)
	addPeerTablets(t, ts, 3)
	otherCellVoter := &topodatapb.TabletAlias{Cell: "cell2", Uid: 2}
	require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias: otherCellVoter, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_REPLICA,
		Hostname: "tablet2", MysqlHostname: "mysql2", MysqlPort: 3306,
	}))
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationVoters = []*topodatapb.TabletAlias{{Cell: "cell1", Uid: 1}, otherCellVoter, {Cell: "cell1", Uid: 3}}
		si.GroupReplicationIncarnation = "1780000001"
		return nil
	})
	require.NoError(t, err)

	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, newGRPeersTMC(), func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	t.Cleanup(func() { f.hang.Store(false) })
	// The tablet learned the voter's server_uuid while the voter's cell was reachable.
	tm.groupReplicationPeers.setServerUUID("cell2-0000000002", testServerUUID(2))

	// cell2's topology server is cut off, the primary (cell1-3) died, and the group elected this
	// member, with the voter of cell2 ONLINE in its view.
	f.hang.Store(true)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:5"))

	stepCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	newGroupReplicationSync(tm).reconcile(stepCtx)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type, "the voter of the unreachable cell is in the view")
}

// TestGroupReplicationSyncPromotionReusesTabletRecords checks that the promotion of the member the
// group elected, while the old primary's cell is cut off, does not wait for that cell: the tablet
// records read a moment before identify every voter, so they are reused.
func TestGroupReplicationSyncPromotionReusesTabletRecords(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	_, mf := memorytopo.NewServerAndFactory(ctx, "cell1", "cell2")
	f := &hangingCellFactory{Factory: mf, cell: "cell2"}
	ts, err := topo.NewWithFactory(f, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplicationCrossCell}))
	_, err = ts.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)
	addPeerTablets(t, ts, 2)
	oldPrimary := &topodatapb.TabletAlias{Cell: "cell2", Uid: 3}
	require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias: oldPrimary, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_PRIMARY,
		Hostname: "tablet3", MysqlHostname: "mysql3", MysqlPort: 3306,
	}))
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = oldPrimary
		si.GroupReplicationVoters = []*topodatapb.TabletAlias{{Cell: "cell1", Uid: 1}, {Cell: "cell1", Uid: 2}, oldPrimary}
		si.GroupReplicationIncarnation = "1780000001"
		return nil
	})
	require.NoError(t, err)

	peers := newGRPeersTMC()
	peers.set(2, &replicationdatapb.FullStatus{ServerUuid: testServerUUID(2)})
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, peers, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	t.Cleanup(func() { f.hang.Store(false) })

	// A step of the sync loop while every cell answers, as a secondary.
	s := newGroupReplicationSync(tm)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:4"))
	s.reconcile(ctx)

	// The old primary's cell is cut off, and the group elected this member.
	f.hang.Store(true)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:5"))
	stepCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	start := time.Now()
	s.reconcile(stepCtx)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type, "the elected member must be promoted")
	assert.Less(t, time.Since(start), groupReplicationCellTimeout/2, "the promotion must not wait for the partitioned cell")
}
