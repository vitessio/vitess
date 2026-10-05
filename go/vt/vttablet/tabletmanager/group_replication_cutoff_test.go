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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// cutOffTopoFactory is a memorytopo factory whose connections, the global one included, stop
// answering while the tablet is cut off from the topology, like those of a tablet whose cell is
// partitioned from the global topology server: a request waits until the caller gives up, or until
// the partition heals and the connection delivers it.
type cutOffTopoFactory struct {
	*memorytopo.Factory

	mu sync.Mutex
	// healed is closed when the partition heals. It is nil while the topology answers.
	healed chan struct{}
	// refused makes every request for a tablet record fail right away, like a topology server
	// that refuses connections.
	refused bool
	// refusals counts the requests refused.
	refusals int
}

func (f *cutOffTopoFactory) refusedRequests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refusals
}

// refuse makes the topology refuse every request for a tablet record until heal.
func (f *cutOffTopoFactory) refuse() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refused = true
}

func (f *cutOffTopoFactory) cut() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.healed == nil {
		f.healed = make(chan struct{})
	}
}

func (f *cutOffTopoFactory) heal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.healed != nil {
		close(f.healed)
		f.healed = nil
	}
	f.refused = false
}

// wait blocks while the topology is cut off, until ctx ends or the partition heals.
func (f *cutOffTopoFactory) wait(ctx context.Context, filePath string) error {
	f.mu.Lock()
	healed := f.healed
	refused := f.refused && strings.HasPrefix(filePath, topo.TabletsPath+"/")
	if refused {
		f.refusals++
	}
	f.mu.Unlock()
	if refused {
		return topo.NewError(topo.Timeout, "the topology is cut off")
	}
	if healed == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-healed:
		return nil
	}
}

// Create is part of the topo.Factory interface.
func (f *cutOffTopoFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil {
		return nil, err
	}
	return &cutOffConn{Conn: conn, f: f}, nil
}

type cutOffConn struct {
	topo.Conn
	f *cutOffTopoFactory
}

// Get is part of the topo.Conn interface.
func (c *cutOffConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if err := c.f.wait(ctx, filePath); err != nil {
		return nil, nil, err
	}
	return c.Conn.Get(ctx, filePath)
}

// List is part of the topo.Conn interface.
func (c *cutOffConn) List(ctx context.Context, filePathPrefix string) ([]topo.KVInfo, error) {
	if err := c.f.wait(ctx, filePathPrefix); err != nil {
		return nil, err
	}
	return c.Conn.List(ctx, filePathPrefix)
}

// ListDir is part of the topo.Conn interface.
func (c *cutOffConn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	if err := c.f.wait(ctx, dirPath); err != nil {
		return nil, err
	}
	return c.Conn.ListDir(ctx, dirPath, full)
}

// Update is part of the topo.Conn interface.
func (c *cutOffConn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	if err := c.f.wait(ctx, filePath); err != nil {
		return nil, err
	}
	return c.Conn.Update(ctx, filePath, contents, version)
}

// Create is part of the topo.Conn interface.
func (c *cutOffConn) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	if err := c.f.wait(ctx, filePath); err != nil {
		return nil, err
	}
	return c.Conn.Create(ctx, filePath, contents)
}

// newCutOffTestTM starts tablet cell1-1 of ks/0, one of the three voters of the shard's group,
// under a group replication policy, on a topology that the test can cut off. Its own joins fail
// until the test allows them.
func newCutOffTestTM(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *cutOffTopoFactory, *topo.Server) {
	t.Helper()
	ctx := t.Context()
	_, mf := memorytopo.NewServerAndFactory(ctx, "cell1")
	f := &cutOffTopoFactory{Factory: mf}
	t.Cleanup(f.heal)
	ts, err := topo.NewWithFactory(f, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplication}))
	setGroupReplicationVoters(t, ts, 1, 2, 3)
	setGroupReplicationIncarnation(t, ts, "1780000001")
	addPeerTablets(t, ts, 2, 3)
	peers := newGRPeersTMC()
	for _, uid := range []uint32{2, 3} {
		peers.set(uid, &replicationdatapb.FullStatus{ServerUuid: testServerUUID(int(uid))})
	}
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, peers, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	return tm, fmd, f, ts
}

// TestGroupReplicationDemotionDoesNotHoldActionLockWhileTopoIsCutOff reproduces the G12 chaos
// scenario: the primary's cell, topology connections included, is cut off, and its group loses
// its majority. The sync loop demoted the tablet under the action lock and waited, still under the
// lock, for the topology server to store the tablet record: until the step of the loop timed out
// (15s), or until the write, issued while the cell was cut off, returned 7-11s after the partition
// healed. VTOrc's bootstrap of the group on that tablet, the only way back to a primary, waited for
// the lock all that time.
// The demotion now waits at most groupReplicationDemotionPublishTimeout for the topology, and the
// record follows in the background.
func TestGroupReplicationDemotionDoesNotHoldActionLockWhileTopoIsCutOff(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, f, ts := newCutOffTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	oldTerm := tm.Tablet().PrimaryTermStartTime
	require.NotNil(t, oldTerm)

	// The cell is cut off, and MySQL left its group after losing the majority.
	f.cut()
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateError, "")))

	// One demotion by the sync loop, with the deadline of one step of the loop.
	stepCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	start := time.Now()
	newGroupReplicationSync(tm).demote(stepCtx)
	elapsed := time.Since(start)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type, "the tablet runs as a REPLICA right away")
	assert.Less(t, elapsed, groupReplicationDemotionPublishTimeout+2*time.Second, "the demotion must not wait for the topology under the action lock")
	// The action lock is free for the RPCs that recover the group.
	require.True(t, tm.actionSema.TryAcquire(1), "the action lock must be free")
	tm.unlock()

	// The partition heals: the record follows the demotion.
	f.heal()
	assert.Eventually(t, func() bool {
		ti, err := ts.GetTablet(ctx, tm.tabletAlias)
		return err == nil && ti.Type == topodatapb.TabletType_REPLICA && ti.PrimaryTermStartTime == nil
	}, 30*time.Second, 10*time.Millisecond, "the tablet record must follow the demotion once the topology answers")
}

// TestGroupReplicationPromotionAfterDemotionPublishedInBackground checks the race between the
// background publication of a demotion and a later promotion of the same tablet, for example on
// the old primary of G12 once the partition healed and VTOrc bootstrapped the group on it. The
// topology did not answer during the demotion, and the background publication waits for its next
// attempt (publishRetryInterval, 30s by default). The promotion writes the record itself, before
// the tablet becomes PRIMARY, with a new term; it then wakes up the background publication, which
// publishes the tablet as it runs now: PRIMARY with that term, never the REPLICA of the demotion.
func TestGroupReplicationPromotionAfterDemotionPublishedInBackground(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	oldInterval := publishRetryInterval
	publishRetryInterval = time.Hour
	t.Cleanup(func() { publishRetryInterval = oldInterval })
	tm, fmd, f, ts := newCutOffTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	oldTerm := tm.Tablet().PrimaryTermStartTime
	require.NotNil(t, oldTerm)

	// The topology does not answer the demotion, nor the first attempt of the background
	// publication, which then waits for its next attempt.
	f.refuse()
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateError, "")))
	newGroupReplicationSync(tm).demote(ctx)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	isPublishing := func() bool {
		tm.tmState.mu.Lock()
		defer tm.tmState.mu.Unlock()
		return tm.tmState.isPublishing
	}
	require.True(t, isPublishing(), "the record must be left to the background publication")
	// The demotion's own attempt to write the tablet record and the first background attempt
	// were refused.
	assert.Eventually(t, func() bool { return f.refusedRequests() >= 2 }, 30*time.Second, 10*time.Millisecond)

	// The topology answers again, and the tablet is promoted.
	f.heal()
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	require.NoError(t, tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_PRIMARY, DBActionNone))
	newTerm := tm.Tablet().PrimaryTermStartTime
	require.NotNil(t, newTerm)
	assert.False(t, proto.Equal(oldTerm, newTerm))

	// The promotion woke up the background publication, which published the running tablet.
	assert.Eventually(t, func() bool { return !isPublishing() }, 30*time.Second, 10*time.Millisecond,
		"the promotion must wake up the background publication")
	ti, err := ts.GetTablet(ctx, tm.tabletAlias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
	assert.True(t, proto.Equal(newTerm, ti.PrimaryTermStartTime), "the record must carry the new primary term")
}

// TestStartGroupReplicationBootstrapDoesNotWaitForCutOffTopo checks that VTOrc can bootstrap the
// shard's group on a tablet that it reaches while the tablet's topology does not answer, for
// example because its cell's topology server is down or cut off from it. The bootstrap read the
// durability policy and the shard's tablet records first, and waited for them until the RPC timed
// out, so the shard stayed without a group although its voter with every acknowledged transaction
// was reachable. These reads only give the member weight and the seeds: a tablet that read them
// before waits at most groupReplicationTopoReadTimeout and uses what it read last.
func TestStartGroupReplicationBootstrapDoesNotWaitForCutOffTopo(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, f, _ := newCutOffTestTM(t)
	// The tablet read the shard's group, as its sync loop does every few seconds.
	_, err := tm.readShardGroupRecord(ctx, nil)
	require.NoError(t, err)
	durability, err := tm.shardDurability(ctx)
	require.NoError(t, err)
	want, err := tm.groupReplicationConfig(ctx, durability)
	require.NoError(t, err)

	// MySQL left its group when the group lost its majority; the topology does not answer.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateError, "")))
	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	f.cut()

	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()
	status, err := tm.StartGroupReplication(rpcCtx, startRequest(true))
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	assert.True(t, fmd.GroupReplicationBootstrapped)
	assert.Less(t, elapsed, groupReplicationTopoReadTimeout+3*time.Second, "the bootstrap must not wait for the topology")
	assert.ElementsMatch(t, []string{"mysql2:3306", "mysql3:3306"}, fmd.GroupReplicationConfig.Seeds, "the seeds come from the tablet records read last")
	assert.Equal(t, want.MemberWeight, fmd.GroupReplicationConfig.MemberWeight, "the weight comes from the durability policy read last")
}

// TestStartGroupReplicationJoinDoesNotWaitForCutOffTopo checks that a join that the
// StartGroupReplication RPC requests does not wait for a topology that does not answer: VTOrc's joins
// after a bootstrap recover the shard's group, typically right after a partition. The join reads its
// peers' status first, to contact the active members first (refreshActiveGroupSeeds); that read of the
// shard record waits at most groupReplicationTopoReadTimeout, and the join then uses the record and the
// seeds that the tablet read last.
func TestStartGroupReplicationJoinDoesNotWaitForCutOffTopo(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, f, _ := newCutOffTestTM(t)
	// The tablet read the shard's group and its policy, as its sync loop does every few seconds.
	_, err := tm.readShardGroupRecord(ctx, nil)
	require.NoError(t, err)
	_, err = tm.shardDurability(ctx)
	require.NoError(t, err)

	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateError, "")))
	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	f.cut()

	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()
	_, err = tm.StartGroupReplication(rpcCtx, startRequest(false))
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.False(t, fmd.GroupReplicationBootstrapped)
	assert.Less(t, elapsed, 2*groupReplicationTopoReadTimeout+3*time.Second, "the join must not wait for the topology")
}
