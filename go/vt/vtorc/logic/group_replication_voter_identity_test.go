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

package logic

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/process"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// publishIdentities writes the voter identities of the tablets to the shard record, as their
// vttablets do.
func publishIdentities(t *testing.T, tablets ...*topodatapb.Tablet) {
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		for _, tablet := range tablets {
			policy.SetGroupVoterIdentity(si.Shard, policy.NewGroupVoterIdentity(tablet, voterTestUUID(tablet)))
		}
		return nil
	})
	require.NoError(t, err)
}

// forgetInBackend removes everything that VTOrc's backend holds about the tablet, as a restart of
// VTOrc does.
func forgetInBackend(t *testing.T, alias *topodatapb.TabletAlias) {
	for _, table := range []string{"vitess_tablet", "database_instance", "vitess_deleted_group_voter"} {
		_, err := db.ExecVTOrc("delete from "+table+" where alias = ?", topoproto.TabletAliasString(alias))
		require.NoError(t, err)
	}
}

// TestUpdateGroupReplicationVotersAfterVTOrcRestart checks the removal of a voter whose tablet record
// the operator deleted after its host died, by a VTOrc that restarted since: VTOrc's backend no longer
// holds the voter's last tablet record or server_uuid. VTOrc probes the voter at the address that the
// voter published in the shard record, and removes it once the probe failed for the grace period,
// measured from the restart. A voter that published nothing keeps its seat: VTOrc cannot tell that it
// is down.
func TestUpdateGroupReplicationVotersAfterVTOrcRestart(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	// atPublishedAddress matches a tablet at the address that voter3 published.
	atPublishedAddress := gomock.Cond(func(x any) bool {
		tablet, ok := x.(*topodatapb.Tablet)
		return ok && topoproto.TabletAliasEqual(tablet.Alias, voter3.Alias) && tablet.Hostname == voter3.Hostname &&
			tablet.PortMap["grpc"] == voter3.PortMap["grpc"]
	})

	tests := []struct {
		name        string
		published   bool
		gracePeriod time.Duration
		wantVoters  []string
		wantErr     string
	}{{
		name:       "the voter published its identity, and its probe fails for the grace period: it is removed",
		published:  true,
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200"},
	}, {
		name:        "the voter published its identity, and the grace period since the restart has not passed: it stays",
		published:   true,
		gracePeriod: time.Hour,
		wantVoters:  []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
		wantErr:     "voter zone3-0000000300 has no tablet record, but VTOrc cannot tell that its vttablet is down",
	}, {
		name:       "the voter published nothing: it stays",
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
		wantErr:    "voter zone3-0000000300 has no tablet record, but VTOrc cannot tell that its vttablet is down",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst.UnreachableGroupTablets.Reset()
			config.SetGroupReplicationVoterReplacementGracePeriod(tt.gracePeriod)
			mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, voter3)
			setVoters(t, primary, voter2, voter3)
			setIncarnation(t, voterTestIncarnation)
			if tt.published {
				publishIdentities(t, primary, voter2, voter3)
			}
			for _, tablet := range []*topodatapb.Tablet{primary, voter2} {
				require.NoError(t, inst.WriteInstance(&inst.Instance{
					InstanceAlias: tablet.Alias, Hostname: tablet.MysqlHostname, Port: int(tablet.MysqlPort), ServerUUID: voterTestUUID(tablet),
				}, true, nil))
			}
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter2), nil)
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(settledMember(voter2, primary, "1-10", primary, voter2), nil)
			if tt.published {
				mockTMC.EXPECT().FullStatus(gomock.Any(), atPublishedAddress).Return(nil, errors.New("unreachable")).MinTimes(1)
			}

			// voter3's host died; the operator deleted its tablet record, and VTOrc restarted.
			require.NoError(t, ts.DeleteTablet(t.Context(), voter3.Alias))
			forgetInBackend(t, voter3.Alias)

			_, _, err := updateGroupReplicationVoters(lockedShard(t), voterRecoveryEntry(primary, inst.GroupVotersOutOfDate), log.NewPrefixedLogger("test"))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantVoters, readVoters(t))
		})
	}
}

// TestRestoreDeletedGroupVoters checks that VTOrc, after it restarted, takes back from the shard record
// the voters whose tablet record was deleted: it stores the tablet record that each published and
// marks it as a deleted voter, so that its analysis plans the voter's removal. A voter whose tablet
// record exists is left to the tablet discovery, and nothing is restored before VTOrc's first
// discovery cycle completed.
func TestRestoreDeletedGroupVoters(t *testing.T) {
	prevCycle := process.FirstDiscoveryCycleComplete.Load()
	t.Cleanup(func() { process.FirstDiscoveryCycleComplete.Store(prevCycle) })
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, voter3)
	setVoters(t, primary, voter2, voter3)
	publishIdentities(t, primary, voter2, voter3)
	require.NoError(t, ts.DeleteTablet(t.Context(), voter3.Alias))
	for _, tablet := range []*topodatapb.Tablet{primary, voter2, voter3} {
		forgetInBackend(t, tablet.Alias)
	}
	si, err := ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)

	process.FirstDiscoveryCycleComplete.Store(false)
	restoreDeletedGroupVoters(t.Context(), si)
	_, err = inst.ReadTablet(voter3.Alias)
	require.ErrorIs(t, err, inst.ErrTabletAliasNil, "nothing is restored before the first discovery cycle")

	process.FirstDiscoveryCycleComplete.Store(true)
	restoreDeletedGroupVoters(t.Context(), si)
	restored, err := inst.ReadTablet(voter3.Alias)
	require.NoError(t, err)
	assert.True(t, proto.Equal(policy.NewGroupVoterIdentity(voter3, voterTestUUID(voter3)).Tablet, restored), "restored %v", restored)
	deleted, err := inst.ReadDeletedGroupVoters()
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"zone3-0000000300": true}, deleted)
	for _, tablet := range []*topodatapb.Tablet{primary, voter2} {
		_, err := inst.ReadTablet(tablet.Alias)
		require.ErrorIs(t, err, inst.ErrTabletAliasNil, "%s has a tablet record: the tablet discovery reads it", topoproto.TabletAliasString(tablet.Alias))
	}

	// The tablet discovery keeps the restored voter: its shard still lists it.
	refreshReachableTabletInfoOfShard(t.Context(), "ks", "0")
	_, err = inst.ReadTablet(voter3.Alias)
	require.NoError(t, err)
}

// beforeShardWriteFactory is a memorytopo factory whose global cell runs a hook, once it is armed and
// only once, right before a write of a shard record: between the read and the versioned write of
// topo.Server.UpdateShardFields. It records the errors of the writes that follow.
type beforeShardWriteFactory struct {
	*memorytopo.Factory
	mu         sync.Mutex
	hook       func()
	writeErrs  []error
	shardWrite bool
}

// Create is part of the topo.Factory interface.
func (f *beforeShardWriteFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil || cell != topo.GlobalCell {
		return conn, err
	}
	return &beforeShardWriteConn{Conn: conn, f: f}, nil
}

// arm makes the next write of a shard record run hook first.
func (f *beforeShardWriteFactory) arm(hook func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hook = hook
}

// errs returns the errors of the writes of shard records since the hook was armed.
func (f *beforeShardWriteFactory) errs() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeErrs
}

type beforeShardWriteConn struct {
	topo.Conn
	f *beforeShardWriteFactory
}

// Update is part of the topo.Conn interface.
func (c *beforeShardWriteConn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	if !strings.HasSuffix(filePath, "/"+topo.ShardFile) {
		return c.Conn.Update(ctx, filePath, contents, version)
	}
	c.f.mu.Lock()
	hook := c.f.hook
	c.f.hook = nil
	armed := hook != nil || c.f.shardWrite
	if hook != nil {
		c.f.shardWrite = true
	}
	c.f.mu.Unlock()
	if hook != nil {
		hook()
	}
	v, err := c.Conn.Update(ctx, filePath, contents, version)
	if armed {
		c.f.mu.Lock()
		c.f.writeErrs = append(c.f.writeErrs, err)
		c.f.mu.Unlock()
	}
	return v, err
}

// TestUpdateGroupReplicationVotersWithConcurrentIdentityWrite checks that a voter's vttablet, which
// publishes its identity in the shard record without the shard lock, never makes VTOrc's change of the
// voters fail or lose either write, wherever its write lands: between VTOrc's read of the shard, on
// which it decides, and its compare-and-swap; or inside the compare-and-swap, between its read of the
// record and its versioned write, which then fails with BadVersion and is retried on the record that
// holds the identity. The compare-and-swap compares the voters and the incarnation, which the identity
// write does not change.
func TestUpdateGroupReplicationVotersWithConcurrentIdentityWrite(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	for _, tt := range []struct {
		name string
		// insideCAS writes the identity between the read and the write of the compare-and-swap,
		// rather than between the decision's read and the compare-and-swap.
		insideCAS bool
	}{{
		name: "between the decision's read and the compare-and-swap",
	}, {
		name:      "between the read and the write of the compare-and-swap",
		insideCAS: true,
	}} {
		t.Run(tt.name, func(t *testing.T) {
			inst.UnreachableGroupTablets.Reset()
			config.SetGroupReplicationVoterReplacementGracePeriod(0)
			primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
			voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
			spare2 := recoveryTablet("zone2", 201, topodatapb.TabletType_REPLICA)
			voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
			mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, spare2, voter3)
			setVoters(t, primary, voter2, voter3)
			setIncarnation(t, voterTestIncarnation)
			// VTOrc writes through the factory that runs the hook; the vttablet writes directly.
			factory := &beforeShardWriteFactory{Factory: recoveryTopoFactory}
			vtorcTS, err := topo.NewWithFactory(factory, "", "")
			require.NoError(t, err)
			t.Cleanup(vtorcTS.Close)
			ts = vtorcTS
			vttabletTS, err := topo.NewWithFactory(recoveryTopoFactory, "", "")
			require.NoError(t, err)
			t.Cleanup(vttabletTS.Close)
			identity := policy.NewGroupVoterIdentity(voter3, voterTestUUID(voter3))
			// publish may run outside of the test's goroutine: it does not stop the test.
			publish := func() {
				_, err := vttabletTS.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
					policy.SetGroupVoterIdentity(si.Shard, identity)
					return nil
				})
				assert.NoError(t, err)
			}

			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter3), nil)
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(nil, errors.New("unreachable"))
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(settledMember(voter3, primary, "1-10", primary, voter3), nil)
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(spare2)).DoAndReturn(
				func(context.Context, *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
					// VTOrc has read the shard record, and decides on this read.
					if tt.insideCAS {
						factory.arm(publish)
					} else {
						publish()
					}
					return spareStatus(spare2, "1-5"), nil
				})
			mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(spare2), startRequest(false)).Return(&replicationdatapb.GroupReplicationStatus{}, nil)

			_, _, err = updateGroupReplicationVoters(lockedShard(t), voterRecoveryEntry(primary, inst.GroupVotersOutOfDate), log.NewPrefixedLogger("test"))
			require.NoError(t, err)
			assert.Equal(t, []string{"zone1-0000000101", "zone2-0000000201", "zone3-0000000300"}, readVoters(t))
			si, err := vttabletTS.GetShard(t.Context(), "ks", "0")
			require.NoError(t, err)
			require.Len(t, si.GroupReplicationVoterIdentities, 1)
			assert.True(t, proto.Equal(identity, si.GroupReplicationVoterIdentities[0]), "the identity write must stand")
			if tt.insideCAS {
				// The identity write did land inside the compare-and-swap: its versioned write failed, and
				// its retry succeeded.
				writeErrs := factory.errs()
				require.Len(t, writeErrs, 2)
				assert.True(t, topo.IsErrType(writeErrs[0], topo.BadVersion), "first write: %v", writeErrs[0])
				assert.NoError(t, writeErrs[1])
			}
		})
	}
}
