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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// endTermPeersTMC answers the new primary's status, which a tablet reads before it replicates from it.
type endTermPeersTMC struct {
	*grPeersTMC
}

// PrimaryStatus is part of the tmclient.TabletManagerClient interface.
func (c *endTermPeersTMC) PrimaryStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	return &replicationdatapb.PrimaryStatus{Position: "MySQL56/" + testServerUUID(2) + ":1-10", ServerUuid: testServerUUID(2)}, nil
}

// TestEndPrimaryTermOnInactiveGroupReplicationMember checks the end of a primary term on a tablet
// that learns from the shard record that another tablet is the primary, while its MySQL is not an
// active member of a group (the sweep's S7c r2 and S2: a voter whose MySQL was in the ERROR state).
// Under a group replication policy, the tablet demotes MySQL (super_read_only, not serving) and
// becomes a REPLICA, but configures no asynchronous replication on the default channel when the
// tablet is a member of the shard's group, which rejoins its group through the sync loop or VTOrc: a
// voter, or while no voter is listed a tablet that the policy allows in the group, or any tablet
// whose shard's policy cannot be read. A tablet that is not a voter replicates from the new primary,
// as every tablet that is not a voter does, and so does a tablet under a semi-sync policy.
func TestEndPrimaryTermOnInactiveGroupReplicationMember(t *testing.T) {
	for _, tt := range []struct {
		name       string
		durability string
		state      string
		// voters are the uids of the voters that the shard record lists.
		voters []uint32
		// unknownPolicy makes the keyspace's policy one that the tablet does not know.
		unknownPolicy bool
		// wantSource is the replication source the tablet configures; empty for none.
		wantSource string
	}{{
		name:       "group replication policy, MySQL in the ERROR state",
		durability: policy.DurabilityGroupReplicationCrossCell,
		state:      mysql.GroupMemberStateError,
	}, {
		name:       "group replication policy, MySQL not in a group",
		durability: policy.DurabilityGroupReplicationCrossCell,
		state:      mysql.GroupMemberStateOffline,
	}, {
		name:       "group replication policy, a voter",
		durability: policy.DurabilityGroupReplicationCrossCell,
		state:      mysql.GroupMemberStateError,
		voters:     []uint32{1, 2, 3},
	}, {
		name:       "group replication policy, a tablet that is not a voter",
		durability: policy.DurabilityGroupReplicationCrossCell,
		state:      mysql.GroupMemberStateOffline,
		voters:     []uint32{2, 3, 4},
		wantSource: "mysql2:3306",
	}, {
		name:          "a policy that the tablet cannot resolve",
		durability:    policy.DurabilityGroupReplicationCrossCell,
		state:         mysql.GroupMemberStateOffline,
		voters:        []uint32{2, 3, 4},
		unknownPolicy: true,
	}, {
		name:       "semi-sync policy",
		durability: policy.DurabilitySemiSync,
		state:      mysql.GroupMemberStateOffline,
		wantSource: "mysql2:3306",
	}} {
		t.Run(tt.name, func(t *testing.T) {
			withGroupReplication(t)
			ts := newGroupReplicationTopo(t, tt.durability)
			addPeerTablets(t, ts, 2)
			var mu sync.Mutex
			var sources []string
			peers := &endTermPeersTMC{grPeersTMC: newGRPeersTMC()}
			tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, peers, func(fmd *mysqlctl.FakeMysqlDaemon) {
				fmd.StartGroupReplicationError = errors.New("no seed reachable")
				if tt.wantSource != "" {
					fmd.ExpectedExecuteSuperQueryList = []string{"START REPLICA"}
				}
				fmd.SetReplicationSourceFunc = func(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore, startReplicationAfter bool) error {
					mu.Lock()
					defer mu.Unlock()
					sources = append(sources, fmt.Sprintf("%s:%d", host, port))
					return nil
				}
			})
			setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
			if len(tt.voters) > 0 {
				setGroupReplicationVoters(t, ts, tt.voters...)
			}
			if tt.unknownPolicy {
				lockCtx, unlock, err := ts.LockKeyspace(t.Context(), "ks", "test")
				require.NoError(t, err)
				ki, err := ts.GetKeyspace(lockCtx, "ks")
				require.NoError(t, err)
				ki.DurabilityPolicy = "a_policy_of_a_newer_version"
				require.NoError(t, ts.UpdateKeyspace(lockCtx, ki))
				unlock(&err)
				require.NoError(t, err)
			}
			fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
				groupMember(testServerUUID(1), tt.state, ""),
			))

			require.NoError(t, tm.endPrimaryTerm(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}))

			assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
			assert.True(t, fmd.SuperReadOnly.Load(), "MySQL must be super_read_only")
			mu.Lock()
			defer mu.Unlock()
			if tt.wantSource == "" {
				assert.Empty(t, sources, "a group member must not replicate on the default channel")
				return
			}
			assert.Equal(t, []string{tt.wantSource}, sources)
		})
	}
}
