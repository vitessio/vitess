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
// becomes a REPLICA, but configures no asynchronous replication on the default channel: the tablet
// is a group member, which rejoins its group through the sync loop or VTOrc. Under a semi-sync
// policy, the tablet still replicates from the new primary.
func TestEndPrimaryTermOnInactiveGroupReplicationMember(t *testing.T) {
	for _, tt := range []struct {
		name       string
		durability string
		state      string
		// wantSource is the replication source the tablet configures; empty for none.
		wantSource string
	}{{
		name:       "group replication policy, MySQL in the ERROR state",
		durability: policy.DurabilityGroupReplication,
		state:      mysql.GroupMemberStateError,
	}, {
		name:       "group replication policy, MySQL not in a group",
		durability: policy.DurabilityGroupReplication,
		state:      mysql.GroupMemberStateOffline,
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
