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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
)

// TestStartGroupReplicationJoinPrefersActiveSeeds reproduces the G12 r1 chaos run: right after VTOrc
// bootstrapped the group, its join RPC on another voter configured the seeds in their sorted order,
// and the first seed was a restarted voter whose own START was stuck. MySQL contacted it first, and
// each such join waited out MySQL's 30s timeout for the group communication engine before it joined
// through the bootstrapped member. A join that the RPC requests contacts the members that are active
// in the shard's group first, as the tablet's own joins do, and the others after them.
func TestStartGroupReplicationJoinPrefersActiveSeeds(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	setGroupReplicationVoters(t, ts, 1, 2, 3)
	addPeerTablets(t, ts, 2, 3)
	peers := newGRPeersTMC()
	// Tablet 2, whose address sorts first, runs a START that is stuck.
	stuck := groupStatus(testServerUUID(2))
	stuck.StartInProgress = true
	peers.set(2, &replicationdatapb.FullStatus{ServerUuid: testServerUUID(2), GroupReplicationStatus: stuck})
	// VTOrc bootstrapped the group on tablet 3.
	bootstrapped := activeGroupPeersIn("1780000001", 3)
	peers.set(3, bootstrapped.statuses["cell1-0000000003"])
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, peers, nil)
	// The tablet does not join on its own while the bootstrap is not recorded: the join is the RPC's,
	// which VTOrc sends once it recorded the bootstrap.
	start, _, _ := fmd.GroupReplicationCalls()
	require.Zero(t, start)
	setGroupReplicationIncarnation(t, ts, "1780000001")
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}

	_, err := tm.StartGroupReplication(t.Context(), startRequest(false))
	require.NoError(t, err)
	assert.Equal(t, []string{"mysql3:3306", "mysql2:3306"}, fmd.GroupReplicationConfig.Seeds)
}
