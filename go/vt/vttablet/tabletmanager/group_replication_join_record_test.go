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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestGroupReplicationJoinWaitsForRecordedIncarnation reproduces the TLA+ model's init_orc_lost trace
// (doc/design-docs/group_replication_tla): VTOrc bootstrapped the group of a new shard on one voter,
// and has not recorded its incarnation yet. While the shard record lists no incarnation, every group
// counts as the shard's group, so another voter joined it on its own; when the bootstrapped member
// left, that join ended in a group of its own, which a third voter joined, and whose primary served
// under the voter rule alone. VTOrc then recorded the first group's incarnation, and the writes that
// the stray group acknowledged were not in the recorded history.
//
// A tablet therefore starts no join on its own, at startup or in its sync loop, while the shard
// record lists no incarnation. VTOrc makes the voters join once it recorded its bootstrap, and the
// tablet joins on its own once the incarnation is recorded.
func TestGroupReplicationJoinWaitsForRecordedIncarnation(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	setGroupReplicationVoters(t, ts, 1, 2, 3)
	addPeerTablets(t, ts, 2, 3)
	// VTOrc's bootstrap on tablet 2 formed incarnation 1780000001, not recorded yet.
	peers := activeGroupPeersIn("1780000001", 2)
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, peers, nil)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start, "the tablet must not join at startup while the shard record lists no incarnation")

	s := newGroupReplicationSync(tm)
	s.reconcile(ctx)
	start, _, _ = fmd.GroupReplicationCalls()
	assert.Zero(t, start, "the sync loop must not join while the shard record lists no incarnation")

	// VTOrc recorded its bootstrap: the tablet joins on its own.
	setGroupReplicationIncarnation(t, ts, "1780000001")
	s.nextRejoin = time.Time{}
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	s.reconcile(ctx)
	start, _, _ = fmd.GroupReplicationCalls()
	assert.Equal(t, 1, start)
	assert.False(t, fmd.GroupReplicationBootstrapped)
	require.NoError(t, fmd.CheckSuperQueryList())
}
