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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestUpdateGroupReplicationVotersKeepsSeatsOfMinorityView reproduces the TLA+ model's
// voters_minority trace (doc/design-docs/group_replication_tla): the group's view shrank to its
// primary through clean leaves, which keep MySQL's view quorum, and the two other voters then
// stayed unreachable for longer than the replacement grace period. The primary does not serve: its
// view holds one of the three voters. VTOrc must not write a voter list under which that view
// holds a majority: the primary would serve again, and acknowledge writes on a single voter, which
// is what "Group shrink fails closed" rules out.
func TestUpdateGroupReplicationVotersKeepsSeatsOfMinorityView(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	inst.UnreachableGroupTablets.Reset()
	config.SetGroupReplicationVoterReplacementGracePeriod(0)

	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, voter3)
	setVoters(t, primary, voter2, voter3)

	errUnreachable := errors.New("unreachable")
	// The primary's view holds the primary only, with quorum in that view.
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, primary, primary), nil).AnyTimes()
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(nil, errUnreachable).AnyTimes()
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(nil, errUnreachable).AnyTimes()

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.GroupVotersOutOfDate,
		AnalyzedInstanceAlias: primary.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	_, _, _ = updateGroupReplicationVoters(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
	require.Equal(t, []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"}, readVoters(t),
		"a view that holds one of three voters must not become a majority of the voter list")
}
