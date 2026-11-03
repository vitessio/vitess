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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/inst"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestFixReplicaOnGroupReplicationVoter reproduces the G13 r1 chaos run: VTOrc's fixReplica ran on a
// voter whose join was in flight, and its SetReplicationSource configured the default replication
// channel on it, next to its group membership. A voter replicates through its group: fixReplica makes
// a writable voter read-only, but never configures its default channel. A tablet that is not a voter
// replicates asynchronously, and is repaired as before.
func TestFixReplicaOnGroupReplicationVoter(t *testing.T) {
	tests := []struct {
		name         string
		analysis     inst.AnalysisCode
		voter        bool
		wantReadOnly int
		wantRepoint  int
	}{
		{name: "a voter not connected to the primary", analysis: inst.NotConnectedToPrimary, voter: true},
		{name: "a voter whose replication stopped", analysis: inst.ReplicationStopped, voter: true},
		{name: "a writable voter is made read-only only", analysis: inst.ReplicaIsWritable, voter: true, wantReadOnly: 1},
		{name: "a tablet that is not a voter is repointed", analysis: inst.NotConnectedToPrimary, wantReadOnly: 1, wantRepoint: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			primary := recoveryTablet("zone1", 100, topodatapb.TabletType_PRIMARY)
			voter := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
			replica := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
			mockTMC := groupReplicationRecoveryTest(t, primary, voter, replica)
			setVoters(t, primary, voter)
			analyzed := replica
			if tt.voter {
				analyzed = voter
			}
			mockTMC.EXPECT().SetReadOnly(gomock.Any(), sameTablet(analyzed)).Return(nil).Times(tt.wantReadOnly)
			mockTMC.EXPECT().SetReplicationSource(gomock.Any(), sameTablet(analyzed), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil).Times(tt.wantRepoint)

			_, _, err := fixReplica(t.Context(), &inst.DetectionAnalysis{
				Analysis:              tt.analysis,
				AnalyzedInstanceAlias: analyzed.Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}, log.NewPrefixedLogger("test"))
			require.NoError(t, err)
		})
	}
}
