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

package reparentutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestPlannedReparentGroupReplicationInitialPromotionAfterBootstrap reproduces the TLA+ model's init_orc
// trace (doc/design-docs/group_replication_tla): VTOrc bootstraps the group of a shard that never had a
// primary on its own (GroupVotersOutOfDate selects the voters, GroupNotBootstrapped bootstraps one of
// them) and records its incarnation. Until a majority of the voters joined it, no tablet is PRIMARY, and
// the shard record has no primary term: a PlannedReparentShard that got the shard lock after VTOrc's
// recovery takes the initial promotion path, and InitPrimary bootstraps a second group on its elect,
// which it records over VTOrc's (the compare-and-swap expects the incarnation PRS read). A shard whose
// record lists an incarnation has a group: the initial promotion must not bootstrap another one.
//
// The same holds while a bootstrap intent of VTOrc is live and no incarnation is recorded yet: VTOrc's
// bootstrap RPC failed while MySQL's START still ran, and VTOrc adopts its group later. The intent
// fences a bootstrap on another tablet; InitPrimary's bootstrap ignored it.
func TestPlannedReparentGroupReplicationInitialPromotionAfterBootstrap(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, c *fakeGRCluster)
	}{{
		name: "VTOrc recorded the incarnation of the group it bootstrapped",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.setIncarnation(t, "1780000001")
		},
	}, {
		name: "a bootstrap intent of VTOrc is live",
		setup: func(t *testing.T, c *fakeGRCluster) {
			_, err := c.ts.UpdateShardFields(t.Context(), c.keyspace, "-", func(si *topo.ShardInfo) error {
				si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
					Target: mustAlias(t, alias200),
					Time:   protoutil.TimeToProto(time.Now()),
					Token:  "1780000001-0123456789abcdef",
				}
				return nil
			})
			require.NoError(t, err)
		},
	}} {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newFakeGRCluster(t, "group_replication_cross_cell",
				fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
				fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
				fakeGRTabletSpec{cell: "zone3", uid: 300, tabletType: topodatapb.TabletType_REPLICA},
			)
			c.setVoters(t, alias101, alias200, alias300)
			tt.setup(t, c)
			before := c.recordedIncarnation(t)
			pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

			_, err := pr.ReparentShard(t.Context(), "ks", "-", PlannedReparentOptions{
				NewPrimaryAlias:     mustAlias(t, alias101),
				WaitReplicasTimeout: 30 * time.Second,
			})
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			assert.Empty(t, callsWithPrefix(c.mutatingCalls(), "InitPrimary"), "InitPrimary must not bootstrap a second group")
			assert.Equal(t, before, c.recordedIncarnation(t), "the shard record's incarnation must stay as it is")
		})
	}
}
