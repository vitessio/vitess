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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestInitPrimaryRefusesGroupReplicationPolicyWithoutFlag checks that a vttablet that does not run
// Group Replication (no --enable-group-replication) refuses InitPrimary in a shard whose policy uses
// it: it would skip the group's bootstrap and make MySQL writable without a group. A shard with a
// semi-sync policy is initialized as before.
func TestInitPrimaryRefusesGroupReplicationPolicyWithoutFlag(t *testing.T) {
	for _, tt := range []struct {
		durability string
		wantErr    bool
	}{
		{durability: policy.DurabilityGroupReplication, wantErr: true},
		{durability: policy.DurabilitySemiSync},
	} {
		t.Run(tt.durability, func(t *testing.T) {
			ts := newGroupReplicationTopo(t, tt.durability)
			tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
			fmd.SuperReadOnly.Store(true)
			_, err := tm.InitPrimary(t.Context(), false)
			if !tt.wantErr {
				// InitPrimary goes on: it makes MySQL writable to create the sidecar database, which
				// the fake MySQL does not expect.
				assert.NotContains(t, fmt.Sprint(err), "--enable-group-replication")
				assert.False(t, fmd.SuperReadOnly.Load(), "InitPrimary must go on in a semi-sync shard")
				return
			}
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			require.ErrorContains(t, err, "--enable-group-replication")
			assert.True(t, fmd.SuperReadOnly.Load(), "MySQL must stay read-only")
			assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
		})
	}
}
