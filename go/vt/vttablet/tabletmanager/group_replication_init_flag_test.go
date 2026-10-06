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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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

// TestInitPrimaryWithoutGroupReplicationDoesNotWaitForCutOffTopo checks that the read of the shard's
// policy that InitPrimary makes on a vttablet without --enable-group-replication, under the action
// lock, is bounded by the topology read timeout and not only by the caller's deadline: a vttablet
// cut off from the global topology goes on with its initialization, as it did before it read the
// policy, instead of holding the action lock until the caller gives up.
func TestInitPrimaryWithoutGroupReplicationDoesNotWaitForCutOffTopo(t *testing.T) {
	ctx := t.Context()
	_, mf := memorytopo.NewServerAndFactory(ctx, "cell1")
	f := &cutOffTopoFactory{Factory: mf}
	t.Cleanup(f.heal)
	ts, err := topo.NewWithFactory(f, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilitySemiSync}))
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.SuperReadOnly.Store(true)
	f.cut()

	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()
	_, err = tm.InitPrimary(rpcCtx, false)
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 2*groupReplicationTopoReadTimeout+3*time.Second, "InitPrimary must not wait for the topology")
	assert.False(t, fmd.SuperReadOnly.Load(), "InitPrimary goes on with the initialization")
	assert.NotErrorIs(t, err, context.DeadlineExceeded)
}
