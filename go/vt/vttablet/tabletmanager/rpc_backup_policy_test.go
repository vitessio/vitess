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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestRestoreReplicationAfterBackupWithUnknownDurabilityPolicy checks the end of an offline backup
// in a shard whose durability policy this vttablet does not know, for example one that a newer
// version wrote: the tablet logs the error and leaves replication alone, instead of dereferencing
// the policy it could not resolve.
func TestRestoreReplicationAfterBackupWithUnknownDurabilityPolicy(t *testing.T) {
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	addPeerTablets(t, ts, 2)
	var mu sync.Mutex
	sourceSet := false
	tm, _ := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.SetReplicationSourceFunc = func(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore, startReplicationAfter bool) error {
			mu.Lock()
			defer mu.Unlock()
			sourceSet = true
			return nil
		}
	})
	// The shard's primary is another tablet, and the keyspace record names a policy that this
	// vttablet does not know.
	_, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}
		return nil
	})
	require.NoError(t, err)
	_, err = ts.UpdateTabletFields(ctx, &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, func(tablet *topodatapb.Tablet) error {
		tablet.Type = topodatapb.TabletType_PRIMARY
		return nil
	})
	require.NoError(t, err)
	lockCtx, unlock, err := ts.LockKeyspace(ctx, "ks", "test")
	require.NoError(t, err)
	ki, err := ts.GetKeyspace(lockCtx, "ks")
	require.NoError(t, err)
	ki.DurabilityPolicy = "a_policy_of_a_newer_version"
	require.NoError(t, ts.UpdateKeyspace(lockCtx, ki))
	unlock(&err)
	require.NoError(t, err)

	logger := logutil.NewMemoryLogger()
	require.NotPanics(t, func() { tm.restoreReplicationAfterBackupLocked(ctx, tm.Tablet(), logger) })
	assert.Contains(t, logger.String(), "Failed to get durability with name a_policy_of_a_newer_version")
	mu.Lock()
	defer mu.Unlock()
	assert.False(t, sourceSet, "the tablet must leave replication alone")
}
