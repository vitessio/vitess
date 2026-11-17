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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestSetReplicationSourceRefusedWhileGroupJoinRuns reproduces the G13 r1 chaos run on the tablet side:
// VTOrc's fixReplica sent SetReplicationSource to a voter whose MySQL was still running the START
// GROUP_REPLICATION of a join, and the tablet configured the default replication channel next to the
// group membership that the START then completed. A tablet whose MySQL runs a START refuses it: the
// member is joining its group, and replicates through it.
func TestSetReplicationSourceRefusedWhileGroupJoinRuns(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplicationCrossCell)
	require.NoError(t, ts.CreateTablet(t.Context(), &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_PRIMARY,
		Hostname: "tablet2", MysqlHostname: "mysql2", MysqlPort: 3306, PortMap: map[string]int32{"grpc": 2},
	}))
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	status := groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOffline, ""))
	status.StartInProgress = true
	fmd.SetGroupReplicationStatus(status)
	configured := false
	fmd.SetReplicationSourceFunc = func(context.Context, string, int32, float64, bool, bool) error {
		configured = true
		return errors.New("the default channel must not be configured on a joining member")
	}
	parent := &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	err := tm.SetReplicationSource(ctx, parent, 0, "", false, false, 0)
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	require.ErrorContains(t, err, "START GROUP_REPLICATION")
	assert.False(t, configured)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}
