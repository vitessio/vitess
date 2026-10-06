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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// legitimatePrimaryView returns the status of tablet 1's MySQL as the primary of the recorded
// incarnation, with the voters 1, 2 and 3 ONLINE in its view.
func legitimatePrimaryView(view string) *replicationdatapb.GroupReplicationStatus {
	return withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), view)
}

// TestGroupReplicationDeletedTabletRecordNeverServesAsPrimary checks that a tablet whose own tablet
// record was deleted, the operator's signal that VTOrc may drop it from the voters (RemoveVoter,
// RemoveVoterNoGroup), neither becomes PRIMARY nor serves as one, and leaves MySQL read-only, on every
// path that makes a primary: the sync loop's promotion and its serving again, PromoteReplica, and
// UndoDemotePrimary. A tablet writes its record before it becomes PRIMARY, and that write fails for
// good (topo NoNode): the promotion must fail at once rather than retry until its context ends, while
// it holds the action lock. A tablet that is PRIMARY already writes nothing to serve again: it reads
// its record first.
func TestGroupReplicationDeletedTabletRecordNeverServesAsPrimary(t *testing.T) {
	// A publish that finds the record gone shuts the tablet down.
	oldExit := servenv.ExitChan
	servenv.ExitChan = make(chan os.Signal, 10)
	t.Cleanup(func() { servenv.ExitChan = oldExit })

	for _, tt := range []struct {
		name string
		// primary makes the tablet PRIMARY, not serving and with MySQL fenced, before its record is
		// deleted.
		primary bool
		act     func(ctx context.Context, tm *TabletManager) error
	}{{
		name: "the sync loop promotes the tablet of the group's primary",
		act: func(ctx context.Context, tm *TabletManager) error {
			newGroupReplicationSync(tm).reconcile(ctx)
			return nil
		},
	}, {
		name: "PromoteReplica",
		act: func(ctx context.Context, tm *TabletManager) error {
			_, err := tm.PromoteReplica(ctx, false)
			return err
		},
	}, {
		name: "UndoDemotePrimary on a REPLICA tablet",
		act: func(ctx context.Context, tm *TabletManager) error {
			return tm.UndoDemotePrimary(ctx, false)
		},
	}, {
		name:    "the sync loop makes a PRIMARY tablet serve again",
		primary: true,
		act: func(ctx context.Context, tm *TabletManager) error {
			newGroupReplicationSync(tm).reconcile(ctx)
			return nil
		},
	}, {
		name:    "UndoDemotePrimary on a PRIMARY tablet",
		primary: true,
		act: func(ctx context.Context, tm *TabletManager) error {
			return tm.UndoDemotePrimary(ctx, false)
		},
	}} {
		t.Run(tt.name, func(t *testing.T) {
			withGroupReplication(t)
			tm, fmd, _, ts := newLegitimacyTestTM(t)
			qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
			wantType := topodatapb.TabletType_REPLICA
			if tt.primary {
				wantType = topodatapb.TabletType_PRIMARY
				setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
				// The primary lost the majority of its voters: it does not serve, and MySQL is fenced.
				fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
					groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:20"))
				newGroupReplicationSync(tm).reconcile(t.Context())
				require.False(t, qsc.IsServing())
			}
			fmd.SuperReadOnly.Store(true)
			fmd.ReadOnly = true
			require.NoError(t, ts.DeleteTablet(t.Context(), tm.tabletAlias))
			// MySQL is the primary of the shard's group, with every voter ONLINE in its view.
			fmd.SetGroupReplicationStatus(legitimatePrimaryView("1780000001:21"))

			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			start := time.Now()
			err := tt.act(ctx, tm)
			assert.Less(t, time.Since(start), 10*time.Second, "a tablet whose record is gone must not retry until its context ends")
			if err != nil {
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "unexpected error: %v", err)
			}
			assert.Equal(t, wantType, tm.Tablet().Type)
			assert.False(t, qsc.IsServing() && tm.Tablet().Type == topodatapb.TabletType_PRIMARY, "the tablet must not serve as the primary")
			assert.True(t, fmd.SuperReadOnly.Load(), "MySQL must stay super_read_only")
			assert.True(t, fmd.ReadOnly, "MySQL must stay read_only")
		})
	}
}
